//go:build integration

// Integration tests for the transactional Idempotency-Key middleware
// (docs/docs/adr for the ADR number — see 0022-idempotency-key-middleware.md)
// against a real Postgres 16, through the REAL chi router
// (inboundhttp.NewRouter) over real net/http requests — not the
// middleware's internals in isolation. Testcontainers-only: the test boots
// and owns its own disposable Postgres, never reads DATABASE_URL or
// hardcodes localhost, so CI cannot silently skip this contract.
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// idempotencyDB empties the package's shared, migrated Postgres (TestMain,
// main_integration_test.go — the test owns its database, never an external
// DATABASE_URL) and returns a pool on it. Every migration in this repo,
// including the idempotency_keys one, was applied once at package start.
func idempotencyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	truncateAppTables(t)
	pool, err := postgres.Connect(context.Background(), sharedDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// newIdempotencyRouter wires the REAL chi router (inboundhttp.NewRouter)
// with Postgres-backed WorkUnitRepo/WorkPoolRepo + UnitOfWork, so
// POST /paths/{pathId}/work-units' full request cycle — idempotency
// bookkeeping, the WorkUnit Save, the WorkPool Save, the outbox insert —
// runs through the exact same transaction-join mechanism production uses
// (internal/pgtx via postgres.UnitOfWork.Execute).
func newIdempotencyRouter(t *testing.T, pool *pgxpool.Pool) http.Handler {
	t.Helper()
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	charges := postgres.NewChargeRepo(pool)
	plans := postgres.NewPlanRepo(pool)
	// The log publisher (never Kafka) keeps this suite hermetic; the
	// point under test is the Postgres transaction boundary, not event
	// delivery. It is still wrapped in the SAME UnitOfWork scope as the
	// WorkUnit/WorkPool Saves via EnqueueWorkUnit's own atomically()
	// call, so the commit-together claim is exercised exactly as in
	// production (the log publisher writes nothing itself, so there is
	// nothing to roll back on its side, but Save+Save+Publish are still
	// one atomically() scope sharing the middleware's transaction).
	publisher := events.NewLogPublisher(nil)
	uow := postgres.NewUnitOfWork(pool)
	clock := memory.FixedClock{At: time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)}

	handlers := &inboundhttp.Handlers{
		ReceiveChargeForecast:   usecases.NewReceiveChargeForecast(charges, publisher, clock).WithUnitOfWork(uow),
		CommitShiftPlan:         usecases.NewCommitShiftPlan(plans, publisher, clock).WithUnitOfWork(uow),
		EnqueueWorkUnit:         usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).WithUnitOfWork(uow),
		ReleaseNextWork:         usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow),
		RecordCompletion:        usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithUnitOfWork(uow),
		SampleBacklog:           usecases.NewSampleBacklog(pools, publisher, clock).WithUnitOfWork(uow),
		RebalanceDecision:       usecases.NewRebalanceDecision(pools, publisher, clock).WithUnitOfWork(uow),
		GetWorkUnitsByReference: usecases.NewGetWorkUnitsByReference(workUnits),
		GetWorkUnit:             usecases.NewGetWorkUnit(workUnits),
		IdempotencyPool:         pool,
	}
	return inboundhttp.NewRouter(handlers, "wes-work-planning-test", nil)
}

func countWorkUnitRows(t *testing.T, pool *pgxpool.Pool, id string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM work_units WHERE id = $1", id).Scan(&n); err != nil {
		t.Fatalf("count work_units: %v", err)
	}
	return n
}

const validWorkUnitBody = `{"workUnitId":"wu-idem-1","cpt":"2026-08-21T12:00:00Z","reference":"order-line-1"}`

// TestIdempotency_FreshKey_CreatesWorkUnitAndRecordsOutcome is scenario
// (a): a fresh key + valid body creates the work unit and the
// idempotency_keys row records the exact 201 outcome.
func TestIdempotency_FreshKey_CreatesWorkUnitAndRecordsOutcome(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	req := httptest.NewRequest(http.MethodPost, "/paths/pick-fresh/work-units", strings.NewReader(validWorkUnitBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-fresh-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := countWorkUnitRows(t, pool, "wu-idem-1"); got != 1 {
		t.Fatalf("work_units rows = %d, want 1", got)
	}

	var statusCode int
	var responseBody []byte
	if err := pool.QueryRow(context.Background(),
		"SELECT status_code, response_body FROM idempotency_keys WHERE key = $1", "key-fresh-1",
	).Scan(&statusCode, &responseBody); err != nil {
		t.Fatalf("read idempotency row: %v", err)
	}
	if statusCode != http.StatusCreated {
		t.Fatalf("stored status_code = %d, want 201", statusCode)
	}
	if string(responseBody) != rec.Body.String() {
		t.Fatalf("stored response_body does not match what was returned to the caller")
	}
}

// TestIdempotency_Replay_SameKeySameBody_ReturnsIdenticalResponseNoDuplicate
// is scenario (b): replaying the same key + identical body returns the
// exact same 201 response and creates no second work unit row.
func TestIdempotency_Replay_SameKeySameBody_ReturnsIdenticalResponseNoDuplicate(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	body := `{"workUnitId":"wu-idem-replay","cpt":"2026-08-21T12:00:00Z","reference":"order-line-replay"}`
	doPost := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/paths/pick-replay/work-units", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-replay-1")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	first := doPost()
	if first.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}
	second := doPost()
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if second.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatalf("replay Location = %q, want %q", second.Header().Get("Location"), first.Header().Get("Location"))
	}
	if got := countWorkUnitRows(t, pool, "wu-idem-replay"); got != 1 {
		t.Fatalf("work_units rows after replay = %d, want exactly 1 (no duplicate work unit created)", got)
	}
}

// TestIdempotency_SameKeyDifferentBody_Returns422 is scenario (c).
func TestIdempotency_SameKeyDifferentBody_Returns422(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	req1 := httptest.NewRequest(http.MethodPost, "/paths/pick-mismatch/work-units",
		strings.NewReader(`{"workUnitId":"wu-idem-mismatch-1","cpt":"2026-08-21T12:00:00Z","reference":"order-line-mismatch-1"}`))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-mismatch-1")
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201 (body: %s)", rec1.Code, rec1.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/paths/pick-mismatch/work-units",
		strings.NewReader(`{"workUnitId":"wu-idem-mismatch-2","cpt":"2026-08-21T13:00:00Z","reference":"order-line-mismatch-2"}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-mismatch-1")
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body: %s)", rec2.Code, rec2.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec2.Body.String())
	}
	if !strings.HasSuffix(problem.Type, "idempotency-key-reused") {
		t.Fatalf("problem.type = %q, want suffix idempotency-key-reused", problem.Type)
	}
	if got := countWorkUnitRows(t, pool, "wu-idem-mismatch-2"); got != 0 {
		t.Fatalf("work_units rows for the mismatched retry's id = %d, want 0 (must not be created)", got)
	}
}

// TestIdempotency_NoHeader_Returns400 is scenario (d).
func TestIdempotency_NoHeader_Returns400(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	req := httptest.NewRequest(http.MethodPost, "/paths/pick-noheader/work-units", strings.NewReader(validWorkUnitBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec.Body.String())
	}
	if !strings.HasSuffix(problem.Type, "idempotency-key-required") {
		t.Fatalf("problem.type = %q, want suffix idempotency-key-required", problem.Type)
	}
	if got := countWorkUnitRows(t, pool, "wu-idem-1"); got != 0 {
		t.Fatalf("work_units rows = %d, want 0 (no work unit should be created without the header)", got)
	}
}

// TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneWorkUnitCreated is
// scenario (e): the real concurrency proof — five real goroutines racing
// the exact same key + body through the real HTTP router and the real
// Postgres unique-index lock, not a sequential simulation.
func TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneWorkUnitCreated(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	body := `{"workUnitId":"wu-idem-concurrent","cpt":"2026-08-21T12:00:00Z","reference":"order-line-concurrent"}`

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/paths/pick-concurrent/work-units", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-concurrent-1")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			results[i] = rec
		}(i)
	}
	wg.Wait()

	for i, rec := range results {
		if rec.Code != http.StatusCreated {
			t.Fatalf("goroutine %d status = %d, want 201 (body: %s)", i, rec.Code, rec.Body.String())
		}
		if rec.Body.String() != results[0].Body.String() {
			t.Fatalf("goroutine %d body differs from goroutine 0:\n%d: %s\n0: %s", i, i, rec.Body.String(), results[0].Body.String())
		}
	}
	if got := countWorkUnitRows(t, pool, "wu-idem-concurrent"); got != 1 {
		t.Fatalf("work_units rows after %d concurrent identical requests = %d, want exactly 1", n, got)
	}
}

// TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed is scenario
// (f): the use case returns a genuine domain validation error (an empty
// reference, rejected by workunit.NewWorkUnit AFTER the idempotency row
// would have been inserted), and the error response itself is cached — a
// retry with the same key+body replays the SAME error rather than
// re-validating.
func TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	invalidBody := `{"workUnitId":"wu-idem-invalid","cpt":"2026-08-21T12:00:00Z","reference":""}`

	doPost := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/paths/pick-invalid/work-units", strings.NewReader(invalidBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-business-error-1")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	first := doPost()
	if first.Code != http.StatusBadRequest {
		t.Fatalf("first call status = %d, want 400 (body: %s)", first.Code, first.Body.String())
	}

	var statusCode int
	if err := pool.QueryRow(context.Background(),
		"SELECT status_code FROM idempotency_keys WHERE key = $1", "key-business-error-1",
	).Scan(&statusCode); err != nil {
		t.Fatalf("read idempotency row: %v", err)
	}
	if statusCode != http.StatusBadRequest {
		t.Fatalf("stored status_code = %d, want 400 — the business error response must be cached, not just a 201", statusCode)
	}

	second := doPost()
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d (cached error response)", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs from the first (cached) error response:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if got := countWorkUnitRows(t, pool, "wu-idem-invalid"); got != 0 {
		t.Fatalf("work_units rows = %d, want 0 (the invalid work unit was never persisted, either time)", got)
	}
}
