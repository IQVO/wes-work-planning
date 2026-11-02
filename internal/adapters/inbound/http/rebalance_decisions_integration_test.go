//go:build integration

package http_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// countingPublisher counts the domain events handed to Publish.
type countingPublisher struct{ n atomic.Int64 }

func (c *countingPublisher) Publish(_ context.Context, evs ...shared.DomainEvent) error {
	c.n.Add(int64(len(evs)))
	return nil
}

func newRebalanceRouter(t *testing.T, pool *pgxpool.Pool, pub *countingPublisher) http.Handler {
	t.Helper()
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	clock := memory.FixedClock{At: time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)}
	h := &inboundhttp.Handlers{
		EnqueueWorkUnit:   usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow),
		ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, pub, clock).WithUnitOfWork(uow),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, pub, clock).WithUnitOfWork(uow),
		ConfigurePool:     usecases.NewConfigurePool(pools),
		IdempotencyPool:   pool,
	}
	return inboundhttp.NewRouter(h, "wes-work-planning-test", nil)
}

// seedSaturatedPath leaves pick-rb release-fed, saturated at its WIP limit
// with backlog remaining, so a rebalance decision is ReassignLabor and
// publishes LaborReassignmentFlagged.
func seedSaturatedPath(t *testing.T, router http.Handler) {
	t.Helper()
	if rec := doKeyed(router, http.MethodPut, "/paths/pick-rb/pool", "", `{"mode":"ReleaseFed","wipLimit":1}`); rec.Code != http.StatusOK {
		t.Fatalf("configure pool = %d %s", rec.Code, rec.Body.String())
	}
	for i := 1; i <= 2; i++ {
		body := fmt.Sprintf(`{"workUnitId":"wu-rb-%d","cpt":"2026-08-21T1%d:00:00Z","reference":"order-rb-%d"}`, i, i, i)
		if rec := doKeyed(router, http.MethodPost, "/paths/pick-rb/work-units", fmt.Sprintf("rb-enq-%d", i), body); rec.Code != http.StatusCreated {
			t.Fatalf("enqueue %d = %d %s", i, rec.Code, rec.Body.String())
		}
	}
	if rec := doKeyed(router, http.MethodPost, "/paths/pick-rb/work-releases", "rb-rel", ""); rec.Code != http.StatusOK {
		t.Fatalf("release = %d %s", rec.Code, rec.Body.String())
	}
}

// TestRebalanceDecisions_KeyedPostContract: first call decides and publishes,
// a same-key replay returns the stored decision with Idempotency-Replayed and
// publishes nothing, a different request on the same key is 409, a missing or
// invalid key is 400, and a fresh key decides (and publishes) again.
func TestRebalanceDecisions_KeyedPostContract(t *testing.T) {
	pub := &countingPublisher{}
	router := newRebalanceRouter(t, idempotencyDB(t), pub)
	seedSaturatedPath(t, router)
	const path = "/paths/pick-rb/rebalance-decisions"

	before := pub.n.Load()
	first := doKeyed(router, http.MethodPost, path, "rb-key-1", "")
	if first.Code != http.StatusCreated || !strings.Contains(first.Body.String(), `"ReassignLabor"`) {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}
	if first.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("first answer carries Idempotency-Replayed")
	}
	afterFirst := pub.n.Load()
	if afterFirst != before+1 {
		t.Fatalf("first call published %d events, want 1", afterFirst-before)
	}

	replay := doKeyed(router, http.MethodPost, path, "rb-key-1", "")
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay = %d %s headers %v", replay.Code, replay.Body.String(), replay.Header())
	}
	if got := pub.n.Load(); got != afterFirst {
		t.Fatalf("replay published %d extra events, want 0", got-afterFirst)
	}

	other := doKeyed(router, http.MethodPost, "/paths/pick-other/rebalance-decisions", "rb-key-1", "")
	if other.Code != http.StatusConflict || !strings.Contains(other.Body.String(), "idempotency-key-reused") {
		t.Fatalf("key reuse = %d %s, want 409 idempotency-key-reused", other.Code, other.Body.String())
	}

	if rec := doKeyed(router, http.MethodPost, path, "", ""); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "idempotency-key-required") {
		t.Fatalf("missing key = %d %s", rec.Code, rec.Body.String())
	}
	if rec := doKeyed(router, http.MethodPost, path, strings.Repeat("k", 129), ""); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "idempotency-key-invalid") {
		t.Fatalf("invalid key = %d %s", rec.Code, rec.Body.String())
	}

	again := doKeyed(router, http.MethodPost, path, "rb-key-2", "")
	if again.Code != http.StatusCreated || again.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("fresh key = %d headers %v", again.Code, again.Header())
	}
	if got := pub.n.Load(); got != afterFirst+1 {
		t.Fatalf("fresh key published %d events, want 1", got-afterFirst)
	}
}
