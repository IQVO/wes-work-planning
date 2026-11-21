//go:build integration

// Real-router, real-Postgres proof of the replay-class rule (ADR 0050) on
// this service's own wip-limit-reached refusal: a RETRYABLE refusal is NOT
// stored — the key row is rolled back with the handler's writes — so a retry
// under the SAME key executes the real work again and is then stored and
// replayed.
//
// The concurrent-modification refusal is proved over the real middleware and
// real Postgres by the rendered fleet template
// (idempotency_replay_classes_integration_test.go: released, retried, stored,
// replayed) rather than through a forced race here: every keyed use case of
// this service retries its optimistic pool save internally
// (retryOnPoolConflict, 12 attempts of full-jitter backoff), and Postgres
// wakes tuple-lock waiters one at a time in a non-FIFO order, so an external
// lock-and-bump cannot deterministically exhaust that budget — five CI runs
// of two hardened killer designs each still let one retry slip through and
// win. Exhausting the budget externally means racing the scheduler, which is
// exactly what a proof must not do.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
)

// replayPost issues a keyed POST against the real router and returns the
// recorder.
func replayPost(t *testing.T, router http.Handler, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func replayKeyRows(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM idempotency_keys WHERE key = $1", key).Scan(&n); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	return n
}

func replayStoredStatus(t *testing.T, pool *pgxpool.Pool, key string) (int, bool) {
	t.Helper()
	var status *int
	if err := pool.QueryRow(context.Background(),
		"SELECT status_code FROM idempotency_keys WHERE key = $1", key).Scan(&status); err != nil {
		t.Fatalf("read idempotency_keys row: %v", err)
	}
	if status == nil {
		return 0, false
	}
	return *status, true
}

// TestReplayClasses_WIPLimitRefusalIsNotStoredAndTheSameKeyRetries drives the
// real router: a release-fed pool capped at WIP 1 answers the second release
// with 409 wip-limit-reached (RETRYABLE). The refusal leaves no key row;
// completing the released unit drains the WIP; the retry under the SAME key
// executes the release for real, its 200 is stored, and the next call under
// that key replays it.
func TestReplayClasses_WIPLimitRefusalIsNotStoredAndTheSameKeyRetries(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)
	path := "pick-wip-retry"

	put := httptest.NewRequest(http.MethodPut, "/paths/"+path+"/pool",
		strings.NewReader(`{"mode":"ReleaseFed","wipLimit":1}`))
	put.Header.Set("Content-Type", "application/json")
	putRec := httptest.NewRecorder()
	router.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("configure pool = %d %s", putRec.Code, putRec.Body.String())
	}

	for i, unit := range []string{"wu-wip-a", "wu-wip-b"} {
		body := `{"workUnitId":"` + unit + `","cpt":"2026-08-21T12:00:00Z","reference":"ref-` + unit + `"}`
		rec := replayPost(t, router, "/paths/"+path+"/work-units", "k-enqueue-"+unit, body)
		if rec.Code != http.StatusCreated {
			t.Fatalf("enqueue %d = %d %s", i, rec.Code, rec.Body.String())
		}
	}

	if rec := replayPost(t, router, "/paths/"+path+"/work-releases", "k-release-1", ""); rec.Code != http.StatusOK {
		t.Fatalf("first release = %d %s", rec.Code, rec.Body.String())
	}

	// WIP is at the limit: the next release is refused — RETRYABLE.
	refusal := replayPost(t, router, "/paths/"+path+"/work-releases", "k-wip-refused", "")
	if refusal.Code != http.StatusConflict || !strings.Contains(refusal.Body.String(), "wip-limit-reached") {
		t.Fatalf("second release = %d %s, want 409 wip-limit-reached", refusal.Code, refusal.Body.String())
	}
	if refusal.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("the refusal itself carries %s", inboundhttp.IdempotencyReplayedHeader)
	}
	if got := replayKeyRows(t, pool, "k-wip-refused"); got != 0 {
		t.Fatalf("a RETRYABLE refusal left %d key rows: the key was not released", got)
	}

	// Complete the released unit: the WIP slot frees (state changed).
	if rec := replayPost(t, router, "/work-units/wu-wip-a/completions", "k-complete-a", ""); rec.Code != http.StatusOK {
		t.Fatalf("completion = %d %s", rec.Code, rec.Body.String())
	}

	// The SAME key retries: the release executes for real and is stored.
	retry := replayPost(t, router, "/paths/"+path+"/work-releases", "k-wip-refused", "")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry under the released key = %d %s, want the real release to execute", retry.Code, retry.Body.String())
	}
	if retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("the retry was answered from the store: the refusal must not have been stored")
	}
	if got := replayKeyRows(t, pool, "k-wip-refused"); got != 1 {
		t.Fatalf("key rows after the retry = %d, want 1 (the 200 is stored)", got)
	}
	if status, ok := replayStoredStatus(t, pool, "k-wip-refused"); !ok || status != http.StatusOK {
		t.Fatalf("stored status = %d/%v, want 200", status, ok)
	}

	// The next call under that key replays the stored 200 byte-identically.
	replay := replayPost(t, router, "/paths/"+path+"/work-releases", "k-wip-refused", "")
	if replay.Code != http.StatusOK || replay.Body.String() != retry.Body.String() {
		t.Fatalf("replay = %d %q, want the stored 200 verbatim", replay.Code, replay.Body.String())
	}
	if replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("the replay does not carry %s: true", inboundhttp.IdempotencyReplayedHeader)
	}

	// Exactly one unit was released by the retry (the second of the two).
	var released int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM work_units WHERE id IN ('wu-wip-a','wu-wip-b') AND state = 'Released'`).Scan(&released); err != nil {
		t.Fatalf("count released units: %v", err)
	}
	if released != 1 {
		t.Fatalf("released units after the retry = %d, want 1 (wu-wip-b)", released)
	}
}
