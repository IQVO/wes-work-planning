//go:build integration

// Real-router, real-Postgres proofs of the replay-class rule (ADR 0050) on
// this service's own RETRYABLE refusals: a RETRYABLE refusal is NOT stored —
// the key row is rolled back with the handler's writes — so a retry under
// the SAME key executes the real work again and is then stored and replayed.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// waitForPoolLockWaiter blocks until some other backend of this database
// waits on a lock while running a statement that touches work_pools.
func waitForPoolLockWaiter(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		var waitEvent, query string
		err := pool.QueryRow(context.Background(), `SELECT pid, wait_event, query FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock' AND query LIKE '%work_pools%' LIMIT 1`).
			Scan(&pid, &waitEvent, &query)
		if err == nil {
			t.Logf("lock waiter: pid=%d wait=%s query=%q", pid, waitEvent, query)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no request ever blocked on a work_pools lock: the race never happened")
}

// poolVersion reads the pool row's version for the diagnostics above.
func poolVersion(t *testing.T, pool *pgxpool.Pool, path string) int64 {
	t.Helper()
	var v int64
	if err := pool.QueryRow(context.Background(),
		`SELECT version FROM work_pools WHERE path_id = $1`, path).Scan(&v); err != nil {
		t.Fatalf("read pool version: %v", err)
	}
	return v
}

// waitForConnWaitingOnLock blocks until backend pid is itself waiting on a
// lock — proof that the next holder's SELECT ... FOR UPDATE has queued
// behind the current holder and will take the row the moment it is freed.
func waitForConnWaitingOnLock(t *testing.T, pool *pgxpool.Pool, pid uint32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock'`, pid).Scan(&n); err != nil {
			t.Fatalf("pg_stat_activity: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the next holder never queued for the pool row lock")
}

// TestReplayClasses_ConcurrentModificationRefusalIsNotStoredAndTheSameKeyRetries
// drives the real router through the fleet's lock-and-bump shape, hardened
// for this service's internal retry loop: the release retries its pool save
// twelve times (retryOnPoolConflict, full-jitter backoff), so every attempt
// must be made to lose. Two killer connections alternate holding the pool's
// row lock — the next holder queues for the lock BEFORE the current one
// commits its version bump — so each of the request's attempts wakes, fails
// its version guard, and retries into the next held lock until the budget is
// spent and the route answers 409 concurrent-modification (RETRYABLE): no
// key row is left. Once the contention stops, the retry under the SAME key
// executes the release, its 200 is stored and replayed.
func TestReplayClasses_ConcurrentModificationRefusalIsNotStoredAndTheSameKeyRetries(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)
	path := "pick-conc-retry"

	for _, unit := range []string{"wu-conc-a"} {
		body := `{"workUnitId":"` + unit + `","cpt":"2026-08-21T12:00:00Z","reference":"ref-` + unit + `"}`
		if rec := replayPost(t, router, "/paths/"+path+"/work-units", "k-enqueue-"+unit, body); rec.Code != http.StatusCreated {
			t.Fatalf("enqueue = %d %s", rec.Code, rec.Body.String())
		}
	}

	// Two killer connections alternate holding the pool's row lock. The
	// release's pool save retries internally (retryOnPoolConflict, 12
	// attempts of full-jitter backoff), so a single holder that commits and
	// re-takes can lose the re-acquire race to an early retry. Instead the
	// NEXT holder takes its place in the lock queue BEFORE the current one
	// commits: when the commit frees the lock, the queued killer — not the
	// request's next attempt — wakes up holding it, the blocked attempt
	// re-checks its version guard against the just-committed bump, matches
	// nothing, and the use case retries into the next holder's lock. Every
	// attempt loses, deterministically, until the budget is spent.
	ctx := context.Background()
	conns := make([]*pgxpool.Conn, 2)
	for i := range conns {
		c, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Release()
		conns[i] = c
	}
	take := func(c *pgxpool.Conn) error {
		sql := "BEGIN; SELECT path_id FROM work_pools WHERE path_id = '" + path + "' FOR UPDATE"
		_, err := c.Conn().PgConn().Exec(ctx, sql).ReadAll()
		return err
	}
	bumpCommit := func(c *pgxpool.Conn) {
		sql := "UPDATE work_pools SET version = version + 1 WHERE path_id = '" + path + "'; COMMIT"
		if _, err := c.Conn().PgConn().Exec(ctx, sql).ReadAll(); err != nil {
			t.Fatalf("bump the pool version under the held lock: %v", err)
		}
	}
	armNext := func(c *pgxpool.Conn) <-chan error {
		ch := make(chan error, 1)
		go func() { ch <- take(c) }()
		return ch
	}

	if err := take(conns[0]); err != nil {
		t.Fatalf("arm the pool row lock: %v", err)
	}
	holder := 0
	t.Logf("armed killer pids=%d/%d; pool version=%d",
		conns[0].Conn().PgConn().PID(), conns[1].Conn().PgConn().PID(), poolVersion(t, pool, path))
	type result struct {
		rec  *httptest.ResponseRecorder
		done chan struct{}
	}
	res := result{done: make(chan struct{})}
	go func() {
		defer close(res.done)
		res.rec = replayPost(t, router, "/paths/"+path+"/work-releases", "k-conc-refused", "")
	}()

	// Kill every optimistic attempt the request makes until it has exhausted
	// retryOnPoolConflict's budget and surfaced the 409.
loop:
	for i := 0; ; i++ {
		select {
		case <-res.done:
			t.Fatalf("the release won after %d killed attempts: the refusal never happened", i)
		default:
		}
		waitForPoolLockWaiter(t, pool)
		next := 1 - holder
		armed := armNext(conns[next]) // queues behind the holder's lock
		// Only commit once the next holder is OBSERVABLY queued: otherwise
		// the commit can free the lock before the next killer's SELECT
		// reaches the server, and the request's next attempt slips in.
		waitForConnWaitingOnLock(t, pool, conns[next].Conn().PgConn().PID())
		bumpCommit(conns[holder]) // the blocked attempt loses; the queued killer takes over
		t.Logf("kill %d committed; pool version=%d", i, poolVersion(t, pool, path))
		if err := <-armed; err != nil {
			t.Fatalf("the next holder could not take the lock: %v", err)
		}
		holder = next
		select {
		case <-res.done:
			break loop // the request finally surfaced the 409
		case <-time.After(150 * time.Millisecond):
			// The request is between attempts (backoff) — the holder keeps
			// the lock; loop and wait for its next attempt to block.
			select {
			case <-res.done:
				break loop
			default:
			}
		}
		// Safety valve: the retry budget is ~1s of backoff; 40 killed
		// attempts is far beyond it.
		if i > 40 {
			t.Fatal("the request never surfaced concurrent-modification")
		}
	}

	t.Logf("request finished: code=%d version=%d", res.rec.Code, poolVersion(t, pool, path))
	for _, c := range conns {
		var state, wait string
		_ = pool.QueryRow(context.Background(),
			`SELECT state, coalesce(wait_event_type,'')||'/'||coalesce(wait_event,'') FROM pg_stat_activity WHERE pid=$1`,
			c.Conn().PgConn().PID()).Scan(&state, &wait)
		var locks int
		_ = pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_locks WHERE pid=$1 AND relation='work_pools'::regclass AND granted`,
			c.Conn().PgConn().PID()).Scan(&locks)
		t.Logf("killer pid=%d state=%s wait=%s granted_work_pools_locks=%d", c.Conn().PgConn().PID(), state, wait, locks)
	}

	if res.rec.Code != http.StatusConflict || !strings.Contains(res.rec.Body.String(), "concurrent-modification") {
		t.Fatalf("release under contention = %d %s, want 409 concurrent-modification", res.rec.Code, res.rec.Body.String())
	}
	if got := replayKeyRows(t, pool, "k-conc-refused"); got != 0 {
		t.Fatalf("a RETRYABLE refusal left %d key rows: the key was not released", got)
	}

	// The contention is over: the retry under the SAME key executes the
	// release for real, its 200 is stored and then replayed.
	retry := replayPost(t, router, "/paths/"+path+"/work-releases", "k-conc-refused", "")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry under the released key = %d %s, want the real release to execute", retry.Code, retry.Body.String())
	}
	if got := replayKeyRows(t, pool, "k-conc-refused"); got != 1 {
		t.Fatalf("key rows after the retry = %d, want 1", got)
	}
	replay := replayPost(t, router, "/paths/"+path+"/work-releases", "k-conc-refused", "")
	if replay.Code != http.StatusOK || replay.Body.String() != retry.Body.String() ||
		replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d %q (replayed %q), want the stored 200 verbatim with the header",
			replay.Code, replay.Body.String(), replay.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
}
