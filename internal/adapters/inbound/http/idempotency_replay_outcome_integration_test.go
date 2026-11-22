//go:build integration

// The OUTCOME branch of the middleware, proved with a probe. No problem type
// of this service declares OUTCOME (ADR 0050: no keyed use case commits state
// before refusing — every refusal of a keyed route is raised before the first
// write), so the rendered fleet template skips its OUTCOME test. The branch
// is real code this store can honour — a 4xx's handler writes commit (the
// savepoint heals an aborted statement only when the handler failed one) —
// and this test pins its semantics: the writes commit WITH the stored answer,
// in one transaction, and a replay records nothing new.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/pgtx"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
)

// TestOutcomeProbeRefusalCommitsItsWritesWithTheStoredAnswer drives the REAL
// middleware over the real Postgres with a handler that writes through the
// request's transaction and then refuses 409 with the OUTCOME probe. The
// write and the key row commit together; replays answer the stored refusal
// and add nothing.
func TestOutcomeProbeRefusalCommitsItsWritesWithTheStoredAnswer(t *testing.T) {
	pool := idempotencyDB(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS replay_probe (tag text NOT NULL)`); err != nil {
		t.Fatalf("create the probe table: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE replay_probe`); err != nil {
		t.Fatalf("truncate the probe table: %v", err)
	}

	refuse := func(w http.ResponseWriter, r *http.Request) {
		if tx, ok := pgtx.TxFrom(r.Context()); ok {
			if _, err := tx.Exec(r.Context(), `INSERT INTO replay_probe (tag) VALUES ('outcome-probe')`); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		inboundhttp.ProbeRefuseForTest(w, r, http.StatusConflict, "OUTCOME")
	}
	mw := inboundhttp.RequireIdempotencyKey(pool)(http.HandlerFunc(refuse))

	post := func(key string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/paths/pick-replay/work-units", strings.NewReader(`{"n":1}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, req)
		return rec
	}

	first := post("k-outcome-probe")
	if first.Code != http.StatusConflict {
		t.Fatalf("first answer = %d %s, want the probe 409", first.Code, first.Body.String())
	}
	var writes int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM replay_probe`).Scan(&writes); err != nil {
		t.Fatalf("count probe writes: %v", err)
	}
	if writes != 1 {
		t.Fatalf("committed writes after the OUTCOME refusal = %d, want 1: the refusal commits what the command did", writes)
	}
	var rows int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM idempotency_keys WHERE key = 'k-outcome-probe'`).Scan(&rows); err != nil {
		t.Fatalf("count key rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("key rows after the OUTCOME refusal = %d, want 1: the answer is stored in the same transaction as the writes", rows)
	}

	for i := 0; i < 2; i++ {
		rep := post("k-outcome-probe")
		if rep.Code != http.StatusConflict || rep.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" || rep.Body.String() != first.Body.String() {
			t.Fatalf("replay %d = %d replayed=%q %q, want the stored 409 verbatim", i, rep.Code, rep.Header().Get(inboundhttp.IdempotencyReplayedHeader), rep.Body.String())
		}
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM replay_probe`).Scan(&writes); err != nil {
		t.Fatalf("count probe writes: %v", err)
	}
	if writes != 1 {
		t.Fatalf("committed writes after 2 replays = %d, want 1: a replay records nothing new", writes)
	}
}
