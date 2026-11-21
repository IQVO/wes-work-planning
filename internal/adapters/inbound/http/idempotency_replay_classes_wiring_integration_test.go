//go:build integration

package http_test

import (
	"context"
	"net/http"
	"testing"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/pgtx"
)

// newReplayOps is the ONLY file of the replay-class test pair that knows the
// store (ADR 0050): the production middleware over a clean Postgres - the key
// row lives in the request's own transaction and the handler runs behind a
// SAVEPOINT (ADR 0022). It names no database driver: the pool comes from the
// package's shared testcontainers Postgres (main_integration_test.go), which
// already has every migration applied, and each adapter starts from a
// truncated database.
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	pool := idempotencyDB(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS replay_probe (tag text NOT NULL)`); err != nil {
		t.Fatalf("create the probe table: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE replay_probe`); err != nil {
		t.Fatalf("truncate the probe table: %v", err)
	}
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	return replayOps{
		Catalogue: func() []problemEntry {
			var out []problemEntry
			for _, p := range inboundhttp.ProblemTypesForTest() {
				out = append(out, problemEntry{Slug: p.Slug, Status: p.Status, Class: p.Class})
			}
			return out
		},
		Middleware: func(_ *testing.T, next http.Handler) http.Handler {
			return inboundhttp.RequireIdempotencyKey(pool)(next)
		},
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			inboundhttp.RefuseForTest(w, r, e.Slug, e.Status)
		},
		Write: func(ctx context.Context, tag string) error {
			tx, ok := pgtx.TxFrom(ctx)
			if !ok {
				return context.Canceled // no transaction bound: the middleware did not join the request
			}
			_, err := tx.Exec(ctx, `INSERT INTO replay_probe (tag) VALUES ($1)`, tag)
			return err
		},
		Committed: func(t *testing.T, tag string) int {
			return count(t, `SELECT count(*) FROM replay_probe WHERE tag = $1`, tag)
		},
		KeyRows: func(t *testing.T, key string) int {
			return count(t, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
		},
	}
}
