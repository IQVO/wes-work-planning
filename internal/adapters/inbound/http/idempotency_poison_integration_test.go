//go:build integration

package http_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/pgtx"
)

// poisonTx simulates what a mapped domain conflict does to the middleware's
// transaction: a statement fails on the server (23505), the repository maps
// it to a typed error, and the handler answers 4xx -- but the transaction is
// ABORTED from then on. What the middleware does with that 4xx depends on
// its replay class (ADR 0050): a FINAL one is healed and stored, an
// unclassified or RETRYABLE one is released. The client never sees a 500.
func poisonTx(t *testing.T, r *http.Request) {
	t.Helper()
	tx, ok := pgtx.TxFrom(r.Context())
	if !ok {
		t.Fatal("the middleware did not bind a transaction into the request context")
	}
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(r.Context(), `INSERT INTO idempotency_keys (key, method, path, request_hash) VALUES ('dup', 'm', 'p', 'h')`); err != nil {
			return // the second insert aborts the transaction (23505)
		}
	}
	t.Fatal("the duplicate insert did not abort the transaction")
}

func poisonRequest(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/paths/p1/work-units", nil)
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	return req
}

// TestIdempotency_AnUnclassified409FromAnAbortedTransactionIsReleased (ADR
// 0050): a handler that answers 4xx WITHOUT the production error writer (a
// raw WriteHeader) declares no class, so the refusal is released like
// RETRYABLE — the whole transaction rolls back, key row included, and the
// same key executes again. The client still sees its truthful 409.
func TestIdempotency_AnUnclassified409FromAnAbortedTransactionIsReleased(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"title":"Conflict","status":409}`))
	})
	mw := inboundhttp.RequireIdempotencyKey(pool)(handler)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-poison"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}

	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys WHERE key = 'k-poison'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("an unclassified 409 left %d key rows: it must be released like RETRYABLE", n)
	}

	retry := httptest.NewRecorder()
	mw.ServeHTTP(retry, poisonRequest("k-poison"))
	if retry.Code != http.StatusConflict || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry = %d replayed=%q, want a FRESH 409 (the key was released)", retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if retry.Body.String() != first.Body.String() {
		t.Fatalf("retry body = %s, want %s", retry.Body.String(), first.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (a released key executes again)", calls.Load())
	}
}

// TestIdempotency_AFinal409FromAnAbortedTransactionIsHealedAndStored (ADR
// 0050): a FINAL refusal behind a statement that aborted the transaction is
// healed by ROLLBACK TO SAVEPOINT and stored like any final answer — the
// client sees the 4xx, never a 500, and a replay answers it without running
// the handler again.
func TestIdempotency_AFinal409FromAnAbortedTransactionIsHealedAndStored(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		inboundhttp.ProbeRefuseForTest(w, r, http.StatusConflict, "FINAL")
	})
	mw := inboundhttp.RequireIdempotencyKey(pool)(handler)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-poison-final"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}

	var statusCode int
	if err := pool.QueryRow(t.Context(), `SELECT status_code FROM idempotency_keys WHERE key = 'k-poison-final'`).Scan(&statusCode); err != nil {
		t.Fatalf("the FINAL 409 outcome was not committed: %v", err)
	}
	if statusCode != http.StatusConflict {
		t.Fatalf("cached status = %d", statusCode)
	}

	replay := httptest.NewRecorder()
	mw.ServeHTTP(replay, poisonRequest("k-poison-final"))
	if replay.Code != http.StatusConflict || replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q, want the cached 409", replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1 (replay must not re-run it)", calls.Load())
	}
}

func TestIdempotency_A2xxFromAnAbortedTransactionIsNeverCached(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			poisonTx(t, r) // only the first attempt dies inside the transaction
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"u1"}`))
	})
	mw := inboundhttp.RequireIdempotencyKey(pool)(handler)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-lie"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500 (the writes never happened)", first.Code, first.Body.String())
	}
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys WHERE key = 'k-lie'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("a failed success left %d idempotency rows: the key could not retry", n)
	}

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, poisonRequest("k-lie"))
	if second.Code != http.StatusCreated {
		t.Fatalf("retry = %d %s, want the real work to succeed", second.Code, second.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}
