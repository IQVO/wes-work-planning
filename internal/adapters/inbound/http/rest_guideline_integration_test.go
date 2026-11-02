//go:build integration

package http_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
)

func doKeyed(router http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIdempotency_EveryPostRequiresAKey: all four formerly unkeyed POSTs
// (and enqueue) answer 400 idempotency-key-required without the header.
func TestIdempotency_EveryPostRequiresAKey(t *testing.T) {
	router := newIdempotencyRouter(t, idempotencyDB(t))
	for _, path := range []string{
		"/paths/pick-a/charge", "/paths/pick-a/plan", "/paths/pick-a/work-units",
		"/paths/pick-a/work-releases", "/work-units/wu-1/completions",
	} {
		rec := doKeyed(router, http.MethodPost, path, "", `{}`)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "idempotency-key-required") {
			t.Fatalf("POST %s without key = %d %s, want 400 idempotency-key-required", path, rec.Code, rec.Body.String())
		}
	}
}

func TestIdempotency_InvalidKeyIs400(t *testing.T) {
	router := newIdempotencyRouter(t, idempotencyDB(t))
	for name, key := range map[string]string{
		"129 chars":     strings.Repeat("k", 129),
		"non-printable": "bad\x01key",
		"non-ASCII":     "chave-é",
	} {
		rec := doKeyed(router, http.MethodPost, "/paths/pick-a/work-releases", key, "")
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "idempotency-key-invalid") {
			t.Fatalf("%s: got %d %s, want 400 idempotency-key-invalid", name, rec.Code, rec.Body.String())
		}
	}
	if rec := doKeyed(router, http.MethodPost, "/paths/pick-a/work-releases", strings.Repeat("k", 128), ""); strings.Contains(rec.Body.String(), "idempotency-key-invalid") {
		t.Fatalf("a 128-char key must be accepted, got %d %s", rec.Code, rec.Body.String())
	}
}

// TestIdempotency_ReplayHeaderAndGlobalKey: first answer has no
// Idempotency-Replayed, a byte-identical retry has it, and the key is
// global: the same key on another route is 409 idempotency-key-reused.
func TestIdempotency_ReplayHeaderAndGlobalKey(t *testing.T) {
	router := newIdempotencyRouter(t, idempotencyDB(t))
	body := `{"buckets":[{"cpt":"2026-08-21T12:00:00Z","quantity":10}]}`

	first := doKeyed(router, http.MethodPost, "/paths/pick-a/charge", "key-guideline-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}
	if got := first.Header().Get("Idempotency-Replayed"); got != "" {
		t.Fatalf("first answer carries Idempotency-Replayed=%q", got)
	}
	replay := doKeyed(router, http.MethodPost, "/paths/pick-a/charge", "key-guideline-1", body)
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay = %d %s, want %d %s", replay.Code, replay.Body.String(), first.Code, first.Body.String())
	}
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay lacks Idempotency-Replayed: true; headers %v", replay.Header())
	}
	if replay.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatalf("replay Location = %q, want %q", replay.Header().Get("Location"), first.Header().Get("Location"))
	}

	other := doKeyed(router, http.MethodPost, "/paths/pick-b/charge", "key-guideline-1", body)
	if other.Code != http.StatusConflict || !strings.Contains(other.Body.String(), "idempotency-key-reused") {
		t.Fatalf("same key other path = %d %s, want 409 idempotency-key-reused", other.Code, other.Body.String())
	}
	diffRoute := doKeyed(router, http.MethodPost, "/paths/pick-a/plan", "key-guideline-1", body)
	if diffRoute.Code != http.StatusConflict {
		t.Fatalf("same key other route = %d, want 409", diffRoute.Code)
	}
}

// TestIdempotency_ReleaseAndCompletionAreKeyed: a retried release/completion
// takes effect once.
func TestIdempotency_ReleaseAndCompletionAreKeyed(t *testing.T) {
	router := newIdempotencyRouter(t, idempotencyDB(t))
	for i := 1; i <= 2; i++ {
		body := fmt.Sprintf(`{"workUnitId":"wu-rel-%d","cpt":"2026-08-21T1%d:00:00Z","reference":"order-rel-%d"}`, i, i, i)
		if rec := doKeyed(router, http.MethodPost, "/paths/pick-rel/work-units", fmt.Sprintf("enq-%d", i), body); rec.Code != http.StatusCreated {
			t.Fatalf("enqueue %d = %d %s", i, rec.Code, rec.Body.String())
		}
	}
	a := doKeyed(router, http.MethodPost, "/paths/pick-rel/work-releases", "rel-key", "")
	b := doKeyed(router, http.MethodPost, "/paths/pick-rel/work-releases", "rel-key", "")
	if a.Code != http.StatusOK || b.Code != http.StatusOK || a.Body.String() != b.Body.String() || b.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("release retry: %d %s vs %d %s", a.Code, a.Body.String(), b.Code, b.Body.String())
	}
	var released struct{ ID string }
	_ = json.Unmarshal(a.Body.Bytes(), &released)
	c := doKeyed(router, http.MethodPost, "/work-units/"+released.ID+"/completions", "done-key", "")
	d := doKeyed(router, http.MethodPost, "/work-units/"+released.ID+"/completions", "done-key", "")
	if c.Code != http.StatusOK || d.Code != http.StatusOK || d.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("completion retry: %d %s vs %d %s (a keyless retry would be 409 already-completed)", c.Code, c.Body.String(), d.Code, d.Body.String())
	}
}

// TestGetWorkUnitsByReference_PostgresWalksEveryPage walks a Postgres-backed
// collection larger than the limit, with rows sharing a CPT.
func TestGetWorkUnitsByReference_PostgresWalksEveryPage(t *testing.T) {
	router := newIdempotencyRouter(t, idempotencyDB(t))
	for i := 0; i < 7; i++ {
		body := fmt.Sprintf(`{"workUnitId":"wu-page-%02d","cpt":%q,"reference":"order-page"}`, i,
			time.Date(2026, 8, 21, 12+i/2, 0, 0, 0, time.UTC).Format(time.RFC3339))
		if rec := doKeyed(router, http.MethodPost, "/paths/pick-a/work-units", fmt.Sprintf("seed-%d", i), body); rec.Code != http.StatusCreated {
			t.Fatalf("seed %d = %d %s", i, rec.Code, rec.Body.String())
		}
	}
	assertCompleteWalk(t, walkWorkUnitPages(t, router, "order-page", 2), 7)
}
