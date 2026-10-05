package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ADR-0005 / ADR-0022: the two idempotency problem types are produced by the
// statusFor/problemFor catalogue like every other problem, not built inline.
func TestIdempotencyProblems_RouteThroughTheCatalogue(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantSlug   string
		wantTitle  string
	}{
		{"required", errIdempotencyKeyRequired, http.StatusBadRequest, "idempotency-key-required", "Idempotency-Key header is required"},
		{"reused", errIdempotencyKeyReused, http.StatusUnprocessableEntity, "idempotency-key-reused", "Idempotency-Key was already used with a different request"},
		{"required wrapped", fmt.Errorf("ctx: %w", errIdempotencyKeyRequired), http.StatusBadRequest, "idempotency-key-required", "Idempotency-Key header is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusFor(tc.err); got != tc.wantStatus {
				t.Fatalf("statusFor = %d, want %d", got, tc.wantStatus)
			}
			typeURI, title := problemFor(tc.err)
			if typeURI != problemBaseURI+tc.wantSlug || title != tc.wantTitle {
				t.Fatalf("problemFor = (%q, %q), want (%q, %q)", typeURI, title, problemBaseURI+tc.wantSlug, tc.wantTitle)
			}

			rec := httptest.NewRecorder()
			writeError(rec, httptest.NewRequest(http.MethodPost, "/paths/pick-a/work-units", nil), tc.err)
			if rec.Code != tc.wantStatus || rec.Header().Get("Content-Type") != "application/problem+json" {
				t.Fatalf("response = %d %q", rec.Code, rec.Header().Get("Content-Type"))
			}
			var body problemDetails
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Type != problemBaseURI+tc.wantSlug || body.Status != tc.wantStatus || body.Instance != "/paths/pick-a/work-units" || body.Detail == "" {
				t.Fatalf("unexpected problem body: %+v", body)
			}
		})
	}
}

// The middleware answers a missing key before touching the pool, so it can be
// exercised without a database.
func TestRequireIdempotencyKey_MissingHeaderIs400Problem(t *testing.T) {
	called := false
	h := RequireIdempotencyKey(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/paths/pick-a/work-units", strings.NewReader("{}")))

	if called {
		t.Fatal("handler must not run without an Idempotency-Key")
	}
	var body problemDetails
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Code != http.StatusBadRequest || body.Type != problemBaseURI+"idempotency-key-required" || !strings.Contains(body.Detail, IdempotencyKeyHeader) {
		t.Fatalf("got %d %+v", rec.Code, body)
	}
}

// A browser must be allowed to send Idempotency-Key cross-origin: it has to be
// in the preflight's allowed headers.
func TestCORS_PreflightAllowsIdempotencyKey(t *testing.T) {
	h := corsMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))

	req := httptest.NewRequest(http.MethodOptions, "/paths/pick-a/work-units", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "content-type, idempotency-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	allowed := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers"))
	if !strings.Contains(allowed, "idempotency-key") {
		t.Fatalf("Access-Control-Allow-Headers = %q, want it to include Idempotency-Key", allowed)
	}
}
