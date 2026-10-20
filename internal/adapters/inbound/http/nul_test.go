package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// nulProbe records whether the guarded handler ran and what body it read.
func nulProbe(reached *bool, body *string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*reached = true
		if r.Body != nil {
			b := new(strings.Builder)
			buf := make([]byte, 512)
			for {
				n, err := r.Body.Read(buf)
				b.Write(buf[:n])
				if err != nil {
					break
				}
			}
			*body = b.String()
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestRejectNUL(t *testing.T) {
	cases := []struct {
		name     string
		target   string
		body     string
		wantCode int
		wantType string
		reaches  bool
	}{
		{"clean body passes", "/paths/p1/work-units", `{"reference":"a"}`, http.StatusNoContent, "", true},
		{"no body passes", "/paths/p1/work-units", "", http.StatusNoContent, "", true},
		{"raw NUL in the body", "/paths/p1/work-units", "{\"reference\":\"a\x00b\"}", http.StatusBadRequest, "malformed-request-body", false},
		{"escaped NUL in the body", "/paths/p1/work-units", `{"reference":"a\u0000b"}`, http.StatusBadRequest, "malformed-request-body", false},
		{"escaped backslash then u0000 is not a NUL", "/paths/p1/work-units", `{"reference":"a\\u0000b"}`, http.StatusNoContent, "", true},
		{"three backslashes then u0000 is a NUL", "/paths/p1/work-units", `{"reference":"a\\\u0000b"}`, http.StatusBadRequest, "malformed-request-body", false},
		{"a later escape after an escaped backslash", "/paths/p1/work-units", `{"a":"\\u0000","b":"\u0000"}`, http.StatusBadRequest, "malformed-request-body", false},
		{"encoded NUL in the path", "/work-units/a%00b/complete", "", http.StatusNotFound, "not-found", false},
		{"encoded NUL in the query", "/work-units?reference=a%00b", "", http.StatusBadRequest, "malformed-request-body", false},
		{"clean query passes", "/work-units?reference=a%2500b", "", http.StatusNoContent, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reached bool
			var seen string
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			rejectNUL(nulProbe(&reached, &seen)).ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if reached != tc.reaches {
				t.Fatalf("handler reached = %v, want %v", reached, tc.reaches)
			}
			if tc.wantType != "" && !strings.Contains(rec.Body.String(), "/"+tc.wantType) {
				t.Errorf("problem type lacks %q: %s", tc.wantType, rec.Body.String())
			}
			if tc.reaches && seen != tc.body {
				t.Errorf("the guarded handler read %q, want the body replayed intact: %q", seen, tc.body)
			}
		})
	}
}

func TestRejectNUL_RawNULInThePath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/work-units", http.NoBody)
	req.URL.Path = "/work-units/a\x00b"
	var reached bool
	var seen string
	rec := httptest.NewRecorder()
	rejectNUL(nulProbe(&reached, &seen)).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || reached {
		t.Fatalf("status = %d reached = %v, want 404 and not reached", rec.Code, reached)
	}
}

// The guard is wired into the real router: a NUL never reaches a handler.
func TestRouter_RejectsNULBeforeAnyHandler(t *testing.T) {
	router := NewRouter(&Handlers{}, "test", nil)
	req := httptest.NewRequest(http.MethodPost, "/paths/p1/work-units", strings.NewReader("{\"reference\":\"a\x00\"}"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 from the NUL guard (a nil handler would have panicked): %s", rec.Code, rec.Body.String())
	}
}
