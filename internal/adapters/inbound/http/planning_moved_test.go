package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ADR-0049 stage E: the two moved planning routes are gone after one release
// of 410 (v2.0.3). POST /paths/{pathId}/charge and /plan answer 404.
func TestPlanningRoutesAreGone(t *testing.T) {
	router := newTestRouter()
	for _, path := range []string{"/paths/PICK/charge", "/paths/PICK/plan"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("POST %s = %d (%s), want 404 (route removed)", path, rec.Code, rec.Body.String())
		}
	}
}
