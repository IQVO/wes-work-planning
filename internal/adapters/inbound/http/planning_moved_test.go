package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPlanningRoutesAnswer410NamingTheNewOwner pins ADR-0049 stage C: both
// retired POSTs answer 410 Gone with their own problem type and a detail
// naming warehouse-planning's route, whatever the body, the path or the
// catalogue says.
func TestPlanningRoutesAnswer410NamingTheNewOwner(t *testing.T) {
	cases := []struct {
		name, path, slug, title, names string
	}{
		{"charge forecast", "/paths/pick-a/charge", "charge-forecast-moved", "Charge forecast moved to warehouse-planning", "PUT /workload-forecasts/{site_id}/{path_id}"},
		{"charge forecast, undeclared path", "/paths/sorter-7/charge", "charge-forecast-moved", "Charge forecast moved to warehouse-planning", "PUT /workload-forecasts/{site_id}/{path_id}"},
		{"shift plan", "/paths/pick-a/plan", "shift-plan-moved", "Shift plan moved to warehouse-planning", "PUT /labour-plans/{site_id}/{shift_id}/{path_id}"},
		{"shift plan, undeclared path", "/paths/sorter-7/plan", "shift-plan-moved", "Shift plan moved to warehouse-planning", "PUT /labour-plans/{site_id}/{shift_id}/{path_id}"},
	}
	bodies := map[string]string{
		"valid charge": `{"buckets":[{"cpt":"2026-08-21T12:00:00Z","quantity":100}]}`,
		"valid plan":   `{"plannedHeads":3,"installedStations":5,"rateUnitsPerHour":50,"hours":8}`,
		"invalid plan": `{"plannedHeads":6,"installedStations":5,"rateUnitsPerHour":50,"hours":8}`,
		"empty":        ``,
		"malformed":    `{not-json`,
	}
	router := newTestRouter()
	for _, c := range cases {
		for bodyName, body := range bodies {
			t.Run(c.name+"/"+bodyName, func(t *testing.T) {
				assertMoved(t, router, c.path, body, c.slug, c.title, c.names)
			})
		}
	}
}

// assertMoved POSTs body to path and checks the 410 problem names the new
// owner's route.
func assertMoved(t *testing.T, router http.Handler, path, body, slug, title, names string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusGone {
		t.Fatalf("got status %d, want 410, body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("got Content-Type %q, want application/problem+json", ct)
	}
	if loc := rec.Header().Get("Location"); loc != "" {
		t.Fatalf("a 410 creates nothing, got Location %q", loc)
	}
	var problem struct {
		Type, Title, Detail, Instance string
		Status                        int
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem+json body: %v", err)
	}
	if want := "https://errors.wes-work-planning.warehouse-systems.dev/" + slug; problem.Type != want {
		t.Fatalf("got type %q, want %q", problem.Type, want)
	}
	if problem.Title != title || problem.Status != http.StatusGone || problem.Instance != path {
		t.Fatalf("got problem %+v, want title %q, status 410, instance %q", problem, title, path)
	}
	if !strings.Contains(problem.Detail, names) || !strings.Contains(problem.Detail, "warehouse-planning") {
		t.Fatalf("detail %q does not name %q on warehouse-planning", problem.Detail, names)
	}
}

// TestPlanningRoutesNoLongerAcceptOtherMethods: only POST was ever routed,
// and the 410 does not widen that.
func TestPlanningRoutesNoLongerAcceptOtherMethods(t *testing.T) {
	router := newTestRouter()
	for _, path := range []string{"/paths/pick-a/charge", "/paths/pick-a/plan"} {
		rec := doJSON(t, router, http.MethodGet, path, nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET %s: got %d, want 405", path, rec.Code)
		}
	}
}
