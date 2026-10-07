package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type poolConfigResponse struct {
	PathId       string `json:"pathId"`
	Mode         string `json:"mode"`
	WIPLimit     int    `json:"wipLimit"`
	WIP          int    `json:"wip"`
	BacklogDepth int    `json:"backlogDepth"`
}

func decodePoolConfig(t *testing.T, body []byte) poolConfigResponse {
	t.Helper()
	var got poolConfigResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return got
}

func TestPutPool_CreatesFlowFedPoolWhenAbsent(t *testing.T) {
	router := newTestRouter()

	rec := doJSON(t, router, http.MethodPut, "/paths/pick-a/pool", map[string]any{"mode": "FlowFed", "wipLimit": 50})
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	got := decodePoolConfig(t, rec.Body.Bytes())
	want := poolConfigResponse{PathId: "pick-a", Mode: "FlowFed", WIPLimit: 50}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// The telemetry read model reflects the explicit configuration.
	tel := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry", nil)
	if tel.Code != http.StatusOK {
		t.Fatalf("telemetry status %d, body=%s", tel.Code, tel.Body.String())
	}
	var snapshot map[string]any
	_ = json.Unmarshal(tel.Body.Bytes(), &snapshot)
	if snapshot["mode"] != "FlowFed" {
		t.Fatalf("telemetry mode = %v, want FlowFed", snapshot["mode"])
	}
}

func TestPutPool_IsIdempotent(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{"mode": "ReleaseFed", "wipLimit": 3}

	first := doJSON(t, router, http.MethodPut, "/paths/pick-a/pool", body)
	second := doJSON(t, router, http.MethodPut, "/paths/pick-a/pool", body)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses %d/%d, want 200/200", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("same body, different result:\n%s\n%s", first.Body.String(), second.Body.String())
	}
}

// Lowering below the current WIP is accepted, reports the untouched WIP, and
// the next release is refused (409 wip-limit-reached) until work completes.
func TestPutPool_LoweringBelowWIPNeverEvictsAndPausesRelease(t *testing.T) {
	router := newTestRouter()
	cpt := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"wu-1", "wu-2"} {
		rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", map[string]any{"workUnitId": id, "cpt": cpt, "reference": "ref-" + id})
		if rec.Code != http.StatusCreated {
			t.Fatalf("enqueue %s: %d %s", id, rec.Code, rec.Body.String())
		}
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil); rec.Code != http.StatusOK {
		t.Fatalf("release 1: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, router, http.MethodPut, "/paths/pick-a/pool", map[string]any{"mode": "ReleaseFed", "wipLimit": 5}); rec.Code != http.StatusOK {
		t.Fatalf("configure 5: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil); rec.Code != http.StatusOK {
		t.Fatalf("release 2: %d %s", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, router, http.MethodPut, "/paths/pick-a/pool", map[string]any{"mode": "ReleaseFed", "wipLimit": 1})
	if rec.Code != http.StatusOK {
		t.Fatalf("lowering below WIP: got %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	got := decodePoolConfig(t, rec.Body.Bytes())
	if got.WIP != 2 || got.WIPLimit != 1 {
		t.Fatalf("got wip=%d limit=%d, want wip=2 limit=1 (no eviction)", got.WIP, got.WIPLimit)
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil); rec.Code != http.StatusConflict {
		t.Fatalf("release while over limit: got %d, want 409", rec.Code)
	}
	if rec := doJSON(t, router, http.MethodPost, "/work-units/wu-1/complete", nil); rec.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body.String())
	}
}

func TestPutPool_ValidationReturns400(t *testing.T) {
	router := newTestRouter()
	cases := []struct {
		name string
		body any
		slug string
	}{
		{"zero limit", map[string]any{"mode": "ReleaseFed", "wipLimit": 0}, "invalid-wip-limit"},
		{"negative limit", map[string]any{"mode": "ReleaseFed", "wipLimit": -3}, "invalid-wip-limit"},
		{"unknown mode", map[string]any{"mode": "Waved", "wipLimit": 3}, "unknown-feed-mode"},
		{"missing mode", map[string]any{"wipLimit": 3}, "unknown-feed-mode"},
		{"missing limit", map[string]any{"mode": "ReleaseFed"}, "malformed-request-body"},
		{"fractional limit", map[string]any{"mode": "ReleaseFed", "wipLimit": 2.5}, "malformed-request-body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, router, http.MethodPut, "/paths/pick-a/pool", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got %d, want 400, body=%s", rec.Code, rec.Body.String())
			}
			var problem struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &problem)
			if want := "https://errors.wes-work-planning.warehouse-systems.dev/" + tc.slug; problem.Type != want {
				t.Fatalf("problem type = %q, want %q", problem.Type, want)
			}
		})
	}
	// A rejected command creates nothing.
	if rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("telemetry after rejections: got %d, want 404 (no pool created)", rec.Code)
	}
}

func TestPutPool_MalformedJSONReturns400(t *testing.T) {
	router := newTestRouter()
	req := httptest.NewRequest(http.MethodPut, "/paths/pick-a/pool", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", rec.Code)
	}
}

func TestPutPool_UnknownPathReturns400(t *testing.T) {
	router := newTestRouter()
	rec := doJSON(t, router, http.MethodPut, "/paths/not-a-family/pool", map[string]any{"mode": "ReleaseFed", "wipLimit": 3})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400 unknown-path-id, body=%s", rec.Code, rec.Body.String())
	}
}
