package http_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// Decision 18 (ADR-0036): POST /paths/{pathId}/work-units accepts an
// OPTIONAL lineNo (integer >= 1), stored on the work unit and echoed back
// (omitted when unknown).

func postLineNoUnit(t *testing.T, router http.Handler, id string, extra map[string]any) (int, map[string]any) {
	t.Helper()
	body := map[string]any{
		"workUnitId": id,
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-77213",
	}
	for k, v := range extra {
		body[k] = v
	}
	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", body)
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, rec.Body.String())
	}
	return rec.Code, resp
}

func TestPostWorkUnit_LineNo_IsStoredAndReturned(t *testing.T) {
	router := newTestRouter()

	code, resp := postLineNoUnit(t, router, "order-77213-line-2", map[string]any{"lineNo": 2})
	if code != http.StatusCreated {
		t.Fatalf("got status %d, want 201, body=%v", code, resp)
	}
	if resp["lineNo"] != float64(2) {
		t.Fatalf("got lineNo %v, want 2", resp["lineNo"])
	}
	if resp["id"] != "order-77213-line-2" {
		t.Fatalf("got id %v, the supplied id must be used untouched", resp["id"])
	}

	// And it is what a later GET sees: the line was stored, not just echoed.
	rec := doJSON(t, router, http.MethodGet, "/work-units/order-77213-line-2", nil)
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["lineNo"] != float64(2) {
		t.Fatalf("GET lineNo = %v, want 2 (body=%s)", got["lineNo"], rec.Body.String())
	}
}

func TestPostWorkUnit_LineNo_OmittedOrNullMeansUnknown(t *testing.T) {
	router := newTestRouter()

	for name, extra := range map[string]map[string]any{
		"omitted": nil,
		"null":    {"lineNo": nil},
	} {
		t.Run(name, func(t *testing.T) {
			code, resp := postLineNoUnit(t, router, "wu-"+name, extra)
			if code != http.StatusCreated {
				t.Fatalf("got status %d, want 201, body=%v", code, resp)
			}
			if _, ok := resp["lineNo"]; ok {
				t.Fatalf("lineNo must be omitted when unknown, got %v", resp["lineNo"])
			}
		})
	}
}

func TestPostWorkUnit_LineNo_BelowOneReturns400Problem(t *testing.T) {
	router := newTestRouter()

	for name, lineNo := range map[string]int{"zero": 0, "negative": -3} {
		t.Run(name, func(t *testing.T) {
			code, resp := postLineNoUnit(t, router, "wu-bad-"+name, map[string]any{"lineNo": lineNo})
			if code != http.StatusBadRequest {
				t.Fatalf("got status %d, want 400, body=%v", code, resp)
			}
			if resp["type"] != "https://errors.wes-work-planning.warehouse-systems.dev/invalid-line-no" {
				t.Fatalf("got problem type %v, want .../invalid-line-no", resp["type"])
			}
		})
	}

	// A rejected request must not leave a unit behind.
	if rec := doJSON(t, router, http.MethodGet, "/work-units/wu-bad-zero", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("rejected work unit was persisted: GET status %d", rec.Code)
	}
}
