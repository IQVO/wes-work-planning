package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
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

func TestPostWorkUnit_LineNo_BoundaryTable(t *testing.T) {
	cases := []struct {
		name   string
		lineNo any
		want   int
	}{
		{"one", 1, http.StatusCreated},
		{"max int32", 2147483647, http.StatusCreated},
		{"max int32 + 1", 2147483648, http.StatusBadRequest},
		{"max int64", int64(9223372036854775807), http.StatusBadRequest},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter()
			id := "wu-bound-" + string(rune('a'+i))
			code, resp := postLineNoUnit(t, router, id, map[string]any{"lineNo": tc.lineNo})
			if code != tc.want {
				t.Fatalf("got status %d, want %d, body=%v", code, tc.want, resp)
			}
			if tc.want == http.StatusCreated {
				if resp["lineNo"] != float64(tc.lineNo.(int)) {
					t.Fatalf("got lineNo %v, want %v", resp["lineNo"], tc.lineNo)
				}
				return
			}
			if resp["type"] != "https://errors.wes-work-planning.warehouse-systems.dev/invalid-line-no" {
				t.Fatalf("got problem type %v, want .../invalid-line-no", resp["type"])
			}
			if detail, _ := resp["detail"].(string); !strings.Contains(detail, "2147483647") {
				t.Fatalf("detail %q must state the 2147483647 upper bound", detail)
			}
			// A rejected request must not leave a unit behind.
			if rec := doJSON(t, router, http.MethodGet, "/work-units/"+id, nil); rec.Code != http.StatusNotFound {
				t.Fatalf("rejected work unit was persisted: GET status %d", rec.Code)
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
