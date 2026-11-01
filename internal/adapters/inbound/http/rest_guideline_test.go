package http_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRetiredRoutesAreGone pins the hard cutover (REST guideline 1.2): the
// verb-segment routes answer 404 or 405.
func TestRetiredRoutesAreGone(t *testing.T) {
	router := newTestRouter()
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/paths/pick-a/release"},
		{http.MethodPost, "/work-units/wu-1/complete"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := doJSON(t, router, tc.method, tc.path, nil)
			if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d, want 404 or 405", tc.method, tc.path, rec.Code)
			}
		})
	}
}

type workUnitPage struct {
	Items []struct {
		ID string `json:"id"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

// seedSharedReference enqueues n work units under one reference; units pair
// up on the same CPT so the (cpt, id) tiebreak is exercised.
func seedSharedReference(t *testing.T, router http.Handler, ref string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		body := map[string]any{
			"workUnitId": fmt.Sprintf("wu-page-%02d", i),
			"cpt":        time.Date(2026, 8, 21, 12+i/2, 0, 0, 0, time.UTC),
			"reference":  ref,
		}
		if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", body); rec.Code != http.StatusCreated {
			t.Fatalf("seed %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
}

// walkWorkUnitPages follows nextCursor to the end and returns every id seen.
func walkWorkUnitPages(t *testing.T, router http.Handler, ref string, limit int) []string {
	t.Helper()
	var ids []string
	cursor := ""
	for pages := 0; pages < 100; pages++ {
		url := fmt.Sprintf("/work-units?reference=%s&limit=%d", ref, limit)
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		rec := doJSON(t, router, http.MethodGet, url, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d %s", url, rec.Code, rec.Body.String())
		}
		var p workUnitPage
		if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, it := range p.Items {
			ids = append(ids, it.ID)
		}
		if p.NextCursor == "" {
			return ids
		}
		cursor = p.NextCursor
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func assertCompleteWalk(t *testing.T, ids []string, want int) {
	t.Helper()
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate row %s across pages", id)
		}
		seen[id] = true
	}
	for i := 0; i < want; i++ {
		if id := fmt.Sprintf("wu-page-%02d", i); !seen[id] {
			t.Fatalf("row %s skipped; got %v", id, ids)
		}
	}
	if len(ids) != want {
		t.Fatalf("got %d rows, want %d", len(ids), want)
	}
}

func TestGetWorkUnitsByReference_WalksEveryPage(t *testing.T) {
	router := newTestRouter()
	seedSharedReference(t, router, "order-page", 7)
	assertCompleteWalk(t, walkWorkUnitPages(t, router, "order-page", 2), 7)
}

func TestGetWorkUnitsByReference_BadPagingParameters(t *testing.T) {
	router := newTestRouter()
	for _, tc := range []struct{ name, query, slug string }{
		{"limit zero", "limit=0", "invalid-limit"},
		{"limit too large", "limit=501", "invalid-limit"},
		{"limit not a number", "limit=abc", "invalid-limit"},
		{"limit empty", "limit=", "invalid-limit"},
		{"limit repeated", "limit=1&limit=2", "invalid-limit"},
		{"cursor garbage", "cursor=!!!", "invalid-cursor"},
		{"cursor repeated", "cursor=a&cursor=b", "invalid-cursor"},
		{"retired after", "after=x", "unknown-query-parameter"},
		{"retired offset", "offset=1", "unknown-query-parameter"},
		{"retired nextToken", "nextToken=x", "unknown-query-parameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, router, http.MethodGet, "/work-units?reference=r&"+tc.query, nil)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), tc.slug) {
				t.Fatalf("got %d %s, want 400 %s", rec.Code, rec.Body.String(), tc.slug)
			}
		})
	}
}
