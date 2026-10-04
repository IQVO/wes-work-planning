//go:build integration

package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
)

// ADR-0010: a gift-wrap request made on POST /paths/{pathId}/work-units must
// survive the Postgres round trip and show up as giftWrap=true on
// GET /work-units. Before the work_units.gift_wrap column existed the
// response of the POST said true but every later read said false.
func TestGetWorkUnits_ReportsGiftWrapPersistedInPostgres(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	body := `{"workUnitId":"wu-gw-http","cpt":"2026-08-21T12:00:00Z","reference":"order-gw","giftWrap":true}`
	post := httptest.NewRequest(http.MethodPost, "/paths/pick-gw/work-units", strings.NewReader(body))
	post.Header.Set("Content-Type", "application/json")
	post.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-gw-1")
	postRec := httptest.NewRecorder()
	router.ServeHTTP(postRec, post)
	if postRec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201 (%s)", postRec.Code, postRec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/work-units?reference=order-gw", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200 (%s)", getRec.Code, getRec.Body.String())
	}
	var units []map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &units); err != nil {
		t.Fatalf("decode %s: %v", getRec.Body.String(), err)
	}
	if len(units) != 1 || units[0]["giftWrap"] != true {
		t.Fatalf("GET /work-units = %v, want one unit with giftWrap=true", units)
	}

	one := httptest.NewRequest(http.MethodGet, "/work-units/wu-gw-http", nil)
	oneRec := httptest.NewRecorder()
	router.ServeHTTP(oneRec, one)
	var single map[string]any
	if err := json.Unmarshal(oneRec.Body.Bytes(), &single); err != nil || single["giftWrap"] != true {
		t.Fatalf("GET /work-units/{id} = %s (err %v), want giftWrap=true", oneRec.Body.String(), err)
	}
}
