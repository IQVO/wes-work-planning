package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// testCatalogue mirrors the fleet's sortable-fc process-path catalogue
// (warehouse-infra's config/process-paths/sortable-fc.yaml — PICK/PACK/
// REBIN/SLAM and their "-"-delimited families), so these handler tests
// exercise the same boot-time validation cmd/wes wires in.
var testCatalogue = pathcatalog.New([]pathcatalog.PathDefinition{
	{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	{Id: "PACK", MatchPrefix: "pack", RequiredCapabilities: []string{"pack"}},
	{Id: "REBIN", MatchPrefix: "rebin", RequiredCapabilities: []string{"rebin"}},
	{Id: "SLAM", MatchPrefix: "slam", RequiredCapabilities: []string{"slam"}},
})

func newTestRouter() http.Handler {
	return inboundhttp.NewRouter(newTestHandlers(), "wes-work-planning", nil)
}

func newTestHandlers() *inboundhttp.Handlers {
	charges := memory.NewChargeRepo()
	plans := memory.NewPlanRepo()
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	laborPlanViews := memory.NewLaborPlanViewRepo()
	inventoryViews := memory.NewInventoryViewRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}

	h := &inboundhttp.Handlers{
		ReceiveChargeForecast:   usecases.NewReceiveChargeForecast(charges, publisher, clock),
		CommitShiftPlan:         usecases.NewCommitShiftPlan(plans, publisher, clock),
		EnqueueWorkUnit:         usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock),
		ReleaseNextWork:         usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
		RecordCompletion:        usecases.NewRecordCompletion(workUnits, pools, publisher, clock),
		SampleBacklog:           usecases.NewSampleBacklog(pools, publisher, clock),
		RebalanceDecision:       usecases.NewRebalanceDecision(pools, publisher, clock),
		LaborPlanView:           usecases.NewLaborPlanView(laborPlanViews),
		InventoryView:           usecases.NewInventoryView(inventoryViews),
		GetWorkUnitsByReference: usecases.NewGetWorkUnitsByReference(workUnits),
		Catalogue:               testCatalogue,
	}

	return h
}

func doJSON(t *testing.T, router http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	router := newTestRouter()
	rec := doJSON(t, router, http.MethodGet, "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200", rec.Code)
	}
}

func TestPostChargeForecast(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"buckets": []map[string]any{
			{"cpt": time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), "quantity": 100},
		},
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/charge", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/paths/pick-a/charge" {
		t.Fatalf("got Location %q, want /paths/pick-a/charge", loc)
	}
}

func TestPostChargeForecast_MalformedJSONReturns400(t *testing.T) {
	router := newTestRouter()
	req := httptest.NewRequest(http.MethodPost, "/paths/pick-a/charge", bytes.NewReader([]byte("{not-json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostChargeForecast_InvalidQuantityReturns400(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"buckets": []map[string]any{
			{"cpt": time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), "quantity": -5},
		},
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/charge", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostChargeForecast_UnknownProcessPathReturns400(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"buckets": []map[string]any{
			{"cpt": time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), "quantity": 100},
		},
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/not-a-declared-path/charge", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem+json body: %v", err)
	}
	if want := "https://errors.wes-work-planning.warehouse-systems.dev/unknown-path-id"; problem.Type != want {
		t.Fatalf("got problem.type %q, want %q", problem.Type, want)
	}
}

func TestPostChargeForecast_MissingRequiredBucketFieldReturns400(t *testing.T) {
	router := newTestRouter()

	for name, body := range map[string]map[string]any{
		"missing cpt": {
			"buckets": []map[string]any{{"quantity": 100}},
		},
		"missing quantity": {
			"buckets": []map[string]any{{"cpt": time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/charge", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPostShiftPlan(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"plannedHeads":      3,
		"installedStations": 5,
		"rateUnitsPerHour":  50,
		"hours":             8,
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/plan", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/paths/pick-a/plan" {
		t.Fatalf("got Location %q, want /paths/pick-a/plan", loc)
	}
}

func TestPostShiftPlan_HeadsExceedStationsReturns400(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"plannedHeads":      6,
		"installedStations": 5,
		"rateUnitsPerHour":  50,
		"hours":             8,
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/plan", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostShiftPlan_MissingRequiredFieldReturns400(t *testing.T) {
	router := newTestRouter()

	for name, body := range map[string]map[string]any{
		"missing plannedHeads": {
			"installedStations": 5,
			"rateUnitsPerHour":  50,
			"hours":             8,
		},
		"missing installedStations": {
			"plannedHeads":     3,
			"rateUnitsPerHour": 50,
			"hours":            8,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/plan", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPostShiftPlan_ThroughputOverflowReturns400(t *testing.T) {
	router := newTestRouter()
	// Each factor is positive and finite, but the product overflows
	// float64 — the plan has no representable JSON response and must be
	// rejected as a client error instead of producing an empty 201 body.
	body := map[string]any{
		"plannedHeads":      32,
		"installedStations": 1800,
		"rateUnitsPerHour":  5010217082820735.0,
		"hours":             1.4080993563511214e+308,
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/plan", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem+json body: %v", err)
	}
	if want := "https://errors.wes-work-planning.warehouse-systems.dev/planned-throughput-not-finite"; problem.Type != want {
		t.Fatalf("got problem.type %q, want %q", problem.Type, want)
	}
}

func TestPostWorkUnit(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/work-units/wu-1" {
		t.Fatalf("got Location %q, want /work-units/wu-1", loc)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, ok := resp["giftWrap"].(bool); !ok || got != false {
		t.Fatalf("got giftWrap %v, want false when omitted from the request", resp["giftWrap"])
	}
}

func TestPostWorkUnit_GiftWrapRequested_ReflectedInResponse(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"workUnitId": "wu-gift-wrap",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
		"giftWrap":   true,
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, ok := resp["giftWrap"].(bool); !ok || got != true {
		t.Fatalf("got giftWrap %v, want true", resp["giftWrap"])
	}
}

func TestPostWorkUnit_MalformedJSONReturns400(t *testing.T) {
	router := newTestRouter()
	req := httptest.NewRequest(http.MethodPost, "/paths/pick-a/work-units", bytes.NewReader([]byte("not json at all")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostWorkUnit_MissingCPTReturns400(t *testing.T) {
	router := newTestRouter()
	body := map[string]any{
		"workUnitId": "wu-1",
		"reference":  "order-line-1",
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostRelease(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostRelease_EmptyPoolReturns409(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil); rec.Code != http.StatusOK {
		t.Fatalf("setup: got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("got status %d, want 409, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostComplete(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil); rec.Code != http.StatusOK {
		t.Fatalf("setup: got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, router, http.MethodPost, "/work-units/wu-1/complete", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostComplete_DoubleCompleteReturns409(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)
	doJSON(t, router, http.MethodPost, "/paths/pick-a/release", nil)
	doJSON(t, router, http.MethodPost, "/work-units/wu-1/complete", nil)

	rec := doJSON(t, router, http.MethodPost, "/work-units/wu-1/complete", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("got status %d, want 409, body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("got Content-Type %q, want application/problem+json", ct)
	}
	var problem struct {
		Type     string `json:"type"`
		Title    string `json:"title"`
		Status   int    `json:"status"`
		Detail   string `json:"detail"`
		Instance string `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem+json body: %v", err)
	}
	if problem.Status != http.StatusConflict {
		t.Fatalf("got problem.status %d, want 409", problem.Status)
	}
	if problem.Instance != "/work-units/wu-1/complete" {
		t.Fatalf("got instance %q, want /work-units/wu-1/complete", problem.Instance)
	}
}

func TestGetTelemetry(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)

	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetTelemetry_UnknownPathReturns404(t *testing.T) {
	router := newTestRouter()
	rec := doJSON(t, router, http.MethodGet, "/paths/unknown-path/telemetry", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("got Content-Type %q, want application/problem+json", ct)
	}
	var problem struct {
		Type     string `json:"type"`
		Title    string `json:"title"`
		Status   int    `json:"status"`
		Detail   string `json:"detail"`
		Instance string `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem+json body: %v", err)
	}
	if problem.Type == "" || problem.Title == "" {
		t.Fatalf("expected non-empty type/title, got %+v", problem)
	}
	if problem.Status != http.StatusNotFound {
		t.Fatalf("got problem.status %d, want 404", problem.Status)
	}
	if problem.Detail == "" {
		t.Fatalf("expected non-empty detail, got %+v", problem)
	}
	if problem.Instance != "/paths/unknown-path/telemetry" {
		t.Fatalf("got instance %q, want /paths/unknown-path/telemetry", problem.Instance)
	}
}

func TestGetTelemetry_CutoffAtOmitted_ResponseHasNoCapacityFields(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)

	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := body["remainingCapacityKnown"]; ok {
		t.Fatalf("did not expect remainingCapacityKnown when cutoffAt is omitted, got %v", body)
	}
	if _, ok := body["remainingCapacityUnits"]; ok {
		t.Fatalf("did not expect remainingCapacityUnits when cutoffAt is omitted, got %v", body)
	}
}

func TestGetTelemetry_CutoffAtSupplied_ReturnsKnownCapacity(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)

	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry?cutoffAt=2026-08-21T18:00:00Z", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	known, ok := body["remainingCapacityKnown"].(bool)
	if !ok || !known {
		t.Fatalf("expected remainingCapacityKnown=true (ReleaseFed default pool), got %v", body)
	}
	if _, ok := body["remainingCapacityUnits"]; !ok {
		t.Fatalf("expected remainingCapacityUnits present when known=true, got %v", body)
	}
}

func TestGetTelemetry_MalformedCutoffAtReturns400(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)

	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry?cutoffAt=not-a-date", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetTelemetry_EmptyCutoffAtReturns400(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)

	// `?cutoffAt=` (present but empty) is not "omitted": the parameter is
	// documented as an RFC3339 date-time and an empty string is not one.
	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/telemetry?cutoffAt=", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetRebalance(t *testing.T) {
	router := newTestRouter()
	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody)

	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/rebalance", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetRebalance_UnknownPathReturns404(t *testing.T) {
	router := newTestRouter()
	rec := doJSON(t, router, http.MethodGet, "/paths/unknown-path/rebalance", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetLaborPlanView_UnknownPathReturns404(t *testing.T) {
	router := newTestRouter()
	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/labor-plan-view", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetLaborPlanView_ReturnsObservedPlanAndSurfacesInRebalance(t *testing.T) {
	laborPlanViews := memory.NewLaborPlanViewRepo()
	pathId, _ := shared.NewPathId("pick-a")
	if err := laborPlanViews.Save(context.Background(), laborview.LaborPlanObserved{
		PathId: pathId, PlannedHeads: 4, PlannedRate: 90, PlannedHours: 8,
		ObservedAt: time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("seed labor plan view: %v", err)
	}

	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}

	h := &inboundhttp.Handlers{
		EnqueueWorkUnit:   usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock),
		SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
		LaborPlanView:     usecases.NewLaborPlanView(laborPlanViews),
	}
	router := inboundhttp.NewRouter(h, "wes-work-planning", nil)

	enqueueBody := map[string]any{
		"workUnitId": "wu-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-line-1",
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, router, http.MethodGet, "/paths/pick-a/labor-plan-view", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var view map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if view["plannedHeads"] != float64(4) {
		t.Fatalf("unexpected labor-plan-view body: %s", rec.Body.String())
	}

	rebalanceRec := doJSON(t, router, http.MethodGet, "/paths/pick-a/rebalance", nil)
	if rebalanceRec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rebalanceRec.Code, rebalanceRec.Body.String())
	}
	var rebalance map[string]any
	if err := json.Unmarshal(rebalanceRec.Body.Bytes(), &rebalance); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	laborPlan, ok := rebalance["laborPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected rebalance response to include laborPlan, got %s", rebalanceRec.Body.String())
	}
	if laborPlan["plannedHeads"] != float64(4) {
		t.Fatalf("unexpected laborPlan in rebalance response: %v", laborPlan)
	}
}

func TestGetInventoryView_UnknownSKUReturns404(t *testing.T) {
	router := newTestRouter()
	rec := doJSON(t, router, http.MethodGet, "/inventory-view/sku-1", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetInventoryView_ReturnsObservedQuantity(t *testing.T) {
	inventoryViews := memory.NewInventoryViewRepo()
	if _, err := inventoryViews.ApplyDelta(context.Background(), "sku-1", -3, time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed inventory view: %v", err)
	}

	h := &inboundhttp.Handlers{InventoryView: usecases.NewInventoryView(inventoryViews)}
	router := inboundhttp.NewRouter(h, "wes-work-planning", nil)

	rec := doJSON(t, router, http.MethodGet, "/inventory-view/sku-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var view map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if view["usableQuantity"] != float64(-3) {
		t.Fatalf("unexpected inventory-view body: %s", rec.Body.String())
	}
}

func TestGetWorkUnitsByReference_ReturnsMatches(t *testing.T) {
	router := newTestRouter()
	enqueueBody1 := map[string]any{
		"workUnitId": "wu-ref-1",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-77213-line-1",
	}
	enqueueBody2 := map[string]any{
		"workUnitId": "wu-ref-2",
		"cpt":        time.Date(2026, 8, 21, 13, 0, 0, 0, time.UTC),
		"reference":  "order-77213-line-1",
	}
	otherBody := map[string]any{
		"workUnitId": "wu-other",
		"cpt":        time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		"reference":  "order-99999-line-1",
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody1); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", enqueueBody2); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, router, http.MethodPost, "/paths/pick-a/work-units", otherBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup: got status %d, want 201, body=%s", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, router, http.MethodGet, "/work-units?reference=order-77213-line-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var units []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &units); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(units) != 2 {
		t.Fatalf("got %d units, want 2, body=%s", len(units), rec.Body.String())
	}
	ids := map[string]bool{}
	for _, u := range units {
		ids[u["id"].(string)] = true
		if u["reference"] != "order-77213-line-1" {
			t.Fatalf("got reference %v, want order-77213-line-1", u["reference"])
		}
		if _, ok := u["pathId"]; !ok {
			t.Fatalf("expected pathId field, got %v", u)
		}
		if _, ok := u["state"]; !ok {
			t.Fatalf("expected state field, got %v", u)
		}
	}
	if !ids["wu-ref-1"] || !ids["wu-ref-2"] {
		t.Fatalf("expected both wu-ref-1 and wu-ref-2 in response, got %v", ids)
	}
}

func TestGetWorkUnitsByReference_NoMatchesReturnsEmptyArray(t *testing.T) {
	router := newTestRouter()

	rec := doJSON(t, router, http.MethodGet, "/work-units?reference=nonexistent", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var units []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &units); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(units) != 0 {
		t.Fatalf("got %d units, want 0", len(units))
	}
}

func TestGetWorkUnitsByReference_MissingReferenceReturns400(t *testing.T) {
	router := newTestRouter()

	rec := doJSON(t, router, http.MethodGet, "/work-units", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("got Content-Type %q, want application/problem+json", ct)
	}
	var problem struct {
		Type     string `json:"type"`
		Title    string `json:"title"`
		Status   int    `json:"status"`
		Detail   string `json:"detail"`
		Instance string `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem+json body: %v", err)
	}
	if problem.Status != http.StatusBadRequest {
		t.Fatalf("got problem.status %d, want 400", problem.Status)
	}
	if problem.Instance != "/work-units" {
		t.Fatalf("got instance %q, want /work-units", problem.Instance)
	}
}

func TestGetWorkUnitsByReference_EmptyReferenceReturns400(t *testing.T) {
	router := newTestRouter()

	rec := doJSON(t, router, http.MethodGet, "/work-units?reference=", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}
