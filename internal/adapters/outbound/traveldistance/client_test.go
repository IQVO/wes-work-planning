package traveldistance_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/traveldistance"
)

// fakeDoer is a stub traveldistance.HTTPDoer so these tests never hit the
// network.
type fakeDoer struct {
	resp *http.Response
	err  error
	req  *http.Request
}

func (f *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestClient_GetDistance_200_Measured(t *testing.T) {
	doer := &fakeDoer{resp: jsonResponse(http.StatusOK, `{"metresM":4.4,"estimated":false,"route":[{"aisleId":"WH1-STOR-AMB-A07","bay":"01"}]}`)} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-AMB-A09-03-01-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !view.Known {
		t.Fatalf("expected Known=true")
	}
	if view.MetresM != 4.4 {
		t.Fatalf("expected MetresM=4.4, got %v", view.MetresM)
	}
	if view.Estimated {
		t.Fatalf("expected Estimated=false")
	}
	if doer.req.URL.Path != "/distance" {
		t.Fatalf("expected path /distance, got %s", doer.req.URL.Path)
	}
	if got := doer.req.URL.Query().Get("from"); got != "WH1-STOR-AMB-A07-01-01-A" {
		t.Fatalf("expected from query param, got %s", got)
	}
	if got := doer.req.URL.Query().Get("to"); got != "WH1-STOR-AMB-A09-03-01-A" {
		t.Fatalf("expected to query param, got %s", got)
	}
}

func TestClient_GetDistance_200_Estimated(t *testing.T) {
	doer := &fakeDoer{resp: jsonResponse(http.StatusOK, `{"metresM":12.0,"estimated":true,"route":[]}`)} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-AMB-A09-03-01-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !view.Estimated {
		t.Fatalf("expected Estimated=true")
	}
}

func TestClient_GetDistance_404_FailsOpen(t *testing.T) {
	doer := &fakeDoer{resp: jsonResponse(http.StatusNotFound, "")} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "WH1-STOR-AMB-A07-01-01-A", "NOPE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false on 404")
	}
}

func TestClient_GetDistance_422_FailsOpen(t *testing.T) {
	// 422 is facility-layout's cross-zone refusal (ADR-0017) — this
	// client treats it identically to 404, never as an error.
	doer := &fakeDoer{resp: jsonResponse(http.StatusUnprocessableEntity, "")} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-FRZ-A01-01-01-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false on 422")
	}
}

func TestClient_GetDistance_500_FailsOpen(t *testing.T) {
	doer := &fakeDoer{resp: jsonResponse(http.StatusInternalServerError, "")} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "A", "B")
	if err != nil {
		t.Fatalf("expected fail-open (nil error), got %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false on 500")
	}
}

func TestClient_GetDistance_TransportError_FailsOpen(t *testing.T) {
	transportErr := errors.New("connection refused")
	doer := &fakeDoer{err: transportErr}
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "A", "B")
	if err != nil {
		t.Fatalf("expected fail-open (nil error), got %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false on a transport error")
	}
}

func TestClient_GetDistance_MalformedJSON_FailsOpen(t *testing.T) {
	doer := &fakeDoer{resp: jsonResponse(http.StatusOK, `{not-json`)} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	view, err := client.GetDistance(context.Background(), "A", "B")
	if err != nil {
		t.Fatalf("expected fail-open (nil error), got %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false on malformed JSON")
	}
}

func TestNewClient_NilDoer_DefaultsToRealHTTPClient(t *testing.T) {
	client := traveldistance.NewClient("http://facility-layout.local", nil)
	if client == nil {
		t.Fatalf("expected a non-nil client")
	}
}

func TestPermissiveLookup_AlwaysReportsUnknown(t *testing.T) {
	lookup := traveldistance.NewPermissiveLookup()
	view, err := lookup.GetDistance(context.Background(), "A", "B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false from PermissiveLookup")
	}
	if view.From != "A" || view.To != "B" {
		t.Fatalf("expected From/To to be echoed back, got %q/%q", view.From, view.To)
	}
}

// sanity: confirm we serialize the request the way facility-layout expects
// (Accept header set, GET method).
func TestClient_GetDistance_SetsAcceptHeaderAndMethod(t *testing.T) {
	doer := &fakeDoer{resp: jsonResponse(http.StatusOK, `{"metresM":1,"estimated":false,"route":[]}`)} //nolint:bodyclose
	client := traveldistance.NewClient("http://facility-layout.local", doer)

	_, err := client.GetDistance(context.Background(), "A", "B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doer.req.Method != http.MethodGet {
		t.Fatalf("expected GET, got %s", doer.req.Method)
	}
	if doer.req.Header.Get("Accept") != "application/json" {
		t.Fatalf("expected Accept: application/json header")
	}
}
