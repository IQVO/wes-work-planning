// Package traveldistance provides outbound ports.TravelDistanceLookup
// implementations: an HTTP client that calls facility-layout's travel-graph
// endpoint, and a permissive no-op used by default so existing tests, CI
// and deployments are unaffected (mirrors this repo's own
// PRODUCT_CLASSIFICATION_MODE=http|permissive productclassification
// adapter pattern, and inventory-storage's LOCATION_LOOKUP_MODE — see
// ADR-0017).
package traveldistance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/traveldistanceview"
)

// DefaultTimeout bounds a single distance lookup request, so a slow or
// hanging facility-layout does not stall CommitShiftPlan indefinitely.
const DefaultTimeout = 5 * time.Second

// ErrUnexpectedStatus wraps a facility-layout response status this client
// does not have specific handling for (anything other than 200, 404, or
// 422).
var ErrUnexpectedStatus = errors.New("facility-layout: unexpected response status")

// HTTPDoer is the subset of *http.Client this adapter depends on, so unit
// tests can substitute a fake transport without a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is a plain net/http implementation of ports.TravelDistanceLookup,
// calling facility-layout's GET /distance?from=&to=.
type Client struct {
	baseURL string
	doer    HTTPDoer
}

// NewClient builds a Client against baseURL (e.g. from
// FACILITY_LAYOUT_BASE_URL). A nil doer defaults to an *http.Client with
// DefaultTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

// travelNodeResponse mirrors facility-layout's travelNodeResponse DTO
// (internal/adapters/inbound/http/dto.go there).
type travelNodeResponse struct {
	AisleID string `json:"aisleId"`
	Bay     string `json:"bay"`
}

// travelDistanceResponse mirrors facility-layout's travelDistanceResponse
// DTO returned by GET /distance.
type travelDistanceResponse struct {
	MetresM   float64              `json:"metresM"`
	Estimated bool                 `json:"estimated"`
	Route     []travelNodeResponse `json:"route"`
}

// GetDistance calls fetch and normalizes ANY problem (transport error,
// unexpected status) to the fail-open Known=false/nil contract this
// port has always had (ADR-0017) — used by every caller that is not the
// retrying BreakerClient (see breaker.go), so this type keeps behaving
// exactly as before if a caller constructs it directly instead of
// through NewBreakerClient.
func (c *Client) GetDistance(ctx context.Context, from, to string) (traveldistanceview.TravelDistanceView, error) {
	view, err := c.fetch(ctx, from, to)
	if err != nil {
		return traveldistanceview.TravelDistanceView{From: from, To: to, Known: false}, nil
	}
	return view, nil
}

// fetch calls facility-layout's GET /distance?from=&to= endpoint,
// WITHOUT swallowing any error into the fail-open contract —
// BreakerClient.retryingFetch (breaker.go) needs to tell "no distance
// known for this pair" (404/422, a real answer) apart from "the request
// itself failed" (a transport error, an unexpected status — a
// candidate for retry), which GetDistance's own swallowed contract
// cannot express.
//
//   - A 404 (unknown location) or 422 (locations in different zones, or
//     any other case facility-layout refuses rather than guesses) are both
//     treated as Known=false (fail-open / permissive): no distance is
//     available for this pair. This is a real answer, not a failure — it
//     returns nil error same as a 200 does.
//   - Any transport error or unexpected status returns a genuine,
//     un-swallowed error.
func (c *Client) fetch(ctx context.Context, from, to string) (traveldistanceview.TravelDistanceView, error) {
	endpoint := fmt.Sprintf("%s/distance?from=%s&to=%s", c.baseURL, url.QueryEscape(from), url.QueryEscape(to))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return traveldistanceview.TravelDistanceView{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.doer.Do(req)
	if err != nil {
		return traveldistanceview.TravelDistanceView{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var body travelDistanceResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return traveldistanceview.TravelDistanceView{}, err
		}
		return traveldistanceview.TravelDistanceView{
			From:      from,
			To:        to,
			MetresM:   body.MetresM,
			Estimated: body.Estimated,
			Known:     true,
		}, nil
	case http.StatusNotFound, http.StatusUnprocessableEntity:
		return traveldistanceview.TravelDistanceView{From: from, To: to, Known: false}, nil
	default:
		return traveldistanceview.TravelDistanceView{}, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}
}
