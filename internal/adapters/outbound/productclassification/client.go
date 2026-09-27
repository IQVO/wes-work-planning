// Package productclassification provides outbound
// ports.ProductClassificationLookup implementations: an HTTP client that
// calls inventory-storage's product-classification endpoint, and a
// permissive no-op used by default so existing tests, CI and deployments
// are unaffected (mirrors inventory-storage's own
// LOCATION_LOOKUP_MODE=http|permissive facilitylayout adapter pattern —
// see ADR-0009).
package productclassification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// DefaultTimeout bounds a single classification lookup request, so a slow
// or hanging inventory-storage does not stall ReleaseNextWork indefinitely.
const DefaultTimeout = 5 * time.Second

// ErrUnexpectedStatus wraps an inventory-storage response status this
// client does not have specific handling for (anything other than 200 or
// 404).
var ErrUnexpectedStatus = errors.New("inventory-storage: unexpected response status")

// HTTPDoer is the subset of *http.Client this adapter depends on, so unit
// tests can substitute a fake transport without a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is a plain net/http implementation of
// ports.ProductClassificationLookup, calling inventory-storage's
// GET /products/{sku}/classification.
type Client struct {
	baseURL string
	doer    HTTPDoer
}

// NewClient builds a Client against baseURL (e.g. from
// INVENTORY_STORAGE_BASE_URL). A nil doer defaults to an *http.Client with
// DefaultTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

// classificationResponse mirrors inventory-storage's
// productClassificationResponse DTO
// (internal/adapters/inbound/http/dto.go there).
type classificationResponse struct {
	SKU              string   `json:"sku"`
	HandlingTags     []string `json:"handlingTags"`
	TemperatureClass string   `json:"temperatureClass"`
}

// GetClassification calls fetch and normalizes ANY problem (transport
// error, unexpected status) to the fail-open Known=false/nil contract
// this port has always had (ADR-0009) — used by every caller that is
// not the retrying BreakerClient (see breaker.go), so this type keeps
// behaving exactly as before if a caller constructs it directly instead
// of through NewBreakerClient.
func (c *Client) GetClassification(ctx context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	view, err := c.fetch(ctx, sku)
	if err != nil {
		return productclassificationview.ProductClassificationView{SKU: sku, Known: false}, nil
	}
	return view, nil
}

// fetch calls inventory-storage's product-classification endpoint for
// sku, WITHOUT swallowing any error into the fail-open contract —
// BreakerClient.retryingFetch (breaker.go) needs to tell "this SKU has
// no classification" (404, a real answer) apart from "the request
// itself failed" (a transport error, an unexpected status — a
// candidate for retry), which GetClassification's own swallowed
// contract cannot express.
//
//   - A 404 is treated as Known=false (fail-open / permissive): that SKU
//     has no registered classification yet. This is a real answer, not
//     a failure — it returns nil error same as a 200 does.
//   - Any transport error or non-2xx/404 status returns a genuine,
//     un-swallowed error.
func (c *Client) fetch(ctx context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	endpoint := fmt.Sprintf("%s/products/%s/classification", c.baseURL, url.PathEscape(sku))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return productclassificationview.ProductClassificationView{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.doer.Do(req)
	if err != nil {
		return productclassificationview.ProductClassificationView{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var body classificationResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return productclassificationview.ProductClassificationView{}, err
		}
		return productclassificationview.ProductClassificationView{
			SKU:              sku,
			HandlingTags:     body.HandlingTags,
			TemperatureClass: body.TemperatureClass,
			Known:            true,
		}, nil
	case http.StatusNotFound:
		return productclassificationview.ProductClassificationView{SKU: sku, Known: false}, nil
	default:
		return productclassificationview.ProductClassificationView{}, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}
}
