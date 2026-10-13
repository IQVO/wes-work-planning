package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- reports REST views (tool + client boundary) ------------------------------

// ThroughputRowView is one row of the "Release Throughput & Backlog Health"
// report as the MCP tool returns it and the reports REST client decodes it.
// Field tags match the reports service's JSON so the same struct round-trips
// both ways.
type ThroughputRowView struct {
	PathId                   string `json:"pathId"`
	HourBucket               string `json:"hourBucket"`
	WorkReleased             int    `json:"workReleased"`
	WorkUnitCompleted        int    `json:"workUnitCompleted"`
	BacklogThresholdBreached int    `json:"backlogThresholdBreached"`
	PathThrottled            int    `json:"pathThrottled"`
	RateDeviationDetected    int    `json:"rateDeviationDetected"`
}

// ThroughputReportView is the throughput report body.
type ThroughputReportView struct {
	Rows []ThroughputRowView `json:"rows"`
}

// FreshnessView is the freshness-lag body.
type FreshnessView struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// ThroughputQuery is the filter set passed to the reports REST client.
type ThroughputQuery struct {
	From        string
	To          string
	PathId      string
	Granularity string
}

// ReportsClient is the narrow port the MCP report tool depends on: a client
// of the wes-reports REST service. It is an interface so the tool can be
// unit-tested with a fake, and so the curated tool never talks to the
// analytical database directly — it goes through the reports REST surface,
// preserving the single read path (ADR-0011).
type ReportsClient interface {
	GetThroughput(ctx context.Context, q ThroughputQuery) (ThroughputReportView, error)
	GetFreshness(ctx context.Context) (FreshnessView, error)
}

// --- reports REST client ------------------------------------------------------

// ReportsRESTClient is the HTTP implementation of ReportsClient. Base URL and
// the *http.Client are injected so the composition root controls the target
// and timeouts, and tests can point it at an httptest server.
type ReportsRESTClient struct {
	baseURL string
	http    *http.Client
}

// NewReportsRESTClient constructs a ReportsRESTClient for the reports service
// at baseURL. A nil httpClient falls back to a client with a sane timeout.
func NewReportsRESTClient(baseURL string, httpClient *http.Client) *ReportsRESTClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &ReportsRESTClient{baseURL: baseURL, http: httpClient}
}

// GetThroughput calls GET /reports/throughput with q as the query string.
func (c *ReportsRESTClient) GetThroughput(ctx context.Context, q ThroughputQuery) (ThroughputReportView, error) {
	vals := url.Values{}
	vals.Set("from", q.From)
	vals.Set("to", q.To)
	if q.PathId != "" {
		vals.Set("pathId", q.PathId)
	}
	if q.Granularity != "" {
		vals.Set("granularity", q.Granularity)
	}
	var out ThroughputReportView
	if err := c.getJSON(ctx, "/reports/throughput?"+vals.Encode(), &out); err != nil {
		return ThroughputReportView{}, err
	}
	return out, nil
}

// GetFreshness calls GET /reports/throughput/freshness.
func (c *ReportsRESTClient) GetFreshness(ctx context.Context) (FreshnessView, error) {
	var out FreshnessView
	if err := c.getJSON(ctx, "/reports/throughput/freshness", &out); err != nil {
		return FreshnessView{}, err
	}
	return out, nil
}

// getJSON performs a GET against baseURL+path and decodes a 2xx JSON body
// into out. A non-2xx response is an error.
func (c *ReportsRESTClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("reports client: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("reports client: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reports client: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("reports client: decode: %w", err)
	}
	return nil
}

// Compile-time assertion that ReportsRESTClient satisfies the port.
var _ ReportsClient = (*ReportsRESTClient)(nil)

// --- get_release_throughput_report tool ---------------------------------------

// ThroughputToolInput is the tool's argument set (untrusted, from a model).
// pathId and granularity are optional and carry `omitempty`, so the inferred
// input schema requires only from and to (gap fix D16).
type ThroughputToolInput struct {
	From        string `json:"from" jsonschema:"start of the window, inclusive, RFC3339 (required)"`
	To          string `json:"to" jsonschema:"end of the window, exclusive, RFC3339 (required)"`
	PathId      string `json:"pathId,omitempty" jsonschema:"optional process-path filter, e.g. pick-zone-a; omit for every path"`
	Granularity string `json:"granularity,omitempty" jsonschema:"optional time bucket granularity; only 'hour' is supported, and omitting it means hour"`
}

// getThroughputReport is the tool handler: it validates the required window,
// delegates to the reports REST client, and returns the report view.
func (d Deps) getThroughputReport(ctx context.Context, in ThroughputToolInput) (ThroughputReportView, error) {
	return GetThroughputReportForTest(ctx, d.Reports, in)
}

// GetThroughputReportForTest is the tool's pure logic, factored out so it can
// be unit-tested with a fake ReportsClient independent of the MCP server
// wiring. It validates from/to and forwards the filters.
func GetThroughputReportForTest(ctx context.Context, client ReportsClient, in ThroughputToolInput) (ThroughputReportView, error) {
	if client == nil {
		return ThroughputReportView{}, fmt.Errorf("reports client not configured")
	}
	if in.From == "" || in.To == "" {
		return ThroughputReportView{}, fmt.Errorf("from and to are required (RFC3339)")
	}
	q := ThroughputQuery{}
	q.From = in.From
	q.To = in.To
	q.PathId = in.PathId
	q.Granularity = in.Granularity
	return client.GetThroughput(ctx, q)
}

// registerReportTool adds the curated read-only throughput report tool. It is
// registered only when a reports client is configured (Deps.Reports != nil),
// so an MCP deployment without the reports service simply does not expose it.
func (d Deps) registerReportTool(server *mcp.Server) {
	if d.Reports == nil {
		return
	}
	readOnly := true
	addTool(server, &mcp.Tool{
		Name:        "get_release_throughput_report",
		Description: "Return the Release Throughput & Backlog Health report (work released, work units completed, backlog-threshold breaches, path throttles, rate-deviation detections) per process path and hour, for a time window, optionally filtered by path. Reads via the wes-reports REST service.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getThroughputReport)
}
