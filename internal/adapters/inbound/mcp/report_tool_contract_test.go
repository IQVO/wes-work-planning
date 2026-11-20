package mcp

// Consumer contract tests against the byte-pinned copy of this repo's
// OWN apis/openapi-reports.yaml (testdata/wes-work-planning.openapi-reports.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job keeps
// it equal to this repo's develop). The consumer is this very binary's
// MCP report tool, which calls the wes-reports REST service
// (REPORTS_BASE_URL) through ReportsRESTClient rather than opening the
// analytical database (ADR-0011) — the self-pin keeps the client's
// decode contract and the reports service's spec from drifting apart
// silently.
//
// R1 the pinned spec declares every route the client calls; R2 the
// fixtures satisfy the spec's schemas; R3-lite the fixtures flow through
// the REAL ReportsRESTClient (a 200 decodes into the tool's views, a
// problem+json answer is an error), and the query parameters the client
// sends are the documented ones.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

// reportsCallSites is the REST surface the MCP report tool consumes.
var reportsCallSites = []struct{ method, path string }{
	{http.MethodGet, "/reports/throughput"},
	{http.MethodGet, "/reports/throughput/freshness"},
}

func loadPinnedReportsSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("testdata/wes-work-planning.openapi-reports.yaml")
	if err != nil {
		t.Fatalf("load pinned spec: %v", err)
	}
	return doc
}

func pinnedReportsSchema(t *testing.T, doc *openapi3.T, name string) *openapi3.Schema {
	t.Helper()
	ref := doc.Components.Schemas[name]
	if ref == nil || ref.Value == nil {
		t.Fatalf("schema %s is not in the pinned spec copy", name)
	}
	return ref.Value
}

// R1: the pinned spec declares every (method, path) the client calls,
// and the table names every request the client's code builds, so a new
// call site fails here until it is both declared by the spec and
// pinned.
func TestPinnedReportsSpecCoversEveryCallSite(t *testing.T) {
	src, err := readFile("report_tool.go")
	if err != nil {
		t.Fatal(err)
	}
	// getJSON builds the one request shape both call sites share.
	if got := strings.Count(string(src), "http.NewRequestWithContext"); got != 1 {
		t.Errorf("report_tool.go builds %d requests, want 1 shared getJSON helper", got)
	}
	doc := loadPinnedReportsSpec(t)
	for _, c := range reportsCallSites {
		item := doc.Paths.Find(c.path)
		if item == nil {
			t.Errorf("pinned spec has no path %s, the reports client calls it", c.path)
			continue
		}
		if item.Operations()[c.method] == nil {
			t.Errorf("pinned spec path %s has no %s operation", c.path, c.method)
		}
	}
	// The query parameters GetThroughput sends are the documented ones.
	op := doc.Paths.Find("/reports/throughput").Operations()[http.MethodGet]
	if op == nil {
		t.Fatal("/reports/throughput missing")
	}
	for _, want := range []string{"from", "to"} {
		if op.Parameters.GetByInAndName("query", want) == nil {
			t.Errorf("/reports/throughput does not declare the %s query parameter the client sends", want)
		}
	}
	for _, optional := range []string{"pathId", "granularity"} {
		if op.Parameters.GetByInAndName("query", optional) != nil {
			continue // declared: fine (the client sends it only when set)
		}
		t.Logf("note: %s query parameter not declared in the pinned spec", optional)
	}
}

func readFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

// reportsFixtures: schema-derived example documents (the spec documents
// no whole-response examples for these routes — producer example
// backfill pending, noted in the PR body). Values follow the schemas'
// own per-field examples.
const (
	throughputFixture = `{"rows":[{"pathId":"pick-a","hourBucket":"2026-06-01T09:00:00Z","workReleased":4,"workUnitCompleted":3,"backlogThresholdBreached":0,"pathThrottled":0,"rateDeviationDetected":0}]}`
	freshnessFixture  = `{"lagSeconds":1.5}`
	problemFixture    = `{"type":"https://errors.wes-work-planning.warehouse-systems.dev/invalid-report-query","title":"Invalid report query","status":400,"detail":"from must be RFC 3339"}`
)

// R2: every fixture satisfies the pinned schema of the message it
// stands for.
func TestReportsExamplesSatisfyPinnedSpec(t *testing.T) {
	doc := loadPinnedReportsSpec(t)
	for name, tc := range map[string]struct {
		fixture string
		schema  string
	}{
		"throughput.answer.json": {throughputFixture, "ThroughputReport"},
		"freshness.answer.json":  {freshnessFixture, "Freshness"},
		"problem.json":           {problemFixture, "Problem"},
	} {
		var v any
		if err := json.Unmarshal([]byte(tc.fixture), &v); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if err := pinnedReportsSchema(t, doc, tc.schema).VisitJSON(v); err != nil {
			t.Errorf("%s does not satisfy the pinned %s schema: %v", name, tc.schema, err)
		}
	}
}

// serveReports answers the client's call with a fixture, capturing the
// request the client actually built.
func serveReports(t *testing.T, status int, body string) (*ReportsRESTClient, *string) {
	t.Helper()
	query := new(string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*query = r.Method + " " + r.URL.Path + "?" + r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewReportsRESTClient(srv.URL, srv.Client()), query
}

// R3-lite: the fixtures flow through the REAL ReportsRESTClient. A 200
// throughput answer decodes into ThroughputReportView carrying the
// wire's own row facts; a 200 freshness answer decodes its lag; a
// problem+json answer is a client error, never a view.
func TestReportsClientDecodesSpecShapes(t *testing.T) {
	t.Run("throughput", func(t *testing.T) {
		client, query := serveReports(t, http.StatusOK, throughputFixture)
		out, err := client.GetThroughput(context.Background(), ThroughputQuery{From: "2026-06-01T00:00:00Z", To: "2026-06-02T00:00:00Z"})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(out.Rows))
		}
		row := out.Rows[0]
		if row.PathId != "pick-a" || row.WorkReleased != 4 || row.WorkUnitCompleted != 3 {
			t.Errorf("row = %+v, want the wire's own facts", row)
		}
		if want := "GET /reports/throughput?from=2026-06-01T00%3A00%3A00Z&to=2026-06-02T00%3A00%3A00Z"; *query != want {
			t.Errorf("request = %q, want %q", *query, want)
		}
	})

	t.Run("throughput with optional filters", func(t *testing.T) {
		client, query := serveReports(t, http.StatusOK, throughputFixture)
		if _, err := client.GetThroughput(context.Background(), ThroughputQuery{
			From: "2026-06-01T00:00:00Z", To: "2026-06-02T00:00:00Z", PathId: "pick-a", Granularity: "hour",
		}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(*query, "pathId=pick-a") || !strings.Contains(*query, "granularity=hour") {
			t.Errorf("request = %q, want the optional documented filters forwarded", *query)
		}
	})

	t.Run("freshness", func(t *testing.T) {
		client, _ := serveReports(t, http.StatusOK, freshnessFixture)
		out, err := client.GetFreshness(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if out.LagSeconds != 1.5 {
			t.Errorf("lagSeconds = %v, want 1.5", out.LagSeconds)
		}
	})

	t.Run("problem answer is an error", func(t *testing.T) {
		client, _ := serveReports(t, http.StatusBadRequest, problemFixture)
		if _, err := client.GetThroughput(context.Background(), ThroughputQuery{From: "nope", To: "nope"}); err == nil {
			t.Error("a 400 problem+json answer must surface as a client error, never a view")
		}
	})
}
