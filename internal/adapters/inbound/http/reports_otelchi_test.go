package http_test

import (
	"context"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/wes-work-planning/internal/analytics/report"
)

// ADR-0013 Tier 1: the reports router carries the otelchi RED metrics
// (request duration + active requests) like the OLTP router does.
func TestReportsRouter_EmitsOtelchiREDMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	// Built AFTER the provider is installed: the metric middleware binds its
	// instruments from the global provider at construction.
	srv := newReportsServer(&fakeReportStore{report: report.ThroughputReport{}})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(stdhttp.MethodGet, "/healthz", nil))
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collect: %v", err)
	}
	seen := map[string]bool{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			seen[m.Name] = true
		}
	}
	for _, want := range []string{"http.server.request.duration", "http.server.active_requests"} {
		if !seen[want] {
			t.Errorf("reports router did not emit %s; saw %v", want, seen)
		}
	}
}
