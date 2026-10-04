package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

const releasedCounterName = "wes.work_units.released"

func useManualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	return reader
}

func counterFor(t *testing.T, collected metricdata.ResourceMetrics, name string, key attribute.Key, want string) (int64, bool) {
	t.Helper()
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is a %T, want an int64 counter", m.Name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(key); ok && v.String() == want {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

// ADR-0013: the Tier 2 release counter is recorded by the outbound telemetry
// adapter behind ports.ReleaseMetrics and carries the dot-separated
// "path.id" attribute (not "path_id").
func TestReleaseMetrics_CountsPerPathWithDotSeparatedAttribute(t *testing.T) {
	reader := useManualReader(t)
	m := telemetry.NewReleaseMetrics()

	pathA, _ := shared.NewPathId("pick-a")
	pathB, _ := shared.NewPathId("pick-b")
	ctx := context.Background()
	m.WorkUnitReleased(ctx, pathA)
	m.WorkUnitReleased(ctx, pathA)
	m.WorkUnitReleased(ctx, pathB)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("collect: %v", err)
	}

	if telemetry.AttrPathId != "path.id" {
		t.Fatalf("AttrPathId = %q, want the dot-separated %q", telemetry.AttrPathId, "path.id")
	}
	if got, found := counterFor(t, collected, releasedCounterName, "path.id", "pick-a"); !found || got != 2 {
		t.Fatalf("%s{path.id=pick-a} = %d (found=%v), want 2", releasedCounterName, got, found)
	}
	if got, found := counterFor(t, collected, releasedCounterName, "path.id", "pick-b"); !found || got != 1 {
		t.Fatalf("%s{path.id=pick-b} = %d (found=%v), want 1", releasedCounterName, got, found)
	}
	if _, found := counterFor(t, collected, releasedCounterName, "path_id", "pick-a"); found {
		t.Fatal("the legacy path_id attribute must no longer be emitted")
	}
}

// When the SDK rejects the instrument, the failure goes to otel.Handle and a
// no-op counter stands in: instrumentation can never fail a use case.
func TestNewReleaseMetrics_InvalidInstrumentFallsBackToNoop(t *testing.T) {
	reader := useManualReader(t)

	var handled []error
	prev := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { handled = append(handled, err) }))
	t.Cleanup(func() { otel.SetErrorHandler(prev) })

	c := telemetry.NewInt64CounterForTest("not a valid instrument name!")
	c.Add(context.Background(), 1) // must not panic

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(collected.ScopeMetrics) != 0 {
		t.Fatalf("expected no metrics from the no-op fallback, got %v", collected.ScopeMetrics)
	}
	if len(handled) == 0 {
		t.Fatal("expected the creation failure to be reported to otel.Handle")
	}
}
