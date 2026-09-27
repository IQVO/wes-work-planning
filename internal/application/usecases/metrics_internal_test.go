package usecases

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// validCounterName is the business metric ReleaseNextWork records — a
// name the SDK's instrument-name validation accepts.
const validCounterName = "wes.work_units.released"

// setGlobalMeterProvider swaps the OTel global meter provider for the test's
// duration and restores the previous one on cleanup, the same faking
// technique metrics_test.go uses — here driven directly against the guard
// rather than through a use case constructor.
func setGlobalMeterProvider(t *testing.T, mp metric.MeterProvider) {
	t.Helper()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
}

// TestNewInt64Counter_ValidInstrumentReturnsRecordingCounter proves the
// happy path hands back the real SDK counter: Add calls land in the
// reader with the business attribute attached, so metric collection can
// never silently degrade on a healthy meter.
func TestNewInt64Counter_ValidInstrumentReturnsRecordingCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	setGlobalMeterProvider(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	counter := newInt64Counter(validCounterName,
		metric.WithDescription("Work units admitted into a process path's work pool by the release policy."),
	)
	if counter == nil {
		t.Fatal("expected a counter for a valid instrument name, got nil")
	}

	ctx := context.Background()
	counter.Add(ctx, 2, metric.WithAttributes(attribute.String(AttrPathId, "pick-a")))

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	found := false
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != validCounterName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is a %T, want an int64 counter", m.Name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(AttrPathId)); ok && v.String() == "pick-a" {
					found = true
					if dp.Value != 2 {
						t.Fatalf("%s{%s=pick-a} = %d, want 2", validCounterName, AttrPathId, dp.Value)
					}
				}
			}
		}
	}
	if !found {
		t.Fatalf("no %s datapoint for %s=pick-a in %v", validCounterName, AttrPathId, collected.ScopeMetrics)
	}
}

// TestNewInt64Counter_FailingMeterFallsBackToNoop proves the guard branch:
// when instrument creation fails (the SDK rejects a malformed instrument
// name), the creation error is reported through otel.Handle and a no-op
// counter stands in — instrumentation can never fail a use case, and the
// fallback records nothing rather than panicking on Add.
func TestNewInt64Counter_FailingMeterFallsBackToNoop(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	setGlobalMeterProvider(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	var handled []error
	prevHandler := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		handled = append(handled, err)
	}))
	t.Cleanup(func() { otel.SetErrorHandler(prevHandler) })

	counter := newInt64Counter("not a valid instrument name!")
	if counter == nil {
		t.Fatal("expected a no-op counter stand-in, got nil")
	}

	// The no-op stand-in must accept Add without panicking — the use case
	// holding it keeps running with metrics degraded, not broken.
	counter.Add(context.Background(), 1)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	if len(collected.ScopeMetrics) != 0 {
		t.Fatalf("expected no metrics from the no-op fallback, got %v", collected.ScopeMetrics)
	}

	if len(handled) == 0 {
		t.Fatal("expected the creation failure to be reported to otel.Handle, got none")
	}
}
