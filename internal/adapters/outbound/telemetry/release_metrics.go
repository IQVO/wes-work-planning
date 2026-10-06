package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// releasedCounterName is the Tier 2 business counter (ADR-0013) for work
// units admitted into a process path's pool by the release policy.
const releasedCounterName = "wes.work_units.released"

// AttrPathId is the process-path attribute every path-scoped business metric
// carries. ADR-0013 mandates dot-separated attribute keys, so this is
// "path.id" (it was "path_id" before the ADR-0013 conformance fix). The OTel
// Collector's prometheus exporter normalises "." to "_", so the Prometheus
// label stays path_id and existing dashboards keep working; only the OTel
// attribute key (visible to OTLP consumers) is renamed.
const AttrPathId = "path.id"

// ReleaseMetrics implements ports.ReleaseMetrics with an OTel counter on the
// global MeterProvider telemetry.Setup installs — the application layer only
// sees the port, never OTel.
type ReleaseMetrics struct {
	released metric.Int64Counter
}

var _ ports.ReleaseMetrics = (*ReleaseMetrics)(nil)

// NewReleaseMetrics builds the counter from the global meter provider. The
// OTel global provider delegates to the real SDK once Setup installs it, so
// a counter built before Setup still records. If the instrument cannot be
// created at all the error goes to otel.Handle and a no-op counter stands in:
// instrumentation can never fail a use case.
func NewReleaseMetrics() *ReleaseMetrics {
	return &ReleaseMetrics{released: newInt64Counter(releasedCounterName,
		metric.WithDescription("Work units admitted into a process path's work pool by the release policy."),
		metric.WithUnit("{work_unit}"),
	)}
}

// WorkUnitReleased implements ports.ReleaseMetrics.
func (m *ReleaseMetrics) WorkUnitReleased(ctx context.Context, pathId shared.PathId) {
	m.released.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrPathId, pathId.String())))
}

func newInt64Counter(name string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	counter, err := otel.Meter(meterName).Int64Counter(name, opts...)
	if err != nil {
		otel.Handle(err)
		c, _ := noop.NewMeterProvider().Meter(meterName).Int64Counter(name)
		return c
	}
	return counter
}
