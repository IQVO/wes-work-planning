//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
)

func readLagGauge(t *testing.T, reader *sdkmetric.ManualReader) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == postgres.OutboxLagGaugeName {
				g, ok := m.Data.(metricdata.Gauge[float64])
				if !ok || len(g.DataPoints) != 1 {
					t.Fatalf("unexpected gauge data %T %+v", m.Data, m.Data)
				}
				return g.DataPoints[0].Value
			}
		}
	}
	t.Fatalf("%s was not reported", postgres.OutboxLagGaugeName)
	return 0
}

// ADR-0014 follow-up: wes.outbox.lag_seconds is the age of the oldest
// unpublished outbox row, 0 when drained — against a real Postgres.
func TestOutboxLagGauge_ReportsOldestUnpublishedAge(t *testing.T) {
	pool := outboxDB(t)

	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	reg, err := postgres.RegisterOutboxLagGauge(pool)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = reg.Unregister() })

	ctx := context.Background()
	if got := readLagGauge(t, reader); got != 0 {
		t.Fatalf("empty outbox lag = %v, want 0", got)
	}

	// Two unpublished rows, the oldest 90s old; plus an OLDER but already
	// published row that must be ignored.
	for _, q := range []string{
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at, published_at) VALUES ('t','e','k','v', now() - interval '1 hour', now() - interval '59 minutes')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at) VALUES ('t','e','k','v', now() - interval '90 seconds')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at) VALUES ('t','e','k','v', now() - interval '10 seconds')`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if got := readLagGauge(t, reader); got < 90 || got > 150 {
		t.Fatalf("lag = %vs, want ~90s (the oldest UNPUBLISHED row)", got)
	}

	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE published_at IS NULL`); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := readLagGauge(t, reader); got != 0 {
		t.Fatalf("drained outbox lag = %v, want 0", got)
	}
}
