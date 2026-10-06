package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeRow struct {
	val float64
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*float64)) = r.val
	return nil
}

type fakeQuerier struct{ row fakeRow }

func (q fakeQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return q.row }

func TestOldestUnpublishedLagSeconds(t *testing.T) {
	ctx := context.Background()

	if got, err := oldestUnpublishedLagSeconds(ctx, fakeQuerier{fakeRow{val: 12.5}}); err != nil || got != 12.5 {
		t.Fatalf("got (%v, %v), want (12.5, nil)", got, err)
	}
	// No unpublished rows is "caught up" (0), not an error.
	if got, err := oldestUnpublishedLagSeconds(ctx, fakeQuerier{fakeRow{err: pgx.ErrNoRows}}); err != nil || got != 0 {
		t.Fatalf("drained outbox: got (%v, %v), want (0, nil)", got, err)
	}
	boom := errors.New("boom")
	if _, err := oldestUnpublishedLagSeconds(ctx, fakeQuerier{fakeRow{err: boom}}); !errors.Is(err, boom) {
		t.Fatalf("a real query failure must be returned, got %v", err)
	}
}

func collectGauge(t *testing.T, reader *sdkmetric.ManualReader) (float64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != OutboxLagGaugeName {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok || len(g.DataPoints) != 1 {
				t.Fatalf("%s: unexpected data %T %+v", m.Name, m.Data, m.Data)
			}
			return g.DataPoints[0].Value, true
		}
	}
	return 0, false
}

func TestRegisterOutboxLagGauge_ReportsLagAndStopsOnUnregister(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	reg, err := registerOutboxLagGauge(meter, fakeQuerier{fakeRow{val: 42}})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if got, ok := collectGauge(t, reader); !ok || got != 42 {
		t.Fatalf("gauge = (%v, %v), want (42, true)", got, ok)
	}

	if err := reg.Unregister(); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if _, ok := collectGauge(t, reader); ok {
		t.Fatal("an unregistered gauge must stop reporting")
	}
}
