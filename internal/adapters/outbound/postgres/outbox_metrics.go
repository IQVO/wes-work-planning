package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// outboxMeterName is this adapter's OpenTelemetry instrumentation scope.
const outboxMeterName = "github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"

// OutboxLagGaugeName is the ADR-0013 Tier 2 name of the outbox-lag gauge
// (<bounded_context>.<noun>.<verb_or_state>, dot-separated).
const OutboxLagGaugeName = "wes.outbox.lag_seconds"

// outboxLagQuery finds the age, in seconds, of the OLDEST unpublished
// outbox_events row — the one the relay would drain next. created_at is set
// once at INSERT time and never touched again, so now() - created_at for the
// oldest row IS how long the relay has been behind for at least that long.
// It says nothing about attempts/last_error on that row (a row can lag because
// the relay has not run yet, or because it is stuck retrying); both raise this
// gauge, which is the point: "how long has SOMETHING been waiting", not "why".
// Follow-up named in ADR-0014.
const outboxLagQuery = `
	SELECT EXTRACT(EPOCH FROM (now() - created_at))::float8
	FROM outbox_events
	WHERE published_at IS NULL
	ORDER BY id
	LIMIT 1
`

// rowQuerier is the slice of *pgxpool.Pool the lag query needs.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// oldestUnpublishedLagSeconds reports the age in seconds of the oldest
// unpublished outbox_events row, or 0 when the outbox is fully drained (no
// unpublished rows — pgx.ErrNoRows is the "caught up" answer, not a failure).
func oldestUnpublishedLagSeconds(ctx context.Context, q rowQuerier) (float64, error) {
	var lag float64
	if err := q.QueryRow(ctx, outboxLagQuery).Scan(&lag); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("postgres: query outbox lag: %w", err)
	}
	return lag, nil
}

// RegisterOutboxLagGauge installs an asynchronous gauge, sampled once per
// collection, reporting wes.outbox.lag_seconds: the age of the oldest
// unpublished outbox_events row (0 when fully drained). It is created against
// the global MeterProvider, so ordering versus telemetry.Setup does not matter.
//
// Callers keep the returned metric.Registration and Unregister it on shutdown
// so the callback stops touching pool before the pool is closed. The database
// is never queried at registration time.
func RegisterOutboxLagGauge(pool *pgxpool.Pool) (metric.Registration, error) {
	return registerOutboxLagGauge(otel.Meter(outboxMeterName), pool)
}

func registerOutboxLagGauge(meter metric.Meter, q rowQuerier) (metric.Registration, error) {
	gauge, err := meter.Float64ObservableGauge(
		OutboxLagGaugeName,
		metric.WithDescription("Age in seconds of the oldest unpublished outbox_events row; 0 when the outbox is fully drained. A growing value means the relay is down, stuck, or behind."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: create outbox lag gauge: %w", err)
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		lag, err := oldestUnpublishedLagSeconds(ctx, q)
		if err != nil {
			return err
		}
		o.ObserveFloat64(gauge, lag)
		return nil
	}, gauge)
	if err != nil {
		return nil, fmt.Errorf("postgres: register outbox lag callback: %w", err)
	}
	return reg, nil
}
