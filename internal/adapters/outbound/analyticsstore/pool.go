package analyticsstore

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the analytics writer's (cmd/wes-projector) per-process
// connection ceiling. The projector is fixed at ONE replica -- its Kafka
// consumer group (kafka.AnalyticsConsumerGroup, "wes-analytics") is a
// STABLE, shared group name, but with no HPA on this workload (see the
// chart's autoscaling comment for why: a single logical writer of the
// analytical database is enough for this workload's throughput), a
// small, flat pool is enough. This pool's DSN stays DIRECT to Postgres
// (not through PgBouncer) -- a single Kafka-consumer connection with low
// QPS gets no benefit from transaction pooling, mirroring
// warehouse-infra PR #43's own documented reasoning for leaving analytics
// DSNs alone. Matches order-management ADR-0026's identical MaxConns=5
// for its equivalent analytics writer pool.
const MaxConns = 5

// ReportsMaxConns is the analytics reader's (cmd/wes-reports) per-process
// connection ceiling. Unlike the projector, reports IS HPA-scalable
// (stateless REST reads, chart's autoscaling.reports block, min 1 / max
// 3): at the HPA ceiling, 3 * 5 = 15 connections against the analytical
// database. This DSN also stays direct to Postgres, same reasoning as
// MaxConns above. See the HPA + pgxpool-tuning ADR for the full
// connection-budget accounting across this service's four processes on
// the ONE shared Postgres instance. Matches order-management ADR-0026's
// identical ReportsMaxConns=5.
const ReportsMaxConns = 5

// StatementTimeout bounds the analytics WRITER's (projector) queries.
// Slightly more generous than the OLTP side's 5s: a Kafka consumer
// replaying a backlog after a redeploy issues its upserts
// (applyCounter/upsertCounter, single-row ON CONFLICT) in a tight loop,
// and a transient lock wait here should not need to be as tight as an
// interactive OLTP request -- but it must still not be unbounded, or one
// poisoned/oversized batch could wedge the single projector instance's
// only connection pool indefinitely (with no replica to fail over to,
// that would stall the ENTIRE analytics pipeline, not just one of
// several api pods). Matches order-management's identical writer value.
const StatementTimeout = "10s"

// ReportsStatementTimeout bounds the analytics READER's (reports)
// queries. Its throughput report aggregates rows across a caller-chosen
// [From, To) time range (postgres_report.go's Query) -- wider than the
// OLTP side's always-single-aggregate-by-id shape -- so it gets more
// headroom than StatementTimeout, but still a hard ceiling: a
// caller-supplied unbounded date range must not be able to hold a
// reports connection forever. Matches order-management's identical
// reader value.
const ReportsStatementTimeout = "15s"

// NewPool builds a pgxpool over the analytical database at databaseURL,
// with MaxConns, StatementTimeout, and the OTel pgx tracer installed
// (mirroring the OLTP postgres.Connect). It is used by the writer
// (cmd/wes-projector).
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout, false)
}

// NewReadOnlyPool builds a pgxpool over the analytical database in which
// every connection is pinned to a read-only transaction default
// (default_transaction_read_only=on), with ReportsMaxConns and
// ReportsStatementTimeout applied. The reader process (cmd/wes-reports)
// uses this so a bug there cannot mutate the read model even if the
// database role itself is not read-only -- defence in depth on top of
// the read-only ANALYTICS_DATABASE_URL role (ADR-0011).
func NewReadOnlyPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPoolWithLimits(ctx, databaseURL, ReportsMaxConns, ReportsStatementTimeout, true)
}

// newPoolWithLimits is the shared implementation behind NewPool/
// NewReadOnlyPool, parameterised so a test can drive a much shorter
// statementTimeout directly (proving the AfterConnect hook actually
// applies the setting to every new connection, by triggering a real
// cancellation) without waiting out the production value.
func newPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string, readOnly bool) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	// See postgres.Connect for why WithQuerySpanNamePrefix is needed with
	// otelpgx v0.12.0+ (default flipped); kept in sync with the OLTP pool.
	cfg.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithQuerySpanNamePrefix())
	if readOnly {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// RecordPoolStats registers observable gauges for pool's connection counts on
// the global MeterProvider, mirroring the OLTP postgres pool stats.
func RecordPoolStats(pool *pgxpool.Pool) error {
	return otelpgx.RecordStats(pool)
}
