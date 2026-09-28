package postgres

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the OLTP pool's per-process connection ceiling, shared by
// both cmd/wes (the api Deployment, HPA-scalable up to
// charts/wes-work-planning values.yaml's autoscaling.maxReplicas, 4) and
// cmd/mcp (the mcp Deployment, fixed at 1 replica -- see that chart
// value's own doc comment for why it does not get an HPA).
//
// This service's DATABASE_URL Secret already points at PgBouncer in
// transaction-pooling mode (warehouse-infra PR #43, pool_size=12 per
// service database), transparently at the DSN level -- no code change
// needed here for that. PgBouncer, not this client-side setting, is what
// bounds the REAL server-side connection count against the shared
// Postgres instance's max_connections=100 (the Bitnami chart's own
// unmodified default; warehouse-infra's terraform/postgres.tf never
// overrides it). MaxConns can therefore stay a generous client-side
// ceiling instead of being squeezed down to protect the server directly:
// at the OLTP Deployment's HPA ceiling of 4 replicas, 4 * 10 = 40
// client-side pgx connections, but PgBouncer's pool_size=12 is the real
// server-side cap regardless of how many client pools exist. Matches
// order-management ADR-0026's identical MaxConns=10 for its equivalent
// OLTP pool. See the HPA + pgxpool-tuning ADR for the full accounting.
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection
// on the OLTP database before Postgres cancels it. wes-work-planning's
// OLTP queries (ReceiveCharge, CommitPlan, ReleaseWorkUnit,
// RebalanceFlow, GetWorkPoolView, and friends) are single-aggregate
// reads/writes keyed by id, normally low-single-digit milliseconds. 5s
// is generous headroom for real transient contention (a lock wait
// behind a concurrent writer) without ever being a normal-path concern,
// while bounding the absolute worst case tightly since this is the
// interactive, latency-sensitive path and also the pool with the most
// connections (40 at max HPA scale) to protect. Matches
// order-management's identical OLTP value.
const StatementTimeout = "5s"

// Connect opens a pgxpool.Pool against databaseURL with MaxConns,
// StatementTimeout, and the OTel pgx tracer installed, so every query,
// batch, copy and connect becomes a child span of whatever span is
// active on the calling context. otelpgx records the normalized SQL
// statement, never the literal argument values.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return ConnectWithLimits(ctx, databaseURL, MaxConns, StatementTimeout)
}

// ConnectWithLimits is Connect's shared implementation, taking maxConns
// and statementTimeout explicitly so an integration test can drive a
// much shorter timeout directly -- proving the AfterConnect hook really
// applies the setting to every new connection, by triggering an actual
// cancellation -- without waiting out the real production value.
// Production callers should use Connect.
func ConnectWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	// otelpgx v0.12.0 changed the default span-name prefix behavior (no
	// longer prefixed by default); WithQuerySpanNamePrefix restores the
	// "query "/"prepare "/"batch query " prefixing this codebase and its
	// tests (see tracing_integration_test.go) depend on.
	cfg.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithQuerySpanNamePrefix())
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
