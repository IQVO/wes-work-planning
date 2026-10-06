package main

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
)

// newHousekeeper builds the retention sweeper (ADR-0032) over the Postgres
// pool, or nil when there is no pool (in-memory mode has nothing to sweep).
// All knobs are optional; "0" disables that table's sweep:
//
//	HOUSEKEEPING_INTERVAL   time between sweeps          (default 1h)
//	IDEMPOTENCY_KEY_TTL     idempotency_keys retention   (default 24h)
//	OUTBOX_RETENTION        published outbox retention   (default 168h = 7d)
func newHousekeeper(pool *pgxpool.Pool, logger *slog.Logger) *postgres.Housekeeper {
	if pool == nil {
		return nil
	}
	return postgres.NewHousekeeper(pool, logger,
		postgres.WithHousekeepingInterval(durationEnv("HOUSEKEEPING_INTERVAL", postgres.DefaultHousekeepingInterval)),
		postgres.WithIdempotencyKeyTTL(retentionEnv("IDEMPOTENCY_KEY_TTL", postgres.DefaultIdempotencyKeyTTL)),
		postgres.WithOutboxRetention(retentionEnv("OUTBOX_RETENTION", postgres.DefaultOutboxRetention)),
	)
}
