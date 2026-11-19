package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
)

// repositories groups the Postgres persistence adapters. pgPool and uow are
// always set (DATABASE_URL is mandatory, ADR-0044): every use case runs Save +
// Publish in one transaction with the outbox rows (ADR-0014). The charge and
// plan tables are kept but no longer written (ADR-0049 decision 4).
type repositories struct {
	pools          ports.WorkPoolRepo
	workUnits      ports.WorkUnitRepo
	laborPlanViews ports.LaborPlanViewRepo
	inventoryViews ports.InventoryViewRepo
	processedEvts  ports.ProcessedEventRepo
	// relocations and slotPlans back ADR-0040's slot-plan relocation work:
	// the same work-unit adapter answers the RELOCATE finders. pickFaces
	// backs ADR-0048's forward-pick-face lookup the same way.
	relocations ports.RelocationWorkRepo
	slotPlans   ports.SlotPlanRepo
	pickFaces   ports.ForwardPickFaceLookup
	pgPool      *pgxpool.Pool
	uow         ports.UnitOfWork
}

// wireRepositories opens the Postgres outbound side; DATABASE_URL is
// mandatory and there is no in-memory fallback (ADR-0044).
//
// The OLTP schema is a precondition this process enforces itself, rather
// than assuming an out-of-band golang-migrate CLI step ran. That
// assumption silently did not hold: the service deployed cleanly against
// an empty database and every Postgres-backed endpoint failed at request
// time with `relation "charge_forecasts" does not exist`. Migrating
// before the pool is opened matches what fulfillment-execution's and
// workforce-management's OLTP binaries already do. Retried, because in
// this fleet EVERY injected pod's first outbound TCP dial is reset ~10s
// after the app starts (Istio native sidecars; see internal/bootretry's
// package doc comment). A single attempt turns that known, transient
// condition into CrashLoopBackOff before this service ever gets far
// enough to serve its own health probe.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below —
// the pgxpool opened just after it (and used for every subsequent
// request) always uses databaseURL. They are deliberately different
// connection strings in a PgBouncer-fronted environment: golang-migrate's
// postgres driver takes a session-scoped `SELECT pg_advisory_lock($1)` to
// serialize concurrent migration runs across replicas starting at the
// same time, and PgBouncer's transaction-pooling mode (this fleet's
// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43) does not
// support session-scoped state — each statement in one logical client
// session can land on a different physical backend connection, so the
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See ADR
// 0026-migrations-direct-postgres-connection.md (mirroring order-management
// ADR-0029) for the full incident and fix. Callers pass
// MIGRATIONS_DATABASE_URL when set (warehouse-infra now provisions it as a
// direct, non-pooled DSN alongside DATABASE_URL) or fall back to
// databaseURL itself for any environment that doesn't provision the split
// (local dev, CI integration tests) — byte-identical to this function's
// behavior before this parameter existed in that case.
func wireRepositories(databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (repositories, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return repositories{}, errors.New("DATABASE_URL is required: this context runs only on a real Postgres (ADR-0044), there is no in-memory mode")
	}
	return postgresRepositories(databaseURL, migrationsDatabaseURL, migrationsPath, logger)
}

// postgresRepositories migrates the schema, opens the pool and wraps it in
// the Postgres adapters.
func postgresRepositories(databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (repositories, error) {
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootCancel()
	if err := bootretry.Retry(bootCtx, logger, "run migrations", func() error {
		return postgres.Migrate(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return repositories{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Connect(ctx, databaseURL)
	if err != nil {
		return repositories{}, err
	}
	// pgxpool.NewWithConfig does not itself dial or establish a
	// connection, so without this retried Ping the first-dial reset
	// would surface inside the first real request rather than at boot —
	// turning a transient sidecar warm-up into an intermittent 500
	// instead of a bounded startup retry.
	if err := bootretry.Retry(bootCtx, logger, "ping postgres", func() error {
		return pool.Ping(bootCtx)
	}); err != nil {
		pool.Close()
		return repositories{}, err
	}
	workUnits := postgres.NewWorkUnitRepo(pool)
	return repositories{
		pools:          postgres.NewWorkPoolRepo(pool),
		workUnits:      workUnits,
		laborPlanViews: postgres.NewLaborPlanViewRepo(pool),
		inventoryViews: postgres.NewInventoryViewRepo(pool),
		processedEvts:  postgres.NewProcessedEventRepo(pool),
		relocations:    workUnits,
		slotPlans:      postgres.NewSlotPlanRepo(pool),
		pickFaces:      workUnits,
		pgPool:         pool,
		uow:            postgres.NewUnitOfWork(pool),
	}, nil
}
