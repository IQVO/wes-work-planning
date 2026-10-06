package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassification"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/traveldistance"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
	"github.com/claudioed/wes-work-planning/internal/resilience"
)

// repositories groups the environment-selected persistence adapters.
// pgPool and uow are nil in the in-memory configuration. A nil
// UnitOfWork makes every use case run Save + Publish back to back
// (ADR-0014); with Postgres they run in one transaction.
type repositories struct {
	charges        ports.ChargeRepo
	plans          ports.PlanRepo
	pools          ports.WorkPoolRepo
	workUnits      ports.WorkUnitRepo
	laborPlanViews ports.LaborPlanViewRepo
	inventoryViews ports.InventoryViewRepo
	processedEvts  ports.ProcessedEventRepo
	pgPool         *pgxpool.Pool
	uow            ports.UnitOfWork
}

// classificationLookup wires the product-classification ACL behind the
// shared circuit-breaker gauge (ADR-0023).
func (r repositories) classificationLookup(logger *slog.Logger) ports.ProductClassificationLookup {
	breakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics not registered; continuing without them", "error", err)
	}
	return buildClassificationLookup(getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), breakerMetrics, logger)
}

// travelDistanceLookup wires the facility-layout travel-distance ACL
// behind the shared circuit-breaker gauge (ADR-0023).
func (r repositories) travelDistanceLookup(logger *slog.Logger) ports.TravelDistanceLookup {
	breakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics not registered; continuing without them", "error", err)
	}
	return buildTravelDistanceLookup(getenv("TRAVEL_DISTANCE_MODE", "permissive"), os.Getenv("FACILITY_LAYOUT_BASE_URL"), breakerMetrics, logger)
}

// wireRepositories selects the in-memory or Postgres outbound side.
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
	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return memoryRepositories(), nil
	}
	return postgresRepositories(databaseURL, migrationsDatabaseURL, migrationsPath, logger)
}

func memoryRepositories() repositories {
	return repositories{
		charges:        memory.NewChargeRepo(),
		plans:          memory.NewPlanRepo(),
		pools:          memory.NewWorkPoolRepo(),
		workUnits:      memory.NewWorkUnitRepo(),
		laborPlanViews: memory.NewLaborPlanViewRepo(),
		inventoryViews: memory.NewInventoryViewRepo(),
		processedEvts:  memory.NewProcessedEventRepo(),
	}
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
	return repositories{
		charges:        postgres.NewChargeRepo(pool),
		plans:          postgres.NewPlanRepo(pool),
		pools:          postgres.NewWorkPoolRepo(pool),
		workUnits:      postgres.NewWorkUnitRepo(pool),
		laborPlanViews: postgres.NewLaborPlanViewRepo(pool),
		inventoryViews: postgres.NewInventoryViewRepo(pool),
		processedEvts:  postgres.NewProcessedEventRepo(pool),
		pgPool:         pool,
		uow:            postgres.NewUnitOfWork(pool),
	}, nil
}

// buildClassificationLookup selects the outbound
// ports.ProductClassificationLookup adapter via PRODUCT_CLASSIFICATION_MODE
// (http|permissive), defaulting to "permissive" so existing tests, CI and
// deployments that do not set the env var are unaffected — mirroring
// inventory-storage's own LOCATION_LOOKUP_MODE=http|permissive pattern (see
// ADR-0009). "http" requires INVENTORY_STORAGE_BASE_URL. In "http" mode the
// client is wrapped with a per-dependency circuit breaker + jittered retry
// (ADR-0023); recorder may be nil (breaker metrics unavailable), a
// documented no-op.
func buildClassificationLookup(mode, inventoryStorageBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.ProductClassificationLookup {
	if !strings.EqualFold(mode, "http") {
		return productclassification.NewPermissiveLookup()
	}
	logger.Info("product classification lookup configured", "mode", "http", "inventory_storage_base_url", inventoryStorageBaseURL)
	return productclassification.NewBreakerClient(productclassification.NewClient(inventoryStorageBaseURL, nil), recorder)
}

// buildTravelDistanceLookup selects the outbound ports.TravelDistanceLookup
// adapter via TRAVEL_DISTANCE_MODE (http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected — mirroring buildClassificationLookup's own
// PRODUCT_CLASSIFICATION_MODE pattern exactly (see ADR-0017, Phase B3).
// "http" requires FACILITY_LAYOUT_BASE_URL. In "http" mode the client is
// wrapped with a per-dependency circuit breaker + jittered retry
// (ADR-0023); recorder may be nil (breaker metrics unavailable), a
// documented no-op.
func buildTravelDistanceLookup(mode, facilityLayoutBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.TravelDistanceLookup {
	if !strings.EqualFold(mode, "http") {
		return traveldistance.NewPermissiveLookup()
	}
	logger.Info("travel distance lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL)
	return traveldistance.NewBreakerClient(traveldistance.NewClient(facilityLayoutBaseURL, nil), recorder)
}
