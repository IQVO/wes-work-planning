//go:build integration

package postgres_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
)

// These tests start their OWN Postgres via testcontainers, so they assert on
// what Migrate actually does to a genuinely empty database rather than on a
// database somebody already prepared. That distinction is the entire point
// here: this repo previously ASSUMED an out-of-band golang-migrate CLI step
// had run, nothing verified it, and the service shipped against a database
// with zero tables.

// migrationsDir resolves ./migrations from this package's location.
func migrationsDir(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "migrations"))
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	return abs
}

// startPostgres returns the connection string of a genuinely EMPTY, unmigrated
// database. These tests need pristine state, so each gets its own database
// inside the package's shared container (see main_integration_test.go).
func startPostgres(t *testing.T) string {
	t.Helper()
	return freshDatabase(t)
}

// The regression test for the bug this change fixes: against a completely
// empty database, Migrate must create the OLTP schema. Before this, nothing
// ran the migrations and charge_forecasts simply did not exist, which only
// surfaced as a 500 at request time.
func TestMigrateCreatesTheSchemaOnAnEmptyDatabase(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	pool, err := postgres.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Precondition: the table genuinely does not exist yet, so a passing
	// assertion below cannot be an artifact of a pre-prepared database.
	var existsBefore bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.charge_forecasts') IS NOT NULL`).
		Scan(&existsBefore); err != nil {
		t.Fatalf("check charge_forecasts before: %v", err)
	}
	if existsBefore {
		t.Fatal("expected an empty database, but charge_forecasts already exists")
	}

	if err := postgres.Migrate(dsn, migrationsDir(t)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Every table the OLTP repos in this package depend on. Names taken
	// from migrations/*.up.sql, not guessed from the repo type names --
	// e.g. the labor plan view table is singular (labor_plan_view) and the
	// inventory view is usable_inventory_view.
	for _, table := range []string{
		"charge_forecasts",
		"shift_plans",
		"work_pools",
		"work_pool_entries",
		"work_units",
		"labor_plan_view",
		"usable_inventory_view",
		"processed_events",
		"product_classification_copy",
		"events",
	} {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT to_regclass('public.' || $1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatalf("check %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("expected table %q to exist after Migrate", table)
		}
	}
}

// Migrate runs on every service start, so a database already at the latest
// version must be a no-op, not an error (golang-migrate reports ErrNoChange,
// which Migrate deliberately swallows).
func TestMigrateIsIdempotent(t *testing.T) {
	dsn := startPostgres(t)
	dir := migrationsDir(t)

	if err := postgres.Migrate(dsn, dir); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := postgres.Migrate(dsn, dir); err != nil {
		t.Fatalf("second Migrate must be a no-op, got: %v", err)
	}
	if err := postgres.Migrate(dsn, dir); err != nil {
		t.Fatalf("third Migrate must be a no-op, got: %v", err)
	}
}

// A bad migrations path must fail loudly at startup rather than leaving the
// service running against an unmigrated database.
func TestMigrateFailsOnAMissingMigrationsPath(t *testing.T) {
	dsn := startPostgres(t)

	if err := postgres.Migrate(dsn, filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatal("expected an error for a missing migrations directory")
	}
}
