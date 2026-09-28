package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// The Kafka consumer group id must be overridable, not a fixed constant.
//
// Kafka consumer-group offsets are shared infrastructure state, not
// per-process state. When this binary runs against the same broker as the
// deployed wes-work-planning Deployment -- which is exactly what the
// e2e-tests harness does, since the fleet has ONE broker platform-wide --
// a hardcoded group id makes both processes join the SAME group. With one
// partition per topic, Kafka's rebalance protocol hands that partition to
// only one member, and the loser silently consumes nothing.
//
// That failure is invisible to any test that constructs the consumer
// directly: it only appears as a scenario timing out waiting for a
// projection that never arrives. Hence this test asserts the resolution
// rule itself.
func TestConsumerGroupID(t *testing.T) {
	t.Run("defaults to the service name so deployments keep one shared group", func(t *testing.T) {
		if got := consumerGroupID(""); got != defaultConsumerGroup {
			t.Fatalf("consumerGroupID(%q) = %q, want %q", "", got, defaultConsumerGroup)
		}
	})

	t.Run("an explicit override wins so a second process can isolate itself", func(t *testing.T) {
		const override = "wes-work-planning-e2e-12345"
		if got := consumerGroupID(override); got != override {
			t.Fatalf("consumerGroupID(%q) = %q, want %q", override, got, override)
		}
	})

	t.Run("whitespace-only is treated as unset rather than a valid group", func(t *testing.T) {
		if got := consumerGroupID("   "); got != defaultConsumerGroup {
			t.Fatalf("consumerGroupID(%q) = %q, want %q", "   ", got, defaultConsumerGroup)
		}
	})
}

// TestMigrationsDatabaseURLFallback proves the fallback wiring run()
// applies before calling wireRepositories:
// getenv("MIGRATIONS_DATABASE_URL", databaseURL) must return
// MIGRATIONS_DATABASE_URL's own value when it is set, and databaseURL
// itself (DATABASE_URL) when it is unset. This is the exact env-lookup
// line the fix for the PgBouncer/pg_advisory_lock incompatibility (ADR
// 0026-migrations-direct-postgres-connection.md, mirroring
// order-management ADR-0029) depends on: any environment that doesn't
// provision the split (local dev, CI integration tests, a cluster whose
// Terraform predates this fix) must keep working exactly as before, using
// DATABASE_URL for everything including migrations.
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/wes_work_planning?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		t.Setenv("MIGRATIONS_DATABASE_URL", "")
		os.Unsetenv("MIGRATIONS_DATABASE_URL")

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != databaseURL {
			t.Fatalf("getenv fallback = %q, want the DATABASE_URL value %q", got, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/wes_work_planning?sslmode=disable"
		t.Setenv("MIGRATIONS_DATABASE_URL", direct)

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != direct {
			t.Fatalf("getenv = %q, want the direct MIGRATIONS_DATABASE_URL value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", got, direct)
		}
		if got == databaseURL {
			t.Fatal("MIGRATIONS_DATABASE_URL and DATABASE_URL collapsed to the same value — the whole point of this env var is that it differs")
		}
	})
}

// TestWireRepositories_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves wireRepositories itself — not just the env-var read above —
// actually threads migrationsDatabaseURL into the migration step and
// databaseURL into the pgxpool, rather than the two ever being
// conflated. Gives databaseURL an address nothing listens on (so opening
// the pgxpool, which happens AFTER migrations succeed, would hang/fail
// loudly if ever reached) and migrationsDatabaseURL a schemeless string
// that migrate.New rejects immediately with a distinctive parse error
// ("failed to parse scheme from database URL") — if wireRepositories
// ignored migrationsDatabaseURL and ran migrations against databaseURL
// instead, this test would see a dial/"connection refused" error, not
// the parse error.
func TestWireRepositories_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/wes_work_planning?sslmode=disable&connect_timeout=1"
	)

	_, err := wireRepositories(unreachableAppURL, bogusMigrationsURL, "migrations", quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}

// quietLogger discards output so a test asserting on an error/retry path
// doesn't spam the test log with the expected warnings.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
