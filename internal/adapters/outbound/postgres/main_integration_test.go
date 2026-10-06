//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
)

// One throwaway testcontainers Postgres per PACKAGE run (TestMain), with the
// real migrations applied once. Tests that touch the migrated schema call
// migratedDSN/outboxDB, which TRUNCATE every application table (never
// schema_migrations) so each test starts empty regardless of order. Tests
// that need a pristine, unmigrated database (the Migrate tests, the pool-limit
// tests) get their own empty DATABASE inside the same container via
// freshDatabase -- a CREATE DATABASE costs milliseconds where a container
// costs seconds. None of this reads DATABASE_URL or skips.

// sharedDSN is the shared, already-migrated database's DSN, set by TestMain
// before any test runs.
var sharedDSN string

var freshDatabaseSeq atomic.Int64

func TestMain(m *testing.M) {
	os.Exit(runWithPostgres(m))
}

// runWithPostgres boots the container, migrates it, runs the package's
// tests, and always terminates the container (os.Exit in TestMain would skip
// a defer, hence the separate function).
func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_work_planning"),
		tcpostgres.WithUsername("wes"),
		tcpostgres.WithPassword("wes"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	sharedDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "migrations"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve migrations dir: %v\n", err)
		return 1
	}
	if err := postgres.Migrate(sharedDSN, migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		return 1
	}
	return m.Run()
}

// truncateAppTables empties every table in the shared database's public
// schema except golang-migrate's bookkeeping, resetting identities.
func truncateAppTables(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, sharedDSN)
	if err != nil {
		t.Fatalf("connect for truncate: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx,
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var quoted []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatalf("scan table name: %v", err)
		}
		quoted = append(quoted, pgx.Identifier{name}.Sanitize())
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(quoted) == 0 {
		t.Fatal("no application tables found: the shared database was not migrated")
	}
	if _, err := conn.Exec(ctx, "TRUNCATE "+strings.Join(quoted, ", ")+" RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// freshDatabase creates an empty, unmigrated database inside the shared
// container and returns its DSN. It is dropped (connections forced closed)
// when the test ends.
func freshDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("fresh_%d", freshDatabaseSeq.Add(1))

	admin, err := pgx.Connect(ctx, sharedDSN)
	if err != nil {
		t.Fatalf("connect for create database: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), sharedDSN)
		if err != nil {
			t.Logf("connect for drop database: %v", err)
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		if _, err := c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)"); err != nil {
			t.Logf("drop database %s: %v", name, err)
		}
	})

	u, err := url.Parse(sharedDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}
