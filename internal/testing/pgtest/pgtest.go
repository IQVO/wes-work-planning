// Package pgtest boots ONE real Postgres through testcontainers per test
// binary and hands every test its own database, cloned from a template that
// already has the repository migrations applied. It never reads DATABASE_URL
// and never skips: a missing Docker is a test failure (fleet rule,
// TestPostgresIntegrationTestsUseTestcontainers in internal/architecture).
//
// Ported from the customer-ordering fleet sibling's internal/testing/pgtest
// as the minimal subset this repo's cmd-level integration tests need: URL,
// EmptyURL and Shutdown. The one adaptation: this repo applies its
// migrations from a directory (postgres.Migrate(dsn, dir)), not from an
// embedded FS, so the template is migrated from <repo>/migrations.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
)

const (
	user         = "wes"
	password     = "wes"
	adminDB      = "postgres"
	templateName = "wes_itcov_template"
)

var (
	once      sync.Once
	container *tcpostgres.PostgresContainer
	baseURL   string
	errStart  error
	seq       atomic.Int64
)

func start() {
	ctx := context.Background()
	var err error
	container, err = tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(adminDB),
		tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		errStart = err
		return
	}
	if baseURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
		errStart = err
		return
	}
	errStart = buildTemplate(ctx)
}

// migrationsDir resolves the repository's migrations directory from THIS
// file's location (internal/testing/pgtest -> <repo>/migrations). It cannot
// use the working directory: go test runs the test binary with the cwd of
// whichever package imports pgtest, not pgtest's own directory.
func migrationsDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("pgtest: cannot locate its own source file")
	}
	return filepath.Abs(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations"))
}

// buildTemplate creates the template database and applies the migrations to
// it twice: every binary in this repo runs them itself on boot, so a second
// run must be a no-op (idempotency is part of the boot contract).
func buildTemplate(ctx context.Context) error {
	if err := adminExec(ctx, `CREATE DATABASE `+templateName); err != nil {
		return err
	}
	dir, err := migrationsDir()
	if err != nil {
		return err
	}
	for range 2 {
		if err := postgres.Migrate(withDB(baseURL, templateName), dir); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	return nil
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(base, name string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// adminExec runs one statement against the container's admin database.
func adminExec(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return err
}

// URL returns the DSN of a FRESH database (migrations applied) on the shared
// container.
func URL(t *testing.T) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("t_%d", seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name+` TEMPLATE `+templateName); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return withDB(baseURL, name)
}

// EmptyURL returns the DSN of a fresh database WITHOUT migrations (for
// binaries that migrate their database themselves on boot).
func EmptyURL(t *testing.T) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("e_%d", seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return withDB(baseURL, name)
}

// Shutdown terminates the shared container; call it from TestMain.
func Shutdown() {
	if container != nil {
		_ = testcontainers.TerminateContainer(container)
	}
}
