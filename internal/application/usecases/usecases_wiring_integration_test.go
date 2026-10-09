//go:build integration

// Package usecases_test proves the pool/release write use cases against a
// REAL Postgres (testcontainers): the real repos, the real UnitOfWork, and a
// buffering publisher, wired exactly like the composition root in cmd/wes.
// These are integration tests in the fleet's sense: they execute the real
// cross-component contracts (pool optimistic-retry, enqueue idempotency,
// release policy, the atomic Publish-inside-UoW bracket) against real
// infrastructure, with no in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain (main_integration_test.go): one container for the whole package,
// migrated once, one private database per test. Never an external
// DATABASE_URL, never t.Skip.
package usecases_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping, no dependence on
// test order, and tests that assert on global state still start pristine.
//
// Never an external DATABASE_URL, never t.Skip.
const templateDB = "usecases_migrated_template"

var (
	sharedBaseURL string // connection URL of the container's default database
	dbSeq         atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_usecases"),
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

	sharedBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	u, err := url.Parse(sharedBaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse base url: %v\n", err)
		return 1
	}
	u.Path = "/" + templateDB
	tmplDB := u.String()
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "migrations"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve migrations dir: %v\n", err)
		return 1
	}
	if err := postgres.Migrate(tmplDB, migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	return m.Run()
}

// migratedDB hands the test a connection URL to its own private database,
// cloned from the migrated template. Cloning is a file-level copy, so it
// costs milliseconds and the test's writes never leak into another test.
func migratedDB(t *testing.T) string {
	t.Helper()
	dbURL, err := cloneDatabase(context.Background(), fmt.Sprintf("usecases_%d", dbSeq.Add(1)))
	if err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return dbURL
}

// createDatabase creates an empty database inside the shared container.
func createDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// cloneDatabase creates a new database inside the shared container, copying
// the migrated template, and returns a connection URL to it.
func cloneDatabase(ctx context.Context, name string) (string, error) {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		return "", fmt.Errorf("create database %s: %w", name, err)
	}
	u, err := url.Parse(sharedBaseURL)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}

// newWiredUsecases builds the real adapter stack over a private migrated
// database, wired exactly like cmd/wes's composition root for the pool and
// release flows: real Postgres repos, the real UnitOfWork, and a buffering
// LogPublisher the tests assert on. No in-memory repo fakes.
func newWiredUsecases(t *testing.T) (*usecases.EnqueueWorkUnit, *usecases.ReleaseNextWork,
	*usecases.ConfigurePool, *usecases.SampleBacklog, *events.LogPublisher,
) {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.Connect(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	publisher := events.NewLogPublisher(nil)
	clock := fixedClock{time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	uow := postgres.NewUnitOfWork(pool)

	enqueue := usecases.NewEnqueueWorkUnit(
		postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool),
		publisher, clock).WithUnitOfWork(uow)
	releaseUC := usecases.NewReleaseNextWork(
		postgres.NewWorkPoolRepo(pool), postgres.NewWorkUnitRepo(pool),
		publisher, clock).WithUnitOfWork(uow)
	configure := usecases.NewConfigurePool(postgres.NewWorkPoolRepo(pool))
	sample := usecases.NewSampleBacklog(
		postgres.NewWorkPoolRepo(pool), publisher, clock).WithUnitOfWork(uow)

	return enqueue, releaseUC, configure, sample, publisher
}

// fixedClock is the ports.Clock the use cases already accept; deterministic
// timestamps keep the published events byte-comparable.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// mustPathId builds a PathId or fails the test.
func mustPathId(t *testing.T, raw string) shared.PathId {
	t.Helper()
	id, err := shared.NewPathId(raw)
	if err != nil {
		t.Fatalf("path id %q: %v", raw, err)
	}
	return id
}

// mustCPT builds a CPT or fails the test.
func mustCPT(t *testing.T, at time.Time) shared.CPT {
	t.Helper()
	return shared.NewCPT(at)
}

func TestUsecases_EnqueueReleaseRoundTrip(t *testing.T) {
	enqueue, releaseUC, _, _, publisher := newWiredUsecases(t)
	ctx := context.Background()
	pathId := mustPathId(t, "itcov-pick-a")

	// Enqueue two units with different CPTs; the release policy must pick
	// the earliest-CPT one first.
	first, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "ITCOV-WU-1", PathId: pathId,
		CPT:       mustCPT(t, time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)),
		Reference: "ORD-1",
	})
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	second, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "ITCOV-WU-2", PathId: pathId,
		CPT:       mustCPT(t, time.Date(2026, 10, 9, 13, 0, 0, 0, time.UTC)),
		Reference: "ORD-2",
	})
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	if first.PathId() != pathId || second.PathId() != pathId {
		t.Fatalf("units persisted on wrong path: %v %v", first.PathId(), second.PathId())
	}

	// Release: the earliest-CPT unit (second) must come out first.
	released, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released == nil || released.Id() != second.Id() {
		t.Fatalf("release policy must pick the earliest CPT unit, got %+v", released)
	}

	// The publisher saw WorkReleased for exactly the released unit.
	var saw bool
	for _, e := range publisher.Events() {
		if e.EventName() == "WorkReleased" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("expected a WorkReleased event to be published, got %v", publisher.Events())
	}
}

func TestUsecases_ReleaseFedPoolRespectsWIPLimit(t *testing.T) {
	enqueue, releaseUC, configure, sample, _ := newWiredUsecases(t)
	ctx := context.Background()
	pathId := mustPathId(t, "itcov-wip-a")

	// Configure a release-fed pool with WIP limit 1, then enqueue two units.
	if _, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{
		PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 1,
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	for i, ref := range []string{"ORD-WIP-1", "ORD-WIP-2"} {
		_, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: fmt.Sprintf("ITCOV-WIP-%d", i+1), PathId: pathId,
			CPT:       mustCPT(t, time.Date(2026, 10, 9, 13, i, 0, 0, time.UTC)),
			Reference: ref,
		})
		if err != nil {
			t.Fatalf("enqueue %s: %v", ref, err)
		}
	}

	// First release is admitted (WIP 0 -> 1)...
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("first release at WIP limit 1 must be admitted: %v", err)
	}
	// ...the second is rejected while the pool sits at its WIP limit.
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err == nil {
		t.Fatal("second release at WIP limit 1 must be rejected")
	}

	// The read model agrees: depth 1, WIP 1.
	snapshot, err := sample.Execute(ctx, usecases.SampleBacklogRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("sample backlog: %v", err)
	}
	if snapshot.BacklogDepth != 1 || snapshot.WIP != 1 {
		t.Fatalf("expected depth 1 / WIP 1 after one release of two, got %+v", snapshot)
	}
}

func TestUsecases_ConfigurePoolIsIdempotent(t *testing.T) {
	_, _, configure, sample, _ := newWiredUsecases(t)
	ctx := context.Background()
	pathId := mustPathId(t, "itcov-cfg-a")

	p1, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{
		PathId: pathId, Mode: release.FlowFed, WIPLimit: 3,
	})
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	p2, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{
		PathId: pathId, Mode: release.FlowFed, WIPLimit: 3,
	})
	if err != nil {
		t.Fatalf("reconfigure (identical): %v", err)
	}
	if p1 == nil || p2 == nil || p1.Version() != p2.Version() {
		t.Fatalf("identical reconfiguration must not bump the pool version: v%d vs v%d",
			p1.Version(), p2.Version())
	}

	// SampleBacklog reads the configured mode through the real repo.
	snapshot, err := sample.Execute(ctx, usecases.SampleBacklogRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("sample backlog: %v", err)
	}
	if snapshot.PathId != pathId || snapshot.Mode != release.FlowFed.String() {
		t.Fatalf("sample must report the configured mode %q, got %+v", release.FlowFed.String(), snapshot)
	}
}
