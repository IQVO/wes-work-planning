//go:build integration

package analyticsstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/wes-work-planning/internal/analytics/report"
)

// These tests own their database: one throwaway testcontainers Postgres
// per package run (TestMain), with the real analytics migrations applied.
// They never read ANALYTICS_DATABASE_URL and never skip — a broken
// projection must fail CI, not silently pass. Each test uses unique ids or
// truncates what it asserts on, so they stay isolated on the shared
// container.

// analyticsURL is the shared, already-migrated container's DSN, set by
// TestMain before any test runs.
var analyticsURL string

func TestMain(m *testing.M) {
	os.Exit(runWithPostgres(m))
}

// runWithPostgres boots the container, migrates it, runs the package's
// tests, and always terminates the container (os.Exit in TestMain would
// skip a defer, hence the separate function).
func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_analytics"),
		tcpostgres.WithUsername("wes"),
		tcpostgres.WithPassword("wes"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start analytics postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate analytics postgres container: %v\n", err)
		}
	}()

	analyticsURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "analytics postgres connection string: %v\n", err)
		return 1
	}
	if err := analyticsstore.Migrate(analyticsURL, "../../../../migrations/analytics"); err != nil {
		fmt.Fprintf(os.Stderr, "migrate analytics: %v\n", err)
		return 1
	}
	return m.Run()
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	pool, err := analyticsstore.NewPool(context.Background(), analyticsURL)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	pathId := "int-path-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM throughput_rollup WHERE path_id = $1`, pathId)
	})

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// Apply the same events twice with the same event ids: idempotent.
	apply := func() {
		must(proj.ApplyWorkReleased(ctx, "int-wr", pathId, base))
		must(proj.ApplyWorkUnitCompleted(ctx, "int-wc", pathId, base.Add(time.Minute)))
		must(proj.ApplyBacklogThresholdBreached(ctx, "int-bt", pathId, base.Add(2*time.Minute)))
		must(proj.ApplyPathThrottled(ctx, "int-pt", pathId, base.Add(3*time.Minute)))
		must(proj.ApplyRateDeviationDetected(ctx, "int-rd", pathId, base.Add(4*time.Minute)))
	}
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		PathId:      pathId,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.WorkReleased != 1 || row.WorkUnitCompleted != 1 || row.BacklogThresholdBreached != 1 ||
		row.PathThrottled != 1 || row.RateDeviationDetected != 1 {
		t.Errorf("counters not idempotent: %+v", row)
	}

	lag, err := rdr.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag < 0 {
		t.Errorf("lag = %v, want >= 0", lag)
	}
}

// TestReadOnlyPool_RejectsWrites asserts the reader pool is genuinely
// read-only: an attempt to write through it must be rejected by Postgres.
func TestReadOnlyPool_RejectsWrites(t *testing.T) {
	roPool, err := analyticsstore.NewReadOnlyPool(context.Background(), analyticsURL)
	if err != nil {
		t.Fatalf("NewReadOnlyPool: %v", err)
	}
	t.Cleanup(roPool.Close)

	ctx := context.Background()
	_, err = roPool.Exec(ctx,
		`INSERT INTO throughput_rollup (path_id, hour_bucket) VALUES ($1, $2)`,
		"ro-path", time.Now().UTC().Truncate(time.Hour))
	if err == nil {
		t.Fatal("expected read-only pool to reject INSERT, but it succeeded")
	}

	// The read side still works over the same read-only pool.
	rdr := analyticsstore.NewPostgresReport(roPool)
	if _, err := rdr.FreshnessLag(ctx); err != nil {
		t.Fatalf("FreshnessLag over read-only pool: %v", err)
	}
}

// TestFreshnessLag_EmptyStore covers the NULL path: max(occurred_at) over an
// empty table returns a single NULL row (not zero rows), which must be read as
// a zero lag rather than a scan error.
func TestFreshnessLag_EmptyStore(t *testing.T) {
	pool, err := analyticsstore.NewPool(context.Background(), analyticsURL)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	// Ensure the processed-events table is empty so max() yields NULL.
	if _, err := pool.Exec(ctx, `TRUNCATE analytics_processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	lag, err := analyticsstore.NewPostgresReport(pool).FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag on empty store: %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty-store lag = %v, want 0", lag)
	}
}
