//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/analyticsstore"
)

// The reader composition root (ADR-0011) driven like deployment does: the
// REAL run() entry point, in-process, over an analytically-migrated EMPTY
// testcontainers Postgres seeded through the repo's own analytics writer
// (analyticsstore.PostgresProjection — the projector's projection), probed
// only over the documented REST surface (docs/docs/analytics/
// release-throughput-report.md, ADR-0038).

// repStartEmptyPostgres boots a throwaway, EMPTY Postgres via testcontainers
// and returns its DSN. It never reads ANALYTICS_DATABASE_URL and never skips.
func repStartEmptyPostgres(t *testing.T) string {
	t.Helper()
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
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

// repFreeHTTPAddr reserves an ephemeral loopback address for the reports
// server.
func repFreeHTTPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// repGetJSON issues a GET and decodes a JSON body; a transport error returns
// status 0 (the service may still be starting).
func repGetJSON(t *testing.T, url string, into any) int {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx // one-shot probe against the local test server
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	_ = json.NewDecoder(resp.Body).Decode(into)
	return resp.StatusCode
}

// repWaitUntil polls cond until it holds or the timeout expires, failing fast
// if run() has already returned (a boot failure would otherwise surface as an
// opaque timeout instead of the actual error).
func repWaitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("run() returned early while waiting for %s: %v", what, err)
		default:
		}
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// TestReportsServeThroughputAndFreshnessAndDrainCleanly proves, through the
// real composition root over a seeded read model: (1) run() serves /healthz
// without owning the schema (the writer applied the analytics migrations);
// (2) GET /reports/throughput answers the documented row shape with [from,to)
// hour-bucket windowing and the exact counters the writer seeded, and a
// bucket outside the window is excluded; (3) GET /reports/throughput/
// freshness answers now minus the most recently applied event's occurred_at;
// (4) a SIGTERM drains to a clean nil-error exit.
func TestReportsServeThroughputAndFreshnessAndDrainCleanly(t *testing.T) {
	ctx := context.Background()
	dsn := repStartEmptyPostgres(t)

	// The reader never migrates: the writer (cmd/wes-projector) owns the
	// analytical schema (ADR-0011), so the database arrives analytically
	// migrated here, exactly as the projector leaves it.
	migrations, err := filepath.Abs(filepath.Join("..", "..", "migrations", "analytics"))
	if err != nil {
		t.Fatalf("analytics migrations dir: %v", err)
	}
	if err := analyticsstore.Migrate(dsn, migrations); err != nil {
		t.Fatalf("migrate analytics: %v", err)
	}

	// Seed through the repo's OWN analytics writer: the projector's
	// PostgresProjection. prevHour is a deterministic, always-past hour
	// bucket; at sits half an hour inside it.
	prevHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	at := prevHour.Add(30 * time.Minute)
	writer, err := analyticsstore.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	projection := analyticsstore.NewPostgresProjection(writer)
	seeds := []func() error{
		func() error { return projection.ApplyWorkReleased(ctx, "reports-itest-1", "pick-zone-a", at) },
		func() error {
			return projection.ApplyWorkReleased(ctx, "reports-itest-2", "pick-zone-a", at.Add(time.Minute))
		},
		func() error {
			return projection.ApplyWorkUnitCompleted(ctx, "reports-itest-3", "pick-zone-a", at.Add(2*time.Minute))
		},
		func() error {
			return projection.ApplyBacklogThresholdBreached(ctx, "reports-itest-4", "pick-zone-a", at.Add(3*time.Minute))
		},
		// Lands in the PREVIOUS hour bucket: outside the [from,to) window
		// below, and old enough that the newest applied event stays
		// deterministically in the past for the freshness assertion.
		func() error {
			return projection.ApplyWorkReleased(ctx, "reports-itest-5", "pick-zone-a", prevHour.Add(-30*time.Minute))
		},
	}
	for _, seed := range seeds {
		if err := seed(); err != nil {
			t.Fatalf("seed read model: %v", err)
		}
	}

	// The test's own handler goes up BEFORE run() starts, so the SIGTERM
	// below can never fall through to the default disposition.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)

	addr := repFreeHTTPAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", dsn)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from dialling at
	// shutdown, exactly like cmd/wes/e2e_integration_test.go.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	done := make(chan error, 1)
	go func() { done <- run() }()

	base := "http://" + addr
	repWaitUntil(t, 30*time.Second, "/healthz = 200", func() bool {
		var health struct {
			Status string `json:"status"`
		}
		return repGetJSON(t, base+"/healthz", &health) == http.StatusOK && health.Status == "ok"
	}, done)

	type row struct {
		PathId                   string `json:"pathId"`
		HourBucket               string `json:"hourBucket"`
		WorkReleased             int    `json:"workReleased"`
		WorkUnitCompleted        int    `json:"workUnitCompleted"`
		BacklogThresholdBreached int    `json:"backlogThresholdBreached"`
		PathThrottled            int    `json:"pathThrottled"`
		RateDeviationDetected    int    `json:"rateDeviationDetected"`
	}
	var report struct {
		Rows []row `json:"rows"`
	}
	url := base + "/reports/throughput?from=" + prevHour.Format(time.RFC3339) +
		"&to=" + prevHour.Add(time.Hour).Format(time.RFC3339)
	if code := repGetJSON(t, url, &report); code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", url, code)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("throughput rows = %v, want exactly the pick-zone-a hour (the previous-hour bucket must be excluded by [from,to))", report.Rows)
	}
	got := report.Rows[0]
	if got.PathId != "pick-zone-a" || got.HourBucket != prevHour.Format(time.RFC3339) ||
		got.WorkReleased != 2 || got.WorkUnitCompleted != 1 ||
		got.BacklogThresholdBreached != 1 || got.PathThrottled != 0 || got.RateDeviationDetected != 0 {
		t.Fatalf("throughput row = %+v, want pick-zone-a @ %s with counters (2, 1, 1, 0, 0)",
			got, prevHour.Format(time.RFC3339))
	}

	// lagSeconds = now − occurred_at of the most recently applied event; the
	// newest seed is at least half an hour old, so the answer must be too.
	var freshness struct {
		LagSeconds float64 `json:"lagSeconds"`
	}
	if code := repGetJSON(t, base+"/reports/throughput/freshness", &freshness); code != http.StatusOK {
		t.Fatalf("GET freshness = %d, want 200", code)
	}
	if freshness.LagSeconds < 1500 {
		t.Fatalf("lagSeconds = %f, want >= 1500 (the newest applied event is over half an hour old)", freshness.LagSeconds)
	}

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to self: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a SIGTERM'd reports server must drain cleanly, got %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("the reports server did not stop after SIGTERM")
	}
}
