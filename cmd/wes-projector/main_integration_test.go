//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// The writer composition root (ADR-0011) driven like deployment does: the REAL
// run() entry point, in-process, against an EMPTY testcontainers Postgres (the
// projector applies its own analytics migrations at boot) and a real
// testcontainers Kafka. The events on warehouse.wes.analytics are produced by
// this repo's own AnalyticsPublisher encoder, and the assertions are on the
// documented projection (docs/docs/analytics/release-throughput-report.md).

// projHour is the UTC hour every projecting event falls into, so the expected
// rollup is one row per path at hourBucket projHour.
var projHour = time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)

// startEmptyPostgres boots a throwaway, EMPTY Postgres via testcontainers and
// returns its DSN. No OLTP or analytics schema: applying migrations is part of
// what run() itself must do. It never reads ANALYTICS_DATABASE_URL and never
// skips.
func startEmptyPostgres(t *testing.T) string {
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

// startKafkaBroker boots a throwaway Kafka via testcontainers and returns its
// brokers, mirroring cmd/wes/e2e_integration_test.go's recipe.
func startKafkaBroker(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-projector-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate kafka container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return brokers
}

// createAnalyticsTopic pre-creates the projector's input topic with a
// partition leader: a kafka-go group consumer that starts before its topic
// exists does not pick the topic up when it appears (same finding as
// cmd/wes/e2e_integration_test.go).
func createAnalyticsTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		parts, err := conn.ReadPartitions(topic)
		if err == nil && len(parts) == 1 && parts[0].Leader.Host != "" {
			return
		}
	}
	t.Fatalf("timed out waiting for topic %s to have a leader", topic)
}

// freeAdminAddr reserves an ephemeral loopback address for the admin server.
func freeAdminAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// waitFor polls cond until it holds or the timeout expires, failing fast if
// run() has already returned (a boot failure would otherwise surface as an
// opaque 90s timeout instead of the actual error).
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool, done <-chan error) {
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
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// sendSIGTERM delivers SIGTERM to this own process so run()'s signal.Notify
// unblocks. The test registers its own handler FIRST (in the test body), so
// the signal never reaches the process's default terminate disposition; the
// handler is deliberately never stopped with signal.Stop before process exit.
func sendSIGTERM(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to self: %v", err)
	}
}

// TestProjectorRunMigratesProjectsAndDrainsCleanly proves, through the real
// composition root: (1) run() applies the analytics migrations to an EMPTY
// database itself and serves /healthz; (2) the consumer replays the topic from
// the beginning (fresh group, StartOffset FirstOffset) and projects the five
// throughput-moving events the repo's own encoder published, counting each
// into its documented rollup column, while a published ShiftPlanCommitted is
// acknowledged WITHOUT being projected or marked processed; (3) a SIGTERM
// drains to a clean nil-error exit.
func TestProjectorRunMigratesProjectsAndDrainsCleanly(t *testing.T) {
	ctx := context.Background()
	dsn := startEmptyPostgres(t)
	brokers := startKafkaBroker(t)
	createAnalyticsTopic(t, ctx, brokers[0], outboundkafka.AnalyticsTopic)

	// Produce with the repo's OWN analytics encoder BEFORE boot: a fresh
	// consumer group must still see every event (docs: "Fresh group reads full
	// history").
	pick, err := shared.NewPathId("pick-zone-a")
	if err != nil {
		t.Fatal(err)
	}
	pack, err := shared.NewPathId("pack-station-3")
	if err != nil {
		t.Fatal(err)
	}
	rate, err := shared.NewPathId("rate-sensor-path")
	if err != nil {
		t.Fatal(err)
	}
	var ids atomic.Int64
	publisher := outboundkafka.NewAnalyticsPublisher(brokers, func() string {
		return fmt.Sprintf("projector-itest-%d", ids.Add(1))
	})
	t.Cleanup(func() { _ = publisher.Close() })
	err = publisher.Publish(ctx,
		shared.NewWorkReleased("WU-PROJECTOR-1", pick, projHour.Add(5*time.Minute)),
		shared.NewWorkUnitCompleted("WU-PROJECTOR-1", pick, projHour.Add(20*time.Minute)),
		shared.NewBacklogThresholdBreached(pack, projHour.Add(10*time.Minute)),
		shared.NewPathThrottled(pack, projHour.Add(40*time.Minute)),
		shared.NewRateDeviationDetected(rate, projHour.Add(45*time.Minute)),
		// Published to the analytics topic but outside the projection
		// contract: the projector must acknowledge it without projecting.
		shared.NewShiftPlanCommitted(pick, projHour.Add(50*time.Minute)),
	)
	if err != nil {
		t.Fatalf("publish analytics events: %v", err)
	}

	// The test's own handler goes up BEFORE run() starts, so the SIGTERM
	// below can never fall through to the default disposition.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)

	addr := freeAdminAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", dsn)
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", "../../migrations/analytics")
	t.Setenv("KAFKA_BROKERS", brokers[0])
	t.Setenv("ADMIN_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from dialling at
	// shutdown, exactly like cmd/wes/e2e_integration_test.go.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	done := make(chan error, 1)
	go func() { done <- run() }()

	waitFor(t, 60*time.Second, "admin /healthz = 200", func() bool {
		resp, err := http.Get("http://" + addr + "/healthz") //nolint:noctx // poll loop with its own deadline
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, done)

	// Assertions go through the reader's read-only pool: the test cannot
	// mutate the read model either.
	pool, err := analyticsstore.NewReadOnlyPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	rollup := func(path string) (released, completed, backlog, throttled, rateDeviation int) {
		_ = pool.QueryRow(ctx,
			`SELECT work_released, work_unit_completed, backlog_threshold_breached,
				path_throttled, rate_deviation_detected
			 FROM throughput_rollup WHERE path_id = $1 AND hour_bucket = $2`,
			path, projHour).Scan(&released, &completed, &backlog, &throttled, &rateDeviation)
		return
	}
	waitFor(t, 90*time.Second, "the five projecting events to land in throughput_rollup", func() bool {
		r1, c1, _, _, _ := rollup("pick-zone-a")
		_, _, b2, t2, _ := rollup("pack-station-3")
		_, _, _, _, d3 := rollup("rate-sensor-path")
		return r1 == 1 && c1 == 1 && b2 == 1 && t2 == 1 && d3 == 1
	}, done)

	if released, completed, backlog, throttled, _ := rollup("pick-zone-a"); released != 1 || completed != 1 || backlog != 0 || throttled != 0 {
		t.Fatalf("pick-zone-a rollup = (%d, %d, %d, %d), want (1, 1, 0, 0)", released, completed, backlog, throttled)
	}
	var processed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM analytics_processed_events`).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	// ShiftPlanCommitted is acknowledged without touching the read model or
	// the processed set, so exactly the five projecting events are claimed.
	if processed != 5 {
		t.Fatalf("analytics_processed_events count = %d, want 5 (ShiftPlanCommitted must not be projected)", processed)
	}

	sendSIGTERM(t)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a SIGTERM'd projector must drain cleanly, got %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("the projector did not stop after SIGTERM")
	}
}
