//go:build integration

package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// startPostgres boots a throwaway Postgres via testcontainers and returns
// its DSN. It never reads DATABASE_URL and never skips.
func startPostgres(t *testing.T) string {
	t.Helper()
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

// TestMCPReleaseNextWork_WritesOutboxRows is the regression test for the
// bug where cmd/mcp wired ReleaseNextWork with a log publisher and no
// UnitOfWork: a WorkReleased raised by the release_next_work MCP tool never
// reached the outbox, so fulfillment-execution never saw it. It drives the
// real MCP handler through the real composition-root wiring against a real
// Postgres, and asserts the WorkReleased rows (integration + analytics
// topic) exist, enriched, in outbox_events.
func TestMCPReleaseNextWork_WritesOutboxRows(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(t)
	migrations, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("migrations dir: %v", err)
	}
	if err := postgres.Migrate(dsn, migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	logger := slog.Default()
	repos, err := wireRepositories(logger, dsn)
	if err != nil {
		t.Fatalf("wireRepositories: %v", err)
	}
	t.Cleanup(repos.close)
	if repos.pool == nil || repos.uow == nil {
		t.Fatal("Postgres wiring must provide a pool and a UnitOfWork")
	}

	// The brokers are never dialled: in outbox mode the publisher only
	// ENCODES; the relay (cmd/wes) owns the Kafka writer.
	publisher, stop, err := wireEventPublisher(logger, "kafka", "kafka.invalid:9092", repos, nil)
	if err != nil {
		t.Fatalf("wireEventPublisher: %v", err)
	}
	t.Cleanup(stop)

	// Seed one pending work unit exactly as the REST/Kafka path would.
	clock := memory.SystemClock{}
	pathID, err := shared.NewPathId("pick-mcp")
	if err != nil {
		t.Fatalf("path id: %v", err)
	}
	enqueue := usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(repos.uow)
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-mcp-1", PathId: pathID, CPT: shared.NewCPT(time.Now().Add(time.Hour)), Reference: "order-mcp", SKU: "sku-1", GiftWrap: true,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := httptest.NewServer(newRouter(inboundmcp.Handler(inboundmcp.NewServer(newMCPServerDeps(logger, repos, publisher))), "wes-work-planning-mcp-test"))
	t.Cleanup(srv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "it", Version: "0.0.1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{}}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "release_next_work", Arguments: map[string]any{"pathId": "pick-mcp"}})
	if err != nil {
		t.Fatalf("call release_next_work: %v", err)
	}
	if res.IsError {
		t.Fatalf("release_next_work returned a tool error: %+v", res.Content)
	}

	const workReleased = "com.warehouse.wes.work-planning.workunit.WorkReleased"
	for _, topic := range []string{cloudevents.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic} {
		var n int
		if err := repos.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE topic = $1 AND event_type = $2 AND published_at IS NULL`,
			topic, workReleased).Scan(&n); err != nil {
			t.Fatalf("count outbox rows on %s: %v", topic, err)
		}
		if n != 1 {
			t.Fatalf("expected exactly 1 pending %s row on %s, got %d", workReleased, topic, n)
		}
	}

	// The integration row carries the enrichment read inside the
	// transaction (ref/cpt/gift_wrap), proving the UnitOfWork was bound.
	var value string
	if err := repos.pool.QueryRow(ctx,
		`SELECT convert_from(value, 'UTF8') FROM outbox_events WHERE topic = $1 AND event_type = $2`,
		cloudevents.TopicWorkPlanningEvents, workReleased).Scan(&value); err != nil {
		t.Fatalf("read WorkReleased row: %v", err)
	}
	for _, want := range []string{`"ref":"order-mcp"`, `"work_unit_id":"wu-mcp-1"`, `"gift_wrap":true`} {
		if !strings.Contains(value, want) {
			t.Fatalf("WorkReleased payload missing %s: %s", want, value)
		}
	}
}
