//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server, driven
// by the SDK's own client (mcp.NewClient + StreamableClientTransport), with
// the REAL Postgres-backed use cases behind it — exactly the deployment
// shape Kong exposes (ADR-0008: Streamable HTTP only). This proves the wire
// contract (initialize, tools/list, tools/call) end-to-end, not the tool
// handlers in isolation.
//
// Postgres comes from testcontainers (main_integration_test.go in this
// package): one container per package run, one private database per test,
// migrated once. Never an external DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	mcpadapter "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	usecases "github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for the
// same pattern's rationale. Never an external DATABASE_URL, never t.Skip.
const templateDB = "mcp_migrated_template"

var (
	sharedBaseURL string
	dbSeq         atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_mcp"),
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

	var err2 error
	sharedBaseURL, err2 = container.ConnectionString(ctx, "sslmode=disable")
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err2)
		return 1
	}
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	tmplDB := withDB(sharedBaseURL, templateDB)
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "migrations"))
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

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
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

// migratedDB hands the test a connection URL to its own private database
// cloned from the migrated template.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_%d", dbSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), sharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return withDB(sharedBaseURL, name)
}

// mcpHarness wires the REAL production stack — Postgres repos, UnitOfWork,
// use cases, mcp.NewServer, mcp.Handler — and serves it over HTTP. It
// returns a connected SDK client session; the test drives tools/list and
// tools/call exactly like a model host would.
type mcpHarness struct {
	server    *httptest.Server
	session   *sdkmcp.ClientSession
	publisher *events.LogPublisher
}

// newMCPHarness seeds one work unit on a fresh private database and serves
// the real MCP stack over Streamable HTTP.
func newMCPHarness(t *testing.T) *mcpHarness {
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

	// Seed: enqueue one unit so release_next_work has real work to admit.
	enqueue := usecases.NewEnqueueWorkUnit(
		postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool),
		publisher, clock).WithUnitOfWork(uow)
	pathId, err := shared.NewPathId("itcov-mcp-path")
	if err != nil {
		t.Fatalf("path id: %v", err)
	}
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "ITCOV-MCP-1", PathId: pathId,
		CPT:       shared.NewCPT(time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC)),
		Reference: "ORD-MCP-1",
	}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}

	releaseUC := usecases.NewReleaseNextWork(
		postgres.NewWorkPoolRepo(pool), postgres.NewWorkUnitRepo(pool),
		publisher, clock).WithUnitOfWork(uow)
	sample := usecases.NewSampleBacklog(
		postgres.NewWorkPoolRepo(pool), publisher, clock).WithUnitOfWork(uow)
	rebalance := usecases.NewRebalanceDecision(
		postgres.NewWorkPoolRepo(pool), publisher, clock)

	server := mcpadapter.NewServer(mcpadapter.Deps{
		SampleBacklog:     sample,
		RebalanceDecision: rebalance,
		ReleaseNextWork:   releaseUC,
	})

	hs := httptest.NewServer(mcpadapter.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{server: hs, session: session, publisher: publisher}
}

// fixedClock is the ports.Clock the use cases accept.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestMCP_ListToolsExposesTheContract(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	list, err := h.session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"get_backlog_telemetry", "get_rebalance_recommendation", "release_next_work"} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
	// The write tool must be annotated non-read-only so a host can gate it.
	for _, tool := range list.Tools {
		if tool.Name == "release_next_work" {
			if tool.Annotations == nil || tool.Annotations.ReadOnlyHint {
				t.Fatal("release_next_work must carry ReadOnlyHint=false")
			}
		}
	}
}

func TestMCP_CallToolRoundTripThroughPostgres(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	// Read tool: backlog telemetry over the real seeded pool.
	telemetry, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_backlog_telemetry",
		Arguments: map[string]any{"pathId": "itcov-mcp-path"},
	})
	if err != nil {
		t.Fatalf("tools/call get_backlog_telemetry: %v", err)
	}
	if telemetry.IsError {
		t.Fatalf("get_backlog_telemetry returned a tool error: %+v", telemetry)
	}

	// Write tool: release the seeded unit through the real use case stack.
	release, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "release_next_work",
		Arguments: map[string]any{"pathId": "itcov-mcp-path"},
	})
	if err != nil {
		t.Fatalf("tools/call release_next_work: %v", err)
	}
	if release.IsError {
		t.Fatalf("release_next_work returned a tool error: %+v", release)
	}

	// The release really persisted: the publisher saw WorkReleased.
	var saw bool
	for _, e := range h.publisher.Events() {
		if e.EventName() == "WorkReleased" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("release_next_work must publish WorkReleased through the real stack")
	}

	// The pool is now empty: a second release is a domain rejection, and
	// the adapter must surface it as a tool error (not a transport error).
	second, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "release_next_work",
		Arguments: map[string]any{"pathId": "itcov-mcp-path"},
	})
	if err != nil {
		t.Fatalf("tools/call release_next_work (empty pool): %v", err)
	}
	if !second.IsError {
		t.Fatal("release on an empty pool must surface a tool error, not success")
	}
}

func TestMCP_CallToolRejectsInvalidInput(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	// Bad path id shape: the adapter must answer a tool error carrying the
	// problem slug, never a transport-level failure.
	res, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_backlog_telemetry",
		Arguments: map[string]any{"pathId": ""},
	})
	if err != nil {
		t.Fatalf("tools/call with invalid input: %v", err)
	}
	if !res.IsError {
		t.Fatal("empty pathId must surface a tool error")
	}
}
