//go:build integration

// Integration tests for the A2A inbound adapter over the REAL JSON-RPC 2.0
// HTTP transport: the executor mounted behind Handler on an httptest.Server,
// driven by the SDK's own client (a2aclient.NewFromCard + SendMessage), with
// the REAL Postgres-backed use cases behind it — exactly the deployment shape
// Kong exposes (ADR-0046: JSON-RPC over HTTP only). This proves the wire
// contract (card discovery, skill dispatch, envelope answers) end-to-end, not
// the skill handlers in isolation.
//
// Postgres comes from testcontainers: one container per package run, one
// private database per test, migrated once. Never an external DATABASE_URL,
// never t.Skip.
package a2a_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	a2aadapter "github.com/claudioed/wes-work-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	usecases "github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See the
// MCP adapter's integration suite for the same pattern's rationale.
const templateDB = "a2a_migrated_template"

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
		tcpostgres.WithDatabase("wes_a2a"),
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
	name := fmt.Sprintf("a2a_%d", dbSeq.Add(1))
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

// a2aHarness wires the REAL production stack — Postgres repos, UnitOfWork,
// use cases, executor, Handler — serves it over HTTP, and drives it with the
// SDK's own client built from a card that points at the test server, exactly
// as a fleet agent client would after card discovery.
type a2aHarness struct {
	server *httptest.Server
	client *a2aclient.Client
}

// newA2AHarness seeds one work unit on a fresh private database and serves
// the real A2A stack over JSON-RPC 2.0 on HTTP.
func newA2AHarness(t *testing.T) *a2aHarness {
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
	pathId, err := shared.NewPathId("itcov-a2a-path")
	if err != nil {
		t.Fatalf("path id: %v", err)
	}
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "ITCOV-A2A-1", PathId: pathId,
		CPT:       shared.NewCPT(time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC)),
		Reference: "ORD-A2A-1",
	}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}

	deps := a2aadapter.Deps{
		SampleBacklog: usecases.NewSampleBacklog(
			postgres.NewWorkPoolRepo(pool), publisher, clock).WithUnitOfWork(uow),
		RebalanceDecision: usecases.NewRebalanceDecision(
			postgres.NewWorkPoolRepo(pool), publisher, clock).WithUnitOfWork(uow),
		ReleaseNextWork: usecases.NewReleaseNextWork(
			postgres.NewWorkPoolRepo(pool), postgres.NewWorkUnitRepo(pool),
			publisher, clock).WithUnitOfWork(uow),
	}

	hs := httptest.NewServer(a2aadapter.Handler(a2aadapter.NewExecutor(deps)))
	t.Cleanup(hs.Close)

	// The client is built from a card whose interface URL points at the test
	// server — the same shape as post-discovery client construction in the
	// fleet (fulfillment-execution reading wes-work-planning's card).
	card := a2aadapter.Card(a2aadapter.Config{AgentURL: hs.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(ctx, card)
	if err != nil {
		t.Fatalf("a2a client from card: %v", err)
	}

	return &a2aHarness{server: hs, client: client}
}

// fixedClock is the ports.Clock the use cases accept.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// send drives one skill request through the SDK client and decodes the reply
// envelope from the agent Message's data part.
func (h *a2aHarness) send(t *testing.T, skill, pathId string) (envelope, *a2a.Message) {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{
		"skill":     skill,
		"arguments": map[string]any{"pathId": pathId},
	}))
	resp, err := h.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("SendMessage(%s): %v", skill, err)
	}
	agentMsg, ok := resp.(*a2a.Message)
	if !ok {
		t.Fatalf("SendMessage(%s) result is %T, want a message answer (message-only agent)", skill, resp)
	}
	var env envelope
	if len(agentMsg.Parts) == 0 {
		t.Fatalf("reply carries no parts: %+v", agentMsg)
	}
	data, ok := agentMsg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want a2a.Data", agentMsg.Parts[0].Content)
	}
	raw, err := json.Marshal(data.Value)
	if err != nil {
		t.Fatalf("marshal data part: %v", err)
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v\n%s", err, raw)
	}
	return env, agentMsg
}

// envelope is the mirrored wire contract of the adapter's answer envelope
// (test-local copy so the integration suite fails on contract drift).
type envelope struct {
	Skill string `json:"skill"`
	Data  struct {
		FlowStatus *struct {
			PathId    string `json:"pathId"`
			Telemetry struct {
				BacklogDepth int    `json:"backlogDepth"`
				WIP          int    `json:"wip"`
				Mode         string `json:"mode"`
			} `json:"telemetry"`
			Recommendation struct {
				Action string `json:"action"`
			} `json:"recommendation"`
		} `json:"flowStatus"`
		ReleasedWork *struct {
			WorkUnitId string `json:"workUnitId"`
			Ref        string `json:"ref"`
		} `json:"releasedWork"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	} `json:"problem"`
}

func TestA2A_FlowStatusRoundTripThroughPostgres(t *testing.T) {
	h := newA2AHarness(t)

	env, msg := h.send(t, "flow_status", "itcov-a2a-path")

	if env.Problem != nil {
		t.Fatalf("flow_status answered a problem: %+v", env.Problem)
	}
	fs := env.Data.FlowStatus
	if fs == nil {
		t.Fatal("envelope carries no flowStatus")
	}
	if fs.PathId != "itcov-a2a-path" {
		t.Errorf("pathId = %q, want itcov-a2a-path", fs.PathId)
	}
	if fs.Telemetry.BacklogDepth != 1 {
		t.Errorf("backlogDepth = %d, want 1", fs.Telemetry.BacklogDepth)
	}
	if fs.Recommendation.Action == "" {
		t.Error("recommendation action is empty")
	}
	if msg.ContextID == "" {
		t.Error("reply carries no contextId (client grouping broken)")
	}
}

func TestA2A_ReleaseNextWorkRoundTripThroughPostgres(t *testing.T) {
	h := newA2AHarness(t)

	env, _ := h.send(t, "release_next_work", "itcov-a2a-path")

	if env.Problem != nil {
		t.Fatalf("release_next_work answered a problem: %+v", env.Problem)
	}
	rw := env.Data.ReleasedWork
	if rw == nil {
		t.Fatal("envelope carries no releasedWork")
	}
	if rw.WorkUnitId != "ITCOV-A2A-1" {
		t.Errorf("workUnitId = %q, want ITCOV-A2A-1", rw.WorkUnitId)
	}
	if rw.Ref != "ORD-A2A-1" {
		t.Errorf("ref = %q, want ORD-A2A-1", rw.Ref)
	}

	// Second release on the now-empty pool must answer the business rule.
	second, _ := h.send(t, "release_next_work", "itcov-a2a-path")
	if second.Problem == nil || second.Problem.Status != 409 {
		t.Fatalf("second release must answer a 409 problem, got %+v", second.Problem)
	}
}

func TestA2A_UnknownSkillAnswersProblemNotProtocolError(t *testing.T) {
	h := newA2AHarness(t)

	env, _ := h.send(t, "create_wave", "itcov-a2a-path")

	if env.Problem == nil || env.Problem.Status != 400 {
		t.Fatalf("unknown skill must answer a 400 problem, got %+v", env.Problem)
	}
}
