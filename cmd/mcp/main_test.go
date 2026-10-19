package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// newTestRouter wires a real MCP handler (over the in-memory test doubles)
// behind newRouter, exactly as run() does.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	clock := memory.SystemClock{}
	publisher := events.NewLogPublisher(slog.Default())
	deps := inboundmcp.Deps{
		SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
		ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
	}
	server := inboundmcp.NewServer(deps)
	return newRouter(inboundmcp.Handler(server), "wes-work-planning-mcp-test")
}

func TestRouter_HealthzIsUnauthenticated(t *testing.T) {
	router := newTestRouter(t)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if body["status"] != "ok" {
		t.Fatalf("body = %v, want status=ok", body)
	}
}

func TestRouter_MCPMountsReachHandler(t *testing.T) {
	router := newTestRouter(t)
	const initialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"router-test","version":"0.0.1"}}}`
	for _, path := range []string{"/", "/mcp"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(initialize))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("POST %s initialize: status = %d, want 200 (body %q)", path, rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Mcp-Session-Id") == "" {
				t.Fatalf("POST %s initialize: no Mcp-Session-Id header — request did not reach the MCP handler", path)
			}
		})
	}
}

func TestRouter_UnknownPathIsNotHealthz(t *testing.T) {
	router := newTestRouter(t)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope status = %d, want 404", rec.Code)
	}
}

func TestWireEventPublisher_RefusesAnythingButOutboxOverPostgres(t *testing.T) {
	if _, _, err := wireEventPublisher(slog.Default(), "localhost:1", repositories{workUnits: memory.NewWorkUnitRepo()}, nil); !errors.Is(err, errNoDatabase) {
		t.Fatalf("without a pool: err = %v, want errNoDatabase", err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	repos := repositories{workUnits: memory.NewWorkUnitRepo(), pool: pool}
	if _, _, err := wireEventPublisher(slog.Default(), " , ", repos, nil); !errors.Is(err, errNoBrokers) {
		t.Fatalf("without brokers: err = %v, want errNoBrokers", err)
	}
	pub, stop, err := wireEventPublisher(slog.Default(), "localhost:1", repos, nil)
	if err != nil {
		t.Fatalf("wireEventPublisher: %v", err)
	}
	defer stop()
	if _, ok := pub.(*postgres.OutboxPublisher); !ok {
		t.Fatalf("publisher = %T, want *postgres.OutboxPublisher", pub)
	}
}

func TestWireRepositories_RefusesAMissingDatabaseURL(t *testing.T) {
	for _, dsn := range []string{"", "   "} {
		repos, err := wireRepositories(slog.Default(), dsn)
		if !errors.Is(err, errNoDatabase) {
			t.Fatalf("wireRepositories(%q) err = %v, want errNoDatabase", dsn, err)
		}
		repos.close() // safe to call on the refusal path
	}
}

func TestBrokerListTrimsAndDropsBlanks(t *testing.T) {
	got := brokerList(" a:1, ,b:2 ,")
	if len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("brokerList = %v", got)
	}
}

func TestNewEventIDIsUUIDv4(t *testing.T) {
	id := newEventID()
	if len(id) != 36 || id[14] != '4' {
		t.Fatalf("newEventID = %q, want a UUID v4", id)
	}
}

func TestBuildClassificationLookup_KafkaIsTheOnlyMode(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	for _, mode := range []string{"", "kafka", " KAFKA "} {
		lookup, err := buildClassificationLookup(mode, pool, slog.Default())
		if err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
		if _, ok := lookup.(*productclassificationcopy.Store); !ok {
			t.Fatalf("mode %q: lookup = %T, want the Postgres copy store", mode, lookup)
		}
	}
	// No Postgres: the MCP server has no copy to read, so it is a boot error,
	// not a silent permissive lookup.
	if _, err := buildClassificationLookup("kafka", nil, slog.Default()); !errors.Is(err, errNoDatabase) {
		t.Fatalf("kafka without a pool: err = %v, want errNoDatabase", err)
	}
}

func TestBuildClassificationLookup_RejectsAtBoot(t *testing.T) {
	for _, mode := range []string{"permissive", "off", "http", "bogus"} {
		_, err := buildClassificationLookup(mode, nil, slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PRODUCT_CLASSIFICATION_MODE") {
			t.Fatalf("PRODUCT_CLASSIFICATION_MODE=%s: err = %v, want one naming the variable", mode, err)
		}
	}
}

func TestNewMCPServerDeps_WiresEveryUseCase(t *testing.T) {
	t.Setenv("REPORTS_BASE_URL", "http://reports.invalid")
	repos := repositories{pools: memory.NewWorkPoolRepo(), workUnits: memory.NewWorkUnitRepo(), close: func() {}}
	deps := newMCPServerDeps(slog.Default(), repos, events.NewLogPublisher(slog.Default()))
	if deps.SampleBacklog == nil || deps.RebalanceDecision == nil || deps.ReleaseNextWork == nil || deps.Reports == nil {
		t.Fatalf("incomplete deps: %+v", deps)
	}
}

func validEnv(over map[string]string) func(string) string {
	env := map[string]string{
		"DATABASE_URL":  "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		"KAFKA_BROKERS": "127.0.0.1:1",
	}
	for k, v := range over {
		env[k] = v
	}
	return func(k string) string { return env[k] }
}

func TestValidateBootConfig_AcceptsTheCompleteEnvironment(t *testing.T) {
	for _, over := range []map[string]string{
		nil,
		{"EVENT_PUBLISHER": "kafka", "PRODUCT_CLASSIFICATION_MODE": "kafka"},
		{"EVENT_PUBLISHER": " KAFKA "},
	} {
		if err := validateBootConfig(validEnv(over)); err != nil {
			t.Fatalf("env %v rejected: %v", over, err)
		}
	}
}

// Every missing or rejected value is a boot error that names the variable.
func TestValidateBootConfig_RejectsAndNamesTheVariable(t *testing.T) {
	cases := []struct {
		name string
		over map[string]string
		want string
	}{
		{"DATABASE_URL unset", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"DATABASE_URL blank", map[string]string{"DATABASE_URL": "  "}, "DATABASE_URL"},
		{"KAFKA_BROKERS unset", map[string]string{"KAFKA_BROKERS": ""}, "KAFKA_BROKERS"},
		{"KAFKA_BROKERS commas", map[string]string{"KAFKA_BROKERS": " , "}, "KAFKA_BROKERS"},
		{"EVENT_PUBLISHER=log", map[string]string{"EVENT_PUBLISHER": "log"}, "EVENT_PUBLISHER=log"},
		{"EVENT_PUBLISHER unknown", map[string]string{"EVENT_PUBLISHER": "nats"}, "EVENT_PUBLISHER"},
		{"PRODUCT_CLASSIFICATION_MODE=permissive", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "permissive"}, "PRODUCT_CLASSIFICATION_MODE"},
		{"PRODUCT_CLASSIFICATION_MODE=off", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "off"}, "PRODUCT_CLASSIFICATION_MODE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateBootConfig(validEnv(c.over))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want one naming %q", err, c.want)
			}
		})
	}
}

// All problems are reported at once, not one per restart.
func TestValidateBootConfig_ReportsEveryProblem(t *testing.T) {
	err := validateBootConfig(func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("err = %v, want both DATABASE_URL and KAFKA_BROKERS", err)
	}
}
