package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// newTestRouter wires a real MCP handler (in-memory adapters) behind
// newRouter, exactly as run() does.
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

func TestWireEventPublisher_DefaultsToLogPublisher(t *testing.T) {
	pub, stop, err := wireEventPublisher(slog.Default(), "", "", repositories{}, nil)
	if err != nil {
		t.Fatalf("wireEventPublisher: %v", err)
	}
	defer stop()
	if _, ok := pub.(*events.LogPublisher); !ok {
		t.Fatalf("publisher = %T, want *events.LogPublisher", pub)
	}
}

func TestWireEventPublisher_KafkaRequiresBrokers(t *testing.T) {
	if _, _, err := wireEventPublisher(slog.Default(), "kafka", "", repositories{}, nil); err == nil {
		t.Fatal("expected EVENT_PUBLISHER=kafka without KAFKA_BROKERS to fail")
	}
}

func TestWireEventPublisher_KafkaWithoutPostgresPublishesDirectly(t *testing.T) {
	pub, stop, err := wireEventPublisher(slog.Default(), "kafka", "localhost:1", repositories{workUnits: memory.NewWorkUnitRepo()}, nil)
	if err != nil {
		t.Fatalf("wireEventPublisher: %v", err)
	}
	defer stop()
	if _, ok := pub.(*events.MultiPublisher); !ok {
		t.Fatalf("publisher = %T, want *events.MultiPublisher", pub)
	}
}

func TestWireRepositories_InMemoryWithoutDatabaseURL(t *testing.T) {
	repos, err := wireRepositories(slog.Default(), "")
	if err != nil {
		t.Fatalf("wireRepositories: %v", err)
	}
	defer repos.close()
	if repos.pool != nil || repos.uow != nil {
		t.Fatalf("in-memory wiring must have no pool/UnitOfWork, got pool=%v uow=%v", repos.pool, repos.uow)
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

func TestBuildClassificationLookup_Modes(t *testing.T) {
	lookup, err := buildClassificationLookup("", nil, slog.Default())
	if err != nil {
		t.Fatalf("default mode: %v", err)
	}
	if _, ok := lookup.(*productclassificationcopy.PermissiveLookup); !ok {
		t.Fatalf("default lookup = %T, want permissive", lookup)
	}
	// kafka without a database: the MCP server has no copy to read and
	// never starts a consumer, so it degrades to permissive.
	lookup, err = buildClassificationLookup("kafka", nil, slog.Default())
	if err != nil {
		t.Fatalf("kafka mode: %v", err)
	}
	if _, ok := lookup.(*productclassificationcopy.PermissiveLookup); !ok {
		t.Fatalf("kafka lookup without DATABASE_URL = %T, want permissive", lookup)
	}
}

func TestBuildClassificationLookup_RejectsHTTPAtBoot(t *testing.T) {
	for _, mode := range []string{"http", "bogus"} {
		if _, err := buildClassificationLookup(mode, nil, slog.Default()); err == nil {
			t.Fatalf("PRODUCT_CLASSIFICATION_MODE=%s must fail at boot", mode)
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
