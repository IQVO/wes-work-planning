package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	inbounda2a "github.com/claudioed/wes-work-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// newTestRouter wires a real A2A handler (over the in-memory test doubles)
// behind newRouter, exactly as run() does.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	clock := memory.SystemClock{}
	publisher := events.NewLogPublisher(slog.Default())
	deps := inbounda2a.Deps{
		SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
		ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
	}
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: "http://localhost:8093/", Version: "test"}),
		"wes-work-planning-a2a-test")
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

func TestRouter_ServesAgentCardAtWellKnownPath(t *testing.T) {
	router := newTestRouter(t)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /.well-known/agent-card.json status = %d, want 200", rec.Code)
	}
	var card a2a.AgentCard
	if err := json.Unmarshal(rec.Body.Bytes(), &card); err != nil {
		t.Fatalf("card is not JSON: %v", err)
	}
	if card.Name != "wes-work-planning" {
		t.Fatalf("card name = %q, want wes-work-planning", card.Name)
	}
	if len(card.Skills) != 2 {
		t.Fatalf("card advertises %d skills, want 2", len(card.Skills))
	}
}

func TestRouter_A2AMountsReachHandler(t *testing.T) {
	for _, mount := range []string{"/", "/a2a"} {
		t.Run(mount, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, mount, strings.NewReader(`{}`))
			req.Header.Set("Content-Type", "application/json")
			newTestRouter(t).ServeHTTP(rec, req)
			// An empty JSON body is a JSON-RPC protocol error answered by the
			// handler — which is exactly the proof the mount reaches it.
			if !strings.Contains(rec.Body.String(), "jsonrpc") {
				t.Fatalf("POST %s did not reach the JSON-RPC handler: %s", mount, rec.Body.String())
			}
		})
	}
}

// TestRouter_MessageSendSmoke drives one real SendMessage over the mounted
// JSON-RPC handler: the malformed request (no parts) is answered by the
// agent's problem envelope, not a transport failure.
func TestRouter_MessageSendSmoke(t *testing.T) {
	rec := httptest.NewRecorder()
	body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"role":"user","messageId":"m1","parts":[{"text":"not a skill request"}]}}}`
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("A2A-Version", string(a2a.Version))
	newTestRouter(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("SendMessage status = %d, want 200 (problems are answered in-band), body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "malformed-skill-request") {
		t.Fatalf("SendMessage did not answer the problem envelope: %s", rec.Body.String())
	}
}

func TestWireEventPublisher_RefusesAMissingPool(t *testing.T) {
	if _, _, err := wireEventPublisher(slog.Default(), "kafka:9092", repositories{close: func() {}}, nil); err == nil {
		t.Fatal("wireEventPublisher must refuse a missing pool")
	}
}

func TestWireRepositories_RefusesAMissingDatabaseURL(t *testing.T) {
	if _, err := wireRepositories(slog.Default(), "  "); !errors.Is(err, errNoDatabase) {
		t.Fatalf("wireRepositories error = %v, want errNoDatabase", err)
	}
}

func TestBrokerListTrimsAndDropsBlanks(t *testing.T) {
	got := brokerList(" a , ,, b ")
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("brokerList = %v, want [a b]", got)
	}
}

func TestNewEventIDIsUUIDv4(t *testing.T) {
	id := newEventID()
	if len(id) != 36 || id[14] != '4' {
		t.Fatalf("newEventID() = %q, want a UUID v4", id)
	}
	if id == newEventID() {
		t.Fatal("newEventID() must not repeat")
	}
}

func TestBuildClassificationLookup_NilPoolIsABootError(t *testing.T) {
	// An unset mode is valid, but this binary never runs without the real
	// Postgres pool (ADR-0044): a nil pool is refused at boot naming the
	// variable, not silently degraded.
	if _, err := buildClassificationLookup("", nil, slog.Default()); !errors.Is(err, errNoDatabase) {
		t.Fatalf("nil pool error = %v, want errNoDatabase", err)
	}
}

func TestBuildClassificationLookup_RejectsAtBoot(t *testing.T) {
	if _, err := buildClassificationLookup("permissive", nil, slog.Default()); err == nil {
		t.Fatal("permissive mode must be refused at boot")
	}
}

func TestNewA2ADeps_WiresEveryUseCase(t *testing.T) {
	repos := repositories{
		pools:     memory.NewWorkPoolRepo(),
		workUnits: memory.NewWorkUnitRepo(),
	}
	deps := newA2ADeps(slog.Default(), repos, events.NewLogPublisher(nil))

	if deps.SampleBacklog == nil || deps.RebalanceDecision == nil || deps.ReleaseNextWork == nil {
		t.Fatal("every A2A use case must be wired")
	}
}

func TestValidateBootConfig_AcceptsTheCompleteEnvironment(t *testing.T) {
	env := func(k string) string {
		switch k {
		case "DATABASE_URL":
			return "postgres://wes:wes@localhost:5432/wes"
		case "KAFKA_BROKERS":
			return "localhost:9092"
		case "A2A_AGENT_URL":
			return "http://wes-work-planning-a2a.warehouse-systems.svc.cluster.local:8093/"
		default:
			return ""
		}
	}
	if err := validateBootConfig(env); err != nil {
		t.Fatalf("complete environment rejected: %v", err)
	}
}

func TestValidateBootConfig_RejectsAndNamesTheVariable(t *testing.T) {
	cases := map[string]map[string]string{
		"missing DATABASE_URL":  {"DATABASE_URL": ""},
		"missing KAFKA_BROKERS": {"KAFKA_BROKERS": ""},
		"missing A2A_AGENT_URL": {"A2A_AGENT_URL": ""},
		"log publisher":         {"EVENT_PUBLISHER": "log"},
		"relative agent URL":    {"A2A_AGENT_URL": "wes-work-planning-a2a:8093"},
		"schemeless agent URL":  {"A2A_AGENT_URL": "//host/path"},
		"ftp agent URL":         {"A2A_AGENT_URL": "ftp://x/"},
	}
	for name, overrides := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateBootConfig(envWith(overrides)); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
}

func TestValidateBootConfig_ReportsEveryProblem(t *testing.T) {
	err := validateBootConfig(func(string) string { return "" })
	if err == nil {
		t.Fatal("an empty environment must be rejected")
	}
	for _, want := range []string{"DATABASE_URL", "KAFKA_BROKERS", "A2A_AGENT_URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err.Error(), want)
		}
	}
}

func TestCheckAgentURL(t *testing.T) {
	for _, ok := range []string{"http://a:1/", "https://agent.example.com/a2a"} {
		if err := checkAgentURL(ok); err != nil {
			t.Errorf("checkAgentURL(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "  ", "localhost:8093", "ftp://x/", "//host/", ":8093"} {
		if err := checkAgentURL(bad); err == nil {
			t.Errorf("checkAgentURL(%q) accepted", bad)
		}
	}
}

// envWith builds the complete valid boot environment with overrides applied.
func envWith(overrides map[string]string) func(string) string {
	env := map[string]string{
		"DATABASE_URL":  "postgres://wes:wes@localhost:5432/wes",
		"KAFKA_BROKERS": "localhost:9092",
		"A2A_AGENT_URL": "http://localhost:8093/",
	}
	for k, v := range overrides {
		env[k] = v
	}
	return func(k string) string { return env[k] }
}
