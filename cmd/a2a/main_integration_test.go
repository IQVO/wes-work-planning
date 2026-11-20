//go:build integration

// Integration test for the cmd/a2a composition root (ADR-0046): the REAL
// wiring run() assembles — wireRepositories (Postgres repositories and
// UnitOfWork, ADR-0044), buildClassificationLookup (the read-only
// product_classification_copy, ADR-0035), wireEventPublisher (the
// transactional outbox publisher, ADR-0014) and newA2ADeps — served through
// the real newRouter over a testcontainers Postgres (internal/testing/pgtest)
// and driven by the SDK's own client built from a card that points at the
// test server, exactly as a fleet agent client does after card discovery.
//
// The adapter's own suite (internal/adapters/inbound/a2a) proves the wire
// contract over the handler directly; this file proves the COMPOSITION ROOT:
// the env-driven wiring bootstraps, the documented skill answers come from
// real persisted state, and a delegated release reaches outbox_events
// unpublished on both documented topics — the relay (cmd/wes) owns the Kafka
// writer, never this binary.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	inbounda2a "github.com/claudioed/wes-work-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/testing/pgtest"
)

// workReleasedType is the integration event a delegated release must raise
// (use-cases.md: ReleaseNextWork -> Events: WorkReleased).
const workReleasedType = "com.warehouse.wes.work-planning.workunit.WorkReleased"

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// a2aStack is the real composition root served over HTTP, plus everything
// the assertions need to observe the database.
type a2aStack struct {
	server *httptest.Server
	client *a2aclient.Client
	repos  repositories
	pathID string
	unitID string
	ref    string
	cpt    time.Time
}

// bootA2A wires the real production stack over a fresh migrated Postgres
// (testcontainers), in run()'s assembly order minus telemetry and signals.
// The broker address is never dialled: in outbox mode the publisher only
// ENCODES — the relay (cmd/wes) owns the Kafka writer. With withHazmatCopy
// set, the row cmd/wes's ProductClassified consumer would maintain is seeded
// and the queued unit carries its SKU, so the released event must carry the
// ADR-0009 hazmat/fragile hints.
func bootA2A(t *testing.T, withHazmatCopy bool) *a2aStack {
	t.Helper()
	ctx := context.Background()
	logger := slog.Default()

	repos, err := wireRepositories(logger, pgtest.URL(t))
	if err != nil {
		t.Fatalf("wireRepositories: %v", err)
	}
	t.Cleanup(repos.close)

	sku := "sku-plain"
	if withHazmatCopy {
		sku = "sku-haz"
		if _, err := repos.pool.Exec(ctx,
			`INSERT INTO product_classification_copy (sku, handling_tags, version, updated_at) VALUES ('sku-haz', '{Hazmat,Fragile}', 2, now())`); err != nil {
			t.Fatalf("seed classification copy: %v", err)
		}
	}

	classifications, err := buildClassificationLookup("kafka", repos.pool, logger)
	if err != nil {
		t.Fatalf("buildClassificationLookup: %v", err)
	}
	publisher, stop, err := wireEventPublisher(logger, "kafka.invalid:9092", repos, classifications)
	if err != nil {
		t.Fatalf("wireEventPublisher: %v", err)
	}
	t.Cleanup(stop)

	st := &a2aStack{
		repos:  repos,
		pathID: "pick-a2a",
		unitID: "wu-a2a-1",
		ref:    "ref-a2a-1",
		cpt:    time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second),
	}

	// Seed one pending unit the documented way: the EnqueueWorkUnit use case
	// the REST and Kafka paths drive (use-cases.md), persisted through the
	// same UnitOfWork.
	pathID, err := shared.NewPathId(st.pathID)
	if err != nil {
		t.Fatalf("path id: %v", err)
	}
	enqueue := usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, memory.SystemClock{}).WithUnitOfWork(repos.uow)
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: st.unitID, PathId: pathID,
		CPT: shared.NewCPT(st.cpt), Reference: st.ref, SKU: sku, GiftWrap: true,
	}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}

	st.server = httptest.NewServer(newRouter(
		inbounda2a.Handler(inbounda2a.NewExecutor(newA2ADeps(repos, publisher))),
		inbounda2a.Card(inbounda2a.Config{AgentURL: "http://placeholder.invalid/", Version: "it"}),
		"wes-work-planning-a2a-test"))
	t.Cleanup(st.server.Close)

	// The client is built from a card whose URL points at the test server —
	// post-discovery client construction, as the fleet's agent clients do.
	st.client, err = a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: st.server.URL + "/", Version: "it"}))
	if err != nil {
		t.Fatalf("a2a client from card: %v", err)
	}
	return st
}

// skillEnvelope mirrors the adapter's answer envelope (a test-local copy so
// this suite fails on contract drift).
type skillEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		FlowStatus *struct {
			PathId    string `json:"pathId"`
			Telemetry struct {
				BacklogDepth       int    `json:"backlogDepth"`
				WIP                int    `json:"wip"`
				Mode               string `json:"mode"`
				OverAlarmThreshold bool   `json:"overAlarmThreshold"`
			} `json:"telemetry"`
			Recommendation struct {
				Action string `json:"action"`
			} `json:"recommendation"`
		} `json:"flowStatus"`
		ReleasedWork *struct {
			WorkUnitId string `json:"workUnitId"`
			CPT        string `json:"cpt"`
			Ref        string `json:"ref"`
		} `json:"releasedWork"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	} `json:"problem"`
}

// send drives one skill request through the SDK client over the real router
// and decodes the reply envelope from the agent Message's data part.
func (s *a2aStack) send(t *testing.T, skill string) skillEnvelope {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{
		"skill":     skill,
		"arguments": map[string]any{"pathId": s.pathID},
	}))
	resp, err := s.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("SendMessage(%s): %v", skill, err)
	}
	reply, ok := resp.(*a2a.Message)
	if !ok {
		t.Fatalf("SendMessage(%s) result is %T, want a message answer (message-only agent)", skill, resp)
	}
	if len(reply.Parts) == 0 {
		t.Fatalf("SendMessage(%s) reply carries no parts", skill)
	}
	data, ok := reply.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want a2a.Data", reply.Parts[0].Content)
	}
	raw, err := json.Marshal(data.Value)
	if err != nil {
		t.Fatalf("marshal data part: %v", err)
	}
	var env skillEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v\n%s", err, raw)
	}
	return env
}

// totalOutbox counts every outbox_events row.
func (s *a2aStack) totalOutbox(t *testing.T) int {
	t.Helper()
	var n int
	if err := s.repos.pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatalf("count outbox_events: %v", err)
	}
	return n
}

// pendingOutbox counts the unpublished rows of one event type on one topic.
func (s *a2aStack) pendingOutbox(t *testing.T, topic, eventType string) int {
	t.Helper()
	var n int
	if err := s.repos.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE topic = $1 AND event_type = $2 AND published_at IS NULL`,
		topic, eventType).Scan(&n); err != nil {
		t.Fatalf("count outbox rows on %s: %v", topic, err)
	}
	return n
}

// outboxValue returns the stored CloudEvents payload of one event type on
// one topic.
func (s *a2aStack) outboxValue(t *testing.T, topic, eventType string) string {
	t.Helper()
	var value string
	if err := s.repos.pool.QueryRow(context.Background(),
		`SELECT convert_from(value, 'UTF8') FROM outbox_events WHERE topic = $1 AND event_type = $2`,
		topic, eventType).Scan(&value); err != nil {
		t.Fatalf("read %s row: %v", eventType, err)
	}
	return value
}

// workUnitState reads the persisted lifecycle state of the seeded unit.
func (s *a2aStack) workUnitState(t *testing.T) (state string, released bool) {
	t.Helper()
	if err := s.repos.pool.QueryRow(context.Background(),
		`SELECT state, released_at IS NOT NULL FROM work_units WHERE id = $1`, s.unitID).Scan(&state, &released); err != nil {
		t.Fatalf("read work unit: %v", err)
	}
	return state, released
}

// TestA2A_FlowStatus_AnswersSeededBacklogAndWritesNothing proves the composed
// read over the real wiring: telemetry from persisted state (backlog depth 1,
// no released WIP, ReleaseFed — use-cases.md: the first enqueue creates the
// pool as ReleaseFed), the Drum-Buffer-Rope recommendation NoActionNeeded,
// and — the governance half — that the read changes nothing: the outbox is
// untouched and the seeded unit stays pending.
func TestA2A_FlowStatus_AnswersSeededBacklogAndWritesNothing(t *testing.T) {
	st := bootA2A(t, false)
	before, releasedBefore := st.totalOutbox(t), st.pendingOutbox(t, cloudevents.TopicWorkPlanningEvents, workReleasedType)

	env := st.send(t, "flow_status")

	if env.Problem != nil {
		t.Fatalf("flow_status answered a problem: %+v", env.Problem)
	}
	fs := env.Data.FlowStatus
	if fs == nil {
		t.Fatal("envelope carries no flowStatus")
	}
	if fs.PathId != st.pathID {
		t.Errorf("pathId = %q, want %q", fs.PathId, st.pathID)
	}
	if fs.Telemetry.BacklogDepth != 1 {
		t.Errorf("backlogDepth = %d, want 1 (one seeded unit)", fs.Telemetry.BacklogDepth)
	}
	if fs.Telemetry.WIP != 0 {
		t.Errorf("wip = %d, want 0 (nothing released yet)", fs.Telemetry.WIP)
	}
	if fs.Telemetry.Mode != "ReleaseFed" {
		t.Errorf("mode = %q, want ReleaseFed", fs.Telemetry.Mode)
	}
	if fs.Recommendation.Action != "NoActionNeeded" {
		t.Errorf("recommendation action = %q, want NoActionNeeded", fs.Recommendation.Action)
	}

	// A read must write nothing.
	if after := st.totalOutbox(t); after != before {
		t.Fatalf("outbox rows %d -> %d: the read published events", before, after)
	}
	if after := st.pendingOutbox(t, cloudevents.TopicWorkPlanningEvents, workReleasedType); after != releasedBefore {
		t.Fatalf("WorkReleased rows %d -> %d: the read released work", releasedBefore, after)
	}
	if state, released := st.workUnitState(t); state != "Pending" || released {
		t.Fatalf("seeded unit state = %q released=%v, want Pending/not released", state, released)
	}
}

// TestA2A_ReleaseNextWork_PersistsAndOutboxesWorkReleased proves the single
// state-changing delegation end to end through the composition root: the
// answer carries the admitted unit's id/reference/CPT, the unit is persisted
// as released, and WorkReleased sits UNPUBLISHED in outbox_events on both
// documented topics (the relay in cmd/wes drains it — this binary never
// opens a Kafka writer). The payload carries the enrichment read inside the
// transaction (ref, gift_wrap) and the ADR-0009 hazmat/fragile hints from
// the classification copy, proving buildClassificationLookup is wired into
// the A2A publisher exactly as into REST and MCP. A second call on the
// now-empty pool answers the domain rejection in-band as a 409 problem.
func TestA2A_ReleaseNextWork_PersistsAndOutboxesWorkReleased(t *testing.T) {
	st := bootA2A(t, true)

	env := st.send(t, "release_next_work")

	if env.Problem != nil {
		t.Fatalf("release_next_work answered a problem: %+v", env.Problem)
	}
	rw := env.Data.ReleasedWork
	if rw == nil {
		t.Fatal("envelope carries no releasedWork")
	}
	if rw.WorkUnitId != st.unitID || rw.Ref != st.ref {
		t.Errorf("released work = %s/%s, want %s/%s", rw.WorkUnitId, rw.Ref, st.unitID, st.ref)
	}
	if want := st.cpt.Format(time.RFC3339); rw.CPT != want {
		t.Errorf("cpt = %q, want %q", rw.CPT, want)
	}

	if state, released := st.workUnitState(t); state != "Released" || !released {
		t.Fatalf("unit state = %q released=%v, want Released/true", state, released)
	}
	for _, topic := range []string{cloudevents.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic} {
		if n := st.pendingOutbox(t, topic, workReleasedType); n != 1 {
			t.Fatalf("expected exactly 1 pending WorkReleased row on %s, got %d", topic, n)
		}
	}

	value := st.outboxValue(t, cloudevents.TopicWorkPlanningEvents, workReleasedType)
	for _, want := range []string{
		`"work_unit_id":"` + st.unitID + `"`,
		`"ref":"` + st.ref + `"`,
		`"gift_wrap":true`,
		`"required_capabilities":["hazmat"]`,
		`"fragile":true`,
	} {
		if !strings.Contains(value, want) {
			t.Fatalf("WorkReleased payload missing %s: %s", want, value)
		}
	}

	second := st.send(t, "release_next_work")
	if second.Problem == nil || second.Problem.Status != 409 {
		t.Fatalf("second release must answer a 409 problem in-band, got %+v", second.Problem)
	}
}

// TestA2A_DiscoveryAndHealthOverTheRealWiring proves the HTTP surface the
// composition root serves: the Agent Card at the well-known path advertises
// exactly the two curated skills with streaming disabled (ADR-0046),
// /healthz answers 200 ok for the probes, and the JSON-RPC endpoint mounted
// at /a2a (the fleet's endpoint convention) answers an unknown skill
// in-band as a problem — never a transport failure.
func TestA2A_DiscoveryAndHealthOverTheRealWiring(t *testing.T) {
	st := bootA2A(t, false)

	res, err := http.Get(st.server.URL + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("get agent card: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("agent card status = %d, want 200", res.StatusCode)
	}
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatalf("card is not JSON: %v", err)
	}
	if card.Name != "wes-work-planning" {
		t.Errorf("card name = %q, want wes-work-planning", card.Name)
	}
	if len(card.Skills) != 2 {
		t.Fatalf("card advertises %d skills, want 2", len(card.Skills))
	}
	ids := map[string]bool{}
	for _, s := range card.Skills {
		ids[s.ID] = true
	}
	if !ids["flow_status"] || !ids["release_next_work"] {
		t.Fatalf("card skills = %v, want flow_status + release_next_work", ids)
	}
	if card.Capabilities.Streaming {
		t.Error("card must declare streaming: false (message-only agent)")
	}

	health, err := http.Get(st.server.URL + "/healthz")
	if err != nil {
		t.Fatalf("get /healthz: %v", err)
	}
	defer func() { _ = health.Body.Close() }()
	body, err := io.ReadAll(health.Body)
	if err != nil {
		t.Fatalf("read /healthz body: %v", err)
	}
	if health.StatusCode != http.StatusOK || !strings.Contains(string(body), `"status":"ok"`) {
		t.Fatalf("/healthz = %d %s, want 200 {\"status\":\"ok\"}", health.StatusCode, body)
	}

	// The /a2a mount: one raw JSON-RPC SendMessage carrying an unknown skill
	// in a text part (the documented request shape) must be ANSWERED as an
	// unknown-skill problem inside the envelope.
	skillJSON, err := json.Marshal(map[string]any{"skill": "create_wave", "arguments": map[string]any{"pathId": st.pathID}})
	if err != nil {
		t.Fatalf("marshal skill request: %v", err)
	}
	rpc, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "SendMessage",
		"params": map[string]any{"message": map[string]any{
			"role": "user", "messageId": "m1",
			"parts": []map[string]any{{"text": string(skillJSON)}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal json-rpc request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, st.server.URL+"/a2a", bytes.NewReader(rpc))
	if err != nil {
		t.Fatalf("build /a2a request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("A2A-Version", string(a2a.Version))
	mount, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post /a2a: %v", err)
	}
	defer func() { _ = mount.Body.Close() }()
	reply, err := io.ReadAll(mount.Body)
	if err != nil {
		t.Fatalf("read /a2a reply: %v", err)
	}
	if mount.StatusCode != http.StatusOK || !strings.Contains(string(reply), "unknown-skill") {
		t.Fatalf("POST /a2a = %d %s, want 200 with an unknown-skill problem", mount.StatusCode, reply)
	}
}
