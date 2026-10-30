package a2a

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness builds Deps over in-memory repos, seeding through the real
// EnqueueWorkUnit use case with a FixedClock, exactly like the MCP adapter's
// unit-test harness. The A2A skills run the REAL use cases; only the
// persistence and the clock are in-memory.
type harness struct {
	deps      Deps
	pools     *memory.WorkPoolRepo
	workUnits *memory.WorkUnitRepo
	enqueue   *usecases.EnqueueWorkUnit
	clock     memory.FixedClock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: base}

	return &harness{
		deps: Deps{
			SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
			RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
			ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
		},
		pools:     pools,
		workUnits: workUnits,
		enqueue:   usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock),
		clock:     clock,
	}
}

// seedPending enqueues one pending work unit on path (auto-creating a
// release-fed pool the first time) and returns the generated work-unit id.
func (h *harness) seedPending(t *testing.T, path, ref string, cptIn time.Duration) string {
	t.Helper()
	unit, err := h.enqueue.Execute(context.Background(), enqueueRequest(path, ref, cptIn))
	if err != nil {
		t.Fatalf("seed enqueue(%s, %s): %v", path, ref, err)
	}
	return unit.Id()
}

// ec builds an ExecutorContext carrying one user message with the given
// parts, as the SDK would for an incoming message/send.
func ec(parts ...*a2a.Part) *a2asrv.ExecutorContext {
	return &a2asrv.ExecutorContext{
		Message:   a2a.NewMessage(a2a.MessageRoleUser, parts...),
		ContextID: "ctx-test",
	}
}

func dataPart(v any) *a2a.Part { return a2a.NewDataPart(v) }

func skillPart(skill, pathId string) *a2a.Part {
	return dataPart(map[string]any{"skill": skill, "arguments": map[string]any{"pathId": pathId}})
}

// run drives one skill answer and decodes its envelope from the reply's Data
// part, failing the test when the reply is malformed.
func run(t *testing.T, h *harness, ec *a2asrv.ExecutorContext) (envelope, *a2a.Message) {
	t.Helper()
	var env envelope
	msg := answer(h.deps, context.Background(), ec)
	m, ok := msg.(*a2a.Message)
	if !ok {
		t.Fatalf("answer returned %T, want *a2a.Message", msg)
	}
	if len(m.Parts) != 2 {
		t.Fatalf("reply has %d parts, want 2 (data + summary)", len(m.Parts))
	}
	data, ok := m.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want a2a.Data", m.Parts[0].Content)
	}
	raw, err := json.Marshal(data.Value)
	if err != nil {
		t.Fatalf("marshal data part: %v", err)
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v\nenvelope: %s", err, raw)
	}
	return env, m
}

func TestAnswerFlowStatus(t *testing.T) {
	h := newHarness(t)
	h.seedPending(t, "pick-zone-a", "SO-1001", 2*time.Hour)

	env, msg := run(t, h, ec(skillPart(skillFlowStatus, "pick-zone-a")))

	if env.Skill != skillFlowStatus {
		t.Fatalf("envelope skill = %q, want %q", env.Skill, skillFlowStatus)
	}
	if env.Problem != nil {
		t.Fatalf("unexpected problem: %+v", env.Problem)
	}
	fs := env.Data.FlowStatus
	if fs == nil {
		t.Fatal("envelope carries no flowStatus")
	}
	if fs.PathId != "pick-zone-a" {
		t.Errorf("pathId = %q, want pick-zone-a", fs.PathId)
	}
	if fs.Telemetry.BacklogDepth != 1 {
		t.Errorf("backlogDepth = %d, want 1", fs.Telemetry.BacklogDepth)
	}
	if fs.Recommendation.Action == "" {
		t.Error("recommendation action is empty")
	}
	if want := "pick-zone-a: backlog 1, WIP 0"; !hasPrefix(replyText(t, msg), want) {
		t.Errorf("summary %q does not start with %q", replyText(t, msg), want)
	}
	if msg.ContextID != "ctx-test" {
		t.Errorf("reply contextId = %q, want ctx-test (caller grouping)", msg.ContextID)
	}
}

func TestAnswerFlowStatusTextPartJSON(t *testing.T) {
	h := newHarness(t)
	h.seedPending(t, "pack-station-3", "SO-1002", time.Hour)

	env, _ := run(t, h, ec(a2a.NewTextPart(`{"skill":"flow_status","arguments":{"pathId":"pack-station-3"}}`)))

	if env.Problem != nil {
		t.Fatalf("text-part JSON rejected: %+v", env.Problem)
	}
	if env.Data.FlowStatus == nil || env.Data.FlowStatus.PathId != "pack-station-3" {
		t.Fatalf("flowStatus = %+v, want pack-station-3", env.Data.FlowStatus)
	}
}

func TestAnswerReleaseNextWork(t *testing.T) {
	h := newHarness(t)
	id := h.seedPending(t, "pick-zone-a", "SO-1001", 2*time.Hour)

	env, _ := run(t, h, ec(skillPart(skillReleaseNextWork, "pick-zone-a")))

	if env.Problem != nil {
		t.Fatalf("unexpected problem: %+v", env.Problem)
	}
	rw := env.Data.ReleasedWork
	if rw == nil {
		t.Fatal("envelope carries no releasedWork")
	}
	if rw.WorkUnitId != id {
		t.Errorf("workUnitId = %q, want %q", rw.WorkUnitId, id)
	}
	if rw.Ref != "SO-1001" {
		t.Errorf("ref = %q, want SO-1001", rw.Ref)
	}
	if rw.CPT == "" {
		t.Error("cpt is empty")
	}
}

func TestAnswerReleaseNextWorkEmptyPool(t *testing.T) {
	h := newHarness(t)
	h.seedPending(t, "pick-zone-a", "SO-1001", 2*time.Hour)

	first, _ := run(t, h, ec(skillPart(skillReleaseNextWork, "pick-zone-a")))
	if first.Problem != nil {
		t.Fatalf("first release failed: %+v", first.Problem)
	}
	second, _ := run(t, h, ec(skillPart(skillReleaseNextWork, "pick-zone-a")))

	if second.Problem == nil {
		t.Fatal("second release of an empty pool must answer a problem")
	}
	if second.Problem.Status != 409 {
		t.Errorf("problem status = %d, want 409", second.Problem.Status)
	}
	if second.Problem.Type != problemTypeBase+"/business-rule" {
		t.Errorf("problem type = %q, want a business-rule problem", second.Problem.Type)
	}
}

func TestAnswerFlowStatusUnknownPath(t *testing.T) {
	h := newHarness(t)

	env, _ := run(t, h, ec(skillPart(skillFlowStatus, "no-such-path")))

	if env.Problem == nil {
		t.Fatal("unknown path must answer a problem")
	}
	if env.Problem.Status != 404 {
		t.Errorf("problem status = %d, want 404", env.Problem.Status)
	}
}

func TestAnswerInvalidPathId(t *testing.T) {
	h := newHarness(t)

	env, _ := run(t, h, ec(skillPart(skillFlowStatus, "")))

	if env.Problem == nil || env.Problem.Status != 400 {
		t.Fatalf("empty pathId must answer a 400 problem, got %+v", env.Problem)
	}
	if env.Problem.Type != problemTypeBase+"/invalid-path-id" {
		t.Errorf("problem type = %q, want invalid-path-id", env.Problem.Type)
	}
}

func TestAnswerUnknownSkill(t *testing.T) {
	h := newHarness(t)

	env, _ := run(t, h, ec(skillPart("create_wave", "pick-zone-a")))

	if env.Problem == nil || env.Problem.Status != 400 {
		t.Fatalf("unknown skill must answer a 400 problem, got %+v", env.Problem)
	}
	if env.Problem.Type != problemTypeBase+"/unknown-skill" {
		t.Errorf("problem type = %q, want unknown-skill", env.Problem.Type)
	}
}

func TestAnswerMalformedRequests(t *testing.T) {
	h := newHarness(t)
	cases := map[string]*a2asrv.ExecutorContext{
		"no parts":            {},
		"no message":          ec(),
		"text not json":       ec(a2a.NewTextPart("hello, what can you do?")),
		"data wrong shape":    ec(dataPart([]string{"a", "b"})),
		"missing skill field": ec(dataPart(map[string]any{"arguments": map[string]any{"pathId": "p"}})),
		"raw part":            ec(a2a.NewRawPart([]byte(`{"skill":"flow_status"}`))),
	}
	for name, execCtx := range cases {
		t.Run(name, func(t *testing.T) {
			env, _ := run(t, h, execCtx)
			if env.Problem == nil || env.Problem.Status != 400 {
				t.Fatalf("malformed request %q must answer a 400 problem, got %+v", name, env.Problem)
			}
			if env.Problem.Type != problemTypeBase+"/malformed-skill-request" {
				t.Errorf("problem type = %q, want malformed-skill-request", env.Problem.Type)
			}
		})
	}
}

func TestReplyCarriesTaskReference(t *testing.T) {
	h := newHarness(t)
	h.seedPending(t, "pick-zone-a", "SO-1001", 2*time.Hour)

	execCtx := ec(skillPart(skillFlowStatus, "pick-zone-a"))
	execCtx.TaskID = "task-42"
	_, msg := run(t, h, execCtx)

	if msg.TaskID != "task-42" {
		t.Errorf("reply taskId = %q, want task-42 (client task grouping)", msg.TaskID)
	}
}

func TestPartJSON(t *testing.T) {
	if _, err := partJSON(a2a.NewTextPart(`{"skill":"x"}`)); err != nil {
		t.Errorf("text part: %v", err)
	}
	if _, err := partJSON(dataPart(map[string]any{"skill": "x"})); err != nil {
		t.Errorf("data part: %v", err)
	}
	if _, err := partJSON(a2a.NewRawPart([]byte("x"))); err == nil {
		t.Error("raw part must be rejected")
	}
}

func TestProblemFromUseCaseUnexpectedError(t *testing.T) {
	p := problemFromUseCase(errBoom{})
	if p == nil || p.Status != 500 {
		t.Fatalf("unexpected error must map to a 500 problem, got %+v", p)
	}
	if p.Type != problemTypeBase+"/internal" {
		t.Errorf("problem type = %q, want internal", p.Type)
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

func hasPrefix(s, prefix string) bool { return len(s) >= len(prefix) && s[:len(prefix)] == prefix }

// replyText returns the reply's text summary part.
func replyText(t *testing.T, msg *a2a.Message) string {
	t.Helper()
	text, ok := msg.Parts[1].Content.(a2a.Text)
	if !ok {
		t.Fatalf("second reply part is %T, want a2a.Text", msg.Parts[1].Content)
	}
	return string(text)
}

// enqueueRequest builds the enqueue use case request for seeding. Mirrors the
// MCP harness: a pending unit with a CPT offset from the fixed clock.
func enqueueRequest(path, ref string, cptIn time.Duration) usecases.EnqueueWorkUnitRequest {
	pathId, _ := shared.NewPathId(path)
	return usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-" + ref,
		PathId:     pathId,
		CPT:        shared.NewCPT(base.Add(cptIn)),
		Reference:  ref,
	}
}
