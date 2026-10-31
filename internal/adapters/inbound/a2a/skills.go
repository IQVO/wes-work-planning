package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans (ADR-0013).
const tracerName = "github.com/claudioed/wes-work-planning/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card).
const (
	skillFlowStatus      = "flow_status"
	skillReleaseNextWork = "release_next_work"
)

// skillRequest is the wire contract of every skill call: the first meaningful
// message part carries (as a Data part, or a Text part holding the same JSON)
// the skill id and its arguments. Strict on purpose — an agent client is
// machine-to-machine, and a documented, validated shape beats a guessed one.
type skillRequest struct {
	Skill     string `json:"skill"`
	Arguments struct {
		PathId string `json:"pathId"`
	} `json:"arguments"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR-0046).
func answer(deps Deps, ctx context.Context, ec *a2asrv.ExecutorContext) a2a.Event {
	req, problem := parseSkillRequest(ec)
	if problem != nil {
		return reply(ec, envelope{Skill: "", Problem: problem}, problem.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	switch req.Skill {
	case skillFlowStatus:
		data, problem := deps.flowStatus(ctx, req.Arguments.PathId)
		if problem != nil {
			span.SetStatus(codes.Error, problem.Detail)
			return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
		}
		dto := skillDTO{FlowStatus: &data, summary: data.Summary()}
		return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
	case skillReleaseNextWork:
		data, problem := deps.releaseNextWork(ctx, req.Arguments.PathId)
		if problem != nil {
			span.SetStatus(codes.Error, problem.Detail)
			return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
		}
		dto := skillDTO{ReleasedWork: &data, summary: data.Summary()}
		return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
	default:
		problem := problemUnknownSkill(req.Skill)
		span.SetStatus(codes.Error, problem.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first
// meaningful part. A Data part (or a Text part holding the same JSON object)
// is accepted; anything else is a request-shape problem, answered — not
// raised — because a malformed delegation is a business answer for the
// calling agent, not a transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid JSON: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// parsePathId validates the pathId argument the same way every other inbound
// adapter does: an empty or malformed id is rejected, never defaulted.
func parsePathId(s string) (shared.PathId, error) {
	pathId, err := shared.NewPathId(s)
	if err != nil {
		return shared.PathId{}, fmt.Errorf("invalid pathId %q: must be a non-empty process path id", s)
	}
	return pathId, nil
}

// flowStatus is the composed read skill: backlog telemetry plus the
// rebalance recommendation for one process path, from this context's own
// state. Both use cases publish their flow-balancing events on the way
// (ADR-0037) — unchanged, exactly as the HTTP and MCP surfaces see it.
func (d Deps) flowStatus(ctx context.Context, rawPathId string) (flowStatus, *problem) {
	pathId, err := parsePathId(rawPathId)
	if err != nil {
		return flowStatus{}, problemInvalidPathId(err)
	}
	snapshot, err := d.SampleBacklog.Execute(ctx, usecases.SampleBacklogRequest{PathId: pathId})
	if err != nil {
		return flowStatus{}, problemFromUseCase(err)
	}
	rec, err := d.RebalanceDecision.Execute(ctx, usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		return flowStatus{}, problemFromUseCase(err)
	}
	return toFlowStatus(snapshot, rec), nil
}

// releaseNextWork is the single state-changing delegation: it admits the next
// priority-ordered unit into a pool through the same use case REST and MCP
// use, so the release policy, the WIP invariant, and the transactional outbox
// behave identically regardless of caller.
func (d Deps) releaseNextWork(ctx context.Context, rawPathId string) (releasedWork, *problem) {
	pathId, err := parsePathId(rawPathId)
	if err != nil {
		return releasedWork{}, problemInvalidPathId(err)
	}
	unit, err := d.ReleaseNextWork.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		return releasedWork{}, problemFromUseCase(err)
	}
	return toReleasedWork(unit), nil
}

// problemFromUseCase maps a use case error to a problem answer. Domain
// rejections (empty pool, WIP limit, already released, not found, optimistic
// concurrency) are EXPECTED business outcomes for a delegating agent and are
// answered as RFC 7807-shaped problems (ADR-0005); anything else is an
// internal failure, still answered as a problem — with status 500 — so the
// calling agent always receives a structured, machine-readable result.
func problemFromUseCase(err error) *problem {
	switch {
	case errors.Is(err, ports.ErrNotFound):
		return &problem{Type: problemTypeBase + "/not-found", Title: "No work pool for that process path", Detail: err.Error(), Status: 404}
	case errors.Is(err, release.ErrEmptyPool),
		errors.Is(err, release.ErrWIPLimitReached),
		errors.Is(err, release.ErrAlreadyReleased),
		errors.Is(err, release.ErrDuplicateEntry):
		return &problem{Type: problemTypeBase + "/business-rule", Title: "Request rejected by a domain rule", Detail: err.Error(), Status: 409}
	case errors.Is(err, ports.ErrConcurrentModification):
		return &problem{Type: problemTypeBase + "/concurrent-modification", Title: "Concurrent modification, nothing was written", Detail: err.Error(), Status: 409}
	case errors.Is(err, shared.ErrInvalidPathId):
		return problemInvalidPathId(err)
	default:
		slog.Error("a2a skill failed", "error", err)
		return &problem{Type: problemTypeBase + "/internal", Title: "Unexpected failure executing the skill", Detail: "the skill failed; see server logs (trace id in the response metadata)", Status: 500}
	}
}
