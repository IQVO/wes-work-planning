package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for MCP tool spans.
const tracerName = "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"

// Deps is everything the MCP tools need, injected by the composition root.
// It carries the same use cases the HTTP adapter uses; the adapter never
// constructs an outbound adapter itself.
type Deps struct {
	// SampleBacklog is the existing read-model use case, reused unchanged. It
	// projects a path's backlog depth/WIP/mode and may publish a
	// BacklogThresholdBreached event when the pool is over its alarm threshold
	// — that behaviour is unchanged, exactly as the HTTP telemetry endpoint
	// sees it.
	SampleBacklog *usecases.SampleBacklog
	// RebalanceDecision is the existing flow-balancing use case, reused
	// unchanged. It may publish PathThrottled or LaborReassignmentFlagged,
	// which is why its tool is not annotated read-only (ADR-0037).
	RebalanceDecision *usecases.RebalanceDecision
	// ReleaseNextWork is the existing write use case, reused unchanged. Its
	// release policy and the pool's WIP invariant make a model-invoked release
	// safe by construction: it admits at most the next priority-ordered unit.
	ReleaseNextWork *usecases.ReleaseNextWork
	// Reports is the optional client of the wes-reports REST service backing
	// the curated, read-only "Release Throughput & Backlog Health" report
	// tool. When nil (no reports service configured) that tool is simply not
	// registered, so an MCP deployment without analytics still works
	// unchanged (ADR-0011). It never opens the analytical database directly.
	Reports ReportsClient
}

// --- get_backlog_telemetry ----------------------------------------------------

type backlogTelemetryInput struct {
	PathId string `json:"pathId" jsonschema:"the process path to sample, e.g. pick-zone-a or pack-station-3"`
}

func (d Deps) getBacklogTelemetry(ctx context.Context, in backlogTelemetryInput) (backlogTelemetry, error) {
	pathId, err := parsePathId(in.PathId)
	if err != nil {
		return backlogTelemetry{}, err
	}
	snapshot, err := d.SampleBacklog.Execute(ctx, usecases.SampleBacklogRequest{PathId: pathId})
	if err != nil {
		return backlogTelemetry{}, err
	}
	return toBacklogTelemetry(snapshot), nil
}

// --- get_rebalance_recommendation ---------------------------------------------

type rebalanceInput struct {
	PathId string `json:"pathId" jsonschema:"the process path to evaluate for flow balancing, e.g. pick-zone-a"`
}

func (d Deps) getRebalanceRecommendation(ctx context.Context, in rebalanceInput) (rebalanceRecommendation, error) {
	pathId, err := parsePathId(in.PathId)
	if err != nil {
		return rebalanceRecommendation{}, err
	}
	rec, err := d.RebalanceDecision.Execute(ctx, usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		return rebalanceRecommendation{}, err
	}
	return toRebalanceRecommendation(rec), nil
}

// --- release_next_work (write) ------------------------------------------------

type releaseNextWorkInput struct {
	PathId string `json:"pathId" jsonschema:"the process path whose next priority-ordered work unit should be released into active work"`
}

func (d Deps) releaseNextWork(ctx context.Context, in releaseNextWorkInput) (releasedWork, error) {
	pathId, err := parsePathId(in.PathId)
	if err != nil {
		return releasedWork{}, err
	}
	unit, err := d.ReleaseNextWork.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		// The use case's domain errors (empty pool, WIP limit reached, path
		// not found, already released) surface unchanged as the tool error;
		// the release policy and WIP invariant make a mistaken model call safe.
		return releasedWork{}, err
	}
	return toReleasedWork(unit), nil
}

// --- registration -------------------------------------------------------------

// registerTools adds every tool to the server, each wrapped so its handler
// runs inside an OTel span named "mcp.tool <name>".
func (d Deps) registerTools(server *mcp.Server) {
	destructive := true
	notIdempotent := false

	// get_backlog_telemetry and get_rebalance_recommendation answer with a
	// read model, but their use cases PUBLISH domain events on the way
	// (BacklogThresholdBreached; PathThrottled or LaborReassignmentFlagged)
	// through the outbox onto both topics, where other contexts act on
	// them. A published event cannot be recalled, so they are NOT
	// read-only (ADR-0037, gap fix D17): non-read-only, destructive per
	// governance charter §4.2, non-idempotent (every qualifying call
	// publishes again). The descriptions name the side effect (§4.4).
	addTool(server, &mcp.Tool{
		Name:        "get_backlog_telemetry",
		Description: "Return the live backlog read model for a process path: pending backlog depth, released WIP, feed mode (ReleaseFed or FlowFed), and whether it is over its alarm threshold. Side effect: when the pool is over its alarm threshold, every call publishes a BacklogThresholdBreached event to the work-planning and analytics topics.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive, IdempotentHint: notIdempotent},
	}, d.getBacklogTelemetry)

	addTool(server, &mcp.Tool{
		Name:        "get_rebalance_recommendation",
		Description: "Return the Drum-Buffer-Rope flow-balancing recommendation for a process path: NoActionNeeded, ThrottleUpstream (a flow-fed path over its alarm threshold), or ReassignLabor (a release-fed path saturated at its WIP limit with work still backlogged). Side effect: every ThrottleUpstream answer publishes a PathThrottled event and every ReassignLabor answer publishes a LaborReassignmentFlagged event to the work-planning and analytics topics.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive, IdempotentHint: notIdempotent},
	}, d.getRebalanceRecommendation)

	// Write tool: releases the next priority-ordered unit into a pool.
	// Annotated destructive (non-read-only, non-idempotent — each call
	// admits a different unit) so a host can see it changes state before
	// letting a model call it. The release policy and the pool's WIP
	// invariant bound the risk of a mistaken call.
	addTool(server, &mcp.Tool{
		Name:        "release_next_work",
		Description: "Release the next highest-priority (earliest-CPT) pending work unit into a process path's pool, per the release policy. Rejected if the path has no pool, the pool is empty, or a release-fed pool is already at its WIP limit. Returns the released unit's id, CPT, and reference.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive, IdempotentHint: notIdempotent},
	}, d.releaseNextWork)
}

// addTool registers one tool. It centralises the cross-cutting concern
// every tool shares: a span per call, and mapping a handler error onto the
// span before returning it.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		var zero Out
		ctx, span := otel.Tracer(tracerName).Start(ctx, "mcp.tool "+tool.Name,
			trace.WithAttributes(
				attribute.String("mcp.tool.name", tool.Name),
			),
		)
		defer span.End()

		out, err := handle(ctx, in)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, zero, err
		}
		return nil, out, nil
	})
}

// parsePathId validates a client-supplied process-path id. Tool arguments come
// from a model and are untrusted, so an empty id is rejected rather than
// silently accepted.
func parsePathId(s string) (shared.PathId, error) {
	pathId, err := shared.NewPathId(s)
	if err != nil {
		return shared.PathId{}, fmt.Errorf("invalid pathId %q: must be a non-empty process path id", s)
	}
	return pathId, nil
}
