// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same application-layer use cases the HTTP and MCP adapters use. It is built
// on the official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0 over
// HTTP, with the Agent Card served at the well-known path.
//
// Per ADR-0046, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel — facts between contexts still travel exclusively
// as CloudEvents on Kafka (ADR-0027), including events raised by these skills
// through the transactional outbox (ADR-0014).
//
// Like the MCP adapter (ADR-0008), this package depends inward on the
// application layer (use cases and ports) and the domain only — never on an
// outbound adapter. The composition root (cmd/a2a) wires concrete repositories
// into the use cases. Domain structs never leak across the message boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the same use cases the HTTP and MCP adapters use; the adapter
// never constructs an outbound adapter itself.
type Deps struct {
	// SampleBacklog projects a path's backlog depth/WIP/mode and may publish
	// BacklogThresholdBreached / PathCapacityChanged — unchanged behaviour,
	// exactly as the HTTP telemetry endpoint and the MCP tool see it.
	SampleBacklog *usecases.SampleBacklog
	// RebalanceDecision returns the Drum-Buffer-Rope recommendation and may
	// publish PathThrottled or LaborReassignmentFlagged (ADR-0037).
	RebalanceDecision *usecases.RebalanceDecision
	// ReleaseNextWork admits the next priority-ordered unit into a pool; the
	// release policy and the pool's WIP invariant make a delegated release
	// safe by construction: it admits at most one unit and is rejected once
	// the WIP limit is reached or the pool is empty.
	ReleaseNextWork *usecases.ReleaseNextWork
}

// Card builds this agent's Agent Card (ADR-0046). The surface is two curated,
// goal-shaped skills — not one skill per endpoint, the same curation
// discipline ADR-0008 applied to MCP tools. flow_status composes the two read
// models a delegating agent needs to answer "how is this path doing, and what
// should be done about it"; release_next_work is the single state-changing
// delegation. The card declares no security schemes on purpose: this surface
// is unauthenticated by the same fleet-wide decision as REST and MCP
// (ADR-0016); schemes are added here, not in a redesign, if that decision is
// ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "wes-work-planning",
		Description: "Work Planning & Release agent for the Warehouse Execution System core: reads a process path's live flow status (backlog telemetry plus Drum-Buffer-Rope rebalance recommendation) and admits the next priority-ordered work unit into a path's pool. Delegates goals, answers from its own state; publishes resulting facts as CloudEvents on Kafka.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/wes-work-planning",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillFlowStatus,
				Name:        "Flow status for a process path",
				Description: "Composed read for one process path: backlog telemetry (pending depth, released WIP, feed mode, alarm state) plus the flow-balancing recommendation (NoActionNeeded, ThrottleUpstream, or ReassignLabor). Side effect: telemetry sampling publishes BacklogThresholdBreached when the pool is over its alarm threshold, and every ThrottleUpstream/ReassignLabor answer publishes PathThrottled/LaborReassignmentFlagged to the work-planning and analytics topics.",
				Tags:        []string{"wes", "flow-balancing", "telemetry", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"flow_status","arguments":{"pathId":"pick-zone-a"}}`,
				},
			},
			{
				ID:          skillReleaseNextWork,
				Name:        "Release the next work unit",
				Description: "Admits the next highest-priority (earliest CPT) pending work unit into a path's work pool, exactly as the REST and MCP surfaces do, and publishes WorkReleased through the transactional outbox. Safe by construction: at most one unit per call, rejected once the WIP limit is reached or the pool is empty.",
				Tags:        []string{"wes", "release", "write"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"release_next_work","arguments":{"pathId":"pick-zone-a"}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response work, so
// there is no long-running Task lifecycle to track in this first A2A
// surface. The response carries the caller's contextId (and taskId when the
// client referenced one) so a delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(deps, ctx, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler — the only transport this
// agent ships (ADR-0046), mirroring ADR-0008's "Streamable HTTP only"
// minimalism for MCP.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
