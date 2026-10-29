---
id: 0046-a2a-inbound-adapter
slug: /adr/0046-a2a-inbound-adapter
title: 0046. Agent2Agent as an inbound adapter — this context as an A2A agent
sidebar_label: 0046. A2A inbound adapter
description: ADR 0046 — expose this bounded context to the fleet's agent ecosystem as a third driving adapter over the existing use cases: official a2a-go SDK, JSON-RPC 2.0 over HTTP, Agent Card at the well-known path, curated goal-shaped skills, delegation only — facts still travel as CloudEvents on Kafka.
---

# 0046. Agent2Agent as an inbound adapter — this context as an A2A agent

## Status

**Accepted**

## Context

The fleet is growing an agent ecosystem on top of the event-driven estate:
each bounded context already speaks MCP ([ADR-0008](./0008-mcp-inbound-adapter.md))
so models and operator tooling can drive its use cases, and the estate's
facts flow exclusively as CloudEvents 1.0 on Kafka
([ADR-0027](./0027-cloudevents-mandatory-event-envelope.md)). What is missing is a
protocol for **agent-to-agent delegation**: one context's process (a saga, an
exception handler, the ops copilot) asking another context's agent to take a
goal-shaped piece of work — "how is this path doing?", "admit the next unit" —
without either side learning the other's internals.

The industry answer is the **Agent2Agent (A2A) protocol**: an open standard
(donated by Google to the Linux Foundation; TSC includes AWS, Microsoft,
Salesforce, SAP) for exactly this. Its model fits the estate's rules
unnaturally well:

- **Opaque agents.** An A2A server is a black box: no shared memory, no
  shared logic, no database handles. That is already the bounded-context
  contract — this decision just gives it a wire protocol.
- **Agent Card discovery.** A JSON card at `/.well-known/agent-card.json`
  advertises identity, endpoint, and skills — declarative discovery, the same
  posture as the fleet's AsyncAPI specs and Kong routes, no runtime registry.
- **MCP is vertical (agent→tools), A2A is horizontal (agent→agent).** The
  protocol's own positioning: MCP deepens one agent, A2A connects peers. This
  context keeps its MCP server for models and tooling; A2A adds the peer
  channel.
- **Microsoft's multi-agent guidance** recommends A2A for cross-platform
  agent messaging with published contracts (agent cards, task/artifact
  model), workflow-oriented orchestration for deterministic business steps,
  and least-privilege, typed payloads between agents.

The forces against a naive adoption:

- **A2A must not become a data-sync channel.** The estate's rule is hard:
  contexts replicate state by consuming each other's CloudEvents into local
  projections; several contexts outright ban live sibling lookups. If A2A
  became a query bus over other contexts' databases, it would re-create the
  coupling Kafka dissolved.
- **The domain must not learn about A2A.** ADR-0001's dependency rule is
  load-bearing, and the arch-go fitness tests (ADR-0007) already enforce it.
- **The spec is young and versioned aggressively** (0.2 → 0.3 → 1.0 within
  2025–2026). Whatever is built must be pinned and transport-minimal.
- **Agent-chosen input is untrusted input**, the same lesson ADR-0008
  recorded for model-chosen tool arguments.

## Decision

**We will expose this bounded context as an A2A agent through a third driving
adapter over the existing use cases — leaving the domain and application
layers untouched — with JSON-RPC 2.0 over HTTP as the only transport, and
delegation (never data sync) as its only job.**

### The adapter, mirroring the HTTP and MCP ones

A new `internal/adapters/inbound/a2a/` sits beside `inbound/http/` and
`inbound/mcp/`:

```
internal/adapters/inbound/a2a/
  server.go    Agent Card + executor + JSON-RPC handler wiring (a2a-go SDK)
  skills.go    goal-shaped skill dispatch -> call use cases; OTel span per skill
  mapping.go   skill I/O DTOs and RFC 7807-shaped problem answers
```

It depends inward on `application` exactly as the other adapters do. No A2A
type appears in `internal/domain/**` or `internal/application/**`. Skill
handlers call the **same** use case structs the HTTP and MCP handlers call —
never a parallel code path, never the domain directly. One action surface,
three protocols.

### A separate `cmd/a2a` binary

The agent ships as its own composition root, `cmd/a2a/main.go`, reusing the
same Postgres repositories, classification lookup, and outbox publisher
wiring as `cmd/mcp`. Three deployables from one module (OLTP API, MCP server,
A2A agent), each independently scalable and least-privileged. It serves
JSON-RPC 2.0 over HTTP on `A2A_ADDR` (default `:8093`), the Agent Card at the
well-known path, and an unauthenticated `GET /healthz` for the probes. Boot
configuration is validated up front (ADR-0044 plus `A2A_AGENT_URL`, the
absolute URL the card advertises).

### JSON-RPC 2.0 over HTTP only

The single supported transport is JSON-RPC 2.0 over HTTP — mirroring
ADR-0008's "Streamable HTTP only" minimalism. No gRPC binding, no SSE
streaming advertised in this first surface (the card declares
`streaming: false`): both curated skills are transactional request/response
work. One transport is one thing to secure, trace, and test.

### A message-only agent with a strict, documented wire contract

The executor maps each incoming message to a skill and answers with exactly
one agent `Message` (A2A's term: a message-only agent — no long-running Task
lifecycle to track yet). The request contract is deliberate and machine
checked: the first meaningful part carries, as a Data part (or a Text part
holding the same JSON), `{"skill":"<id>","arguments":{...}}`. Anything else —
unknown skill, malformed JSON, missing or invalid `pathId` — is **answered**
as an RFC 7807-shaped problem inside the envelope (ADR-0005 posture), never
silently defaulted: a malformed delegation is a business answer for the
calling agent, not a transport failure. The reply carries the caller's
`contextId` (and `taskId` when present) so a delegating saga can group its
interactions.

### Curated, goal-shaped skills — delegation, not a query bus

Two skills, the same curation discipline ADR-0008 applied to MCP tools:

- `flow_status` (composed read) — SampleBacklog **plus** RebalanceDecision in
  one answer: "how is this path doing and what should be done about it". The
  two use cases' event side effects (BacklogThresholdBreached,
  PathThrottled, LaborReassignmentFlagged — ADR-0037) are unchanged.
- `release_next_work` (write) — the single state-changing delegation, safe by
  construction: the release policy and the pool's WIP invariant bound what a
  mistaken delegation can do, and `WorkReleased` reaches
  fulfillment-execution through the transactional outbox (ADR-0014) exactly
  as a REST- or MCP-raised release does.

The boundary that keeps this from becoming a data-sync channel: **skills
answer from this context's own state through its own use cases; they never
proxy another context, and no skill exists whose purpose is replicating
data.** Facts between contexts continue to travel exclusively as CloudEvents
on Kafka — including the events these skills raise.

### Unauthenticated, like every fleet surface

No auth on the A2A surface, by the same fleet-wide decision that removed the
static-bearer layer from REST and MCP
([ADR-0016](./0016-remove-rest-mcp-static-bearer-auth.md)): the in-cluster
ClusterIP boundary is the access control. The card declares no security
schemes on purpose — adding them later is a card change, not a redesign. Do
not re-introduce per-request auth here without re-adopting it fleet-wide.

### Chart, image, and deployment

The image ships `/app/a2a` alongside the other binaries; the chart gains an
`a2a.*` block (`a2a.enabled`, default `false`) rendering a `<release>-a2a`
Deployment and ClusterIP Service on port 8093, with an opt-in Kong route
`/api/a2a/<context>` rewritten to `/` (the well-known card path stays
reachable under the same prefix). Deployment to the `warehouse` cluster is
driven by `warehouse-infra` flipping `a2a.enabled=true`, exactly as for MCP.

### Reuse the existing observability

Every skill runs inside an OTel span (`a2a.skill <id>`, mirroring
`mcp.tool <name>`), with error status on problem answers; the HTTP surface
carries the same otelchi request spans and metrics as the REST router
(ADR-0013). A2A skill calls appear in Jaeger and Grafana next to HTTP, Kafka,
and MCP traffic.

## Consequences

### Easier

- **The domain and application layers do not change at all.** A2A is purely
  additive; the dependency rule is preserved and checked by the existing
  arch-go fitness tests.
- **One action surface, three protocols.** HTTP, MCP, and A2A call the same
  use cases, so every invariant — including the WIP limit and the outbox
  atomicity — behaves identically regardless of caller.
- **Delegated writes are safe by construction.** The release policy and pool
  invariant bound `release_next_work`; domain rejections come back as
  structured problems the calling agent can reason about.
- **Discovery is declarative.** The card is a static JSON document served at
  a well-known path — no registry to run, no new moving part in the control
  plane.

### Harder

- **A third deployable to run.** Another binary, container, Deployment, and
  Service per context adopting this pattern; the fleet's Terraform must
  enable it service by service. The isolation is deliberate but real.
- **The A2A spec is a moving target.** The SDK (`a2aproject/a2a-go/v2`) is
  pinned in `go.mod` and must be revisited; the JSON-RPC method names and
  part wire shapes already changed between protocol 0.x and 1.0.
- **Skill curation is an ongoing discipline.** Nothing stops a future PR from
  growing one skill per endpoint; the boundary — delegation only, never data
  sync — must be enforced in review, not by the compiler.
- **Agent-chosen input is untrusted.** Every skill handler validates its
  arguments defensively; a malformed request is answered as a problem, never
  guessed at.
- **Message-only is a Phase-1 constraint, not a permanent one.** When a
  genuinely long-running delegation appears (wave-less replan across paths,
  negotiated rebalancing), the agent must grow Task lifecycle and push
  notifications — a planned, contained extension of the adapter, but a new
  protocol surface to test and secure.
