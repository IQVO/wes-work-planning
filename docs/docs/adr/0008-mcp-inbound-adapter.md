---
id: 0008-mcp-inbound-adapter
title: 8. Model Context Protocol as an inbound adapter, not a new service
sidebar_label: 8. MCP inbound adapter
sidebar_position: 8
description: "Expose this bounded context to the AI ecosystem via an MCP server built as a second driving adapter over the existing use cases — Streamable HTTP, official Go SDK, static bearer-key auth, curated intent-level tools."
---

# 8. Model Context Protocol as an inbound adapter, not a new service

## Status

**Accepted — auth section superseded by [ADR-0016](./0016-remove-rest-mcp-static-bearer-auth.md)**
(the static-bearer layer was removed; MCP, like REST, is unauthenticated by
decision). The reference implementation is
[`fulfillment-execution`](https://github.com/claudioed/fulfillment-execution);
this record adapts that decision to the **Work Planning & Release** context and
is bound by the estate-wide [MCP Governance Charter](../mcp/governance-charter.md).

**Addendum (2026-09-07) — made deployable.** Until this date `cmd/mcp` existed
only as code: the Dockerfile did not build it and the Helm chart had no MCP
workload, so the server had never run in the cluster. As of 2026-09-07 the
image ships `/app/mcp`, and `charts/wes-work-planning` gains an `mcp.*` block
(`mcp.enabled`, default `false`) rendering a `<release>-mcp` Deployment,
ClusterIP Service on port 8090, and a Secret carrying `MCP_READ_KEY` /
`MCP_READWRITE_KEY`. Two small additive changes to `cmd/mcp/main.go` were
required for that: an **unauthenticated `GET /healthz`** for the Kubernetes
probes (the bearer middleware previously answered every request, including
probes, with 401), and mounting the Streamable HTTP handler at **both `/` and
`/mcp`** so warehouse-ops-agent's `*_MCP_ENDPOINT` convention
(`http://<svc>-mcp.<ns>.svc.cluster.local:8090/mcp`) and the original root
mount both work. Deployment to the `warehouse` cluster is driven by
`warehouse-infra` (`mcp.enabled=true` + generated keys per service) in a
follow-up PR there.

## Context

The platform is being connected to the AI ecosystem (Claude, Cursor, ChatGPT,
agent frameworks). The interoperability standard those clients speak is the
**Model Context Protocol (MCP)**: a client discovers a server's *tools*
(model-callable functions), *resources* (read-only context), and *prompts*
(reusable templates), then an LLM decides which to call.

The forces:

- **There is already a clean action surface.** Every capability of this service
  is an application-layer **use case** (`internal/application/usecases`), one
  struct per use case, reached through ports. The `chi` HTTP adapter is a thin
  driving adapter over exactly those use cases. An AI client needs the same
  actions the HTTP client already has.
- **The domain must not learn about MCP.** ADR-0001's dependency rule is
  load-bearing: domain depends on nothing, application depends on domain,
  adapters depend inward. A protocol whose shape is set by an external LLM
  ecosystem is precisely the kind of concern that must stay in an adapter. The
  arch-go fitness tests (ADR-0007) already enforce this and cover the new
  package.
- **MCP has an idiomatic Go path now.** The official **MCP Go SDK**
  (`github.com/modelcontextprotocol/go-sdk`) is a Tier-1 SDK. Building the
  server in Go keeps it in the same language, module, and quality gate as the
  rest of the service — no Python sidecar, no second toolchain.
- **The spec is versioned aggressively.** Revisions in 2025-06, 2025-11, and
  2026-07 have already deprecated features (`roots`, `sampling`, `logging` —
  SEP-2577). Whatever is built will need to track a moving contract, so the SDK
  is version-pinned in `go.mod`.
- **Tools are model-controlled and can act.** Unlike an HTTP client driven by
  code we wrote, an LLM chooses *when* to call a tool and *with what arguments*.
  The spec's own guidance is emphatic: curate a small set of intent-level
  tools, treat tool invocation as requiring host consent, and guard
  state-changing tools most heavily. This is doubly true here — a `release`
  admits real work into a process path's pool.
- **This is an internal, non-user-facing deployment.** The servers run inside
  the `warehouse` kind cluster for agent and developer use, not on the public
  internet for end users. The MCP authorization spec permits a static bearer
  token for exactly this case; full OAuth 2.1 is required only when a server
  faces real end users.

## Decision

**We will expose this bounded context to the AI ecosystem through an MCP server
built as a second driving adapter over the existing use cases — leaving the
domain and application layers untouched.**

### The adapter, mirroring the HTTP one

A new `internal/adapters/inbound/mcp/` sits beside `internal/adapters/inbound/http/`:

```
internal/adapters/inbound/mcp/
  server.go      MCP Server wiring (Go SDK), capability registration, auth middleware
  tools.go       intent-level tool handlers -> call use cases; scope-gated, OTel span per call
  resources.go   read-model resources (scoped, not bulk)
  prompts.go     workflow prompts (operational SOPs)
  auth.go        bearer-key auth (Authenticator interface; OAuth-ready seam)
  mapping.go     tool I/O DTOs; domain structs never cross the boundary
```

It depends inward on `application` exactly as the HTTP adapter does. No MCP type
appears in `internal/domain/**` or `internal/application/**`. The tool handlers
call the **same** use case structs the HTTP handlers call — never a parallel
code path, never the domain directly.

### A separate `cmd/mcp` binary

The MCP server ships as its own composition root, `cmd/mcp/main.go`, reusing the
same repositories and in-memory-vs-Postgres wiring as `cmd/wes`. Two deployables
from one module: the HTTP service and the MCP server. This isolates blast
radius, lets the two scale independently, and keeps least-privilege clean (the
MCP process can be given a narrower footprint). It serves Streamable HTTP on its
own port (`MCP_ADDR`, default `:8090`).

### Streamable HTTP only

The single supported transport is **Streamable HTTP**. We do not ship stdio
builds; local desktop-client use goes through the same HTTP endpoint. One
transport is one thing to secure, trace, and test.

### Curated, intent-level tools — not one tool per endpoint

Tools are designed around decisions an agent makes, not around REST endpoints.
Mechanically wrapping all eight HTTP routes would overwhelm the model — the
documented number-one MCP anti-pattern. The surface for this context is three
tools built around the flow-balancing loop this service exists to run:

- `get_backlog_telemetry` (read) — wraps `SampleBacklog`: backlog depth,
  released WIP, feed mode (ReleaseFed / FlowFed), and whether the pool is over
  its alarm threshold, for one process path.
- `get_rebalance_recommendation` (read) — wraps `RebalanceDecision`: the
  Drum-Buffer-Rope action (`NoActionNeeded`, `ThrottleUpstream`, or
  `ReassignLabor`) for one path.
- `release_next_work` (write, annotated destructive) — wraps `ReleaseNextWork`;
  admits the next priority-ordered (earliest-CPT) pending unit into a pool. The
  release policy and the pool's WIP invariant make a model-invoked release safe
  by construction: it admits at most one unit and is rejected once the WIP limit
  is reached or the pool is empty. It returns a narrow DTO (id, CPT, ref) — the
  `WorkUnit` aggregate never leaks.

Resources expose the existing read model as a **scoped** context contract
(`telemetry://{pathId}/backlog`), never a database dump. Prompts encode
operational SOPs (`balance_flow`: how to read telemetry, when to throttle vs
release vs reassign, and when to escalate).

### Static bearer-key auth, behind an OAuth-ready seam

`auth.go` validates a per-client API key (from a Kubernetes Secret) on every
request; missing or invalid key returns `401` with a `WWW-Authenticate`
challenge; the key is never logged. Two key classes — read-only and read-write —
gate the write tool: a read-only key calling `release_next_work` is denied. The
middleware is an **interface** (`Authenticator`), so an OAuth 2.1
resource-server implementation (short-lived tokens, `.well-known` discovery, no
token passthrough) can drop in later without touching any tool handler.

### Reuse the existing observability

The adapter is instrumented with the same OpenTelemetry setup as the HTTP and
Kafka boundaries: a span per tool call (`mcp.tool <name>`, carrying the tool
name and required scope, with error status on failure or denial). MCP calls
appear in Jaeger and Grafana next to HTTP requests.

## Consequences

### Easier

- **The domain and application layers do not change at all.** MCP is purely
  additive; the dependency rule (ADR-0001) is preserved and checked by the
  existing arch-go fitness tests (ADR-0007).
- **One action surface, two protocols.** HTTP and MCP call the same use cases,
  so behaviour — including every invariant — is identical regardless of caller.
- **Model-invoked writes are safe by construction.** The release policy and the
  WIP invariant already reject an over-limit or empty-pool release; the typed
  domain error surfaces as a clean structured tool error.
- **It stays in Go, in one quality gate.** The MCP adapter is unit-tested to the
  same ≥90% bar, linted, and CI-gated like every other package.
- **The auth upgrade is contained.** Moving to OAuth later is an adapter change
  behind a stable interface, not a rewrite.

### Harder

- **A second deployable to run and secure.** `cmd/mcp` is another binary, image,
  Helm release, and ingress. The isolation is deliberate but it is real
  operational surface that did not exist before.
- **Auth is deliberately minimal.** A static bearer key is appropriate for an
  internal, non-user-facing server, but it does **not** cover user-facing,
  multi-tenant use. The servers must stay in-cluster until the OAuth seam is
  taken.
- **The MCP spec is a moving target.** Aggressive versioning and deprecations
  mean the SDK must be pinned and revisited; deprecated features (`roots`,
  `sampling`, `logging`) must be avoided in favour of tool parameters.
- **Tool curation is an ongoing discipline, not a one-time choice.** Nothing in
  the compiler stops a future PR from adding a tool per endpoint. The MCP
  governance charter holds the line; without it the surface degrades.
- **LLM-chosen arguments are untrusted input.** Every tool handler validates its
  inputs defensively (an empty or malformed `pathId` is rejected, never
  defaulted) — stricter than what the HTTP DTO layer assumes.
- **`release_next_work` is a state change an autonomous agent can trigger.** It
  is annotated destructive, scope-gated, and requires the read-write key, and
  the spec expects host-side consent, but the residual risk of an agent
  releasing work at the wrong moment is higher than for a human-driven HTTP
  call. The release policy and WIP invariant bound the damage; they do not
  eliminate the judgement risk.
