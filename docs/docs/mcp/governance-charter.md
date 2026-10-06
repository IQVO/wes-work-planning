---
id: governance-charter
title: MCP Governance Charter
sidebar_label: MCP Governance Charter
description: "The estate-wide rules every warehouse-systems MCP server follows — tool curation, naming, annotations, auth scopes, audit, and the review gate. Federated: global standards, domain-owned servers."
---

# MCP Governance Charter

This charter is the **federated computational governance** for MCP across
warehouse-systems: one set of global standards, enforced the same way in every
repository, while each bounded context owns its own server. It is the MCP
counterpart to the platform's existing 5-stage quality gate and its ADR
discipline. `fulfillment-execution` is the reference implementation
(see [ADR-0008](../adr/0008-mcp-inbound-adapter.md)); the other contexts —
including `inventory-storage`, `wes-work-planning`, `workforce-management`,
`facility-layout` — copy it.

Keywords **MUST**, **SHOULD**, **MAY** are used per RFC 2119.

## 1. Architecture rules (non-negotiable)

1. An MCP server **MUST** be an inbound adapter at
   `internal/adapters/inbound/mcp/`, depending inward on `application` only.
2. Tool handlers **MUST** call existing application-layer **use cases**. They
   **MUST NOT** touch the domain layer directly, run SQL, or duplicate use-case
   logic. No MCP type may appear in `internal/domain/**` or
   `internal/application/**` — the arch-go fitness tests (ADR-0006) enforce the
   dependency rule and **MUST** be extended to cover the mcp adapter.
3. Each server **MUST** ship as a separate `cmd/mcp` binary reusing the service's
   existing composition wiring.
4. Transport **MUST** be Streamable HTTP. stdio builds **MUST NOT** be shipped.
5. The official Go SDK (`github.com/modelcontextprotocol/go-sdk`) **MUST** be
   used and **MUST** be version-pinned in `go.mod`. Deprecated MCP features
   (`roots`, `sampling`, `logging`; SEP-2577) **MUST NOT** be used — prefer tool
   parameters, resource URIs, and configuration.

## 2. Tool curation — the surface is a product

The single most important rule: **expose intent-level tools, not one tool per
REST endpoint.** Tools are designed around decisions an agent makes.

1. A tool **MUST** map to an outcome an agent wants (`diagnose_stuck_tasks`),
   not to a transport route (`post_tasks_id_complete`).
2. A server **SHOULD** expose **no more than 8 tools**. A PR that pushes a server
   over that count **MUST** carry an explicit justification in its description
   and be approved by a second reviewer. This is the "curated surface" review
   rule; the Phase-6 CI lint enforces the count mechanically.
3. Bulk read access **MUST NOT** be exposed as a tool. Large read models are
   **resources**, scoped to a decision (see §5).

## 3. Naming conventions

| Element | Convention | Example |
| --- | --- | --- |
| Tool name | `snake_case`, `verb_noun`, intent-level | `get_queue_status` |
| Resource URI | `<kind>://<context>/<scope>` | `queue://fulfillment/PICK/status` |
| Auth scope | `mcp:<context>:<read\|write>` | `mcp:fulfillment:write` |
| Prompt name | `snake_case`, names the SOP | `triage_backlog` |

`<context>` is the bounded-context short name (`fulfillment`, `inventory`,
`work-planning`, `workforce`, `facility`).

## 4. Tool annotations (mandatory)

Every tool **MUST** declare annotations so a host can reason about risk before
letting a model call it:

1. A **read** tool **MUST** be annotated read-only (no state change).
2. A **write** tool **MUST** be annotated destructive and **MUST** require the
   `:write` scope.
3. Annotations and descriptions are treated as **untrusted** across servers; a
   host **MUST NOT** rely on another server's annotations for its own safety
   decisions. (Within our own trusted servers they are authoritative.)
4. Descriptions **MUST** state what the tool does and its side effects plainly —
   the description is read by the model and is part of the safety surface.

## 5. Resources — scoped context contracts

1. A resource **MUST** be scoped to a decision, backed by an existing read model
   / projection. It **MUST NOT** dump an entire table, config, or log.
2. Resources are read-only. Anything that changes state is a write **tool**, not
   a resource.

## 6. Prompts — operational SOPs

Prompts **SHOULD** encode operational discipline the model should follow: how to
interpret a tool result, when to stop and escalate, what "done" means. They are
user-initiated and carry the least risk, but they standardize agent behaviour
across clients and **SHOULD** be used rather than leaving procedure implicit.

## 7. Security & authorization (current posture: no IdP)

:::warning[This repository's actual state]
The static-bearer layer described in rules 1–4 below was implemented and then
**removed fleet-wide**
([ADR-0016](../adr/0016-remove-rest-mcp-static-bearer-auth.md), superseding
[ADR-0015](../adr/0015-rest-identity-static-bearer-scopes.md)). This
service's `cmd/mcp` server is currently **unauthenticated**: there are no API
keys, no read/read-write scope check, and no `401`/`403` responses. It also
does not yet emit the per-call audit record of §9 or rate-limit write tools
(§8.2). The rules below remain the charter's target standard.
:::

Per ADR-0008, the current posture for these internal, non-user-facing servers:

1. Every request **MUST** be authenticated with a static bearer API key held in
   a Kubernetes Secret. Missing/invalid key **MUST** return `401`.
2. Two key classes **MUST** exist: read-only and read-write. A `:write` tool
   **MUST** reject a read-only key (`403`), audited.
3. The API key **MUST NEVER** be logged. No secret, token, or key may appear in
   any log line.
4. The auth check **MUST** be a middleware behind a stable interface, so the
   OAuth 2.1 upgrade is a drop-in with no change to tool handlers.
5. Servers **MUST** remain reachable only in-cluster; ingress **MUST** enforce
   HTTPS. A server **MUST NOT** be exposed to public/end-user traffic until the
   OAuth 2.1 resource-server seam is taken (a future ADR-0009).
6. When a tool must call another service, the server **MUST** authenticate as
   its own client for that hop and **MUST NOT** pass a client token through
   (confused-deputy prevention) — applies the day any upstream hop exists.

## 8. Guardrails (regardless of auth)

1. Every tool handler **MUST** validate its inputs defensively — the caller is a
   model, arguments are untrusted.
2. Write tools **MUST** be rate-limited.
3. Domain errors **MUST** surface as clean structured tool errors, mapped from
   RFC 7807 (ADR-0005). The existing invariants (at-most-once, ownership, lease,
   SLAM tolerance) are the safety net for model-invoked writes and **MUST NOT**
   be bypassed.

## 9. Auditability

Every tool call **MUST** emit an audit record with, at minimum:

- `client_id` (which key/caller),
- `tool` name,
- `scope` presented,
- `outcome` (allowed / denied / error),
- timestamp and trace id.

Audit records **MUST** carry the OpenTelemetry trace id so a call links to its
span. The adapter **MUST** be instrumented with the platform's existing OTel
setup: a span per tool call plus invocation and denial counters, visible in
Jaeger and Grafana alongside HTTP.

## 10. Quality gate (same bar as the rest of the service)

1. Tool handlers **MUST** be unit-tested (table-driven, in-memory adapters) to
   the platform's ≥90% coverage bar, plus at least one transport-level test.
2. The MCP adapter **MUST** pass `make check` (fmt, vet, build, lint, test) and
   the arch-go fitness tests.
3. **Phase-6 CI gate (planned):** a workflow that lints tool schemas, enforces
   the naming conventions and mandatory annotations, and fails a PR that exceeds
   the tool-count budget without justification — the left-shift equivalent of
   `make check` for the MCP surface.
4. **Eval gate (E1–E3):** the tool surface **MUST** pass the eval suites in
   `internal/adapters/inbound/mcp/eval_*_test.go` and
   `evalsuite_test.go`, all plain `go test`s inside the CI `test` job:
   - **E1 — schema & metadata** (`eval_governance_test.go`): every
     advertised tool's input schema resolves as a JSON Schema, accepts a
     schema-shaped arguments object, and REJECTS wrong-typed values (it
     constrains model input, not just decorates it); every parameter
     carries a non-empty description; the advertised surface matches
     `testdata/tool_registry.golden`; and this repo's tools are present,
     correctly credited, and globally unique in
     `testdata/fleet_tool_snapshot.golden` (the federated registry kept
     identical across all fleet repos — a model host mounts several of
     these servers together, so tool names MUST NOT collide). The one
     conditional registration is pinned too:
     `get_release_throughput_report` appears only when a reports client
     is configured (ADR-0011), so the golden registry pins the
     default-deps surface and a dedicated eval pins the conditionality.
   - **E2 — wire conformance** (`eval_conformance_test.go`): over the real
     Streamable HTTP handler — initialize handshake carries server info
     and non-empty instructions; unknown tools, wrong-typed arguments,
     unknown extra arguments, unknown resources and prompts are rejected;
     the resource template and prompts are discoverable; a closed session
     fails loudly.
   - **E3 — behavioral evals** (`evalsuite_test.go` +
     `testdata/features/mcp_tools.feature`): Gherkin scenarios driving
     `tools/call` with model-realistic arguments (stray keys, wrong types,
     unknown ids) against seeded state, pinning structured results and
     side effects (domain events, state visible through other tools).

### Pinned behavioral contracts the evals found

- Typed tool schemas are **strict** (`additionalProperties: false`, the
  SDK default): stray model-generated argument keys are rejected with a
  validation error naming the key, not silently ignored.
- Error shapes split at the tool boundary: argument-schema violations and
  tool-handler errors surface as **tool-level error results**
  (`IsError: true`), while calling a never-advertised tool (e.g. the
  report tool on a deployment without the reports service) is a
  **protocol-level JSON-RPC error**. Hosts must handle both.
- Unknown-path reads (`get_backlog_telemetry`,
  `get_rebalance_recommendation`) are clean tool errors (`not found`) —
  unlike inventory-storage's `check_availability`, which answers zero for
  an unknown SKU. A path with no provisioned pool has no projection to
  report, so ambiguity is surfaced rather than masked as empty telemetry.
- `get_backlog_telemetry` is annotated read-only, yet sampling a flow-fed
  pool over its alarm threshold publishes `BacklogThresholdBreached`, and
  `get_rebalance_recommendation` publishes `PathThrottled` /
  `LaborReassignmentFlagged` alongside its recommendation — the same event
  side effects the HTTP telemetry/rebalance endpoints have (the use cases
  are reused unchanged). Pinned so the read-only annotation is read as
  "no state mutation", not "no events".
- `release_next_work` is bounded by the pool's enforced WIP invariant: at
  the limit the release is rejected (`WIP limit reached`) rather than
  queued — a mistaken model call cannot flood a release-fed path.

## 11. Changing this charter

This charter is versioned with the docs. A change to a global standard **MUST**
be proposed as a PR and, because it binds every context, **SHOULD** be
recorded as an ADR when it changes an architecturally significant rule.
