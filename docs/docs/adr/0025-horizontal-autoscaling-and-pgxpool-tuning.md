---
id: 0025-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0025-horizontal-autoscaling-and-pgxpool-tuning
title: 25. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning
sidebar_label: 25. HPA + pgxpool tuning
description: "ADR 0025 — Phase 3 (scalability) for wes-work-planning, ported from order-management's reference PR #110 (ADR-0026): an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-projector max 2, analytics-reports max 3, frontend max 3; mcp explicitly excluded for the same in-memory-session reason order-management found), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against the shared Postgres instance's real max_connections=100 ceiling now fronted by PgBouncer (warehouse-infra PR #43) on the OLTP path."
---

# 25. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record.
This is Phase 3 (scalability) of the fleet production-readiness plan for
`wes-work-planning`, ported from `order-management`'s reference PR #110
(ADR-0026), the same role that PR played for the rest of the ~9-service
fan-out. Phase 3.1 (the chart-selector-check fix so every Deployment's
`spec.selector` is scoped correctly) was already done before this PR
started — this chart's `api` Service selector is already scoped to
`app.kubernetes.io/component: api` (verified directly in
`charts/wes-work-planning/templates/service.yaml`), matching every other
fleet chart's pass of `warehouse-infra`'s chart-selector-check script.

Separately, `warehouse-infra` PR #43 (already merged) put PgBouncer
(transaction-pooling mode, `pool_size=12` per service database) in front
of the fleet's single shared Postgres instance, and re-pointed every
service's OLTP `DATABASE_URL` Secret at it — transparently at the DSN
level, no code change needed. This service's OLTP pool already benefits
from that cutover with zero changes here. Analytics DSNs deliberately
stay DIRECT to Postgres, per PgBouncer PR #43's own documented reasoning
(low QPS, a single Kafka-consumer connection each, no pooling benefit).

## Context

Before this change, `wes-work-planning`'s Helm chart had exactly one
`HorizontalPodAutoscaler` template, unconditionally targeting the `api`
Deployment only (a flat `autoscaling.enabled/minReplicas/maxReplicas/
targetCPUUtilizationPercentage` block, min 1 / max 5 / target 80% CPU)
— untested against the other four Deployments this chart renders
(`mcp`, `frontend`, `analytics-projector`, `analytics-reports`), and
never assessed for whether scaling `analytics-projector` past 1 replica
was even safe given its Kafka consumer-group membership.

No pool in the codebase set an explicit `pgxpool.Config.MaxConns`
either, so every pool — the OLTP pool (`internal/adapters/outbound/
postgres/connect.go`'s `Connect`, used by `cmd/wes` and `cmd/mcp`) and
the two analytics pools (`internal/adapters/outbound/analyticsstore/
pool.go`'s `NewPool` used by `cmd/wes-projector`, and `NewReadOnlyPool`
used by `cmd/wes-reports`) — ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query (a bad index, a
lock wait, an unbounded `report.ReportQuery` date range on
`postgres_report.go`'s `Query`) could hold a pooled connection
indefinitely, with nothing to cancel it.

### Finding: shared Postgres, PgBouncer, and the real connection ceiling

Confirmed fleet-wide facts, not re-derived from scratch: ALL 10 backend
services (including this one) share ONE Postgres server instance
(`warehouse-infra/terraform/postgres.tf`), each with its own logical
database/role, `max_connections=100` unmodified Bitnami default.
`warehouse-infra` PR #43 (merged) put PgBouncer in front of it in
transaction-pooling mode, `pool_size=12` per service database — every
service's OLTP `DATABASE_URL` Secret, including this service's, is
already re-pointed at PgBouncer, transparent at the DSN level. Analytics
DSNs deliberately stay direct to Postgres per that same PR's documented
reasoning.

This changes the shape of the client-side `MaxConns` decision from what
it would have been pre-PgBouncer: PgBouncer, not `pgxpool.MaxConns`, is
now what bounds the REAL server-side connection count against the
shared instance's 100-connection ceiling, regardless of how many
client-side pools or HPA-scaled replicas exist upstream of it. A
client-side `MaxConns` that looks generous in isolation (e.g. `api` at
its HPA ceiling of 4 replicas × 10 = 40 client-side pgx connections) is
therefore safe to leave at order-management's reference values rather
than shrinking it defensively — PgBouncer's own `pool_size=12` is the
real cap on live server-side connections for this service's OLTP path,
however many client-side pools ask for more. This is the deliberate
reason this PR does not repeat order-management ADR-0026's "80-of-100
worst case" arithmetic verbatim for the OLTP side: that arithmetic
predates PgBouncer's introduction in this fleet and answered "what if
PgBouncer didn't exist" — a question this service's rollout no longer
needs to ask for its OLTP pool, because PgBouncer already answers it at
the infra layer. The analytics pools (direct to Postgres, no PgBouncer)
still reason about their own worst-case contribution to the 100
connection ceiling directly, same as before.

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

Assessed each of the five Deployments this chart renders on its own
merits — statefulness, and (for Kafka consumers) consumer-group-id
convention — rather than blanket-enabling HPA everywhere, mirroring
order-management ADR-0026's exact methodology:

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/wes`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP. Its only per-process cache — the `PATH_CATALOGUE_SOURCE=kafka` local catalogue cache (`kafkacatalog.Consumer`) — is read-only and rebuilt from a full Kafka replay on every process start via a **per-process-UNIQUE** consumer group id (`kafkacatalog.uniqueConsumerGroup`, verified in `internal/adapters/outbound/kafkacatalog/consumer.go`) — safe to run N independent copies of by design; this is exactly the fleet's own documented "never share this kind of group" lesson, already correctly applied here. Its inbound integration-event consumer uses a **stable, shared** consumer group (`defaultConsumerGroup = "wes-work-planning"`, `KAFKA_CONSUMER_GROUP` override — `cmd/wes/main.go`) — the fleet's normal horizontally-scalable pattern, N replicas share partitions via ordinary Kafka group rebalancing. Nothing here breaks at N>1. |
| `analytics-projector` (`cmd/wes-projector`) | Yes, capped at **2**, not api's 4 | 1 | 2 | 70% | Its analytics Kafka consumer group (`kafka.AnalyticsConsumerGroup == "wes-analytics"`, ADR-0011) IS a **stable, shared** group with no per-instance uniqueness, verified in `internal/adapters/inbound/kafka/analytics_consumer.go`; the publisher partitions by the raising aggregate's own id (`kafka.AnalyticsPublisher.marshalAnalyticsData` — `PathId` or `WorkUnit` id), so 2 replicas legitimately share the topic's partitions via normal rebalancing — the same safe shape as `api`'s inbound consumer. Capped at 2, not left at api's 4, for the same two reasons order-management's ADR-0026 gives for its own projector: (a) aggregate-id-keyed partitioning means ordering is only guaranteed per-aggregate, so a wide fan-out buys little extra throughput for what is a lightweight `ON CONFLICT`-upsert workload (`PostgresProjection.applyCounter`/`upsertCounter`); (b) every write is already idempotent on `event_id` via `PostgresProjection.claim` (`analytics_processed_events`, ADR-0011), so correctness does not regress at 2, but there is no throughput case yet that justifies more. |
| `analytics-reports` (`cmd/wes-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`. |
| `frontend` (nginx-unprivileged serving the built SPA bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. The most trivially horizontally-scalable workload in this chart. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | `internal/adapters/inbound/mcp/server.go`'s `Handler` wraps the same `github.com/modelcontextprotocol/go-sdk/mcp` `StreamableHTTPHandler` order-management's MCP adapter uses, and keeps the identical **per-process, in-memory session state** keyed by the MCP protocol's own `Mcp-Session-Id` header (a real multi-request session, not just a TCP/HTTP connection). `charts/wes-work-planning/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` (e.g. `tools/call` following an earlier `initialize`) could land on a different pod than the one that created the session, which has never heard of it and would reject or silently start a new one. Fixing it for real needs either `sessionAffinity: ClientIP` (a partial mitigation only) or wiring the SDK's `StreamableHTTPOptions.EventStore` to a shared/external session store — a real code change, out of scope for this chart-and-pool-tuning PR. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists in `values.yaml` at all. Revisit if/when `cmd/mcp` adopts an external session store or the SDK's stateless mode. |

Every enabled block is namespaced under a single top-level `autoscaling:`
key in `values.yaml`
(`autoscaling.<api|projector|reports|frontend>.{enabled,minReplicas,
maxReplicas,targetCPUUtilizationPercentage}`), **every `enabled` value
defaults to `false`**. This PR makes per-workload HPA possible and
verified-correct; it deliberately does not turn any of it on — the
fleet enables each workload's HPA later, once, as a conscious rollout
decision, the same "ship the mechanism, not the behavior change" shape
this fleet's earlier idempotency/resilience phases used.

**No replicas-vs-HPA fight.** Each Deployment template guards its
`spec.replicas` field with `{{- if not .Values.autoscaling.<x>.enabled
}}` — when a workload's HPA is enabled, its Deployment renders with NO
`replicas` field at all (a hardcoded `replicas:` next to an active HPA
would otherwise fight it on every reconcile, most visibly right after a
`helm upgrade` resets it back to the chart's static value). Verified
directly with `helm template`:

- Default values (`analytics.enabled=true`, `frontend.enabled=true`,
  `mcp.enabled=true`, all `autoscaling.*.enabled` left at their
  `false` default) → 0 `HorizontalPodAutoscaler` resources render,
  every Deployment keeps its static `replicas: 1`.
- All four `autoscaling.*.enabled=true` → exactly 4
  `HorizontalPodAutoscaler` resources render (one per scalable
  workload, `mcp` has none by design), and none of those four
  Deployments has a `replicas:` field — `mcp`'s Deployment still does
  (`replicas: 1`).
- Only `autoscaling.api.enabled=true` → exactly 1 HPA renders, only the
  `api` Deployment loses its `replicas:` field; `projector`/`reports`/
  `frontend`/`mcp` keep theirs untouched. Mixed enablement is safe and
  independent per workload, as designed.

`helm lint` passes; `go vet`/`gofmt`/`make check`/`make arch-test` all
pass unchanged (no Go code path is affected by the chart changes).

### 2. pgxpool MaxConns

All three pools now set an explicit `pgxpool.Config.MaxConns` instead of
inheriting the CPU-derived library default (`max(4, runtime.NumCPU())`),
matching order-management ADR-0026's exact numbers:

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.Connect`) | `cmd/wes` (`api`), `cmd/mcp` (`mcp`) | **10** | Fronted by PgBouncer (transaction pooling, `pool_size=12`) since `warehouse-infra` PR #43 — already wired via the existing `DATABASE_URL` Secret, no code change needed. PgBouncer, not this client-side setting, bounds the real server-side connection count, so `MaxConns` can stay a generous client-side ceiling matching order-management's identical value rather than being squeezed down defensively. At `api`'s HPA ceiling of 4 replicas, 4 × 10 = 40 client-side pgx connections — comfortably absorbed by PgBouncer's `pool_size=12` regardless. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/wes-projector` | **5** | Direct to Postgres (no PgBouncer, per PR #43's low-QPS/single-consumer reasoning). The projector has no HPA ceiling above 2 (see the table above) and does single-row `ON CONFLICT` upserts against one `(path_id, hour_bucket)` key at a time; a small, flat pool is enough. At 2 replicas × 5 = 10 connections against the analytical database. |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/wes-reports` | **5** (`ReportsMaxConns`) | Direct to Postgres, same reasoning. `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections against the analytical database. |

Because the OLTP path is now fronted by PgBouncer, this ADR does not
repeat order-management ADR-0026's "sum every workload at its HPA
ceiling against the shared instance's 100 connections" worst-case
arithmetic for the OLTP pool — PgBouncer's `pool_size=12` is the
relevant real ceiling there, not the client-side `MaxConns` × replica
count. The analytics pools (direct to Postgres, uncapped by PgBouncer)
still contribute directly to the shared instance's 100-connection
budget: at every analytics workload's own HPA ceiling simultaneously,
this service's analytics footprint is `2 × 5 (projector) + 3 × 5
(reports)` = **25** connections, alongside whatever the other 9 fleet
services' own analytics pools draw from the same instance. That is a
real, documented residual number, not a solved one — it depends on what
the other services' own Phase 3 PRs choose for their analytics pools,
the same open point order-management ADR-0026 left for the fleet.

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.
AfterConnect`, running `SET statement_timeout = '<value>'` on every new
physical connection as it's established (not per-query, so it survives
connection reuse across pooled acquisitions). Values match
order-management's identical pools exactly, because the query shapes
are the same kind (single-aggregate OLTP vs. tight-loop upsert writer
vs. wide-range reader):

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (`ReceiveCharge`, `CommitPlan`, `ReleaseWorkUnit`, `RebalanceFlow`, `GetWorkPoolView`, and friends) is a single-aggregate read/write keyed by id, normally low-single-digit milliseconds. 5s is roughly 1000x that — generous headroom for real transient contention without ever being a normal-path concern, while bounding the absolute worst case tightly since this is the interactive, latency-sensitive path and also the pool with the most client-side connections (40 at max HPA scale) to protect. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying a backlog after a redeploy issues upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request. Still bounded — an unbounded query here could stall the projector's own progress on the analytics pipeline, which is exactly why it isn't left unbounded either. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The throughput report aggregates rows across a caller-chosen `[From, To)` time range (`postgres_report.go`'s `Query`) — wider than the OLTP side's always-single-aggregate-by-id shape — so it gets more headroom, but still a hard ceiling: a caller-supplied wide date range must not be able to hold a reports connection forever. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`), not a mock and not just reading `pg_settings`:

- `TestConnectWithLimits_AppliesStatementTimeoutToNewConnections` —
  opens a pool against a real `postgres:16-alpine` container with a
  short test-only timeout (200ms, via the shared `ConnectWithLimits`
  the production `Connect` wraps), confirms `SHOW statement_timeout`
  reads back `200ms` on a freshly acquired connection, then runs
  `SELECT pg_sleep(2)` and asserts Postgres itself cancels it (SQLSTATE
  `57014`, "canceling statement due to statement timeout") rather than
  letting it run the full 2s — proving the setting is genuinely
  enforced server-side, not merely set and ignored — and finally
  confirms the pool is still usable afterward (the cancelled statement
  doesn't poison the connection).
- `TestConnectWithLimits_AppliesMaxConns` — acquires exactly `maxConns`
  connections from a pool configured with `MaxConns=2`, then asserts a
  further `Acquire` blocks until `context.DeadlineExceeded`, proving
  `MaxConns` is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container.

## Consequences

- HPA is now possible, correct, and independently verified per
  workload for four of this chart's five Deployments — but **off by
  default everywhere**. Merging this PR changes nothing about
  production replica counts; `MaxConns`/`statement_timeout` are the
  only behavior change that takes effect on deploy, and both are
  conservative relative to today's unbounded defaults (they can only
  reduce, never increase, worst-case connection usage and hung-query
  duration).
- `mcp` remains explicitly un-autoscaled, with the exact reason
  (in-memory session state, no sticky routing) recorded here and in
  `values.yaml`'s comments, so a future contributor doesn't
  mechanically copy `api`'s HPA block onto it without re-solving the
  session-affinity problem first.
- The OLTP pool's `MaxConns=10` is deliberately generous relative to
  what it would have been sized to pre-PgBouncer, because PgBouncer
  (warehouse-infra PR #43) is now the real server-side bound. If
  PgBouncer were ever removed or misconfigured, this pool's real
  worst-case server-side footprint would jump to 4 × 10 = 40
  connections against the shared instance directly — a risk this ADR
  accepts explicitly because PgBouncer is a already-merged, verified
  fleet-wide fixture, not a future dependency.
- The analytics pools' 25-connection worst-case footprint (at both
  workloads' proposed HPA ceilings simultaneously) is an honest,
  documented number, not a solved one — it depends on what the other 9
  fleet services' own analytics pools draw from the same shared
  instance, the same open point order-management ADR-0026 left
  unresolved for the fleet as a whole.
- A read replica for the analytics/reports read path is explicitly OUT
  OF SCOPE for this PR — not evaluated, not designed, not decided.
  Revisit only once actually needed and confirmed by the person
  requesting it.
- `max_connections=100` itself is an unexamined Bitnami chart default,
  not a value anyone has deliberately sized for this fleet's real
  demand. This ADR treats it as a hard external constraint to work
  within, not something in scope to change — same treatment as
  order-management ADR-0026 and PgBouncer PR #43 already gave it.
