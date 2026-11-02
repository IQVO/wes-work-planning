---
id: index
title: Architecture Decision Records
sidebar_label: About ADRs
sidebar_position: 0
slug: /adr/
description: Why these records exist, the template they follow, and how to propose a new one.
---

# Architecture Decision Records

An **Architecture Decision Record** captures one architecturally significant
decision: the forces that applied, the choice that was made, and what that
choice costs. These records use
[Michael Nygard's template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions),
the de facto standard: one Markdown file per decision, numbered `0001-`,
`0002-`, …, never renumbered.

## Why they exist

Code shows *what* was decided. It almost never shows *what else was considered*
or *what the decision cost* — and those are exactly the things a future reader
needs before changing it.

A record is written when the answer to "why is it like this?" would otherwise
be lost, or when someone would reasonably assume the current design was an
accident. Two of the records here exist precisely because the design looks
*wrong* at first glance:
[ADR-0006](./0006-labor-plan-view-not-shift-plan.md) (two types with the same
name that must not be merged) and
[ADR-0003](./0003-flow-balancing-as-domain-service.md) (a decision engine with
no scheduler).

## The records

| # | Title | Status |
|---|---|---|
| [0001](./0001-hexagonal-ports-and-adapters.md) | Hexagonal (ports & adapters) architecture | Accepted |
| [0002](./0002-waveless-continuous-release.md) | Waveless continuous release over wave-based batching | Accepted |
| [0003](./0003-flow-balancing-as-domain-service.md) | Flow balancing as a domain service, not a scheduled batch job | Accepted |
| [0004](./0004-kafka-integration-events.md) | Kafka integration events with a shared envelope and a CloudEvents type convention | Superseded by 0027 (envelope) |
| [0005](./0005-rfc-7807-problem-details.md) | RFC 7807 `application/problem+json` for every error response | Accepted |
| [0006](./0006-labor-plan-view-not-shift-plan.md) | Project Workforce's `ShiftPlanCommitted` into a separate read model, not our `ShiftPlan` aggregate | Accepted |
| [0007](./0007-arch-go-fitness-tests.md) | Executable architecture fitness tests with arch-go | Accepted |
| [0008](./0008-mcp-inbound-adapter.md) | Model Context Protocol as an inbound adapter, not a new service | Accepted — auth section superseded by 0016; telemetry tools' annotations amended by 0037 |
| [0009](./0009-product-classification-propagation-to-work-released.md) | Propagate inventory-storage's `ProductClassification` onto `WorkReleased` via a synchronous read at release time | Accepted — live lookup superseded by 0035 |
| [0010](./0010-gift-wrap-as-a-work-released-characteristic.md) | Gift wrap as a caller-stated `WorkReleased` characteristic, not a product attribute | Accepted |
| [0011](./0011-analytical-data-product.md) | Per-service analytical data product (report) via a separate analytics topic | Accepted |
| [0012](./0012-process-path-catalogue-validation.md) | Process-path catalogue validation, mirroring fulfillment-execution's ADR-0017 | Accepted — amended by 0030 |
| [0013](./0013-standard-metrics-convention.md) | Standard metrics convention across the fleet: Tier 1 OTel baseline + Tier 2 business-metric naming | Accepted |
| [0014](./0014-transactional-outbox.md) | Transactional outbox feeding both the integration and the analytics topic, with an in-process relay | Accepted — per-key ordering amended by 0039; in-memory/log/direct paths removed by 0044 |
| [0015](./0015-rest-identity-static-bearer-scopes.md) | REST identity: adopt the fleet's static bearer keys with read/read-write scopes (warehouse-ops-agent ADR 0005) | Superseded by 0016 |
| [0016](./0016-remove-rest-mcp-static-bearer-auth.md) | Remove the REST + MCP static-bearer auth layer (fleet decision reversed) | Accepted |
| [0017](./0017-travel-distance-lookup-on-commit-shift-plan.md) | Read facility-layout's travel graph once, at shift-plan-commit time, to enrich `PathPlan` | Accepted |
| [0018](./0018-path-capacity-changed.md) | Publish per-path, per-CPT remaining admission capacity as `PathCapacityChanged`, correlated by CPT cutoff timestamp | Accepted |
| [0019](./0019-labor-plan-committed-shift-plan-reconciliation.md) | Reconcile our committed `PathPlan` against Workforce's `LaborPlanObserved`, triggered on either side's commit | Accepted |
| [0020](./0020-flowfed-path-observed-throughput-signal.md) | FlowFed paths stay `Known=false` permanently; an observed-throughput signal is proposed alongside, not instead | Accepted |
| [0021](./0021-cloudevents-envelope-migration.md) | Migrate `warehouse.work-planning.events` to a CloudEvents 1.0 structured envelope via a dual-read/dual-write bake period; companion to fulfillment-execution's own envelope-migration ADR | Superseded by 0027 |
| [0022](./0022-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for `POST /paths/{pathId}/work-units`, ported from order-management's reference implementation | Accepted |
| [0023](./0023-resilience-circuit-breakers-retry-dlq-shutdown.md) | Per-dependency circuit breakers, read-only retry, Kafka DLQ, and graceful shutdown hardening | Accepted — product-classification breaker and retry retired by 0035 |
| [0024](./0024-kafka-hash-balancer-partition-affinity.md) | Switch every outbound Kafka writer's `Balancer` from `LeastBytes` to `Hash`, so the existing per-aggregate `Message.Key` actually drives partition placement | Accepted |
| [0025](./0025-horizontal-autoscaling-and-pgxpool-tuning.md) | Per-workload `HorizontalPodAutoscaler` (api/projector/reports/frontend, mcp excluded) and `pgxpool.MaxConns`/`statement_timeout` tuning, ported from order-management's reference PR #110 | Accepted |
| [0026](./0026-migrations-direct-postgres-connection.md) | Run golang-migrate against a direct Postgres connection (`MIGRATIONS_DATABASE_URL`), not PgBouncer | Accepted |
| [0027](./0027-cloudevents-mandatory-event-envelope.md) | CloudEvents 1.0 as the mandatory event envelope on every topic (no flat envelope, no dual mode) | Accepted |
| [0028](./0028-processed-event-mark-atomic-with-handling.md) | The processed-event mark commits atomically with the handling it guards; `TaskCompleted` for an unknown work unit is a logged skip | Accepted — error enumeration amended by 0043 |
| [0029](./0029-work-pool-optimistic-concurrency.md) | Optimistic concurrency (version column + retry) for the WorkPool aggregate; release heals entries left behind by a lost update | Accepted |
| [0030](./0030-kafka-sourced-path-catalogue.md) | Kafka-sourced process-path catalogue (`PATH_CATALOGUE_SOURCE=kafka`) behind `ports.PathCatalogue`, and boot-time dial retry | Accepted |
| [0031](./0031-order-allocated-choreography.md) | Consume `OrderAllocated`/`OrderPartiallyAllocated` by choreography, fire-and-forget, with deterministic work unit ids | Accepted |
| [0032](./0032-housekeeping-retention-sweeper.md) | Retention sweeper for `idempotency_keys` (24h) and published `outbox_events` (7d) | Accepted |
| [0033](./0033-transfer-work-demand-choreography.md) | Consume NIP `WorkDemandReleased` into transfer-referenced work units; `WorkReleased` gains optional transfer fields on the same v1 payload | Accepted |
| [0034](./0034-configure-pool-command.md) | `ConfigurePool` command (`PUT /paths/{pathId}/pool`) sets a pool's mode and WIP limit; lowering below WIP never evicts | Accepted |
| [0035](./0035-product-classification-local-copy.md) | Product classification from a local copy of product-master's `ProductClassified` events; `PRODUCT_CLASSIFICATION_MODE=kafka\|permissive`, `http` rejected at boot | Accepted — `permissive` default and in-memory copy removed by 0044 |
| [0036](./0036-work-unit-line-no-on-work-released.md) | `WorkUnit` stores an optional `line_no` (nullable column, optional `lineNo` on the REST enqueue); `WorkReleased` v1 carries it on both topics (per-line confirm-pick, fleet decision 18) | Accepted |
| [0037](./0037-mcp-annotations-for-event-raising-tools.md) | MCP tools that publish events (`get_backlog_telemetry`, `get_rebalance_recommendation`) are annotated `readOnlyHint: false`, `destructiveHint: true`, and name the event in their description | Accepted |
| [0038](./0038-reports-api-openapi-contract.md) | The `cmd/wes-reports` routes get their own OpenAPI contract, `apis/openapi-reports.yaml`: Spectral-linted, rendered into the docs, contract-tested against the real router | Accepted |
| [0039](./0039-outbox-relay-attempt-cap.md) | Outbox relay attempt cap: a row is no longer claimed after `OUTBOX_MAX_ATTEMPTS` failed sends (default 20, `0` = unlimited), logged at ERROR and kept for re-arming; later same-key rows overtake it | Accepted |
| [0040](./0040-slot-plan-relocation-work.md) | Consume slotting-optimization's `SlotPlanApproved` into one `RELOCATE` work unit per move: a move into a slot another move vacates waits for that move's completion, a newer plan cancels older not-yet-released units, `WorkReleased` v1 gains an optional `relocation` block | Accepted |
| [0041](./0041-work-unit-created-origin.md) | `WorkUnitCreated` on the integration stream carries the optional `reference` (the order id for order work) and `line_no`, so order-management can confirm a release by matching attributes instead of parsing the work-unit id; additive, `v1` stays `v1`, analytics stream unchanged | Accepted |
| [0042](./0042-pick-face-replenishment-work.md) | Consume pick-face-replenishment's `ReplenishmentRequested`/`ReplenishmentCancelled` (opt-in `REPLENISHMENT_MODE=kafka`) into ONE `REPLENISH` work unit per request id: URGENT gets the short CPT horizon, a cancellation withdraws only a still-Pending unit, `WorkReleased` v1 gains an optional `replenishment` block | Accepted — `permissive` default removed by 0044 |
| [0043](./0043-taskcompleted-already-completed-skip.md) | A `TaskCompleted` carrying a NEW event id for a work unit that is already completed is a stale fact: `TaskCompletedAlreadyCompleted`, an INFO-logged, processed skip — never retried, never dead-lettered; amends 0028's error enumeration | Accepted |
| [0044](./0044-postgres-and-kafka-are-the-only-runtime.md) | Postgres and Kafka are the only runtime: `cmd/wes`, `cmd/mcp` and `cmd/wes-projector` have no in-memory repositories, no log sink, no direct publisher and no optional relay/consumer; mandatory variables and `permissive`/`off`/`log` are boot errors; real `/readyz`; NUL and `wipLimit` bounds are client errors; memory adapters are test doubles | Accepted |
| [0045](./0045-pack-aware-work-units.md) | Pack-aware work units: `CASE_PICK` / `EACH_PICK` / `BREAK_CASE` released as the `pick` / `case_break` objects fulfillment-execution consumes; local copy of product-master pack-level events (own `PACK_VIEW_CONSUMER_GROUP`); case-based charge and travel estimates | Accepted |
| [0046](./0046-a2a-inbound-adapter.md) | Agent2Agent as an inbound adapter: this context as an A2A agent (official a2a-go SDK, JSON-RPC 2.0 over HTTP, Agent Card at the well-known path, curated goal-shaped skills, delegation only — facts still travel as CloudEvents) | Accepted |
| [0047](./0047-fleet-rest-api-guideline.md) | Adopt the fleet REST API guideline: noun routes, idempotent POST on a global `Idempotency-Key` (409 on reuse, `Idempotency-Replayed`), keyset-paged collections, Spectral-enforced | Accepted |

## The template

```markdown
# ADR-NNNN — Short present-tense title

## Status
Proposed | Accepted | Deprecated | Superseded by ADR-NNNN

## Context
The forces at play: technical, business, domain. Written so a reader who was
not in the room can see why a decision was needed at all. Facts, not
justification.

## Decision
The choice, stated in the active voice: "We will …".

## Consequences
What becomes easier, what becomes harder, and what is now hard to reverse.
Both the good and the bad — a record that lists only benefits is marketing,
not a decision record.
```

## Status lifecycle

| Status | Meaning |
|---|---|
| **Proposed** | Open for discussion; not yet acted on. |
| **Accepted** | In force. The code reflects it. |
| **Deprecated** | No longer applied, with no direct replacement. |
| **Superseded by ADR-NNNN** | Replaced by a later record. |

An accepted record is **never edited to reflect a change of mind** and never
deleted. Superseding it with a new record preserves the reasoning that applied
at the time, which is the only reason the archive is worth keeping.

## Proposing a new one

1. Copy the template into `docs/docs/adr/NNNN-short-title.md`, taking the next
   free number.
2. Set `Status: Proposed`.
3. Add it to the table above and to the `adr/` section of
   [`docs/sidebars.ts`](https://github.com/IQVO/wes-work-planning/blob/main/docs/sidebars.ts).
4. Open a pull request. The discussion happens on the PR; merging with
   `Status: Accepted` is the act of accepting it.

## Scope

These records cover the **Work Planning & Release** bounded context only. Each
sibling service keeps its own. Where a decision is genuinely platform-wide —
the integration envelope, for instance — the record states so explicitly and
notes that the same decision is recorded in the other repositories.
