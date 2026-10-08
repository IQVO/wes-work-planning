---
id: bounded-context-canvas
title: Bounded context canvas
sidebar_label: Bounded context canvas
sidebar_position: 3
description: "ddd-crew Bounded Context Canvas v5 for Work Planning & Release: purpose, strategic classification, roles, every inbound and outbound message, decisions, assumptions and metrics."
---

# Bounded context canvas

A [ddd-crew Bounded Context Canvas](https://github.com/ddd-crew/bounded-context-canvas)
(v5). Every message row maps to a real route in
`internal/adapters/inbound/http/router.go`, a real MCP tool in
`internal/adapters/inbound/mcp/`, or a real Kafka topic and CloudEvents type
in `internal/adapters/kafka/cloudevents/cloudevents.go` and
`internal/adapters/outbound/kafka/publisher.go`.

## Name

**Work Planning & Release** — repository `wes-work-planning`, Go module
`github.com/claudioed/wes-work-planning`, CloudEvents source
`/warehouse/wes-work-planning`, type prefix `com.warehouse.wes.work-planning`.
The core of the WES tier: *the conductor*.

## Purpose

Turn plans and stock reality into **continuously released, priority-ordered
work** on each process path, and keep every path's buffer healthy while the
shift runs. In one loop: plan (charge forecast, committed shift plan) →
release (earliest CPT first, WIP-limited) → observe (completions, backlog
telemetry) → correct (throttle upstream or reassign labour).

It does **not** pick, pack or ship (fulfillment-execution), own stock
(inventory-storage), staff people (workforce-management) or own the
process-path catalogue (process-path-management).

## Strategic Classification

| Dimension | Value |
|---|---|
| **Domain** | **Core** — see [Subdomain classification](./subdomain-classification.md) and the [Core domain chart](./core-domain-chart.md) |
| **Business model** | **Cost reduction / throughput** — making every CPT with less labour and less WIP; not directly revenue-generating, not compliance |
| **Evolution** | **Custom-built** — release policy, WIP backpressure and flow balancing are written and tuned here |

## Domain Roles

| Role (ddd-crew archetype) | How it shows up |
|---|---|
| **Execution** | Admits work one unit at a time (`ReleaseNextWork`) and tracks it to completion. |
| **Enforcer** | Enforces the WIP limit on release-fed pools and `plannedHeads ≤ installedStations` on plans. |
| **Analysis** | Backlog telemetry, rebalance recommendations, remaining-capacity and plan-vs-labor drift signals. |

## Inbound Communication

| Collaborator | Message | Type | Channel | Relationship |
|---|---|---|---|---|
| Planner / supervisor (warehouse-console MFE or any client) | `ReceiveChargeForecast` | Command | REST `POST /paths/{pathId}/charge` | OHS (we host) |
| Planner / supervisor | `CommitShiftPlan` | Command | REST `POST /paths/{pathId}/plan` | OHS |
| Any upstream caller | `EnqueueWorkUnit` | Command | REST `POST /paths/{pathId}/work-units` (`Idempotency-Key` required with Postgres) | OHS |
| Supervisor / agent | `ReleaseNextWork` | Command | REST `POST /paths/{pathId}/release`; MCP tool `release_next_work` | OHS |
| Operator | `ConfigurePool` | Command | REST `PUT /paths/{pathId}/pool` (no MCP tool; [ADR-0034](../adr/0034-configure-pool-command.md)) | OHS |
| Station / supervisor | `RecordCompletion` | Command | REST `POST /work-units/{id}/complete` | OHS |
| Supervisor / warehouse-ops-agent | `SampleBacklog` | Query (may emit events) | REST `GET /paths/{pathId}/telemetry?cutoffAt=`; MCP tool `get_backlog_telemetry`; MCP resource `telemetry://{pathId}/backlog` | OHS |
| Supervisor / warehouse-ops-agent | `RebalanceDecision` | Query (may emit events) | REST `GET /paths/{pathId}/rebalance`; MCP tool `get_rebalance_recommendation` | OHS |
| warehouse-ops-agent and UI | `GetWorkUnitsByReference`, `GetWorkUnit` | Query | REST `GET /work-units?reference=`, `GET /work-units/{id}` | OHS |
| UI | `LaborPlanView`, `InventoryView` | Query | REST `GET /paths/{pathId}/labor-plan-view`, `GET /inventory-view/{sku}` | OHS |
| warehouse-ops-agent | Release throughput report | Query | REST `GET /reports/throughput`, `GET /reports/throughput/freshness` (`cmd/wes-reports`); MCP tool `get_release_throughput_report` when `REPORTS_BASE_URL` is set | OHS |
| workforce-management | `ShiftPlanCommitted` | Event | Kafka `warehouse.workforce.events`, `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | Customer/Supplier, we conform behind an ACL |
| inventory-storage | `StockReserved`, `ReservationRevoked` | Event | Kafka `warehouse.inventory.events`, `com.warehouse.wms.inventory-storage.reservation.StockReserved` / `...ReservationRevoked` | Customer/Supplier, ACL |
| fulfillment-execution | `TaskCompleted` | Event | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | Customer/Supplier (feedback edge), ACL |
| order-management | `OrderAllocated`, `OrderPartiallyAllocated` | Event | Kafka `warehouse.order-management.events`, `com.warehouse.wes.order-management.order.OrderAllocated` / `...OrderPartiallyAllocated` | Customer/Supplier, ACL ([ADR-0031](../adr/0031-order-allocated-choreography.md)) |
| process-path-management | `ProcessPathCreated`, `ProcessPathUpdated`, `ProcessPathDeactivated` | Event | Kafka `warehouse.process-path-management.events`, `com.warehouse.wes.process-path-management.processpath.*` (only with `PATH_CATALOGUE_SOURCE=kafka`) | Conformist ([ADR-0030](../adr/0030-kafka-sourced-path-catalogue.md)) |
| product-master | `ProductClassified` | Event | Kafka `warehouse.product-master.events`, `com.warehouse.wms.product-master.product.ProductClassified` (only with `PRODUCT_CLASSIFICATION_MODE=kafka`; own group `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`) | Published Language into a local, version-guarded copy read at release ([ADR-0035](../adr/0035-product-classification-local-copy.md)) |

## Outbound Communication

| Collaborator | Message | Type | Channel | Relationship |
|---|---|---|---|---|
| fulfillment-execution | `WorkReleased` | Event | Kafka `warehouse.work-planning.events`, `com.warehouse.wes.work-planning.workunit.WorkReleased` | OHS + Published Language (we supply) |
| order-management | `PathCapacityChanged` | Event | Kafka `warehouse.work-planning.events`, `com.warehouse.wes.work-planning.workpool.PathCapacityChanged` | OHS + PL ([ADR-0018](../adr/0018-path-capacity-changed.md)) |
| network-fulfillment | `PathCapacityChanged` | Event | same topic and type | OHS + PL; network-fulfillment is Conformist |
| *(no consumer today)* | `ChargeForecastReceived`, `ShiftPlanCommitted`, `WorkUnitCreated`, `WorkUnitCompleted`, `BacklogThresholdBreached`, `RateDeviationDetected` (reserved, not emitted; decided 2026-10-06), `PathThrottled`, `LaborReassignmentFlagged`, `PathPlanDriftDetected` | Event | Kafka `warehouse.work-planning.events`, `com.warehouse.wes.work-planning.<entity>.<Event>` | Published for observability |
| own analytics projector (`cmd/wes-projector`) | all 11 domain events | Event | Kafka `warehouse.wes.analytics`, same CloudEvents types, dataschema `urn:warehouse:wes-work-planning:analytics:<Event>:v1` | Internal data product ([ADR-0011](../adr/0011-analytical-data-product.md)) |
| facility-layout | travel distance | Query | REST `GET /distance?from=&to=`, once per `CommitShiftPlan` with both codes | Conformist to its OHS ([ADR-0017](../adr/0017-travel-distance-lookup-on-commit-shift-plan.md)) |

## Ubiquitous Language

Full glossary: [Ubiquitous language](../business-context/ubiquitous-language.md).
Top terms:

| Term | Code identifier |
|---|---|
| Charge | `charge.ChargeForecast`, `charge.CPTBucket` |
| CPT | `shared.CPT` |
| Process path | `shared.PathId`, `pathcatalog.PathDefinition` |
| Work pool | `release.WorkPool` |
| Feed mode (release-fed / flow-fed) | `release.FeedMode` (`ReleaseFed`, `FlowFed`) |
| Work unit | `workunit.WorkUnit` |
| Release | `usecases.ReleaseNextWork`, `release.ReleasePolicy` |
| WIP limit / alarm threshold | `wipLimit`, `alarmThreshold` on `WorkPool` |
| Flow balancing | `usecases.RebalanceDecision` |
| Drift | `laborview.Drift`, `PathPlanDriftDetected` |

## Business Decisions

- **Earliest CPT first** is the entire priority function (`nextPendingIndex`,
  [ADR-0002](../adr/0002-waveless-continuous-release.md)).
- The **WIP limit is enforced only on release-fed pools**; flow-fed pools only
  alarm ([ADR-0003](../adr/0003-flow-balancing-as-domain-service.md)).
- The first enqueue on a path whose pool was never configured creates it
  **release-fed with WIP limit and alarm threshold 1000** (`defaultWIPLimit`,
  `defaultAlarmThreshold`). An operator sets a path's mode and WIP limit
  explicitly with `PUT /paths/{pathId}/pool` (`ConfigurePool`,
  [ADR-0034](../adr/0034-configure-pool-command.md)); lowering a limit below the
  current WIP never evicts work, releases pause until WIP < limit.
- A flow-fed path reports remaining capacity as **`Known=false`**, always
  ([ADR-0018](../adr/0018-path-capacity-changed.md),
  [ADR-0020](../adr/0020-flowfed-path-observed-throughput-signal.md)).
- Plan-vs-labor drift is **reported, never auto-corrected**
  ([ADR-0019](../adr/0019-labor-plan-committed-shift-plan-reconciliation.md)).
- Workforce's `ShiftPlanCommitted` is a **read model**, never fed into our
  `ShiftPlan` aggregate ([ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md)).
- Every `pathId` is validated against process-path-management's catalogue
  ([ADR-0012](../adr/0012-process-path-catalogue-validation.md)); an
  `OrderAllocated` with one unknown path fails as a whole.
- `TaskCompleted` for a work unit this context never planned is a logged,
  processed skip; `OrderAllocated` lines already enqueued are skipped.
- The two REST lookups are **fail-open**: a failure omits an optional hint and
  never blocks the use case.
- Only `POST /paths/{pathId}/work-units` requires an `Idempotency-Key`
  ([ADR-0022](../adr/0022-idempotency-key-middleware.md)); charge and plan
  are natural-key upserts.

## Assumptions

- One work pool per process path, and a path's work fits one aggregate row
  set (entries are never pruned).
- `promise_date` on `OrderAllocated` is the CPT of every line.
- Workforce's planned heads and ours are comparable numbers for the same
  `path_id`.
- Kafka is at-least-once: every consumer is idempotent on the CloudEvents `id`
  ([ADR-0028](../adr/0028-processed-event-mark-atomic-with-handling.md)).
- REST and MCP are reached inside the cluster (through Kong locally) and are
  **unauthenticated** ([ADR-0016](../adr/0016-remove-rest-mcp-static-bearer-auth.md)).

## Verification Metrics

| Metric | Source |
|---|---|
| `wes.work_units.released` counter, attribute `path.id` | `internal/adapters/outbound/telemetry/release_metrics.go` ([ADR-0013](../adr/0013-standard-metrics-convention.md)) |
| `wes.outbox.lag_seconds` gauge — age of the oldest unpublished outbox row | `internal/adapters/outbound/postgres/outbox_metrics.go` |
| Hourly `work_released`, `work_unit_completed`, `backlog_threshold_breached`, `path_throttled` per path | `throughput_rollup`, `GET /reports/throughput` ([Release throughput report](../analytics/release-throughput-report.md)) |
| Backlog depth vs alarm threshold, WIP vs WIP limit | `GET /paths/{pathId}/telemetry` |
| Planned heads drift per path | `GET /paths/{pathId}/labor-plan-view` (`driftHeads`) |
| Dead-lettered messages | `<topic>.dlq` topics |

## Open Questions

- `RateDeviationDetected` is declared, catalogued and counted by the analytics
  projector but **no use case raises it** — it needs a time-windowed
  actual-rate projection that does not exist. **Decided 2026-10-06: kept
  reserved** (declared for a future detection rule, [ADR-0020](../adr/0020-flowfed-path-observed-throughput-signal.md)
  defers it; not emitted today; not removed).
- ~~Nothing in production code creates a **flow-fed** pool or changes a pool's
  WIP limit.~~ **Resolved 2026-10-06:** `PUT /paths/{pathId}/pool`
  ([ADR-0034](../adr/0034-configure-pool-command.md)).
- `UsableInventoryObserved` feeds no decision. **Decided 2026-10-06:
  read-only context by design ([ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md));
  gating release on it would be a new business rule** (reservations in
  inventory-storage already guard availability).
- The `events` table (migration `0001`) is unused. **Decided 2026-10-06:
  legacy, unused; retained, additive migrations only.**
- `WorkPool` entries are never removed, so a long-lived path's pool grows
  without bound and every save rewrites all entries.
- `PathPlanDriftDetected` has no consumer yet; who acts on drift?
- `order-management` gets no reply event for `OrderAllocated`; it cannot learn
  from Kafka whether its lines were enqueued (a deliberate v1 choice, ADR-0031).
