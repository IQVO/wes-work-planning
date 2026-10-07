---
id: read-models
title: Read models
sidebar_label: Read models
sidebar_position: 4
description: The projections this context maintains, and the rule that keeps them out of aggregate state.
---

# Read models

> **Read models (backlog depth, actual rate, plan-vs-actual) are PROJECTIONS —
> not state on aggregates.**

That rule is architectural, not stylistic. This page says what the projections
are and why the rule holds.

## The projections

| Projection | Keyed by | Built from | Exposed at |
|---|---|---|---|
| **Backlog telemetry** | `path_id` | computed from `WorkPool.entries` on read | `GET /paths/{pathId}/telemetry` |
| **Rebalance recommendation** | `path_id` | computed from the same pool snapshot on read | `GET /paths/{pathId}/rebalance` |
| **`LaborPlanObserved`** | `path_id` | `ShiftPlanCommitted` events from `workforce-management`, plus the drift outcome of comparing it with our `PathPlan` ([ADR-0019](../adr/0019-labor-plan-committed-shift-plan-reconciliation.md)) | `GET /paths/{pathId}/labor-plan-view` (`driftHeads`, `driftDetectedAt`) |
| **`UsableInventoryObserved`** | **`sku`** | `StockReserved` / `ReservationRevoked` events from `inventory-storage` | `GET /inventory-view/{sku}` |
| **Product classification copy** | **`sku`** | `ProductClassified` events from `product-master`, applied only when `version` is newer ([ADR-0035](../adr/0035-product-classification-local-copy.md)) | not exposed; read once per `WorkReleased` as `ProductClassificationView` |

One further read-only view is **not persisted at all**: the
`TravelDistanceView` read from facility-layout once
per `CommitShiftPlan`. The analytics `throughput_rollup` is a further,
out-of-process projection owned by `cmd/wes-projector`
([Release throughput report](../analytics/release-throughput-report.md)).

The two groups are different in kind:

- **Computed-on-read** (telemetry, rebalance) — derived synchronously from
  aggregate state. Nothing is stored; they cannot go stale.
- **Event-sourced projections** (`LaborPlanObserved`, `UsableInventoryObserved`)
  — materialised by consuming another context's events. Stored in their own
  tables (`labor_plan_view`, `usable_inventory_view`), never joined to
  aggregate tables.

## Why not just store the numbers on the aggregate?

It would be easy to keep a `backlogDepth int` field on `WorkPool` and increment
it. Three reasons that was not done:

1. **Two sources of truth drift.** The entries are the truth. A counter beside
   them is a cache, and any code path that touches entries without touching the
   counter silently corrupts it. `BacklogDepth()` counting pending entries
   cannot disagree with the entries.
2. **It expands the invariant surface for no benefit.** A stored counter
   introduces "counter equals actual pending count" as a new thing to enforce
   and test — an invariant that exists only because of the optimisation.
3. **Query needs change faster than the model.** Plan-vs-actual, actual rate,
   per-CPT burn-down — each is a new *question*, and questions should not each
   require a new field on an aggregate that has to be migrated.

The measured cost of computing on read is a slice scan per call, on a pool that
holds one path's queue. That is the right trade at this size, and if it ever
stops being, the fix is a projection built from events — not a counter on the
aggregate.

## The projections keyed by someone else's key

Two details here were deliberate and are worth stating plainly.

### `UsableInventoryObserved` is keyed by SKU, not by path

Inventory reservations are **SKU-scoped**. Everything else in this context is
path-scoped, so there was an obvious temptation to force a path key onto the
projection for consistency. That mapping does not exist in the real world — a
SKU is picked from many bins across many paths — so inventing it would have
produced a projection that was wrong in a way no test could catch.

The projection therefore keeps Inventory's key. Its endpoint is
`GET /inventory-view/{sku}`, deliberately *not* nested under `/paths/`.

`StockReserved` decrements the observed usable count for the SKU;
`ReservationRevoked` increments it back. The port exposes a single
`ApplyDelta(ctx, sku, delta, observedAt)` operation so the adjustment is atomic
at the adapter, rather than a read-modify-write in application code.

### `LaborPlanObserved` is a value, not an aggregate

```go
type LaborPlanObserved struct {
    PathId          shared.PathId
    PlannedHeads    int
    PlannedRate     float64
    PlannedHours    float64
    ObservedAt      time.Time
    DriftHeads      *int       // ADR-0019: observed minus ours, nil until compared
    DriftDetectedAt *time.Time // set only when DriftHeads != 0
}
```

No constructor, no validation, exported fields. That shape is the point: this
type protects nothing, because the facts in it are owned by another bounded
context. Giving it invariants would mean this service could reject a fact that
Workforce has already committed — which is not a decision this context is
entitled to make. The drift columns record a *comparison* with our plan;
they never change either plan.

Contrast with `PathPlan`, which has a private field set and a validating
constructor, because `plannedHeads ≤ installedStations` *is* our rule.

## Projection staleness is explicit

Both event-sourced projections carry `ObservedAt`, and both endpoints return it.
A consumer can therefore tell how old the observation is rather than assuming
it is current. Neither projection is read-repaired, back-filled, or reconciled
against the source: it is exactly "the last thing that context told us,
and when".

If nothing has been observed yet, the endpoints return **404**, not a
zero-valued body — the same distinction `ChargeForecast.QuantityForCPT` makes
between "zero" and "unknown".
