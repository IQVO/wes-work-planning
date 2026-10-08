---
id: ubiquitous-language
title: Ubiquitous language
sidebar_label: Ubiquitous language
sidebar_position: 2
description: Every term used by the Work Planning & Release bounded context, its exact definition, the code identifier it maps to, and the traps to avoid.
---

# Ubiquitous language

These are the exact terms used in the code, the API, the events and the
conversation. Every term maps to a code identifier; where the code name
differs from the spoken term, the **Code name differs** column says so.
Where a term is easy to get wrong, the wrong reading is stated explicitly — a
glossary that only lists right answers does not prevent the mistakes people
actually make.

## Core terms

| Term | Definition | Code identifier | Code name differs | **Not** |
|---|---|---|---|---|
| **Charge** | The volume that must clear a process path, bucketed by CPT. A set of `(CPT, quantity)` pairs. | `charge.ChargeForecast`, `charge.CPTBucket` | yes — *ChargeForecast* | Not "total due today". Flattening the buckets destroys the only signal that tells you whether you are on track. |
| **CPT** — Critical Pull Time | The last moment a parcel can be manifested and still make its truck. A value object on work; **priority derives from it**. | `shared.CPT` | no | Not a due date, not an SLA, not a soft target. It is a physical departure constraint. |
| **Process path** | A named station that owns a **queue**: unit-in → unit-out, with a service rate and a staffed capacity. Declared in process-path-management's catalogue. | `shared.PathId`; `pathcatalog.PathDefinition` | yes — the id is *PathId*, the catalogue entry *PathDefinition* | Not a workflow step or a stage in a pipeline. |
| **Work pool** | The queue for exactly one process path: its entries, feed mode, WIP limit and alarm threshold. | `release.WorkPool` | no | Not a global task list. One pool per path, always. It does **not** store arrival or service rates. |
| **Work unit** | A releasable unit of work (e.g. one order line). Carries a CPT, is assigned at most once, and cannot complete twice. | `workunit.WorkUnit` | no | Not the downstream `Task` in `fulfillment-execution` — see the traps below. |
| **Shift plan / path plan** | This service's committed split of headcount across paths: rate × heads × hours per path. | `plan.ShiftPlan`, `plan.PathPlan` | no | Not `workforce-management`'s `ShiftPlan` — see the traps below. |
| **Release** | Continuous, priority-ordered admission of work into a pool. Waveless. | `usecases.ReleaseNextWork`, `release.ReleasePolicy`, `WorkPool.ReleaseNext` | yes — *ReleaseNextWork* | Not a schedule, not a batch job. The release decision is a **policy object**. |
| **Flow balancing** | On telemetry (backlog vs plan): throttle upstream release, or flag labour reassignment. Drum-Buffer-Rope with CPT as the drum. | `usecases.RebalanceDecision`, `RebalanceAction` (`NoActionNeeded`, `ThrottleUpstream`, `ReassignLabor`) | **yes — *RebalanceDecision*** | Not "rebalance the warehouse". It is a bounded, two-lever correction on one path. |

## Feed modes

A work pool is one of two kinds (`release.FeedMode`), and the distinction
changes which invariants are enforceable:

| Feed mode | Code | Meaning | WIP limit is… |
|---|---|---|---|
| **Release-fed** | `release.ReleaseFed` | WES controls the volume entering this pool. | …an **enforceable invariant**. `ReleaseNext` refuses past the limit (`ErrWIPLimitReached`). |
| **Flow-fed** | `release.FlowFed` | Work arrives by physical conveyance; WES controls priority only. | …only an **alarm threshold**. You cannot refuse a tote that a conveyor already delivered. |

This is not a configuration nicety. You can only *enforce* a limit on an input
you actually control; pretending otherwise produces an invariant that the
physical world violates several times a minute. Today every pool is created
release-fed by `EnqueueWorkUnit`; no API creates a flow-fed pool.

## Supporting terms

| Term | Definition | Code identifier | Code name differs |
|---|---|---|---|
| **Backlog depth** | Count of *pending* (not yet released) entries in a pool. A projection, not stored state. | `WorkPool.BacklogDepth()` | no |
| **WIP** | Count of *released* entries in a pool that have not yet completed. | `WorkPool.WIP()` | no |
| **Alarm threshold** | The backlog depth above which `SampleBacklog` raises `BacklogThresholdBreached` (either feed mode), and above which a flow-fed path is throttled. | `WorkPool.alarmThreshold`, `IsOverAlarmThreshold()` | no |
| **WIP limit** | The maximum outstanding released work on a release-fed pool. Enforced at release time. | `WorkPool.wipLimit` | no |
| **Remaining capacity** | `max(0, wipLimit − WIP)` for a release-fed pool, reported per CPT cutoff; unknown for a flow-fed pool. | `WorkPool.RemainingCapacity()`, `PathCapacityChanged` | no |
| **Cutoff** | The CPT instant a remaining-capacity report is correlated with. | `SampleBacklogRequest.CutoffAt`, `cutoff_at` | yes — *CutoffAt* |
| **Installed stations** | The physical station count on a path. The ceiling for planned heads. | `PathPlan.installedStations` (`shared.StationCount`) | no |
| **Planned heads** | The people planned onto a path. | `PathPlan.plannedHeads` | **yes — typed `shared.StationCount`, not a headcount type** |
| **Planned throughput** | `rate × plannedHeads × hours` for one path plan. | `PathPlan.PlannedThroughput()` | no |
| **Rate** | A service rate in units per hour. Must be positive. | `shared.Rate` | no |
| **Reference** | The external identifier a work unit points back at (e.g. an order id). Required and non-empty. | `WorkUnit.reference`, `ref` on `WorkReleased` | yes — *ref* on the wire |
| **Labor plan (observed)** | Read-only projection, keyed by `path_id`, of the labour plan Workforce Management last committed, plus its drift from ours. | `laborview.LaborPlanObserved` | yes — *LaborPlanObserved* |
| **Drift** | The signed difference `observed planned heads − our planned heads` for a path. Reported, never auto-corrected. | `laborview.Drift`, `PathPlanDriftDetected`, `drift_heads` | no |
| **Usable inventory (observed)** | Read-only projection, keyed by **SKU**, of usable quantity as Inventory last reported it. | `inventoryview.UsableInventoryObserved` | yes — *UsableInventoryObserved* |
| **Process-path catalogue** | The declared set of paths every `pathId` is validated against (longest-prefix match). | `pathcatalog.Catalogue`, `ports.PathCatalogue` | no |
| **SKU** (on a work unit) | Optional inventory SKU the work unit's order line corresponds to. Empty is valid. Exists so release can look up a product classification. | `WorkUnit.sku` | no |
| **Product classification** | This context's **local copy** of product-master's classification for a SKU: one row per SKU in `product_classification_copy`, fed by `ProductClassified` events (version-guarded) and read once at release time (ADR-0035). | `productclassificationview.ProductClassificationView` | yes — *ProductClassificationView* |
| **Gift wrap** (on a work unit) | Optional, caller-stated at enqueue time: whether the requester asked for a gift package. Stamped onto `WorkReleased` as `gift_wrap`. | `WorkUnit.giftWrap` | no |
| **Line number** (on a work unit) | Optional 1-based order line the work unit was made for (valid range 1 to 2147483647), taken from the `OrderAllocated` line (or the optional `lineNo` of the REST enqueue). `NULL`/absent means unknown. Stamped onto `WorkReleased` as `line_no` on both topics (ADR-0036); it never changes the `{order_id}-line-{line_no}` id. | `WorkUnit.lineNo` | no |
| **Travel distance** | Optional hint: metres between two location codes, read once from facility-layout at plan commit. | `traveldistanceview.TravelDistanceView`, `PathPlan.travelDistanceM` | no |

## The traps

DDD's classic hazard is *same word, different model*. This context sits at the
intersection of several others, and hits it more than once.

### Trap 1 — `ShiftPlan` means two different things

`workforce-management` has a `ShiftPlan`. This service has a `ShiftPlan`. They
are **not** the same model and are deliberately not shared.

- **Ours** is a committed decision this context makes and enforces
  (`plannedHeads ≤ installedStations`). It is an aggregate with invariants.
- **Theirs** is a fact about labour that arrives over Kafka as
  `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted`. It is
  projected into a *different type* named `LaborPlanObserved` — a plain
  read-model value with no invariants at all. The two are *compared*
  (drift, [ADR-0019](../adr/0019-labor-plan-committed-shift-plan-reconciliation.md)),
  never merged.

Feeding Workforce's event into our aggregate would make an external system able
to violate our invariant. See
[ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md).

### Trap 2 — `WorkUnit` here is not `Task` downstream

A business-level demand signal and an execution-level unit bound to a worker
at a moment are two different classes with two different lifecycles. Here, a
`WorkUnit` is *releasable volume with a deadline*. In
`fulfillment-execution`, a `Task` is *claimable work with a lease*. The
`WorkReleased` integration event is the translation point — an
anti-corruption boundary, not a shared type.

### Trap 3 — read models are not aggregate state

Backlog depth, remaining capacity and plan-vs-labor drift are
**projections**. They are computed from pool state or built from events;
none of them is a field maintained on an aggregate. See
[Read models](../ddd/read-models.md).

### Trap 4 — `ProductClassificationView` is not `UsableInventoryObserved`

Both are keyed by SKU and both are local copies built from Kafka events, but
they come from different owners and answer different questions.

`UsableInventoryObserved` is a **persisted Kafka projection** of
inventory-storage's `StockReserved` and `ReservationRevoked`: a running
tally that changes with every reservation.

`ProductClassificationView` is read from a **local copy of product-master's
`ProductClassified`** events: a full-state replacement per SKU, applied only
when the event's `version` is newer. `ReleaseNextWork`'s outbound publisher
reads it **once**, when a unit is released, and stamps the result onto that
one `WorkReleased` event. Until ADR-0035 it was a synchronous HTTP read from
inventory-storage and was not persisted at all. See
[ADR-0009](../adr/0009-product-classification-propagation-to-work-released.md)
and [ADR-0035](../adr/0035-product-classification-local-copy.md).

### Trap 5 — `GiftWrap` is not a `ProductClassification` tag

Both `gift_wrap` and `fragile` are optional booleans on the same
`WorkReleased.data` payload, both omitted when false.

`fragile` is **derived**: read from the local copy of product-master's
classification for the released unit's SKU. It says something about the
*product*.

`gift_wrap` is **caller-supplied** on `EnqueueWorkUnitRequest` and read
straight off the `WorkUnit`. It never touches the classification and says
something about *this particular unit of work*. See
[ADR-0010](../adr/0010-gift-wrap-as-a-work-released-characteristic.md).

### Trap 6 — two `ShiftPlanCommitted` CloudEvents types

`com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` (theirs,
consumed) and `com.warehouse.wes.work-planning.plan.ShiftPlanCommitted` (ours,
published) share an event name. Consumers must dispatch on the **full**
`type`, never the short name.
