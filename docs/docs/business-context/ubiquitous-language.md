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
| **Charge** | The volume that must clear a process path, bucketed by CPT. Since ADR-0049 it is warehouse-planning's *workload forecast*; this context no longer records it (`POST /paths/{pathId}/charge` answers 410). | — (retired, ADR-0049) | — | Not "total due today". Flattening the buckets destroys the only signal that tells you whether you are on track. |
| **CPT** — Critical Pull Time | The last moment a parcel can be manifested and still make its truck. A value object on work; **priority derives from it**. | `shared.CPT` | no | Not a due date, not an SLA, not a soft target. It is a physical departure constraint. |
| **Process path** | A named station that owns a **queue**: unit-in → unit-out, with a service rate and a staffed capacity. Declared in process-path-management's catalogue. | `shared.PathId`; `pathcatalog.PathDefinition` | yes — the id is *PathId*, the catalogue entry *PathDefinition* | Not a workflow step or a stage in a pipeline. |
| **Work pool** | The queue for exactly one process path: its entries, feed mode, WIP limit and alarm threshold. | `release.WorkPool` | no | Not a global task list. One pool per path, always. It does **not** store arrival or service rates. |
| **Work unit** | A releasable unit of work (e.g. one order line). Carries a CPT, is assigned at most once, and cannot complete twice. | `workunit.WorkUnit` | no | Not the downstream `Task` in `fulfillment-execution` — see the traps below. |
| **Shift plan / path plan** | The committed split of headcount across paths: rate × heads × hours per path. Since ADR-0049 it is warehouse-planning's *labour plan*; this context no longer commits one (`POST /paths/{pathId}/plan` answers 410). | — (retired, ADR-0049) | — | Not `workforce-management`'s `ShiftPlan` — see the traps below. |
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
| **Remaining admission capacity** | What this context publishes, live: `max(0, wipLimit − WIP)` for a release-fed pool, reported per path and CPT cutoff; unknown for a flow-fed pool. Not warehouse-planning's *planned capacity* (ADR-0049 decision 6). | `WorkPool.RemainingCapacity()`, `PathCapacityChanged` | yes — *RemainingCapacity* |
| **Planned capacity** | warehouse-planning's term: a path's capacity for a planning window. This context neither computes nor publishes it. | — (warehouse-planning) | — |
| **Cutoff** | The CPT instant a remaining-capacity report is correlated with. | `SampleBacklogRequest.CutoffAt`, `cutoff_at` | yes — *CutoffAt* |
| **Installed stations** | The physical station count on a path. The ceiling for planned heads in the labour plan, now warehouse-planning's. | — (retired, ADR-0049) | — |
| **Planned heads** | The people planned onto a path. Still read here from Workforce Management's plan (`LaborPlanObserved`). | `LaborPlanObserved.PlannedHeads` | no |
| **Planned throughput** | `rate × plannedHeads × hours` for one path plan. Moved with the plan to warehouse-planning. | — (retired, ADR-0049) | — |
| **Rate** | A service rate in units per hour. Must be positive. | `shared.Rate` | no |
| **Reference** | The external identifier a work unit points back at (e.g. an order id). Required and non-empty. | `WorkUnit.reference`, `ref` on `WorkReleased` | yes — *ref* on the wire |
| **Labor plan (observed)** | Read-only projection, keyed by `path_id`, of the labour plan Workforce Management last committed. A pure projection since ADR-0049. | `laborview.LaborPlanObserved` | yes — *LaborPlanObserved* |
| **Drift** | The signed difference `observed planned heads − planned heads` for a path. ADR-0049 moved the check to warehouse-planning: `PathPlanDriftDetected` is retired and `drift_heads` on the labor-plan view is deprecated, only read back until the next observed plan clears it. | `LaborPlanObserved.DriftHeads`, `drift_heads` (deprecated) | no |
| **Usable inventory (observed)** | Read-only projection, keyed by **SKU**, of usable quantity as Inventory last reported it. | `inventoryview.UsableInventoryObserved` | yes — *UsableInventoryObserved* |
| **Process-path catalogue** | The declared set of paths every `pathId` is validated against (longest-prefix match). | `pathcatalog.Catalogue`, `ports.PathCatalogue` | no |
| **SKU** (on a work unit) | Optional inventory SKU the work unit's order line corresponds to. Empty is valid. Exists so release can look up a product classification. | `WorkUnit.sku` | no |
| **Product classification** | This context's **local copy** of product-master's classification for a SKU: one row per SKU in `product_classification_copy`, fed by `ProductClassified` events (version-guarded) and read once at release time (ADR-0035). | `productclassificationview.ProductClassificationView` | yes — *ProductClassificationView* |
| **Gift wrap** (on a work unit) | Optional, caller-stated at enqueue time: whether the requester asked for a gift package. Stamped onto `WorkReleased` as `gift_wrap`. | `WorkUnit.giftWrap` | no |
| **Line number** (on a work unit) | Optional 1-based order line the work unit was made for (valid range 1 to 2147483647), taken from the `OrderAllocated` line (or the optional `lineNo` of the REST enqueue). `NULL`/absent means unknown. Stamped onto `WorkReleased` as `line_no` on both topics (ADR-0036); it never changes the `{order_id}-line-{line_no}` id. | `WorkUnit.lineNo` | no |
| **Travel distance** | Optional hint: metres between two location codes, read from facility-layout at plan commit. Retired with the plan (ADR-0049). | — (retired, ADR-0049) | — |

## The traps

DDD's classic hazard is *same word, different model*. This context sits at the
intersection of several others, and hits it more than once.

### Trap 1 — `ShiftPlan` means two different things

Since ADR-0049 this context has no `ShiftPlan` of its own (the labour plan is
warehouse-planning's); the trap below stays for readers of its history.

`workforce-management` has a `ShiftPlan`. This service had one too. They
were **not** the same model and were deliberately not shared.

- **Ours** was a committed decision this context made and enforced
  (`plannedHeads ≤ installedStations`), an aggregate with invariants. It now
  lives in warehouse-planning as its `LabourPlan`.
- **Theirs** is a fact about labour that arrives over Kafka as
  `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted`. It is
  projected into a *different type* named `LaborPlanObserved` — a plain
  read-model value with no invariants at all. The two used to be *compared*
  (drift, [ADR-0019](../adr/0019-labor-plan-committed-shift-plan-reconciliation.md),
  superseded by ADR-0049), never merged.

Feeding Workforce's event into our aggregate would have let an external
system violate our invariant. See
[ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md).

### Trap 2 — `WorkUnit` here is not `Task` downstream

A business-level demand signal and an execution-level unit bound to a worker
at a moment are two different classes with two different lifecycles. Here, a
`WorkUnit` is *releasable volume with a deadline*. In
`fulfillment-execution`, a `Task` is *claimable work with a lease*. The
`WorkReleased` integration event is the translation point — an
anti-corruption boundary, not a shared type.

### Trap 3 — read models are not aggregate state

Backlog depth, remaining capacity and the observed labour plan are
**projections**. They are computed from pool state or built from events;
none of them is a field maintained on an aggregate (plan-vs-labour drift is
warehouse-planning's since ADR-0049). See
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

Ours is retired (ADR-0049) and no longer published, but both topics' history
still holds it.

`com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` (theirs,
consumed) and `com.warehouse.wes.work-planning.plan.ShiftPlanCommitted` (ours,
retired) share an event name. Consumers must dispatch on the **full**
`type`, never the short name.
