---
paths:
  - "internal/**"
  - "cmd/**"
  - "features/**"
  - "apis/**"
  - "migrations/**"
---

# Domain model reference — wes-work-planning

## Ubiquitous Language (use these exact names)

- **Charge** — volume that must clear, bucketed by CPT. Not "total due today".
- **CPT (Critical Pull Time)** — last moment a parcel can be manifested and
  still make its truck. A value object on work; priority derives from it.
- **Process Path** — a named station that owns a QUEUE (not a workflow step):
  unit-in → unit-out, direct/indirect, a service rate, a staffed capacity.
- **Work Pool** — the queue for one process path: backlog depth, arrival
  rate, service rate. Release-fed (WES controls volume) vs flow-fed
  (priority only).
- **WorkUnit** — a releasable unit of work (e.g. a pick task). Assigned at
  most once at a time; cannot complete twice.
- **ShiftPlan / PathPlan** — committed split of headcount across paths: rate
  × heads × hours per path. Invariant: plannedHeads ≤ installedStations.
- **Release** — continuous, priority-ordered admission of work into a pool
  (waveless). The release decision is a POLICY object, not a schedule.
- **Flow balancing** — on telemetry (backlog vs plan): throttle upstream
  release or flag labor reassignment. Drum-Buffer-Rope with CPT as the drum.
- **WorkUnit.SKU** — optional SKU carried by a WorkUnit (threaded from
  `EnqueueWorkUnitRequest.SKU`), used ONLY to look up the SKU's
  `ProductClassification` from `inventory-storage` once, at release time.
- **Product classification propagation** — `ReleaseNextWork`'s outbound
  `WorkReleased` publisher reads a released WorkUnit's SKU classification (via
  `ports.ProductClassificationLookup`, a synchronous HTTP read mirroring
  inventory-storage's own facilitylayout adapter pattern;
  permissive-by-default, `PRODUCT_CLASSIFICATION_MODE=http|permissive`) and
  stamps derived `required_capabilities: ["hazmat"]` / `fragile: true` onto
  the outbound `WorkReleased` event's `data` payload when applicable. Both
  fields are OPTIONAL and OMITTED (not defaulted false/empty) when the SKU
  is unclassified or the lookup is unavailable — fail-open, deliberately
  asymmetric with inventory-storage's fail-closed `StowStock` check
  (ADR-0009). `fulfillment-execution`'s Task carries these onward without
  ever calling inventory-storage directly — the same "Task carries what a
  station needs to know" design already used for CPT and
  requiredCapabilities.
- **Known gap**: classification drift after release is not retroactively
  applied — a WorkUnit stamps classification once, at release time.
- **Travel-distance hint** — `CommitShiftPlan` accepts an OPTIONAL
  `fromLocationCode`/`toLocationCode` pair; when both are set it reads the
  distance once from facility-layout's `GET /distance` (via
  `ports.TravelDistanceLookup`, `TRAVEL_DISTANCE_MODE=http|permissive`,
  permissive by default, fail-open) and stamps it on the `PathPlan`
  (`TravelDistanceM`/`TravelDistanceKnown`/estimated flag), surfaced as
  optional `travelDistanceM`/`travelDistanceEstimated` in the response
  (ADR-0017). Omitted, never zero-filled, when unknown.
- **LaborPlanObserved** — a read-only projection of Workforce Management's
  OWN `ShiftPlanCommitted` event, keyed by `path_id`. Deliberately NOT fed
  into this service's own `ShiftPlan`/`PathPlan` aggregate — same term
  ("ShiftPlan"), different bounded context (ADR-0006). Never conflate them.
- **UsableInventoryObserved** — a read-only projection of inventory-storage's
  `StockReserved`/`ReservationRevoked` events, keyed by SKU (not by path —
  inventory reservations are SKU-scoped).

## Aggregates & invariants (enforced in domain, unit-tested)

- **WorkPool**: hands out work in priority order, at most once. WIP limit is
  an enforceable invariant on release-fed pools; an alarm threshold on
  flow-fed.
- **WorkUnit**: at most one active assignment; no double-complete; carries
  CPT.
- **ShiftPlan**: plannedHeads(path) ≤ installedStations(path); sum of hours
  valid.
- Read models (backlog depth, actual rate, plan-vs-actual, LaborPlanObserved,
  UsableInventoryObserved) are PROJECTIONS built from events — NOT state on
  aggregates.

## Domain events (past tense)

`ChargeForecastReceived`, `ShiftPlanCommitted`, `WorkUnitCreated`,
`WorkReleased`, `WorkUnitCompleted`, `BacklogThresholdBreached`,
`RateDeviationDetected` (declared in the catalogue and in
`apis/asyncapi.yaml`, but no use case raises it today — stated honestly in
`docs/docs/ddd/domain-events.md` rather than silently dropped),
`PathThrottled`, `LaborReassignmentFlagged`, `PathCapacityChanged` (raised by
`SampleBacklog` only when the caller supplies a CPT `CutoffAt` — see
ADR-0018; reports this path's remaining admission capacity, correlated by
CPT cutoff timestamp; consumed by order-management's `kafkapathcapacity`
adapter behind its `PathCapacity` port and by network-fulfillment's
`pathcapacitycache` adapter), `PathPlanDriftDetected` (raised by
`CommitShiftPlan` or `ObserveLaborPlan`, whichever commits second, when our
`PathPlan` and Workforce's `LaborPlanObserved` disagree on planned heads —
ADR-0019).

`OccurredAt` on every event comes from the injected `Clock` port, never
`time.Now()` inside the domain — event-timing assertions in tests are exact,
not approximate.

Kafka publication is opt-in at runtime via `EVENT_PUBLISHER=kafka`; the
default `EVENT_PUBLISHER=log` writes to the log publisher instead. "Declared
in the catalogue" is not the same claim as "flowing in your environment
right now" — check the actual env var.

Two events are actually **consumed** by another service today:
`WorkReleased`, by `fulfillment-execution`, which turns it into a `Task`, and
`PathCapacityChanged`, by `order-management` (its path-capacity cache) and
`network-fulfillment` (its `pathcapacitycache`). The rest are published for
observability/future subscribers.

## Use cases (application layer)

1. `ReceiveChargeForecast(path, cptBuckets)` → ChargeForecast
2. `CommitShiftPlan(path, heads, rate, hours)` → ShiftPlan (validates
   invariant); optional travel-distance hint (ADR-0017) and drift
   reconciliation against `LaborPlanObserved` (ADR-0019)
3. `EnqueueWorkUnit(path, cpt, ref)` → WorkUnit added to pool
4. `ReleaseNextWork(path)` → applies release policy, returns released
   unit(s); also stamps product-classification hints (see above)
5. `RecordCompletion(workUnitId)` → WorkUnitCompleted, updates telemetry
6. `SampleBacklog(path)` → returns depth/rate read model; may raise
   threshold events
7. `RebalanceDecision(path)` → throttle vs reassign recommendation
8. `GetWorkUnitsByReference(reference)` → read-only, backs the fleet's
   cross-service Order Lifecycle console screen (see ADR-0002 in
   `warehouse-ops-agent`'s docs)
9. `GetWorkUnit(workUnitId)` → read-only identity lookup of one WorkUnit
   (`GET /work-units/{id}`); unknown id → `ports.ErrNotFound` (404)
10. `LaborPlanView(path)` / `InventoryView(sku)` → read-only projection
    queries
11. Inbound-event use cases, each idempotent via `onceAtomically`
    (ADR-0028): `ObserveLaborPlan`, `ObserveInventoryChange`,
    `ApplyTaskCompleted` (wraps `RecordCompletion`), `ApplyOrderAllocated`
    (wraps `EnqueueWorkUnit`, ADR-0031)
