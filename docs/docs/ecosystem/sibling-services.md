---
id: sibling-services
title: Sibling services
sidebar_label: Sibling services
sidebar_position: 3
description: What each warehouse-systems bounded context this service integrates with owns, and how it relates to this one.
---

# Sibling services

The eight bounded contexts this service integrates with directly, summarised
from each repository's own `CLAUDE.md` and adapter code. Each is an independent Go service with its own model, its own
database and its own deployment lifecycle.

---

## `inventory-storage` — WMS tier, **Core**

> The WMS-tier authoritative record of **what is held where, and what portion is
> usable.**

Implements e-commerce-retailer-style **chaotic (random) stow**: no fixed product location, an
item goes to any free bin and the system records the exact bin. Its defining
design rule is the **revocable reservation** — allocation binds a quantity to
demand with a timeout, and revoking returns the quantity to usable, so a
physical delivery failure (blocked pod, lost tote, chute jam, short pick) never
strands an order.

| | |
|---|---|
| Aggregates | `StockUnit` (SKU@bin), `Bin`/`Location`, `Reservation` |
| Key invariants | every item has exactly one known bin or is flagged `Unlocated`; a stow requires **both** item-scan and location-scan; `sum(qty) ≤ bin capacity`; `reserved ≤ usable` |
| Publishes | `StockReserved`, `ReservationRevoked` on `warehouse.inventory.events` |

**Relationship to this service:** upstream Customer/Supplier. We consume its two
reservation events and project them into `UsableInventoryObserved`, keyed by
SKU. That projection is read-only context: **usable, not total, is what
constrains release**, and this is the only channel through which we learn it.

inventory-storage no longer owns product classification (its ADR 0034): that
moved to `product-master`, below. This service never called inventory-storage
for anything else, so since
[ADR-0035](../adr/0035-product-classification-local-copy.md) it makes no
call to it at all.

---

## `product-master` — WMS tier, **Supporting**

> The single source of truth for SKU-level product master data: handling
> classification and physical profile.

Owns the `Product` aggregate: a SKU's handling classification (`Hazmat`,
`Fragile`, `TemperatureSensitive`, `Oversized`, `HighValue`, a temperature
class and a DOT hazard class) and its physical profile (declared vs measured
unit dimensions and weight). It calls no sibling service.

| | |
|---|---|
| Aggregate | `Product` (classification, physical profile) |
| Publishes | `ProductRegistered`, `ProductDescriptionChanged`, `ProductClassified`, `ProductDimensionsDeclared`, `ProductMeasured` on `warehouse.product-master.events` (CloudEvents `com.warehouse.wms.product-master.product.*`, key and `subject` = the SKU) |

**Relationship to this service:** upstream, Published Language into a local
copy. With `PRODUCT_CLASSIFICATION_MODE=kafka` we consume only
`ProductClassified` (stable group `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`)
into `product_classification_copy`, applying a message only when its
`version` is newer than the stored row, and read that copy once per
`WorkReleased` to stamp the `hazmat`/`fragile` hints. We never call it
([ADR-0035](../adr/0035-product-classification-local-copy.md)).

---

## `workforce-management` — **Supporting**

> Owns "who is on shift, on which process path, at what rate; direct vs indirect
> hours."

Two horizons: shift-start headcount planning (a human commits a split across
paths) and intra-shift assignment tracking. It **stops at the path boundary** —
it never links an associate to a specific task, because task dispatch and
workforce planning change at completely different cadences (shifts versus
seconds).

| | |
|---|---|
| Aggregates | `AssociateShift`, `ShiftPlan`, `LaborAssignment` |
| Key invariants | exactly one **active** assignment per associate at a time; an assignment must satisfy the path's certification requirement |
| Publishes | `ShiftPlanCommitted` — **one message per path line** — on `warehouse.workforce.events` |

**Relationship to this service:** upstream Customer/Supplier. Its `ShiftPlan`
and ours share a name and nothing else: theirs is a labour-side commitment
across a building's paths; ours is this context's own rate × heads × hours
decision with the `plannedHeads ≤ installedStations` invariant. We project
theirs into `LaborPlanObserved` and never near our aggregate —
[ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md).

Note the pleasing symmetry with our `RebalanceDecision`: Workforce surfaces
`PathUnderstaffed` as *a flag, not a decision*; we surface `ReassignLabor` the
same way. Neither context moves a person — that is a human call, recorded in
Workforce.

---

## `fulfillment-execution` — **Core**

> Turns released work into completed physical operations: the **task lifecycle**
> for Pick, Pack and SLAM.

Its defining rule is **pull, not push**: a station calls
`claimNext(stationId, capabilities)` and the system selects work, not workers.
Claims carry a **lease**, so an unconfirmed task returns to the pool rather than
vanishing.

| | |
|---|---|
| Aggregates | `Task` (Pick/Pack/SLAM, leased), `Station`, `Package` |
| Key invariants | at most one active claim; no double-complete; a claim requires matching capabilities; an expired lease frees the task; SLAM diverts when weight is out of tolerance |
| Consumes | our `WorkReleased` on `warehouse.work-planning.events` |
| Publishes | `TaskCompleted` on `warehouse.fulfillment.events` |

**Relationship to this service:** the only *bidirectional* pair in the platform —
two directed Customer/Supplier relationships. We supply released work; it
supplies completion back, which drops our WIP and lets the next release proceed.

The model correspondence is worth being precise about. Our `WorkUnit` and its
`Task` are **different models of the same reality**, and both independently
enforce at-most-once and no-double-complete on their own side of the boundary.
Both also carry a CPT and derive priority from it — the drum is visible in both
contexts, which is what makes the handoff coherent without a shared type.

---

## `order-management` — **Generic/Supporting**

> Order intake, per-line stock allocation, promise-date calculation, and
> choreographed release.

It used to call this service's
`POST /paths/{pathId}/work-units` synchronously to release work — a coupling
this service's owners rejected once order-management existed as its own
context, in favor of the same event-choreography pattern already used by
`inventory-storage` and `workforce-management`.

| | |
|---|---|
| Publishes | `OrderAllocated`, `OrderPartiallyAllocated` on `warehouse.order-management.events` |
| Consumes | our `PathCapacityChanged` on `warehouse.work-planning.events` (its `kafkapathcapacity` adapter) |

**Relationship to this service:** upstream Customer/Supplier, behind our ACL,
same as `inventory-storage` and `workforce-management`. We consume both event
types identically — each carries `order_id`, `promise_date`, and a `lines`
array — and call the existing `EnqueueWorkUnit` use case once per line, with
a deterministic `work_unit_id` derived as `"{order_id}-line-{line_no}"`. This
edge is deliberately **fire-and-forget**: no reply event is published back to
order-management. See
[Integration events](../ecosystem/integration-events.md#warehouseorder-managementevents--comwarehousewesorder-managementorderorderallocated-orderpartiallyallocated).

In the other direction, order-management consumes our `PathCapacityChanged`
to learn each path's remaining admission capacity per CPT cutoff
([ADR-0018](../adr/0018-path-capacity-changed.md)); there we are the supplier.

---

## `facility-layout` — **Generic**

> The system of record for **where things physically are in the building**: the
> site's structural hierarchy and the coded storage slots inside it.

Owns whether a coded location *exists, is active, and is legal for a given kind
of storage unit* — the warehouse map other contexts read but never write. It
explicitly does **not** own occupancy or stock; that stays in
`inventory-storage`.

| | |
|---|---|
| Model | `Site → Zone → Aisle → LocationSlot`, plus `PlacementRules` |
| Location code | industry-standard `Site-Area-Zone-Aisle-Bay-Level-Position` |
| Read endpoints | `GET /sites/{siteCode}/layout`, `GET /distance`, among others |
| Publishes | `warehouse.facility.events` (not consumed by this service) |

**Relationship to this service: Conformist, read-only.** `CommitShiftPlan`
calls `GET /distance` once, at commit time, for two caller-supplied location
codes and stamps the result on the `PathPlan`
([ADR-0017](../adr/0017-travel-distance-lookup-on-commit-shift-plan.md)). The
lookup is off by default (`TRAVEL_DISTANCE_MODE=permissive`) and fails open.
We do not consume its topic. Classified Generic for the same reason the
reference model puts Cartonization there: extract it once rather than
duplicating geography in every context.

---

## `process-path-management` — **Generic**

> Owns the declared process-path catalogue — which paths exist, their
> prefixes and whether they are active.

| | |
|---|---|
| Publishes | `ProcessPathCreated`, `ProcessPathUpdated`, `ProcessPathDeactivated` on `warehouse.process-path-management.events` |

**Relationship to this service: Conformist.** Every `pathId` this service
accepts is validated against that catalogue
([ADR-0012](../adr/0012-process-path-catalogue-validation.md)). With
`PATH_CATALOGUE_SOURCE=kafka` we replay its topic into an in-memory catalogue
at boot and follow it live; with the default `file` source we read the same
catalogue from `warehouse-infra`'s YAML.

---

## `network-fulfillment` — **Supporting**

> The anti-corruption layer between the fleet and an external retail
> fulfillment network; owns `NetworkOrder` and `CapabilityOffer`.

| | |
|---|---|
| Consumes | our `PathCapacityChanged` on `warehouse.work-planning.events` (its `pathcapacitycache` adapter, one of the Kafka-fed caches behind `CapabilityOffer`, opt-in via its `CAPABILITY_OFFER_ENABLED`) |

**Relationship to this service:** downstream, Conformist to our Published
Language. Its `internal/adapters/outbound/pathcapacitycache` replays our topic
from the earliest offset under a process-unique consumer group, keeps only
`com.warehouse.wes.work-planning.workpool.PathCapacityChanged`, and caches
`remaining_units`/`known` per exact `(path_id, cutoff_at)` pair behind its
own `ports.PathCapacity`. We publish nothing specifically for it and do not
call it.

---

## What they all share

Conventions, not code. Each service re-implements these; none of them is a
shared library, so no service can force another to redeploy:

- Hexagonal / ports-and-adapters with the same strict dependency rule
- Go, chi, pgx/v5, golang-migrate, `segmentio/kafka-go`
- The same CloudEvents 1.0 envelope and `type` naming convention
  ([ADR-0027](../adr/0027-cloudevents-mandatory-event-envelope.md)), with the
  cross-service `type` strings duplicated by agreement, never imported
- The same `EVENT_PUBLISHER=kafka|log` / `KAFKA_BROKERS` configuration switches
- RFC 7807 problem details, an `apis/openapi.yaml` linted by Spectral in CI,
  and a Helm chart

That last point is the real payoff of bounded contexts that agree on
conventions while sharing no types: the *shape* is familiar everywhere, and the
*models* stay independent.
