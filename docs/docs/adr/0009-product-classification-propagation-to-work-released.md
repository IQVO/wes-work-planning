---
id: 0009-product-classification-propagation-to-work-released
title: ADR-0009 — Propagate inventory-storage's ProductClassification onto WorkReleased via a synchronous read at release time
sidebar_label: 0009 · Product classification on WorkReleased
sidebar_position: 9
description: Why fulfillment-execution's hazmat-capability/fragile hints are read once from inventory-storage at release time and stamped onto WorkReleased, and why that read is synchronous HTTP rather than a Kafka projection.
---

# ADR-0009 — Read-once-at-release, stamp onto `WorkReleased`

## Status

**Accepted. Decision §1 (synchronous HTTP lookup) and §2 (un-persisted
read model) superseded by
[ADR-0035](./0035-product-classification-local-copy.md)**: the
classification now comes from a local copy of product-master's
`ProductClassified` events. §3–§5 still apply.

## Context

A parallel effort added a `ProductClassification` concept to
`inventory-storage`: SKU-level master data tagging a SKU `Hazmat`, `Fragile`,
`TemperatureSensitive`, `Oversized`, and/or `HighValue`, exposed at
`GET /products/{sku}/classification` and written by a `ClassifyProduct` use
case that raises a `ProductClassified` domain event.

Downstream, `fulfillment-execution`'s `Task` aggregate already carries a
`requiredCapabilities` set that a station must hold to claim a task (used
today for the `pick`/`pack`/`slam` process-path capability), and the
strategic direction is for it to also carry a `fragile` hint — both consulted
at `claimNext(stationId, capabilities)` time, when a station is being matched
to work. The requirement is for `fulfillment-execution` to have these hints
**on the `Task` at claim time**, without adding a live per-task callback into
`inventory-storage` — the same design already used for `cpt` and `ref` on
this service's own `WorkReleased` payload (see
[ADR-0004](./0004-kafka-integration-events.md) §4: "The outbound adapter
enriches `WorkReleased` … so the published contract is self-sufficient and no
consumer has to call back").

The question was not *whether* to enrich `WorkReleased` — that pattern is
already established — but *how this service itself learns the
classification*, since `inventory-storage`'s events are this service's
upstream integration surface and Task 7
([ADR-0004](./0004-kafka-integration-events.md)) established Kafka as the
default mechanism for exactly this kind of cross-context read.

### Finding: `ProductClassified` is not on the wire

Before building a consumer, `inventory-storage`'s actual outbound adapter was
inspected rather than assumed. Its
`internal/adapters/outbound/kafka/publisher.go` carries this doc comment,
verbatim:

> Only `StockReserved` and `ReservationRevoked` are part of the published
> integration contract … every other domain event is a local concern and is
> not forwarded here.

Its `apis/asyncapi.yaml` confirms this explicitly under "Full catalog vs.
actually published": `ProductClassified` is catalogued as a domain event (it
has a documented CloudEvents schema, an example payload, everything a
consumer would need to build against) but the document says outright that it
"is raised in-process only, delivered to the configured
`ports.EventPublisher`… and dropped by the Kafka adapter's `default` branch"
— and that a consumer should "not build a consumer against a catalog-only
message until it has been wired."

This matters because this service's own
[ADR-0004](./0004-kafka-integration-events.md) established Kafka projection
as the *default* pattern for consuming a sibling's facts
(`UsableInventoryObserved` from `StockReserved`/`ReservationRevoked`,
`LaborPlanObserved` from `ShiftPlanCommitted`). Following that default
blindly here would mean building an inbound consumer, a topic subscription,
and an idempotent projector for an event that never reaches
`warehouse.inventory.events` — a consumer with nothing to consume.

`inventory-storage` did, however, ship a synchronous read endpoint for the
same data (`GET /products/{sku}/classification`), and its own `StowStock` use
case already established the pattern of a synchronous outbound HTTP lookup
for a related concern: a `LocationClassificationLookup` port with an HTTP
`Client` implementation and a `PermissiveLookup` no-op default, selected via
`LOCATION_LOOKUP_MODE=http|permissive`. That is the mechanism actually
available to integrate against.

## Decision

**We will read a released unit's SKU classification once, synchronously,
from `inventory-storage`'s `GET /products/{sku}/classification`, at the
moment `ReleaseNextWork` publishes `WorkReleased` — and stamp two derived,
OPTIONAL fields onto that event's `data` payload — rather than build a Kafka
projector for an event that is not actually published.**

1. **Mechanism: synchronous outbound HTTP, mirroring inventory-storage's own
   `facilitylayout` adapter pattern, not a Kafka projector.** A new outbound
   port `ports.ProductClassificationLookup`
   (`GetClassification(ctx, sku) (productclassificationview.ProductClassificationView, error)`),
   a new package `internal/adapters/outbound/productclassification/` with a
   plain `net/http` `Client` and a `PermissiveLookup` no-op default, selected
   via `PRODUCT_CLASSIFICATION_MODE=http|permissive` (default
   `permissive`), requiring `INVENTORY_STORAGE_BASE_URL` in `http` mode. This
   is a direct structural mirror of `inventory-storage`'s own
   `LocationClassificationLookup`/`facilitylayout` pattern — the same
   integration shape this platform already uses when a synchronous read is
   the actual contract, just applied on the other side of the same
   boundary.
2. **A new, un-persisted read model, not a projection table.**
   `internal/domain/productclassificationview.ProductClassificationView{SKU,
   HandlingTags, TemperatureClass, Known}` is a plain value returned by the
   lookup call and discarded after use — there is no `processed_events` row,
   no Postgres table, and no idempotency concern, because nothing is being
   applied to persisted state. This is deliberately unlike
   `UsableInventoryObserved`/`LaborPlanObserved`, which *are* persisted
   Kafka projections; conflating the two would misrepresent how fresh the
   data is (see the "harder" consequence below and [the ubiquitous-language
   trap](../business-context/ubiquitous-language.md#trap-4--productclassificationview-is-not-usableinventoryobserved)).
3. **`WorkUnit` carries an optional `SKU`.** `EnqueueWorkUnitRequest` gains an
   optional `SKU` field (empty by default, so every existing caller keeps
   compiling), threaded onto the `WorkUnit` aggregate via a plain setter —
   deliberately not a `NewWorkUnit` constructor parameter, so this stays
   additive rather than touching the aggregate's existing invariants.
4. **The lookup and the stamp both live in the outbound Kafka adapter**, next
   to the existing `cpt`/`ref` enrichment in
   `internal/adapters/outbound/kafka/publisher.go`'s `dataFor`. When the
   released unit carries a SKU, and a classification is found: `"hazmat"` is
   appended to `required_capabilities` if the SKU is tagged `Hazmat`;
   `fragile` is set `true` if the SKU is tagged `Fragile`. Both fields are
   **omitted from the payload entirely** — not defaulted to an explicit empty
   array / `false` — when there is nothing to say, so a consumer that already
   treats "absent" as "no hint" sees no difference from before this feature
   existed. This mirrors ADR-0004's own enrichment discipline: the
   translation/enrichment lives in the adapter, and the domain event
   (`shared.WorkReleased`) stays ignorant of what a downstream consumer
   wants.
5. **Fail-open, always — never blocks release.** An empty SKU, a `nil`
   `ProductClassificationLookup` (mirrors `PermissiveLookup`'s own
   `Known=false`), an unclassified SKU, or a lookup error (timeout, 5xx,
   `inventory-storage` down) are all treated identically: no hints, and
   `ReleaseNextWork` still succeeds and still publishes. This is a
   **deliberate asymmetry** with `inventory-storage`'s own `StowStock`
   placement check, which *fails closed* for a Hazmat/TemperatureSensitive
   SKU when its lookup is unavailable (`ErrLocationClassificationUnavailable`,
   a `409`) — that check protects a physical placement invariant; this one
   only enriches an optional integration hint, and blocking a release
   decision on a sibling service's uptime would reintroduce exactly the
   coupling [ADR-0004](./0004-kafka-integration-events.md) chose Kafka to
   avoid in the first place.

## Consequences

### Easier

- **fulfillment-execution's `Task` never calls back.** The hints arrive on
  the same event that already builds the `Task`, so claim-time dispatch
  logic (matching a station's capabilities against the task's
  `requiredCapabilities`) never has a network dependency on
  `inventory-storage` being up.
- **No new idempotency mechanism.** Because nothing is persisted, there is no
  `processed_events` row to write and nothing to double-apply on retry —
  unlike every Kafka-projected read model in this service.
- **Matches what was actually shipped.** Building against
  `GET /products/{sku}/classification` instead of a Kafka projector for
  `ProductClassified` means this integration compiles and works against
  `inventory-storage`'s real, current contract rather than a schema that
  exists only in its domain-event catalogue.
- **Symmetric with `inventory-storage`'s own adapter shape.** A developer who
  has read `facilitylayout.Client`/`PermissiveLookup` recognizes this
  adapter immediately; the `PRODUCT_CLASSIFICATION_MODE=http|permissive` env
  var mirrors `LOCATION_LOOKUP_MODE` exactly.

### Harder

- **Classification drift after release is not retroactively applied.** If a
  SKU's classification changes *after* its work unit was released — say it
  is reclassified `Hazmat` five minutes later — the already-published
  `WorkReleased` event, and therefore the `Task` built from it, does not
  pick up the change. This is a known and accepted gap, in the same spirit
  as `inventory-storage`'s own ADR-0003 "no expiry sweeper" gap: the
  classification is a snapshot taken once, at release time, not a live
  binding. A future task could add a `ProductReclassified`-driven
  correction path if this proves operationally necessary; today it is not
  built.
- **Two different freshness models for two similarly-shaped reads.**
  `UsableInventoryObserved` is continuously kept current by Kafka;
  `ProductClassificationView` is only as fresh as the single call made at
  release time and is not stored anywhere in this service. A reader who
  assumes both work the same way will reach the wrong conclusion about
  staleness — the ubiquitous-language trap documents this explicitly so the
  question keeps getting the same answer.
- **A synchronous cross-service call sits on the release path.** Unlike
  every other integration this service performs, `ReleaseNextWork`'s publish
  step now makes a real HTTP call (in `http` mode) to `inventory-storage`
  before a `WorkReleased` message reaches Kafka. The fail-open design
  bounds the *availability* risk (a slow/unavailable lookup degrades to "no
  hints," not to a failed release), but it does add real latency to every
  release when `PRODUCT_CLASSIFICATION_MODE=http` is enabled — a cost this
  service's core release path never paid before.
- **`required_capabilities` is derived from a fixed, hand-picked mapping.**
  Only `Hazmat` maps to `"hazmat"`; `Oversized`/`HighValue`/
  `TemperatureSensitive` are read from inventory-storage but not currently
  translated into any `WorkReleased` field. If a future need requires
  surfacing those too, this mapping in
  `internal/adapters/outbound/kafka/publisher.go`'s `classificationHints`
  is the place to extend it — documented here so it isn't rediscovered as a
  surprise.
