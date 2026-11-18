---
id: 0017-travel-distance-lookup-on-commit-shift-plan
slug: /adr/0017-travel-distance-lookup-on-commit-shift-plan
title: 0017. Read facility-layout's travel graph once, at shift-plan-commit time, to enrich PathPlan
sidebar_label: 0017. Travel distance on PathPlan
description: ADR 0017 — why PathPlan gains an optional real travel-distance hint from facility-layout's ADR-0017 travel graph, why the read is synchronous HTTP rather than a Kafka projection, and why the enrichment is fail-open and caller-opt-in.
---

# 0017. Read facility-layout's travel graph once, at shift-plan-commit time, to enrich `PathPlan`

## Status

> Retired by [ADR-0049](./0049-charge-forecast-and-shift-plan-move-to-warehouse-planning.md) on cutover (2026-11-18): the lookup goes with `CommitShiftPlan`.

Accepted — implemented in the same change that introduced this record.

## Context

facility-layout's own ADR-0017 ("Geometry & Travel Graph") built a
pure-domain travel graph over aisle centrelines and cross-aisles, exposed
at `GET /distance?from=&to=`: the shortest travel distance, in metres,
between two coded locations, flagged `estimated` when any leg used a
zone's bay-pitch fallback rather than real geometry. That same ADR's
boundary statement is explicit and non-negotiable:

> FL publishes distances and adjacency (facts). Travel TIME, congestion,
> route choice = wes-work-planning. Slotting optimization = NOT FL.

This service's own `AGENTS.md` had named the absence of a travel/adjacency
input as a known gap since before facility-layout's geometry work existed:
`ShiftPlan`/`PathPlan` commit a rate × heads × hours split for a process
path with no notion of where that path's stations physically sit relative
to the zones it draws work from. The question this ADR answers is not
*whether* to consume facility-layout's new fact — that follow-on was
already scoped in facility-layout's own plan as "Phase B3: wes-work-planning
new outbound port `TravelDistanceLookup`" — but *how this service reads
it*, since facility-layout's `estimateTravelDistance`/`GET /distance`
endpoint is a synchronous REST read, not a Kafka-published event (there is
no "distance changed" domain event — a travel distance between two static
locations changes only when the physical layout itself changes).

### Precedent: this is the same shape as ADR-0009, one boundary over

This service already has exactly this integration shape for a different
sibling: [ADR-0009](./0009-product-classification-propagation-to-work-released.md)
reads inventory-storage's product classification once, synchronously, at
`ReleaseNextWork` publish time, via `ports.ProductClassificationLookup`
with an HTTP `Client` and a `PermissiveLookup` no-op default selected by
`PRODUCT_CLASSIFICATION_MODE=http|permissive`. That ADR's own rationale —
"build against the sibling's real, current contract rather than a Kafka
consumer for an event that doesn't exist," "fail-open, always," "never
blocks the use case it enriches" — applies here verbatim, just against
facility-layout's `/distance` endpoint instead of inventory-storage's
`/products/{sku}/classification`.

## Decision

**We will read the travel distance between two caller-supplied
facility-layout LocationCodes once, synchronously, from
`GET /distance?from=&to=`, at the moment `CommitShiftPlan` commits a
`PathPlan` — and stamp two derived, OPTIONAL fields onto that `PathPlan` —
rather than build a Kafka projector for a fact that has no publish event.**

1. **Mechanism: synchronous outbound HTTP, mirroring this service's own
   `productclassification` adapter pattern exactly, not a Kafka
   projector.** A new outbound port `ports.TravelDistanceLookup`
   (`GetDistance(ctx, from, to) (traveldistanceview.TravelDistanceView, error)`),
   a new package `internal/adapters/outbound/traveldistance/` with a plain
   `net/http` `Client` and a `PermissiveLookup` no-op default, selected via
   `TRAVEL_DISTANCE_MODE=http|permissive` (default `permissive`),
   requiring `FACILITY_LAYOUT_BASE_URL` in `http` mode.
2. **A new, un-persisted read model, not a projection table.**
   `internal/domain/traveldistanceview.TravelDistanceView{From, To,
   MetresM, Estimated, Known}` is a plain value returned by the lookup
   call and discarded after use — no `processed_events` row, no Postgres
   table, no idempotency concern, exactly like
   `productclassificationview.ProductClassificationView` and for the same
   reason: nothing is being applied to persisted state that survives past
   the single request.
3. **`PathPlan` carries an optional travel-distance hint.**
   `CommitShiftPlanRequest` gains optional `FromLocationCode`/
   `ToLocationCode` fields (empty by default, so every existing caller
   keeps compiling), and `PathPlan` gains
   `SetTravelDistance(metresM float64, estimated bool)` plus
   `TravelDistanceKnown()`/`TravelDistanceM()`/`TravelDistanceEstimated()`
   accessors — a plain setter after construction, not a `NewPathPlan`
   parameter, so this stays additive rather than touching the aggregate's
   existing `plannedHeads <= installedStations` invariant. Both caller-
   supplied location codes must be present to attempt the lookup; either
   omitted skips it entirely — this is a caller opt-in enrichment, not a
   required field on every shift-plan commit.
4. **The lookup and the stamp both live in the use case**, not the
   outbound Kafka adapter — unlike ADR-0009's `WorkReleased` enrichment
   (which lives in `internal/adapters/outbound/kafka/publisher.go` because
   it enriches an outbound *event* payload), this enriches the *returned
   aggregate itself* (`PathPlan`), which the REST response DTO then
   reflects directly. `CommitShiftPlan.enrichWithTravelDistance` runs
   right after `plan.NewPathPlan` succeeds and before `plan.NewShiftPlan`
   wraps it.
5. **REST surface: `travelDistanceM`/`travelDistanceEstimated` are OMITTED
   from `ShiftPlanResponse`** — not defaulted to `0`/`false` — when no
   hint was recorded, so a consumer that already treats "absent" as "no
   hint" sees no difference from before this feature existed (same
   omit-when-unknown discipline as `WorkReleased`'s
   `required_capabilities`/`fragile`, ADR-0009).
6. **Fail-open, always — never blocks committing a shift plan.** Missing
   location codes, a `nil` `TravelDistanceLookup` (mirrors
   `PermissiveLookup`'s own `Known=false`), an unknown location, a
   cross-zone pair (facility-layout refuses those rather than guessing —
   see its own ADR-0017), or a lookup error (timeout, 5xx,
   facility-layout down) are all treated identically: no hint, and
   `CommitShiftPlan.Execute` still succeeds and still publishes
   `ShiftPlanCommitted`. This mirrors ADR-0009's own asymmetry with
   inventory-storage's fail-closed `StowStock` check: a travel-distance
   hint is an enrichment of a planning aggregate, not a placement safety
   invariant, so blocking a commit on a sibling service's uptime would be
   the wrong trade.

## Consequences

### Easier

- **A `PathPlan` can now say how far apart its two representative
  locations are**, closing the gap this service's own `AGENTS.md` had
  named since before facility-layout's geometry work existed, without
  this service ever computing travel time, congestion, or route choice
  itself — those remain squarely wes-work-planning's concern per
  facility-layout's own boundary statement, and this ADR only consumes
  the *distance fact*, never derives a time/rate from it.
- **No new idempotency mechanism.** Because nothing is persisted beyond
  the committed `PathPlan` itself, there is no `processed_events` row to
  write and nothing to double-apply on retry.
- **Matches what facility-layout actually shipped.** Building against
  `GET /distance?from=&to=` means this integration compiles and works
  against facility-layout's real, current, spec-first contract
  (`apis/openapi.yaml`'s `TravelDistance` schema) rather than a
  speculative shape.
- **Symmetric with this service's own ADR-0009 adapter shape.** A
  developer who has read `productclassification.Client`/
  `PermissiveLookup` recognizes `traveldistance.Client`/
  `PermissiveLookup` immediately; `TRAVEL_DISTANCE_MODE=http|permissive`
  mirrors `PRODUCT_CLASSIFICATION_MODE` exactly, down to the env var
  shape.

### Harder

- **A travel-distance hint recorded at commit time is not retroactively
  refreshed.** If the underlying facility-layout geometry changes after a
  shift plan is committed (a new cross-aisle registered, an aisle's
  centreline corrected), the already-committed `PathPlan`'s hint does not
  pick up the change — a known and accepted gap, in the same spirit as
  ADR-0009's "classification drift after release is not retroactively
  applied."
- **`FromLocationCode`/`ToLocationCode` are caller-supplied, not derived
  from the process-path catalogue.** This service has no notion today of
  "this path's representative pick location" or "this path's station
  location" — the caller (an operator, a console screen, or a future
  fulfillment-execution `Station.locationCode`, itself a separate Phase B3
  follow-on) must know and supply the two LocationCodes explicitly. A
  future enhancement could derive these automatically once
  fulfillment-execution's optional `Station.locationCode` (also
  facility-layout Phase B3) exists and can be looked up by `pathId`; that
  is out of scope here.
- **A synchronous cross-service call sits on the commit path.** Unlike
  every read this service previously performed inside `CommitShiftPlan`,
  the use case now makes a real HTTP call (in `http` mode, and only when
  both location codes are supplied) to facility-layout before the
  `ShiftPlan` is saved and `ShiftPlanCommitted` is published. The
  fail-open design bounds the *availability* risk (a slow/unavailable
  lookup degrades to "no hint," not to a failed commit), but it does add
  real latency to a commit that supplies location codes when
  `TRAVEL_DISTANCE_MODE=http` is enabled — a cost this service's core
  planning path never paid before, mirroring ADR-0009's own documented
  trade-off exactly.
