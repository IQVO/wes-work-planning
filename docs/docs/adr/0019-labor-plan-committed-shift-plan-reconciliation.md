---
id: 0019-labor-plan-committed-shift-plan-reconciliation
slug: /adr/0019-labor-plan-committed-shift-plan-reconciliation
title: 0019. Reconcile our committed PathPlan against Workforce's LaborPlanObserved
sidebar_label: 0019. Labor plan reconciliation
description: ADR 0019 (Accepted) — closes ADR-0006's explicitly deferred reconciliation gap. Proposes an event-triggered, same-process comparison of this repo's own PathPlan against the already-projected LaborPlanObserved read model, a new PathPlanDriftDetected fact surfaced additively (never enforced), and why the comparison belongs here rather than in workforce-management or a new shared service.
---

# 0019. Reconcile our committed `PathPlan` against Workforce's `LaborPlanObserved`

## Status

**Accepted** (2026-09-26). Implementation follows in a subsequent PR.

## Context

[ADR-0006](./0006-labor-plan-view-not-shift-plan.md) deliberately kept
Workforce's committed headcount (`ShiftPlanCommitted`, projected here into
`LaborPlanObserved`) out of this service's own `PathPlan`/`ShiftPlan`
aggregate — two models, same shape, different owners, never merged. That
ADR named the resulting gap explicitly and left it open:

> **Reconciliation is out of scope.** Nothing detects that we planned 6
> while Workforce committed 7. The data is exposed; noticing is a human's
> job today.

That gap is still open. Both facts already exist in this repo's own
storage today, and neither the code nor any operator surface compares
them:

- **Our own fact**: `PathPlan.PlannedHeads()`, committed by this
  service's own `CommitShiftPlan` use case
  (`internal/application/usecases/commit_shift_plan.go`), read from
  `PlanRepo`.
- **Workforce's fact, already ACL'd**: `LaborPlanObserved.PlannedHeads`,
  projected from Workforce's `ShiftPlanCommitted` Kafka event by the
  existing `ObserveLaborPlan` use case
  (`internal/adapters/inbound/kafka/consumer.go`'s `handleWorkforceEvent`),
  read from `LaborPlanViewRepo`, keyed by `PathId`.

One thing worth stating plainly, because it changes what a solution needs
to do: **this service's own `ShiftPlanCommitted` event carries only
`PathId`, not `PlannedHeads`**
(`internal/adapters/outbound/kafka/publisher.go`'s `dataFor` case for
`shared.ShiftPlanCommitted` marshals `{"path_id": ...}` alone). A
reconciliation design that consumed WES's own wire event to compare
against Workforce would need to widen that event's payload first. A
design that instead reads `PathPlan` directly out of `PlanRepo` — the
same repository the use case that committed it already reads and writes
— needs no such change.

### Does workforce-management's new all-paths staffing-gap endpoint change what's feasible here?

Fleet-wide session PR #94 on `workforce-management` added
`GET /buildings/{buildingId}/shifts/{shiftId}/staffing-gap` — the
fast-follow that repo's own ADR-0011 named as deferred. It answers a
**different** question from the one this ADR needs answered:
`plannedHeads` (Workforce's own committed `ShiftPlan`, per path) versus
**currently active** `LaborAssignment` count, for every path in one
building+shift, raising `PathUnderstaffed` per line. It never touches
this repo's `PathPlan`, and this repo has no need to call it: Workforce's
`plannedHeads` — the exact figure that endpoint's response carries — is
already the same fact `LaborPlanObserved.PlannedHeads` already holds
here, delivered asynchronously via ADR-0006's existing projection. Adding
a synchronous call to the new endpoint would introduce a cross-context
call this repo does not need for anything the endpoint offers beyond what
it already has: the endpoint's genuinely new information —
Workforce's **active** (not planned) headcount — answers "is Workforce
currently short-staffed against its own plan," not "does Workforce's plan
agree with ours," which is what this ADR is about. The precedent it does
confirm is useful, though: Workforce's own `GetStaffingGap` is exactly
the same shape this ADR proposes — a read-only projection that surfaces a
gap and raises an event, never auto-corrects — reinforcing that this
fleet already has a working, accepted idiom for this kind of signal.

### The fleet's stated preference this design has to respect

[ADR-0004](./0004-kafka-integration-events.md) rejected synchronous
cross-context HTTP for exactly this pair of contexts, on the grounds that
it "would put another context on the critical path" and "invert the
ownership of facts." ADR-0006 already built the correct-shaped answer for
*getting* Workforce's fact here (an async ACL projection); this ADR must
not undo that by introducing a live call to Workforce merely to compare
against a fact this repo already has.

## Decision

**We will compute the comparison inside wes-work-planning, triggered at
the two moments either side's fact changes, over data already resident in
this repo's own stores — no new cross-repo call, no new consumed topic,
no scheduler.**

### 1. Where the comparison lives, and why here and not Workforce or a new service

This repo is the only place both facts exist without a new integration:

- Workforce-management does not have this repo's `PlannedHeads`. Giving
  it that fact would require (a) widening this repo's own
  `ShiftPlanCommitted` wire event to carry `PlannedHeads` — a breaking
  change to an existing, already-consumed contract — and (b) a brand-new
  inbound Kafka dependency in workforce-management on
  `warehouse.work-planning.events`, a dependency that repo has never
  needed for anything else it does. Both costs are paid to relocate a
  comparison this repo can already make for free.
- A new, third, shared reconciliation service is rejected for the same
  reason [ADR-0018](./0018-path-capacity-changed.md) rejected a
  brand-new bounded context for path capacity: there is no new domain
  concept here, only a comparison of two facts two existing contexts
  already separately own. A service that owns zero domain logic of its
  own is exactly the anemic-context anti-pattern this fleet's DDD
  conventions warn against.
- Doing it here also means literally zero new cross-context calls, hot
  path or otherwise — the opposite of what a workforce-management-side or
  shared-service design would require.

### 2. Trigger: evaluated at write-time on either side, not a scheduled sweep

A scheduled batch job was considered and rejected for the same reasons
[ADR-0003](./0003-flow-balancing-as-domain-service.md) rejected one for
flow balancing: it adds a scheduler to a core domain that has
deliberately never had one, it has a staleness window equal to its
period, and — the sharper point here — **it would re-derive a fact this
repo already knows the instant either side commits.** Both facts already
arrive as domain events inside this process:

1. **This repo's own `CommitShiftPlan`** already saves a `PathPlan` and
   publishes `ShiftPlanCommitted` inside one `UnitOfWork` scope
   (ADR-0014). After the save, it would additionally read
   `LaborPlanViewRepo.FindByPathId` for the same `PathId`. If a
   `LaborPlanObserved` already exists there, compute the comparison.
2. **This repo's existing `ObserveLaborPlan` use case** (invoked by the
   Kafka consumer on Workforce's `ShiftPlanCommitted`) already saves a
   `LaborPlanObserved` row. After that save, it would additionally read
   `PlanRepo` for the same `PathId`. If a committed `PathPlan` already
   exists there, compute the comparison.

Both call one shared, pure comparison function — the same
never-let-two-call-sites-disagree discipline workforce-management's own
`GetStaffingGap`/`ExecuteAll` (`gapForLine`, PR #94) and this repo's own
`SampleBacklog`/`RebalanceDecision` split already use — so whichever side
commits second is the one that raises the drift, and the two trigger
points can never compute a different answer for the same pair of facts.
Whichever side commits **first** finds nothing to compare against yet and
raises nothing — no fabricated drift against an absent fact, the same
discipline `LaborPlanObserved`'s own 404-when-nothing-observed contract
already applies.

### 3. The signal: `PathPlanDriftDetected`, surfacing only, never a verdict

A new domain event, additive to `internal/domain/shared/events.go`:

```go
type PathPlanDriftDetected struct {
    baseEvent
    PathId             PathId
    WesPlannedHeads    int
    ObservedPlannedHeads int  // Workforce's committed figure (LaborPlanObserved.PlannedHeads)
    DriftHeads         int    // ObservedPlannedHeads - WesPlannedHeads; signed, never absolute
    ObservedAt         time.Time // Workforce's own commit timestamp, from LaborPlanObserved
}
```

Raised only when `DriftHeads != 0` — an agreeing pair is not an event,
mirroring `RebalanceDecision`'s own `NoActionNeeded` discipline (a
recommender/detector that always fires trains people to ignore it).
`DriftHeads` is signed and reports no opinion about which side is
"right" — exactly the same posture ADR-0006 already took ("both facts are
available side by side... genuinely useful... exactly why the projection
appears as *context*"). This service is not entitled to reject
Workforce's number and does not attempt to.

Published on the **existing** integration topic
(`warehouse.work-planning.events`) and the **existing** analytics topic
(`warehouse.wes.analytics`), through the **existing** transactional
outbox (ADR-0014) — a new `case` in `dataFor`'s type switch and in
`marshalAnalyticsData`, exactly mirroring how `PathCapacityChanged` was
wired (ADR-0018). No new table, no new relay, no new topic.

**Additive surfacing on the existing read endpoint.** `GET
/paths/{pathId}/labor-plan-view` gains OPTIONAL `driftHeads` and
`driftDetectedAt` fields, present only when a comparison has actually
been computed for that path, omitted (not zeroed) otherwise — the same
omit-when-absent discipline as `travelDistanceM`/`travelDistanceEstimated`
(ADR-0017) and `required_capabilities`/`fragile` (ADR-0009). A caller
already reading this endpoint for the existing "planned vs observed"
context (ADR-0006's own stated rationale for exposing the projection)
sees the drift without a second call.

### 4. Who consumes it

- **`warehouse-ops-agent`** is the natural first consumer. Its own
  `FlowBalanceAdvisory` use case already correlates wes-work-planning's
  rebalance signal with workforce-management's staffing-gap signal in one
  cross-context recommendation, and its documented charter is precisely
  "none of the five contexts is the natural owner of cross-context
  correlation." A future MCP tool there (mirroring its existing
  `get_rebalance_recommendation`/`get_staffing_gap` client shape) reading
  this repo's now-widened `labor-plan-view` is the natural v1 consumption
  path, deferred to that repo's own ADR to decide, exactly as ADR-0018
  left order-management's consuming adapter to order-management's own
  future PR.
- **A dashboard/operator console** reading the same additive fields
  directly is equally valid for v1 and requires nothing further from this
  repo.
- **labor-performance** is explicitly NOT the natural consumer: its
  bounded context measures actual task durations against a declared
  standard, not plan-vs-plan agreement between two other contexts: nothing
  about `PathPlanDriftDetected` is inside its stated boundary.

## What is IN v1 and what is explicitly deferred

**In v1:**

- Same-process comparison triggered from `CommitShiftPlan` and
  `ObserveLaborPlan`, over data already in `PlanRepo` and
  `LaborPlanViewRepo`.
- `PathPlanDriftDetected` on the existing integration + analytics topics,
  via the existing outbox.
- `driftHeads`/`driftDetectedAt` surfaced additively on `GET
  /paths/{pathId}/labor-plan-view`.
- Comparison is over `PlannedHeads` only.

**Deferred:**

- Any consuming context's reaction (an ops-agent correlation rule, a
  dashboard visualization, an alert/paging threshold) — this ADR proposes
  the signal only, exactly as ADR-0018 published `PathCapacityChanged`
  without prescribing order-management's consuming adapter.
- Comparing `Rate`/`Hours` in addition to `PlannedHeads` —
  `LaborPlanObserved` already carries `PlannedRate`/`PlannedHours`
  (ADR-0006), so widening is additive and mechanically identical, but is
  left out of v1 to keep the first cut small and reviewable (the same
  narrow-scope discipline labor-performance's own ADR-0013 applied to its
  first integration event).
- Drift **trend** over multiple shifts ("this path has drifted three
  shifts running") — v1 is stateless per commit pair, no historical
  drift-over-time aggregate. The existing analytics report
  (ADR-0011) could carry this later without a new topic.
- Reconciling a **draft/proposed** plan before either side commits — v1
  compares committed facts on both sides only.
- Any verdict about which side is correct, and any automatic correction
  of either plan. Reconciliation surfaces; a human (or a future,
  separately-decided policy) still decides, mirroring
  `RebalanceDecision`'s and Workforce's `GetStaffingGap`'s own
  "recommend/surface, never act" discipline.

## Alternatives considered

- **A scheduled batch reconciliation job.** Rejected — re-derives a fact
  this repo already knows the instant either commit happens; adds a
  scheduler to a core domain that has deliberately never needed one (see
  ADR-0003's identical reasoning for flow balancing).
- **Reconciliation logic living in workforce-management.** Rejected —
  requires widening this repo's `ShiftPlanCommitted` wire event
  (currently `PathId` only) to carry `PlannedHeads`, plus a brand-new
  inbound Kafka dependency in workforce-management on this repo's topic —
  a strictly larger change to relocate a comparison this repo can already
  make for free against data it already holds.
- **A new shared reconciliation service/read model.** Rejected — no new
  domain concept exists to own; see ADR-0018's identical rejection of a
  new bounded context for path capacity.
- **A synchronous call from this repo to Workforce's new fleet-wide
  `GET /buildings/{id}/shifts/{id}/staffing-gap` (PR #94) at commit
  time.** Rejected — this repo already has Workforce's `plannedHeads` via
  the existing ADR-0006 projection; a live call would add exactly the
  cross-context critical-path coupling ADR-0004 rejected, for a fact
  already available locally.

## References

- [ADR-0006 — Project Workforce's `ShiftPlanCommitted` into
  `LaborPlanObserved`, not our `ShiftPlan`](./0006-labor-plan-view-not-shift-plan.md) —
  the ADR whose "reconciliation is out of scope" gap this record closes.
- [ADR-0003 — Flow balancing as a domain service, not a scheduled
  job](./0003-flow-balancing-as-domain-service.md) — the precedent
  against a scheduler for a decision this repo can compute on write.
- [ADR-0004 — Kafka integration events](./0004-kafka-integration-events.md) —
  why a synchronous cross-context call is rejected here too.
- [ADR-0014 — Transactional outbox](./0014-transactional-outbox.md) — the
  existing delivery mechanism this ADR's new event reuses unchanged.
- [ADR-0018 — `PathCapacityChanged`](./0018-path-capacity-changed.md) —
  the precedent for publishing an internally-derived fact additively, and
  for rejecting a new bounded context with no domain logic of its own.
- workforce-management PR #94 (`list-staffing-gaps-for-shift`) — the
  fleet-wide staffing-gap endpoint evaluated above and found orthogonal
  to this ADR's need.
