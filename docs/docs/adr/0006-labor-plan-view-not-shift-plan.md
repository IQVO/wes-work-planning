---
id: 0006-labor-plan-view-not-shift-plan
title: ADR-0006 — Project Workforce's ShiftPlanCommitted into a separate read model
sidebar_label: 0006 · LaborPlanObserved ≠ ShiftPlan
sidebar_position: 6
description: Why an inbound event named ShiftPlanCommitted is deliberately not fed into the aggregate of the same name.
---

# ADR-0006 — Project `ShiftPlanCommitted` into `LaborPlanObserved`, not into our `ShiftPlan`

## Status

> Amended by [ADR-0049](./0049-charge-forecast-and-shift-plan-move-to-warehouse-planning.md) (2026-11-18): the `ShiftPlan` half moves to warehouse-planning; the `LaborPlanObserved` projection described here stays.

**Accepted.**

## Context

`workforce-management` publishes `ShiftPlanCommitted` when a human commits a
split of headcount across process paths, one message per path line:

```json
{"building_id": "BLD1", "shift_id": "S1", "path_id": "pick-a",
 "planned_heads": 7, "planned_rate": 95.5, "planned_hours": 8}
```

This service has an aggregate called `ShiftPlan`, containing `PathPlan` lines
with `plannedHeads`, `rate` and `hours`.

Same name. Nearly the same fields. The obvious implementation is to feed the
event into `CommitShiftPlan` and be done.

That would be the classic DDD mistake. The strategic reference names it
directly:

> **Same word, different model is allowed — and expected.** `Task` in WMS and
> `Task` in WES are deliberately different classes with different lifecycles.
> Don't force a shared DTO/entity across the boundary; translate at the ACL.

The two models genuinely differ:

| | Workforce's `ShiftPlan` | Our `ShiftPlan` |
|---|---|---|
| **What it is** | A labour commitment: which associates, which paths, direct vs indirect hours, one per building per shift | This context's own throughput decision: rate × heads × hours on a path |
| **Owns** | associates, certifications, breaks, assignments | the `plannedHeads ≤ installedStations` invariant |
| **Concept of "installed stations"** | none — it does not model physical station counts | central; it is the invariant's right-hand side |
| **Who may change it** | a human in Workforce | this service's `CommitShiftPlan` use case |
| **Lifecycle** | committed at shift start, revised intra-shift | committed by us, then read by release and rebalance |

Three concrete failures follow from merging them.

1. **An external system could violate our invariant.** Workforce has no
   `installedStations` and no reason to respect it. A `planned_heads: 12` event
   for a path with 8 stations would either be rejected — meaning we drop a fact
   Workforce has already committed and is entitled to commit — or accepted,
   destroying the invariant. Neither is acceptable.
2. **Our commit logic would be coupled to their publish cadence.** Every
   Workforce revision would silently overwrite a decision this context made.
3. **The failure would be invisible.** No compiler and no test catches "these
   two `ShiftPlan`s are the same thing", because from a distance they look
   identical. The only defence is a deliberate boundary.

## Decision

**We will project `ShiftPlanCommitted` into a new, separate, read-only type
called `LaborPlanObserved`, and it will never touch the `ShiftPlan` aggregate or
the `CommitShiftPlan` use case.**

1. **A new package, `internal/domain/laborview`**, holding a plain value:

   ```go
   type LaborPlanObserved struct {
       PathId       shared.PathId
       PlannedHeads int
       PlannedRate  float64
       PlannedHours float64
       ObservedAt   time.Time
   }
   ```

   Exported fields, no constructor, no validation, no behaviour. That shape is
   the decision made visible: this type protects nothing, because the facts in
   it are owned by another bounded context and we are not entitled to reject
   them. Compare `PathPlan`, which has private fields and a validating
   constructor because its rule is ours.

2. **Deliberately different names.** Not `ShiftPlanView` or `ExternalShiftPlan`
   — `LaborPlanObserved`. A reader who sees it beside `ShiftPlan` should have to
   ask why, and get this answer.

3. **Its own port, table and endpoint.** `LaborPlanViewRepo` with memory and
   Postgres adapters, a `labor_plan_view` table joined to nothing, and
   `GET /paths/{pathId}/labor-plan-view` — returning `404` when nothing has been
   observed, so "no observation" is distinguishable from "observed zero".

4. **Idempotent projection.** The `event_id` is checked against
   `processed_events` before writing, so redelivery does not re-write the row.

5. **Additive context only in `RebalanceDecision`.** The response may carry the
   projection as an extra field. The decision logic does **not** consult it, and
   the existing rebalance tests were unchanged by adding it.

The same reasoning produced `UsableInventoryObserved` for Inventory's events —
keyed by **SKU**, not by path, because Inventory reservations are SKU-scoped and
a SKU-to-path mapping does not exist in the domain. Forcing one for the sake of
consistency with our other endpoints would have produced a projection wrong in a
way no test could catch.

## Consequences

### Easier

- **Our invariant is inviolable from outside.** No external event can reach
  `PathPlan`'s constructor.
- **Both facts are available side by side.** "We planned 6 heads; Workforce
  committed 7" is answerable, and it is genuinely useful next to a
  `ReassignLabor` recommendation — which is exactly why the projection appears
  as *context* on that response.
- **Workforce's schema can change freely.** Its payload struct is unexported
  inside our inbound Kafka adapter; nothing downstream of the ACL knows its
  shape.
- **The read model is trivial.** No invariants means no invariant tests, no
  migration risk from a rule change, and a table with no foreign keys.

### Harder

- **Two similar types in one codebase.** `ShiftPlan`, `PathPlan`,
  `LaborPlanObserved` — a newcomer will ask whether the third is redundant. This
  ADR is the answer, and it needs to keep being given.
- **Nothing enforces the separation.** No test fails if someone wires the
  consumer into `CommitShiftPlan`. It is a convention, defended by naming,
  package boundaries and this record. (The
  [arch-go fitness tests](./0007-arch-go-fitness-tests.md) enforce layering, not
  semantic boundaries.)
- **Duplication is now permanent.** Both models carry planned heads and rate.
  They will drift, and drift is the *correct* outcome — but it will look like a
  bug to someone.
- **Reconciliation is out of scope.** Nothing detects that we planned 6 while
  Workforce committed 7. The data is exposed; noticing is a human's job today.
- **The name is longer.** `LaborPlanObserved` reads awkwardly next to
  `ShiftPlan`. Accepted: awkwardness that prompts a question is cheaper than
  familiarity that hides one.
