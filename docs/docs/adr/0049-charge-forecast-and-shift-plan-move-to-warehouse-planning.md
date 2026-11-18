---
id: 0049-charge-forecast-and-shift-plan-move-to-warehouse-planning
slug: /adr/0049-charge-forecast-and-shift-plan-move-to-warehouse-planning
title: "0049. The charge forecast and the shift plan move to warehouse-planning"
sidebar_label: "49. Charge + shift plan move to warehouse-planning"
sidebar_position: 49
description: "ADR 0049 — ChargeForecast and ShiftPlan/PathPlan are handed over to warehouse-planning (its ADR 0023): POST /paths/{pathId}/charge and /plan answer 410 naming the new routes, ReceiveChargeForecast, CommitShiftPlan and the drift reconciliation are removed, ChargeForecastReceived, plan.ShiftPlanCommitted and PathPlanDriftDetected are retired. Release, flow balancing, LaborPlanObserved and PathCapacityChanged are unchanged."
---

# 0049. The charge forecast and the shift plan move to warehouse-planning

Date: 2026-11-18
Status: Accepted

Amends [ADR-0006](./0006-labor-plan-view-not-shift-plan.md) (the `ShiftPlan` half; the
`LaborPlanObserved` projection stays), supersedes
[ADR-0019](./0019-labor-plan-committed-shift-plan-reconciliation.md) (the drift check moves
with the plan) and retires the commit-time lookup of
[ADR-0017](./0017-travel-distance-lookup-on-commit-shift-plan.md). Leaves
[ADR-0018](./0018-path-capacity-changed.md) untouched. Counterpart: warehouse-planning ADR 0023.

## Context

This context holds two planning aggregates next to its execution model:

- `charge.ChargeForecast`, the volume due by each CPT on a path, saved by
  `ReceiveChargeForecast`;
- `plan.ShiftPlan` / `plan.PathPlan`, rate x heads x hours on a path with the
  `plannedHeads <= installedStations` invariant, saved by `CommitShiftPlan`.

Read on `develop`: no use case reads a `ChargeForecast` after saving it, and the only readers
of a `ShiftPlan` are the drift reconciliation of ADR-0019 and the plan's own `GET`. Release
(`ReleaseNextWork`), admission (`WorkPool`), flow balancing (`RebalanceDecision`, backlog
telemetry) and `PathCapacityChanged` use neither. The only caller of the two POST routes is the
fleet's warehouse-day simulation, which sends the same planner's morning to three contexts. The
published `ChargeForecastReceived` and `plan.ShiftPlanCommitted` carry only `path_id`, and no
context outside this one consumes them or `PathPlanDriftDetected` (this repo's own analytics
consumer acknowledges them).

Deciding a shift's forecast and heads is planning; releasing and balancing work against the
live floor is execution. warehouse-planning, which already owns capacity plans, takes both
(its ADR 0023) and publishes the result.

## Decision

1. **Stage C cutover, after warehouse-planning's routes are on `develop` and the simulation
   uses them.**
   - `POST /paths/{pathId}/charge` answers `410` with problem type `charge-forecast-moved`,
     whose detail names warehouse-planning's `PUT /workload-forecasts/{site_id}/{path_id}`.
   - `POST /paths/{pathId}/plan` answers `410` with problem type `shift-plan-moved`, naming
     `PUT /labour-plans/{site_id}/{shift_id}/{path_id}`.
   - The two `GET` routes stay one release, marked `deprecated`, then go in stage E.
   - The contract test excludes the two always-410 operations with a stage-E removal note.
2. **Removed:** `ReceiveChargeForecast`, `CommitShiftPlan`, the drift reconciliation in
   `ObserveLaborPlan` (the projection into `LaborPlanObserved` stays), their REST wiring, the
   travel-distance lookup of ADR-0017 and the pick-load annotations of a bucket and a plan
   (they were only echoed back in responses). `TRAVEL_DISTANCE_MODE` and its chart value go
   with the lookup, and so does `FACILITY_LAYOUT_BASE_URL` (the REST sync edge to
   facility-layout) unless another feature still reads it, checked at implementation; the
   warehouse-infra values entries are removed in the same rollout.
3. **Retired types**, removed from both channels of `apis/asyncapi.yaml` and marked retired
   in the CloudEvents type catalogue (the fitness test keeps one row per type):
   `com.warehouse.wes.work-planning.charge.ChargeForecastReceived`,
   `com.warehouse.wes.work-planning.plan.ShiftPlanCommitted`,
   `com.warehouse.wes.work-planning.pathplan.PathPlanDriftDetected`. The analytics projector
   keeps acknowledging the historical types. Before removal, every sibling's `develop` is
   grepped again for a non-test consumer; a hit stops the retirement.
4. **Tables are kept.** The charge and plan tables stop being written. Dropping them is
   destructive and waits for an explicit approval and its own migration.
5. **Unchanged:** `WorkPool`, waveless release, flow balancing, backlog telemetry,
   `PathCapacityChanged` (remaining admission capacity per path and CPT, ADR-0018),
   `LaborPlanObserved` and its `GET /paths/{pathId}/labor-plan-view`, which `RebalanceDecision`
   still returns as context.
6. **Language.** This context publishes *remaining admission capacity* (live, per path and
   CPT). *Planned capacity* is warehouse-planning's (per window). The glossary says so; no
   event is renamed.

## Consequences

- The context keeps only what it executes with; the planner's morning no longer touches it.
- Two routes answer 410 for one release, then disappear with their `GET`s.
- Three published types leave the contract. Nothing outside this repo consumed them.
- The `plannedHeads <= installedStations` invariant is not lost: it moves verbatim into
  warehouse-planning's `LabourPlan`.

## Alternatives rejected

- **Keep the aggregates and also consume warehouse-planning's plan.** Two copies of one
  decision, and a third reconciliation.
- **Keep a local copy of the published labour plan "for later".** Nothing here reads a plan;
  a copy nobody reads is dead state. A future rule that needs it (a WIP limit derived from
  planned heads) adds the copy with its own ADR.
- **Import the stored rows into warehouse-planning.** Both are overwritten every shift; the
  planner sends them again at the next shift boundary.
