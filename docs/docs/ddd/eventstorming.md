---
id: eventstorming
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 6
description: Design-level EventStorming of the three processes of Work Planning & Release, in ddd-crew sticky notation, with code evidence for every sticky and real hotspots.
---

# EventStorming

Design-level EventStorming in the notation of the ddd-crew
[EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet),
reconstructed from the code rather than from a workshop. Three processes: plan
the shift, run the release loop, and observe and correct.

## Legend

```mermaid
flowchart LR
    A["Actor"]:::actor
    C["Command"]:::command
    G["Aggregate"]:::aggregate
    E["Domain event"]:::event
    P["Policy"]:::policy
    R["Read model"]:::readmodel
    X["External system"]:::external
    H["Hotspot"]:::hotspot
    classDef actor fill:#fff59d,stroke:#c9b800,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#b59a00,color:#000
    classDef event fill:#f6a04d,stroke:#b8651b,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#a93226,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
```

## Process 1 — Plan the shift

```mermaid
flowchart LR
    Planner["Planner"]:::actor
    WM["workforce-management"]:::external
    FL["facility-layout"]:::external

    C1["Receive charge forecast"]:::command
    A1["ChargeForecast"]:::aggregate
    E1["Charge forecast received"]:::event

    C2["Commit shift plan"]:::command
    A2["ShiftPlan with PathPlan"]:::aggregate
    E2["Shift plan committed"]:::event
    R0["Travel distance view"]:::readmodel

    EW["Workforce shift plan committed"]:::event
    P1["Whenever a labor plan arrives, project it and compare heads"]:::policy
    P2["Whenever we commit a plan, compare heads with the labor plan"]:::policy
    R1["LaborPlanObserved with drift"]:::readmodel
    E3["Path plan drift detected"]:::event
    H1["No context consumes drift yet"]:::hotspot

    Planner --> C1 --> A1 --> E1
    Planner --> C2
    FL --> R0 --> C2
    C2 --> A2 --> E2 --> P2
    WM --> EW --> P1 --> R1
    P2 --> R1
    P1 --> E3
    P2 --> E3
    E3 -.- H1

    classDef actor fill:#fff59d,stroke:#c9b800,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#b59a00,color:#000
    classDef event fill:#f6a04d,stroke:#b8651b,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#a93226,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
```

Source: `internal/application/usecases/receive_charge_forecast.go`,
`commit_shift_plan.go`, `observe_labor_plan.go`, `drift_reconciliation.go`,
`internal/domain/plan/*.go`, `internal/domain/laborview/*.go`. Omits:
validation failures (`ErrHeadsExceedStations`, `ErrNoBuckets`).

## Process 2 — The release loop

```mermaid
flowchart LR
    OM["order-management"]:::external
    Sup["Supervisor or ops agent"]:::actor
    PM["product-master"]:::external
    FE["fulfillment-execution"]:::external
    Caller["Upstream caller"]:::actor

    EO["Order allocated"]:::event
    P1["Whenever an order is allocated, enqueue one work unit per line"]:::policy
    C1["Enqueue work unit"]:::command
    A1["WorkPool"]:::aggregate
    A2["WorkUnit"]:::aggregate
    E1["Work unit created"]:::event

    C2["Release next work"]:::command
    P2["Earliest CPT first, refuse past WIP limit on release-fed pools"]:::policy
    E2["Work released"]:::event
    R1["Product classification view"]:::readmodel

    ET["Task completed"]:::event
    P3["Whenever a task completes, record completion of its work unit"]:::policy
    C3["Record completion"]:::command
    E3["Work unit completed"]:::event

    H1["Pool entries are never pruned, every save rewrites them"]:::hotspot
    H2["Hot WorkPool row under concurrent release, retried up to 12 times"]:::hotspot

    OM --> EO --> P1 --> C1
    Caller --> C1
    C1 --> A1
    C1 --> A2 --> E1
    Sup --> C2 --> P2 --> A1
    C2 --> A2
    A2 --> E2
    PM --> R1 --> E2
    E2 --> FE
    FE --> ET --> P3 --> C3
    Sup --> C3
    C3 --> A2
    C3 --> A1
    A2 --> E3
    A1 -.- H1
    A1 -.- H2

    classDef actor fill:#fff59d,stroke:#c9b800,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#b59a00,color:#000
    classDef event fill:#f6a04d,stroke:#b8651b,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#a93226,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
```

Source: `internal/application/usecases/apply_order_allocated.go`,
`enqueue_work_unit.go`, `release_next_work.go`, `record_completion.go`,
`apply_task_completed.go`, `pool_retry.go`,
`internal/domain/release/*.go`, `internal/domain/workunit/*.go`,
`internal/adapters/outbound/postgres/work_pool_repo.go`. Omits: the
idempotency layers (`Idempotency-Key`, processed-event mark) and the
stale-entry reconciliation inside release.

## Process 3 — Observe and correct

```mermaid
flowchart LR
    Sup["Supervisor or ops agent"]:::actor
    INV["inventory-storage"]:::external
    OM["order-management"]:::external
    NF["network-fulfillment"]:::external

    C1["Sample backlog"]:::command
    A1["WorkPool"]:::aggregate
    R1["Backlog telemetry"]:::readmodel
    E1["Backlog threshold breached"]:::event
    E2["Path capacity changed"]:::event

    C2["Decide rebalance"]:::command
    R2["Rebalance recommendation"]:::readmodel
    P1["Flow-fed over threshold means throttle upstream"]:::policy
    P2["Release-fed at WIP limit with backlog means reassign labor"]:::policy
    E3["Path throttled"]:::event
    E4["Labor reassignment flagged"]:::event

    C3["Configure pool: mode and WIP limit (ADR-0034)"]:::command

    ES["Stock reserved or reservation revoked"]:::event
    R3["UsableInventoryObserved"]:::readmodel

    E5["Rate deviation detected"]:::event
    H1["Reserved, not emitted (decided 2026-10-06)"]:::decided
    H3["Read-only context by design (decided 2026-10-06, ADR-0006)"]:::decided

    Sup --> C1 --> A1 --> R1
    R1 --> E1
    R1 --> E2
    E2 --> OM
    E2 --> NF
    Sup --> C2 --> A1
    Sup --> C3 --> A1
    A1 --> R2
    R2 --> P1 --> E3
    R2 --> P2 --> E4
    INV --> ES --> R3
    E5 -.- H1
    R3 -.- H3

    classDef actor fill:#fff59d,stroke:#c9b800,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#b59a00,color:#000
    classDef event fill:#f6a04d,stroke:#b8651b,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#a93226,color:#000
    classDef decided fill:#d5dbdb,stroke:#7b7d7d,color:#000
```

Source: `internal/application/usecases/sample_backlog.go`,
`rebalance_decision.go`, `configure_pool.go`, `observe_inventory_change.go`,
`internal/domain/shared/events.go`. Omits: `PathCapacityChanged` is raised only
when `cutoffAt` is supplied, and `BacklogThresholdBreached` only over the
alarm threshold. `Configure pool` raises no event
([ADR-0034](../adr/0034-configure-pool-command.md)).

## Stickies and their evidence

| Sticky | Kind | Evidence |
|---|---|---|
| Planner, Supervisor or ops agent, Upstream caller | Actor | REST callers of `internal/adapters/inbound/http/router.go`; MCP host of `internal/adapters/inbound/mcp/tools.go` |
| Receive charge forecast | Command | `usecases.ReceiveChargeForecast`, `POST /paths/{pathId}/charge` |
| Commit shift plan | Command | `usecases.CommitShiftPlan`, `POST /paths/{pathId}/plan` |
| Enqueue work unit | Command | `usecases.EnqueueWorkUnit`, `POST /paths/{pathId}/work-units` |
| Release next work | Command | `usecases.ReleaseNextWork`, `POST /paths/{pathId}/release`, MCP `release_next_work` |
| Record completion | Command | `usecases.RecordCompletion`, `POST /work-units/{id}/complete` |
| Sample backlog | Command (query that can emit events) | `usecases.SampleBacklog`, `GET /paths/{pathId}/telemetry` |
| Decide rebalance | Command (query that can emit events) | `usecases.RebalanceDecision`, `GET /paths/{pathId}/rebalance` |
| Configure pool | Command | `usecases.ConfigurePool`, `PUT /paths/{pathId}/pool` ([ADR-0034](../adr/0034-configure-pool-command.md)); raises no event |
| ChargeForecast, ShiftPlan, WorkPool, WorkUnit | Aggregate | `internal/domain/{charge,plan,release,workunit}` |
| Charge forecast received … Path plan drift detected (11) | Domain event | `internal/domain/shared/events.go` |
| Workforce shift plan committed, Order allocated, Task completed, Stock reserved | Domain event (external) | type constants in `internal/adapters/kafka/cloudevents/cloudevents.go` |
| Project labor plan and compare heads | Policy | `usecases.ObserveLaborPlan`, `reconcileHeads` |
| Compare heads on commit | Policy | `CommitShiftPlan.reconcileDrift` |
| Enqueue one work unit per line | Policy | `usecases.ApplyOrderAllocated` |
| Earliest CPT first, WIP limit | Policy | `release.ReleasePolicy`, `WorkPool.nextPendingIndex`, `ErrWIPLimitReached` |
| Record completion of its work unit | Policy | `usecases.ApplyTaskCompleted` |
| Throttle upstream, reassign labor | Policy | `usecases.RebalanceDecision` switch on `pool.Mode()` |
| LaborPlanObserved, UsableInventoryObserved | Read model | `internal/domain/laborview`, `internal/domain/inventoryview` |
| Backlog telemetry, Rebalance recommendation | Read model | `usecases.BacklogSnapshot`, `usecases.RebalanceRecommendation` |
| Product classification view, Travel distance view | Read model | `internal/domain/productclassificationview`, `internal/domain/traveldistanceview` |
| workforce-management, order-management, inventory-storage, product-master, fulfillment-execution, facility-layout, network-fulfillment | External system | topics and REST clients listed on the [Bounded context canvas](./bounded-context-canvas.md) |

## Hotspots

Each hotspot is a known gap visible in code or ADRs, not a guess. Rows marked
**Decided** or **Resolved** record the 2026-10-06 decision so the question does
not resurface.

| Hotspot | Evidence |
|---|---|
| `PathPlanDriftDetected` has no consumer | No sibling references the type; [ADR-0019](../adr/0019-labor-plan-committed-shift-plan-reconciliation.md) reports drift, it does not correct it |
| `WorkPool` entries are never pruned and every save rewrites them | `WorkPoolRepo.Save` deletes and re-inserts all `work_pool_entries` for the path |
| Hot `WorkPool` row under concurrent writes | [ADR-0029](../adr/0029-work-pool-optimistic-concurrency.md); `maxPoolSaveAttempts = 12`, then 409 `concurrent-modification` |
| `RateDeviationDetected` is declared but never raised | **Decided 2026-10-06: kept reserved.** Declared for a future detection rule ([ADR-0020](../adr/0020-flowfed-path-observed-throughput-signal.md) defers it); not emitted today. Detection needs a business rule nobody has specified, and removing the declaration would be a contract removal |
| ~~No code path creates a flow-fed pool or changes WIP limits~~ | **Resolved 2026-10-06 (ADR-0034):** `PUT /paths/{pathId}/pool` (`usecases.ConfigurePool`) sets a path's mode and WIP limit, creating the pool if absent; lowering below the current WIP never evicts, releases pause until WIP < limit. An unconfigured path keeps the `ReleaseFed` / 1000 fallback of `EnqueueWorkUnit` |
| `UsableInventoryObserved` feeds no decision | **Decided 2026-10-06: read-only context by design ([ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md)); gating release on it would be a new business rule.** Reservations in inventory-storage already guard availability. Only `ObserveInventoryChange` writes it and only `InventoryView` (`GET /inventory-view/{sku}`) reads it |
| `events` table is unused | **Decided 2026-10-06: legacy, unused; retained, additive migrations only.** Dropping it is destructive and needs explicit approval |
