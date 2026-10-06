---
id: domain-message-flow
title: Domain message flow
sidebar_label: Domain message flow
sidebar_position: 5
description: "ddd-crew Domain Message Flow Modelling: four business scenarios across bounded contexts, every message a real REST route, MCP tool or CloudEvents type."
---

# Domain message flow

[ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling)
for four business scenarios. Participants are people, bounded contexts and
the broker; every arrow is a real message, prefixed `cmd:` (command),
`evt:` (event) or `qry:` (query) and numbered.

Event types are written in full the first time and shortened to their last
segment afterwards inside the same diagram.

## Scenario 1 — An allocated order becomes released, picked and completed work

```mermaid
sequenceDiagram
    autonumber
    participant OM as order-management
    participant K as Kafka
    participant WP as wes-work-planning
    actor Sup as Supervisor or agent
    participant INV as inventory-storage
    participant FE as fulfillment-execution

    OM->>K: evt: com.warehouse.wes.order-management.order.OrderAllocated
    K->>WP: evt: OrderAllocated
    Note over WP: ApplyOrderAllocated enqueues one WorkUnit per line
    WP->>K: evt: com.warehouse.wes.work-planning.workunit.WorkUnitCreated
    Sup->>WP: cmd: POST /paths/{pathId}/release or MCP release_next_work
    WP->>INV: qry: GET /products/{sku}/classification
    INV-->>WP: Hazmat and Fragile hints
    WP->>K: evt: com.warehouse.wes.work-planning.workunit.WorkReleased
    K->>FE: evt: WorkReleased
    Note over FE: creates its own Task, claimNext, lease, pick
    FE->>K: evt: com.warehouse.wes.fulfillment-execution.task.TaskCompleted
    K->>WP: evt: TaskCompleted
    Note over WP: ApplyTaskCompleted frees the WIP slot
    WP->>K: evt: com.warehouse.wes.work-planning.workunit.WorkUnitCompleted
```

Source: `internal/application/usecases/apply_order_allocated.go`,
`release_next_work.go`, `apply_task_completed.go`,
`internal/adapters/outbound/kafka/publisher.go`. Omits: the analytics copy of
every event on `warehouse.wes.analytics`, and REST enqueue
(`POST /paths/{pathId}/work-units`), which is an alternative to step 1.

## Scenario 2 — Planning a shift and detecting plan-vs-labor drift

```mermaid
sequenceDiagram
    autonumber
    actor Planner
    participant WP as wes-work-planning
    participant FL as facility-layout
    participant WM as workforce-management
    participant K as Kafka

    Planner->>WP: cmd: POST /paths/{pathId}/charge
    WP->>K: evt: com.warehouse.wes.work-planning.charge.ChargeForecastReceived
    Planner->>WP: cmd: POST /paths/{pathId}/plan with location codes
    WP->>FL: qry: GET /distance?from=&to=
    FL-->>WP: travel distance hint
    WP->>K: evt: com.warehouse.wes.work-planning.plan.ShiftPlanCommitted
    WM->>K: evt: com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted
    K->>WP: evt: workforce ShiftPlanCommitted
    Note over WP: ObserveLaborPlan updates LaborPlanObserved and compares heads
    WP->>K: evt: com.warehouse.wes.work-planning.pathplan.PathPlanDriftDetected
    Planner->>WP: qry: GET /paths/{pathId}/labor-plan-view
```

Source: `internal/application/usecases/receive_charge_forecast.go`,
`commit_shift_plan.go`, `observe_labor_plan.go`,
`drift_reconciliation.go`. Omits: the opposite order (Workforce commits
first, then `CommitShiftPlan` raises the drift event) and the no-drift case,
which raises nothing. The two `ShiftPlanCommitted` types are different events
from different contexts ([ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md)).

## Scenario 3 — Publishing remaining path capacity for promising

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Supervisor or scheduler
    participant WP as wes-work-planning
    participant K as Kafka
    participant OM as order-management
    participant NF as network-fulfillment

    Caller->>WP: qry: GET /paths/{pathId}/telemetry?cutoffAt=
    Note over WP: SampleBacklog computes max of 0 and wipLimit minus WIP
    WP->>K: evt: com.warehouse.wes.work-planning.workpool.PathCapacityChanged
    WP->>K: evt: com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached
    K->>OM: evt: PathCapacityChanged
    Note over OM: kafkapathcapacity caches remaining units per path and cutoff
    K->>NF: evt: PathCapacityChanged
    Note over NF: pathcapacitycache feeds CapabilityOffer
```

Source: `internal/application/usecases/sample_backlog.go`;
order-management `internal/adapters/outbound/kafkapathcapacity/consumer.go`;
network-fulfillment `internal/adapters/outbound/pathcapacitycache/consumer.go`
(both on `origin/develop`). Omits: `BacklogThresholdBreached` is raised only
when the backlog exceeds the alarm threshold.

## Scenario 4 — Flow balancing driven by an agent

```mermaid
sequenceDiagram
    autonumber
    actor Agent as warehouse-ops-agent
    participant WP as wes-work-planning
    participant K as Kafka
    participant WM as workforce-management

    Agent->>WP: qry: MCP get_backlog_telemetry
    Agent->>WP: qry: MCP get_rebalance_recommendation
    alt flow-fed path over alarm threshold
        WP->>K: evt: com.warehouse.wes.work-planning.workpool.PathThrottled
    else release-fed path at WIP limit with backlog
        WP->>K: evt: com.warehouse.wes.work-planning.workpool.LaborReassignmentFlagged
    end
    Note over K,WM: no context consumes these today, a human acts on the recommendation
    Agent->>WP: cmd: MCP release_next_work
    WP->>K: evt: com.warehouse.wes.work-planning.workunit.WorkReleased
```

Source: `internal/adapters/inbound/mcp/tools.go`,
`internal/application/usecases/rebalance_decision.go`; warehouse-ops-agent
`internal/config/config.go` allow-lists the two read tools. Omits: the
`balance_flow` MCP prompt and the `telemetry://{pathId}/backlog` resource.
