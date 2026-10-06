---
id: sequence-diagrams
title: Sequence diagrams
sidebar_label: Sequence diagrams
sidebar_position: 9
description: UML sequence diagrams for every command use case exposed over REST, MCP and the Kafka consumers, derived from the use-case function bodies.
---

# Sequence diagrams

UML sequence diagrams for every command use case this context exposes, traced
from the function bodies in `internal/application/usecases/` and the inbound
adapters. Participants follow the hexagon: client → inbound adapter →
application service (use case) → aggregate → repository / outbox → broker.

Conventions used throughout:

- **Postgres mode** is drawn (`DATABASE_URL` set). In in-memory mode
  `UnitOfWork` is nil, `atomically` just calls the function, and there is no
  outbox — the `EventPublisher` is the log or Kafka publisher directly.
- `atomically(...)` is one Postgres transaction: aggregate rows **and** the
  outbox rows commit or roll back together
  ([ADR-0014](../adr/0014-transactional-outbox.md)). Each domain event becomes
  two outbox rows: `warehouse.work-planning.events` and
  `warehouse.wes.analytics`.
- `retryOnPoolConflict` re-runs the whole load-mutate-save attempt when
  `WorkPoolRepo.Save` reports `ports.ErrConcurrentModification` — up to 12
  attempts with jittered backoff (2 ms doubling, capped at 100 ms)
  ([ADR-0029](../adr/0029-work-pool-optimistic-concurrency.md)).

## 1. Enqueue a work unit over REST (with `Idempotency-Key`)

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant HTTP as HTTP adapter
    participant IDEM as RequireIdempotencyKey
    participant UC as EnqueueWorkUnit
    participant PoolRepo as WorkPoolRepo
    participant Pool as WorkPool
    participant UnitRepo as WorkUnitRepo
    participant Outbox as outbox_events

    Client->>HTTP: POST /paths/{pathId}/work-units with Idempotency-Key
    HTTP->>HTTP: validatePathId against process-path catalogue
    HTTP->>IDEM: route-scoped middleware
    alt header missing
        IDEM-->>Client: 400 idempotency-key-required
    end
    IDEM->>IDEM: BEGIN, INSERT idempotency_keys ON CONFLICT DO NOTHING
    alt key already stored
        IDEM->>IDEM: ROLLBACK and read stored row
        alt same request hash
            IDEM-->>Client: replay stored status, headers and body
        else different request hash
            IDEM-->>Client: 422 idempotency-key-reused
        end
    else new key
        IDEM->>UC: Execute inside the middleware transaction
        loop retryOnPoolConflict, at most 12 attempts
            UC->>UC: workunit.NewWorkUnit, SetSKU, SetGiftWrap
            UC->>PoolRepo: FindByPathId
            alt no pool yet
                UC->>Pool: NewWorkPool ReleaseFed, WIP 1000, alarm 1000
            end
            UC->>Pool: Enqueue id and cpt
            alt id already in pool
                Pool-->>UC: ErrDuplicateEntry
            end
            UC->>PoolRepo: Save with version check
            UC->>UnitRepo: Save
            UC->>Outbox: insert WorkUnitCreated rows
        end
        UC-->>IDEM: WorkUnit
        IDEM->>IDEM: UPDATE idempotency_keys with outcome, COMMIT
        IDEM-->>Client: 201 Created with Location /work-units/{id}
    end
```

Source: `internal/adapters/inbound/http/router.go`,
`internal/adapters/inbound/http/idempotency.go`,
`internal/adapters/inbound/http/handlers.go` (`postWorkUnit`),
`internal/application/usecases/enqueue_work_unit.go`,
`internal/application/usecases/pool_retry.go`,
`internal/adapters/outbound/postgres/work_pool_repo.go`. Omits: the
in-memory route (no middleware), the 400 `malformed-request-body` and
`unknown-path-id` branches before the middleware, and a handler panic (rolls
back, never cached).

## 2. Release the next work unit (REST or MCP)

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Client or MCP host
    participant IN as HTTP adapter or MCP tool release_next_work
    participant UC as ReleaseNextWork
    participant PoolRepo as WorkPoolRepo
    participant Policy as ReleasePolicy
    participant Pool as WorkPool
    participant UnitRepo as WorkUnitRepo
    participant Unit as WorkUnit
    participant Outbox as outbox_events
    participant Metrics as ReleaseMetrics

    Caller->>IN: POST /paths/{pathId}/release
    IN->>UC: Execute
    loop retryOnPoolConflict, at most 12 attempts
        UC->>PoolRepo: FindByPathId
        alt no pool
            PoolRepo-->>UC: ErrNotFound
        end
        loop until a Pending unit is found
            UC->>Policy: Apply pool
            Policy->>Pool: ReleaseNext earliest CPT
            alt no pending entry
                Pool-->>UC: ErrEmptyPool
            else ReleaseFed and WIP at limit
                Pool-->>UC: ErrWIPLimitReached
            end
            UC->>UnitRepo: FindById
            alt unit already Released or Completed
                UC->>Pool: Reconcile stale entry, then pick again
            end
        end
        UC->>Unit: Release now
        UC->>PoolRepo: Save with version check
        UC->>UnitRepo: Save
        UC->>Outbox: insert WorkReleased rows
    end
    UC->>Metrics: WorkUnitReleased path.id
    UC-->>IN: WorkUnit
    IN-->>Caller: 200 with the released unit
```

Source: `internal/application/usecases/release_next_work.go`,
`internal/domain/release/release_policy.go`,
`internal/domain/release/work_pool.go`,
`internal/adapters/inbound/mcp/tools.go`. Omits: the error-to-problem
mapping (`errors.go`) and the `WorkReleased` payload enrichment, which
happens when the outbox row is encoded (diagram 8).

## 3. Record a completion (REST or `TaskCompleted`)

```mermaid
sequenceDiagram
    autonumber
    participant FE as fulfillment-execution
    participant KIN as Kafka consumer
    participant ATC as ApplyTaskCompleted
    participant PE as ProcessedEventRepo
    participant UC as RecordCompletion
    participant UnitRepo as WorkUnitRepo
    participant Unit as WorkUnit
    participant PoolRepo as WorkPoolRepo
    participant Pool as WorkPool
    participant Outbox as outbox_events

    FE->>KIN: evt com.warehouse.wes.fulfillment-execution.task.TaskCompleted
    KIN->>KIN: cloudevents.Decode, dispatch on full type
    KIN->>ATC: Execute with CloudEvents id and work_unit_id
    ATC->>PE: TryMarkProcessed id, inside one transaction
    alt already processed
        PE-->>ATC: seen
        ATC-->>KIN: TaskCompletedAlreadyProcessed, ack
    else first delivery
        ATC->>UC: Execute WorkUnitId
        loop retryOnPoolConflict
            UC->>UnitRepo: FindById
            alt unknown work unit
                UnitRepo-->>UC: ErrNotFound
                UC-->>ATC: TaskCompletedUnknownWorkUnit, processed skip
            end
            UC->>Unit: Complete now
            alt already completed or not released
                Unit-->>UC: ErrAlreadyCompleted or ErrNotReleased
            end
            UC->>PoolRepo: FindByPathId
            UC->>Pool: Reconcile entry to completed
            UC->>PoolRepo: Save with version check
            UC->>UnitRepo: Save
            UC->>Outbox: insert WorkUnitCompleted rows
        end
        ATC-->>KIN: applied, commit offset
    end
    Note over KIN: a failing handler is tried 3 times in total, then sent to warehouse.fulfillment.events.dlq
```

`POST /work-units/{id}/complete` calls the same `RecordCompletion.Execute`
directly, without the processed-event mark.

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/application/usecases/apply_task_completed.go`,
`internal/application/usecases/apply_integration_event.go`,
`internal/application/usecases/record_completion.go`. Omits: the in-memory
`ReleaseProcessed` path that undoes the mark on failure when there is no
transaction.

## 4. Commit a shift plan (travel hint and drift reconciliation)

```mermaid
sequenceDiagram
    autonumber
    actor Planner
    participant HTTP as HTTP adapter
    participant UC as CommitShiftPlan
    participant Plan as PathPlan and ShiftPlan
    participant FL as facility-layout
    participant PlanRepo as PlanRepo
    participant LV as LaborPlanViewRepo
    participant Outbox as outbox_events

    Planner->>HTTP: POST /paths/{pathId}/plan
    HTTP->>UC: Execute heads, stations, rate, hours, from, to
    UC->>Plan: NewPathPlan
    alt heads above installed stations
        Plan-->>HTTP: ErrHeadsExceedStations, 422
    end
    opt both location codes supplied
        UC->>FL: GET /distance?from=&to=
        FL-->>UC: TravelDistanceView, failure means no hint
    end
    UC->>Plan: NewShiftPlan with one PathPlan
    UC->>PlanRepo: Save, upsert on path_id
    UC->>Outbox: insert ShiftPlanCommitted rows
    UC->>LV: FindByPathId
    alt labor plan already observed
        UC->>LV: SaveDrift signed heads difference
        opt heads differ
            UC->>Outbox: insert PathPlanDriftDetected rows
        end
    end
    UC-->>HTTP: ShiftPlan
    HTTP-->>Planner: 201 with plannedThroughput and optional travel distance
```

Source: `internal/application/usecases/commit_shift_plan.go`,
`internal/application/usecases/drift_reconciliation.go`,
`internal/domain/plan/path_plan.go`,
`internal/adapters/outbound/traveldistance/`. Omits: the circuit breaker and
retry around the facility-layout call (ADR-0023) and
`TRAVEL_DISTANCE_MODE=permissive`, which never calls out.

## 5. Receive a charge forecast

```mermaid
sequenceDiagram
    autonumber
    actor Planner
    participant HTTP as HTTP adapter
    participant UC as ReceiveChargeForecast
    participant CF as ChargeForecast
    participant Repo as ChargeRepo
    participant Outbox as outbox_events

    Planner->>HTTP: POST /paths/{pathId}/charge with buckets
    HTTP->>HTTP: validate path and each bucket quantity
    HTTP->>UC: Execute
    UC->>CF: NewChargeForecast
    alt no buckets
        CF-->>HTTP: ErrNoBuckets, 422
    end
    UC->>Repo: Save, upsert on path_id
    UC->>Outbox: insert ChargeForecastReceived rows
    UC-->>HTTP: ChargeForecast
    HTTP-->>Planner: 201 with totalQuantity
```

Source: `internal/application/usecases/receive_charge_forecast.go`,
`internal/domain/charge/forecast.go`,
`internal/adapters/inbound/http/handlers.go` (`postChargeForecast`).

## 6. Sample backlog and decide a rebalance (REST or MCP)

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Supervisor, agent or order-management poller
    participant IN as HTTP adapter or MCP tool
    participant SB as SampleBacklog
    participant RD as RebalanceDecision
    participant PoolRepo as WorkPoolRepo
    participant Pool as WorkPool
    participant Outbox as outbox_events

    Caller->>IN: GET /paths/{pathId}/telemetry?cutoffAt=
    IN->>SB: Execute
    SB->>PoolRepo: FindByPathId
    SB->>Pool: BacklogDepth, WIP, IsOverAlarmThreshold
    opt over alarm threshold
        SB->>Outbox: insert BacklogThresholdBreached rows
    end
    opt cutoffAt supplied
        SB->>Pool: RemainingCapacity
        SB->>Outbox: insert PathCapacityChanged rows
    end
    SB-->>Caller: BacklogSnapshot

    Caller->>IN: GET /paths/{pathId}/rebalance
    IN->>RD: Execute
    RD->>PoolRepo: FindByPathId
    alt FlowFed and over alarm threshold
        RD->>Outbox: insert PathThrottled rows
    else ReleaseFed at WIP limit with backlog
        RD->>Outbox: insert LaborReassignmentFlagged rows
    end
    RD-->>Caller: RebalanceRecommendation
```

Source: `internal/application/usecases/sample_backlog.go`,
`internal/application/usecases/rebalance_decision.go`,
`internal/adapters/inbound/mcp/tools.go`. Omits: the MCP tools take no
`cutoffAt`, so `get_backlog_telemetry` never raises `PathCapacityChanged`.

## 7. Apply `OrderAllocated` from order-management

```mermaid
sequenceDiagram
    autonumber
    participant OM as order-management
    participant KIN as Kafka consumer
    participant AOA as ApplyOrderAllocated
    participant PE as ProcessedEventRepo
    participant Cat as PathCatalogue
    participant EWU as EnqueueWorkUnit

    OM->>KIN: evt com.warehouse.wes.order-management.order.OrderAllocated
    KIN->>AOA: Execute order_id, promise_date, lines
    AOA->>PE: TryMarkProcessed id, inside one transaction
    alt already processed
        AOA-->>KIN: skip and ack
    else first delivery
        loop every line
            AOA->>Cat: Lookup path_id
            alt unknown path
                Cat-->>AOA: ErrUnknownPath, whole event fails
            end
        end
        loop every line
            AOA->>EWU: Execute id order_id-line-line_no, CPT promise_date
            alt line already enqueued
                EWU-->>AOA: ErrDuplicateEntry, skip line
            end
        end
        AOA-->>KIN: applied, commit offset
    end
```

`OrderPartiallyAllocated` follows the same path. Source:
`internal/application/usecases/apply_order_allocated.go`,
`internal/adapters/inbound/kafka/consumer.go`. Omits: the inner
`EnqueueWorkUnit` steps, shown in diagram 1.

## 8. Project `ShiftPlanCommitted` and inventory changes

```mermaid
sequenceDiagram
    autonumber
    participant WM as workforce-management
    participant INV as inventory-storage
    participant KIN as Kafka consumer
    participant OLP as ObserveLaborPlan
    participant OIC as ObserveInventoryChange
    participant PE as ProcessedEventRepo
    participant PlanRepo as PlanRepo
    participant LV as LaborPlanViewRepo
    participant IV as InventoryViewRepo
    participant Outbox as outbox_events

    WM->>KIN: evt com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted
    KIN->>KIN: validate path_id against catalogue
    KIN->>OLP: Execute
    OLP->>PE: TryMarkProcessed id
    OLP->>PlanRepo: FindByPathId
    opt our PathPlan exists
        OLP->>OLP: reconcileHeads, drift is observed minus ours
    end
    OLP->>LV: Save LaborPlanObserved with drift
    opt drift not zero
        OLP->>Outbox: insert PathPlanDriftDetected rows
    end

    INV->>KIN: evt com.warehouse.wms.inventory-storage.reservation.StockReserved
    KIN->>OIC: Execute delta minus quantity
    OIC->>PE: TryMarkProcessed id
    OIC->>IV: ApplyDelta sku
    INV->>KIN: evt com.warehouse.wms.inventory-storage.reservation.ReservationRevoked
    KIN->>OIC: Execute delta plus quantity
```

Source: `internal/application/usecases/observe_labor_plan.go`,
`internal/application/usecases/observe_inventory_change.go`,
`internal/application/usecases/drift_reconciliation.go`. Omits: the
retry-then-DLQ handling common to every consumed event (diagram 3).

## 9. Outbox relay to Kafka

```mermaid
sequenceDiagram
    autonumber
    participant UC as Any use case
    participant OP as OutboxPublisher
    participant ENC as kafka Publisher Encode
    participant IS as inventory-storage
    participant Outbox as outbox_events
    participant Relay as OutboxRelay
    participant K as Kafka

    UC->>OP: Publish domain events, inside the transaction
    OP->>ENC: Encode CloudEvents, key is subject
    opt WorkReleased
        ENC->>ENC: read cpt, ref, sku, gift_wrap from WorkUnitRepo
        ENC->>IS: GET /products/{sku}/classification
        IS-->>ENC: Hazmat or Fragile hints, failure omits them
    end
    OP->>Outbox: INSERT one row per topic
    Note over Outbox: commits with the aggregate change
    loop every OUTBOX_RELAY_INTERVAL, default 1s
        Relay->>Outbox: SELECT unpublished rows in id order
        Relay->>K: produce to warehouse.work-planning.events and warehouse.wes.analytics
        Relay->>Outbox: set published_at, or attempts and last_error
    end
```

Source: `internal/adapters/outbound/postgres/outbox_publisher.go`,
`internal/adapters/outbound/kafka/publisher.go`,
`internal/adapters/outbound/kafka/encoded.go`,
`internal/adapters/outbound/kafka/analytics_publisher.go`,
`cmd/wes/main.go`. Omits: the housekeeping sweeper that later deletes
published rows older than `OUTBOX_RETENTION`
([ADR-0032](../adr/0032-housekeeping-retention-sweeper.md)).
