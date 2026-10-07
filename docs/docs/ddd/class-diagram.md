---
id: class-diagram
title: UML class diagrams
sidebar_label: Class diagrams
sidebar_position: 7
description: UML class diagrams of internal/domain (aggregates, value objects, domain events, read models) and of the hexagonal ports and adapters.
---

# UML class diagrams

Four diagrams, split so none exceeds about twenty classes: the aggregates and
value objects, the domain events, the read models, and the hexagon of ports
and adapters. Type and member names are the real Go identifiers; Go has no
visibility keyword for fields, so unexported fields are shown with `-` and
exported methods with `+`.

Stereotypes: `<<AggregateRoot>>`, `<<Entity>>`, `<<ValueObject>>`,
`<<Enumeration>>`, `<<DomainService>>`, `<<DomainEvent>>`, `<<ReadModel>>`,
`<<Repository>>` (an outbound port).

## 1. Aggregates and value objects

```mermaid
classDiagram
    direction LR

    class ChargeForecast {
        <<AggregateRoot>>
        -PathId pathId
        -CPTBucket[] buckets
        -Time receivedAt
        +NewChargeForecast(pathId, buckets, receivedAt) ChargeForecast
        +TotalQuantity() Quantity
        +QuantityForCPT(cpt) Quantity
    }
    class CPTBucket {
        <<ValueObject>>
        +CPT CPT
        +Quantity Quantity
    }

    class ShiftPlan {
        <<AggregateRoot>>
        -PathPlan[] pathPlans
        +NewShiftPlan(pathPlans) ShiftPlan
        +PathPlan(pathId) PathPlan
        +TotalHours() float64
    }
    class PathPlan {
        <<Entity>>
        -PathId pathId
        -StationCount plannedHeads
        -StationCount installedStations
        -Rate rate
        -float64 hours
        -float64 travelDistanceM
        -bool travelDistanceEstimated
        +NewPathPlan(pathId, heads, stations, rate, hours) PathPlan
        +PlannedThroughput() float64
        +SetTravelDistance(metresM, estimated)
    }

    class WorkPool {
        <<AggregateRoot>>
        -PathId pathId
        -FeedMode mode
        -int wipLimit
        -int alarmThreshold
        -poolEntry[] entries
        -int64 version
        +Enqueue(workUnitId, cpt) error
        +Configure(mode, wipLimit) bool, error
        +ReleaseNext() string
        +Release(workUnitId) error
        +Complete(workUnitId) error
        +Reconcile(workUnitId, released, completed) error
        +BacklogDepth() int
        +WIP() int
        +RemainingCapacity() int, bool
        +IsOverAlarmThreshold() bool
    }
    class poolEntry {
        <<Entity>>
        -string workUnitId
        -CPT cpt
        -entryState state
    }
    class FeedMode {
        <<Enumeration>>
        ReleaseFed
        FlowFed
    }
    class entryState {
        <<Enumeration>>
        pending
        released
        completed
    }
    class ReleasePolicy {
        <<DomainService>>
        +Apply(pool) string
    }

    class WorkUnit {
        <<AggregateRoot>>
        -string id
        -PathId pathId
        -CPT cpt
        -string reference
        -string sku
        -bool giftWrap
        -State state
        -Time releasedAt
        -Time completedAt
        +NewWorkUnit(id, pathId, cpt, reference) WorkUnit
        +Release(at) error
        +Complete(at) error
        +SetSKU(sku)
        +SetGiftWrap(giftWrap)
    }
    class State {
        <<Enumeration>>
        Pending
        Released
        Completed
    }

    class PathId {
        <<ValueObject>>
        -string value
    }
    class CPT {
        <<ValueObject>>
        -Time at
        +Before(other) bool
    }
    class Rate {
        <<ValueObject>>
        -float64 unitsPerHour
    }
    class StationCount {
        <<ValueObject>>
        -int value
        +GreaterThan(other) bool
    }
    class Quantity {
        <<ValueObject>>
        -int value
        +Add(other) Quantity
    }

    ChargeForecast "1" *-- "1..*" CPTBucket
    ShiftPlan "1" *-- "1..*" PathPlan
    WorkPool "1" *-- "0..*" poolEntry
    WorkPool --> FeedMode
    poolEntry --> entryState
    WorkUnit --> State
    ReleasePolicy ..> WorkPool : applies to
    poolEntry ..> WorkUnit : refers by workUnitId
    PathPlan --> StationCount
    PathPlan --> Rate
    CPTBucket --> Quantity
    CPTBucket --> CPT
    WorkUnit --> PathId
    WorkPool --> PathId
```

Source: `internal/domain/charge/forecast.go`, `internal/domain/plan/shift_plan.go`,
`internal/domain/plan/path_plan.go`, `internal/domain/release/work_pool.go`,
`internal/domain/release/release_policy.go`, `internal/domain/workunit/work_unit.go`,
`internal/domain/shared/*.go`. Omits: read-only accessors (`PathId()`,
`Mode()`, `Entries()`, ...), `RestoreEntry` / `SetVersion` (repository
rehydration only), `travelDistanceKnown`, and the `PathId` /
`CPT` associations of `ChargeForecast`, `PathPlan` and `poolEntry`.
`releasedAt` / `completedAt` are `*time.Time` (nil until the transition).

The association between `WorkPool` and `WorkUnit` is **by identity only**:
the pool's entries hold a work unit id, never a `WorkUnit`. They are two
aggregates with two consistency boundaries.

## 2. Domain events

```mermaid
classDiagram
    direction TB
    class DomainEvent {
        <<interface>>
        +EventName() string
        +OccurredAt() Time
    }
    class baseEvent {
        -string name
        -Time at
    }
    class ChargeForecastReceived {
        <<DomainEvent>>
        +PathId PathId
    }
    class ShiftPlanCommitted {
        <<DomainEvent>>
        +PathId PathId
    }
    class WorkUnitCreated {
        <<DomainEvent>>
        +WorkUnitId string
        +PathId PathId
    }
    class WorkReleased {
        <<DomainEvent>>
        +WorkUnitId string
        +PathId PathId
    }
    class WorkUnitCompleted {
        <<DomainEvent>>
        +WorkUnitId string
        +PathId PathId
    }
    class BacklogThresholdBreached {
        <<DomainEvent>>
        +PathId PathId
    }
    class RateDeviationDetected {
        <<DomainEvent>>
        +PathId PathId
    }
    note for RateDeviationDetected "Reserved, not emitted (decided 2026-10-06)"
    class PathThrottled {
        <<DomainEvent>>
        +PathId PathId
    }
    class LaborReassignmentFlagged {
        <<DomainEvent>>
        +PathId PathId
    }
    class PathCapacityChanged {
        <<DomainEvent>>
        +PathId PathId
        +Time CutoffAt
        +int RemainingUnits
        +bool Known
    }
    class PathPlanDriftDetected {
        <<DomainEvent>>
        +PathId PathId
        +int WesPlannedHeads
        +int ObservedPlannedHeads
        +int DriftHeads
        +Time ObservedAt
    }

    DomainEvent <|.. baseEvent
    baseEvent <|-- ChargeForecastReceived
    baseEvent <|-- ShiftPlanCommitted
    baseEvent <|-- WorkUnitCreated
    baseEvent <|-- WorkReleased
    baseEvent <|-- WorkUnitCompleted
    baseEvent <|-- BacklogThresholdBreached
    baseEvent <|-- RateDeviationDetected
    baseEvent <|-- PathThrottled
    baseEvent <|-- LaborReassignmentFlagged
    baseEvent <|-- PathCapacityChanged
    baseEvent <|-- PathPlanDriftDetected
```

Source: `internal/domain/shared/events.go`. Go has no inheritance: each event
**embeds** `baseEvent`, which is drawn as generalisation. Omits: the `New...`
constructors. `DriftHeads` is computed by the constructor as
`ObservedPlannedHeads - WesPlannedHeads`.

## 3. Read models and catalogue views

```mermaid
classDiagram
    direction LR
    class LaborPlanObserved {
        <<ReadModel>>
        +PathId PathId
        +int PlannedHeads
        +float64 PlannedRate
        +float64 PlannedHours
        +Time ObservedAt
        +int DriftHeads
        +Time DriftDetectedAt
    }
    class Drift {
        <<ValueObject>>
        +int WesPlannedHeads
        +int ObservedPlannedHeads
        +int Heads
        +Detected() bool
    }
    class UsableInventoryObserved {
        <<ReadModel>>
        +string SKU
        +int UsableQuantity
        +Time ObservedAt
    }
    class ProductClassificationView {
        <<ReadModel>>
        +string SKU
        +string[] HandlingTags
        +string TemperatureClass
        +bool Known
        +HasTag(tag) bool
    }
    class TravelDistanceView {
        <<ReadModel>>
        +string From
        +string To
        +float64 MetresM
        +bool Estimated
        +bool Known
    }
    class Catalogue {
        <<ReadModel>>
        -PathDefinition[] defs
        +Lookup(id) PathDefinition
        +Ids() string[]
    }
    class PathDefinition {
        <<ValueObject>>
        +string Id
        +string MatchPrefix
        +string[] RequiredCapabilities
        +string DestinationLocationRole
    }
    Catalogue "1" *-- "0..*" PathDefinition
    Drift ..> LaborPlanObserved : CompareHeads
```

Source: `internal/domain/laborview/*.go`, `internal/domain/inventoryview/usable_inventory_observed.go`,
`internal/domain/productclassificationview/product_classification_view.go`,
`internal/domain/traveldistanceview/travel_distance_view.go`,
`internal/domain/pathcatalog/path_definition.go`. `DriftHeads` and
`DriftDetectedAt` are pointers (nil until a comparison was made). Omits:
`pathcatalog.ErrUnknownPath`, returned by `Lookup` on no prefix match.

## 4. Ports and adapters

The application layer owns the ports (`internal/application/ports`); adapters
implement them and `cmd/wes/main.go` wires one adapter per port.

```mermaid
classDiagram
    direction LR
    class ChargeRepo {
        <<Repository>>
        +Save(forecast)
        +FindByPathId(pathId)
    }
    class PlanRepo {
        <<Repository>>
        +Save(pathId, shiftPlan)
        +FindByPathId(pathId)
    }
    class WorkPoolRepo {
        <<Repository>>
        +Save(pool)
        +FindByPathId(pathId)
    }
    class WorkUnitRepo {
        <<Repository>>
        +Save(unit)
        +FindById(id)
        +FindByPathId(pathId)
        +FindByReference(reference)
    }
    class LaborPlanViewRepo {
        <<Repository>>
        +Save(view)
        +FindByPathId(pathId)
        +SaveDrift(pathId, driftHeads, detectedAt)
    }
    class InventoryViewRepo {
        <<Repository>>
        +ApplyDelta(sku, delta, observedAt)
        +FindBySKU(sku)
    }
    class ProcessedEventRepo {
        <<Repository>>
        +TryMarkProcessed(eventId, processedAt) bool
    }
    class EventPublisher {
        <<interface>>
        +Publish(events)
    }
    class UnitOfWork {
        <<interface>>
        +Execute(fn)
    }
    class PathCatalogue {
        <<interface>>
        +Lookup(id)
    }
    class ProductClassificationLookup {
        <<interface>>
        +GetClassification(sku)
    }
    class TravelDistanceLookup {
        <<interface>>
        +GetDistance(from, to)
    }
    class ReleaseMetrics {
        <<interface>>
        +WorkUnitReleased(pathId)
    }

    class PostgresRepos["postgres.*Repo"]
    class MemoryRepos["memory.*Repo"]
    class OutboxPublisher["postgres.OutboxPublisher"]
    class KafkaPublisher["kafka.Publisher"]
    class LogPublisher["events.LogPublisher"]
    class PgUnitOfWork["postgres.UnitOfWork"]
    class FileCatalogue["filecatalog"]
    class KafkaCatalogue["kafkacatalog.Consumer"]
    class ClassificationCopy["productclassificationcopy.Store"]
    class TravelClient["traveldistance.BreakerClient"]
    class OtelReleaseMetrics["telemetry release metrics"]

    PostgresRepos ..|> WorkPoolRepo
    PostgresRepos ..|> WorkUnitRepo
    PostgresRepos ..|> ChargeRepo
    PostgresRepos ..|> PlanRepo
    PostgresRepos ..|> LaborPlanViewRepo
    PostgresRepos ..|> InventoryViewRepo
    PostgresRepos ..|> ProcessedEventRepo
    MemoryRepos ..|> WorkPoolRepo
    MemoryRepos ..|> ProcessedEventRepo
    OutboxPublisher ..|> EventPublisher
    KafkaPublisher ..|> EventPublisher
    LogPublisher ..|> EventPublisher
    PgUnitOfWork ..|> UnitOfWork
    FileCatalogue ..|> PathCatalogue
    KafkaCatalogue ..|> PathCatalogue
    ClassificationCopy ..|> ProductClassificationLookup
    TravelClient ..|> TravelDistanceLookup
    OtelReleaseMetrics ..|> ReleaseMetrics
```

```mermaid
flowchart LR
    subgraph inbound["Inbound adapters"]
        HTTP["HTTP chi router<br/>internal/adapters/inbound/http"]
        MCP["MCP server cmd/mcp<br/>internal/adapters/inbound/mcp"]
        KIN["Kafka consumer<br/>internal/adapters/inbound/kafka"]
        AKIN["Analytics consumer<br/>cmd/wes-projector"]
    end
    subgraph app["Application: internal/application/usecases"]
        UC["7 control-loop use cases<br/>4 queries<br/>4 inbound-event use cases"]
    end
    subgraph domain["Domain: internal/domain"]
        D["charge, plan, release, workunit<br/>shared, views, pathcatalog"]
    end
    subgraph outbound["Outbound adapters"]
        PG["postgres repos, UnitOfWork,<br/>OutboxPublisher, OutboxRelay, Housekeeper"]
        MEM["memory repos"]
        KOUT["kafka Publisher and AnalyticsPublisher"]
        CAT["filecatalog or kafkacatalog"]
        PC["productclassificationcopy<br/>(local copy of product-master)"]
        TD["traveldistance client"]
        AS["analyticsstore"]
    end
    HTTP --> UC
    MCP --> UC
    KIN --> UC
    AKIN --> AS
    UC --> D
    UC -->|ports| PG
    UC -->|ports| MEM
    UC -->|ports| KOUT
    UC -->|ports| CAT
    UC -->|ports| PC
    UC -->|ports| TD
```

Source: `internal/application/ports/*.go`, `internal/adapters/**`,
`cmd/wes/main.go`, `cmd/mcp/main.go`, `cmd/wes-projector/main.go`,
`cmd/wes-reports/main.go`. Omits: `Clock` (`memory.SystemClock`), the
`ProcessedEventReleaser` optional port, the permissive / no-op variants of the
two REST lookups, `events.MultiPublisher`, and `cmd/wes-reports` (serves
`GET /reports/throughput` from `analyticsstore.PostgresReport`). The MCP
server calls the use cases in-process through its own composition root,
not over REST.
