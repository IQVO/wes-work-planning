---
id: hexagonal-layering
title: Hexagonal layering
sidebar_label: Hexagonal layering
sidebar_position: 2
description: How the ports-and-adapters layering is organised in this repository, and how the dependency rule is enforced.
---

# Hexagonal layering

The repository is organised strictly as **ports & adapters**. The dependency
rule is non-negotiable and *executable* — it is asserted by architecture
fitness tests in CI, not just documented ([ADR-0007](../adr/0007-arch-go-fitness-tests.md)).

> **domain** depends on nothing · **application** depends on domain ·
> **adapters** depend on application and domain · only `cmd/` wires everything

```mermaid
flowchart TB
    subgraph inbound["Inbound adapters (driving)"]
        HTTP["adapters/inbound/http<br/>chi router, DTOs, RFC 7807"]
        KIN["adapters/inbound/kafka<br/>integration-event consumer"]
        MCPIN["adapters/inbound/mcp<br/>MCP tools, resources, prompts"]
    end

    subgraph app["Application layer"]
        UC["application/usecases<br/>7 control-loop use cases,<br/>4 queries + 4 inbound-event use cases"]
        PORTS["application/ports<br/>driven-port interfaces"]
    end

    subgraph domain["Domain layer — pure Go, zero framework imports"]
        CH["charge<br/>ChargeForecast"]
        PL["plan<br/>ShiftPlan · PathPlan"]
        RE["release<br/>WorkPool · ReleasePolicy"]
        WU["workunit<br/>WorkUnit"]
        SH["shared<br/>CPT · Rate · PathId · Quantity · StationCount · events"]
        LV["laborview / inventoryview /<br/>productclassificationview / traveldistanceview<br/>read-model values"]
        PC["pathcatalog<br/>process-path catalogue"]
    end

    subgraph outbound["Outbound adapters (driven)"]
        PG["adapters/outbound/postgres<br/>pgx/v5 repositories"]
        MEM["adapters/outbound/memory<br/>in-memory repositories"]
        EV["adapters/outbound/events<br/>log publisher"]
        KOUT["adapters/outbound/kafka<br/>Kafka publishers"]
        CAT["adapters/outbound/filecatalog · kafkacatalog<br/>process-path catalogue"]
        LOOK["adapters/outbound/productclassification · traveldistance<br/>sibling REST lookups"]
    end

    HTTP --> UC
    KIN --> UC
    MCPIN --> UC
    UC --> PORTS
    UC --> domain
    PORTS -.implemented by.-> PG
    PORTS -.implemented by.-> MEM
    PORTS -.implemented by.-> EV
    PORTS -.implemented by.-> KOUT
    PORTS -.implemented by.-> CAT
    PORTS -.implemented by.-> LOOK
    PG --> domain
    MEM --> domain
```

## Package map

```
cmd/wes/                        OLTP composition root — the only place every layer meets
cmd/wes-projector/              analytics writer (ADR-0011)
cmd/wes-reports/                analytics read-only reports API (ADR-0011)
cmd/mcp/                        MCP server (ADR-0008)
internal/
  domain/                       pure Go: aggregates, value objects, events, errors
    charge/                     ChargeForecast aggregate
    plan/                       ShiftPlan + PathPlan aggregates
    release/                    WorkPool aggregate + ReleasePolicy domain service
    workunit/                   WorkUnit aggregate
    shared/                     CPT, Rate, PathId, Quantity, StationCount, DomainEvent
    laborview/                  LaborPlanObserved read-model value
    inventoryview/              UsableInventoryObserved read-model value
    pathcatalog/                process-path catalogue + prefix lookup (ADR-0012)
    productclassificationview/  SKU classification read-model value (ADR-0009)
    traveldistanceview/         travel-distance read-model value (ADR-0017)
  analytics/report/             analytics report model (ADR-0011)
  application/
    ports/                      driven-port interfaces (repos, publisher, clock)
    usecases/                   one struct per use case
  adapters/
    inbound/http/               chi router, DTOs, domain-error → HTTP mapping
    inbound/kafka/              integration-event consumer (4 topics) + analytics consumer
    inbound/mcp/                MCP tools, resources, prompts (ADR-0008)
    outbound/postgres/          pgxpool repositories, UnitOfWork, outbox + relay (ADR-0014)
    outbound/memory/            thread-safe in-memory repositories
    outbound/events/            log EventPublisher + MultiPublisher
    outbound/kafka/             integration + analytics publishers, outbox encoders, RelaySink
    outbound/filecatalog/       process-path catalogue from YAML (PATH_CATALOGUE_SOURCE=file)
    outbound/kafkacatalog/      process-path catalogue from Kafka (PATH_CATALOGUE_SOURCE=kafka)
    outbound/productclassification/  inventory-storage REST lookup
    outbound/traveldistance/    facility-layout REST lookup
    outbound/analyticsstore/    analytics Postgres store
    outbound/telemetry/         OpenTelemetry setup
    kafka/cloudevents/          CloudEvents 1.0 envelope helpers (New/Decode), topics, consumed types
  architecture/                 arch-go fitness tests
migrations/                     golang-migrate SQL files (analytics/ for the projector)
apis/                           openapi.yaml + asyncapi.yaml (the published contracts)
features/                       godog (Gherkin) acceptance specs
```

## The driven ports

Every outbound dependency is an interface owned by the **application** layer,
so the domain never learns that Postgres or Kafka exist:

| Port | Purpose |
|---|---|
| `ChargeRepo` | persist/retrieve `ChargeForecast`, one per path |
| `PlanRepo` | persist/retrieve `ShiftPlan`, one per path |
| `WorkPoolRepo` | persist/retrieve `WorkPool`, one per path |
| `WorkUnitRepo` | persist/retrieve `WorkUnit` by id and by path |
| `EventPublisher` | publish domain events (`log` or `kafka` implementation) |
| `Clock` | abstract "now" so use cases and tests are deterministic |
| `LaborPlanViewRepo` | persist the `LaborPlanObserved` projection, one per path |
| `InventoryViewRepo` | atomically apply a delta to `UsableInventoryObserved`, keyed by SKU |
| `ProcessedEventRepo` | record consumed CloudEvents `id`s so redelivery is a no-op |
| `ProcessedEventReleaser` | optional: undo a non-transactional (in-memory) mark when the effect fails (ADR-0028) |
| `UnitOfWork` | run a use case's save + publish in one Postgres transaction (ADR-0014) |
| `ReleaseMetrics` | record the `wes.work_units.released` business counter (ADR-0013) |
| `PathCatalogue` | look up a `pathId` in the declared process-path catalogue (ADR-0012) |
| `ProductClassificationLookup` | read a SKU's classification from inventory-storage (ADR-0009) |
| `TravelDistanceLookup` | read a travel distance from facility-layout (ADR-0017) |

The composition root picks each implementation from environment variables:
in-memory vs Postgres (`DATABASE_URL`), log vs Kafka (`EVENT_PUBLISHER`),
file vs Kafka catalogue (`PATH_CATALOGUE_SOURCE`), permissive vs HTTP lookups
(`*_MODE`).

## Why this shape

The reason is not tidiness — it is that the interesting rules of this domain
(priority by CPT, at-most-once handout, WIP limits, `plannedHeads ≤
installedStations`) are *decisions*, and decisions belong somewhere with no
infrastructure in it, so they can be exercised exhaustively and cheaply. The
domain and application packages carry the bulk of the test suite precisely
because they have no I/O to mock. See
[ADR-0001](../adr/0001-hexagonal-ports-and-adapters.md).
