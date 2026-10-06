---
id: index
title: WES — Work Planning & Release
sidebar_label: Introduction
sidebar_position: 0
slug: /overview/
description: The core bounded context of the Warehouse Execution System — the conductor that turns a shift's charge into a plan and releases work continuously.
---

# WES — Work Planning & Release

:::warning[Study project]
This documentation site is an educational Domain-Driven Design exercise. It
follows real industry-standard patterns and terminology, but it is **not a
production system** and is **not affiliated with, endorsed by, or
representative of any real-world company**.
:::

This is the **core bounded context of the WES (Warehouse Execution System) tier**
of the `warehouse-systems` platform. Its job in one sentence:

> Turn a shift's **charge** (the volume due by each CPT) into a **plan**
> (rate × heads per process path), then **release work continuously** in
> priority order and **flow-balance** the floor from live buffer telemetry.

It is the **conductor**: downstream of WMS planning and inventory truth,
upstream of WCS equipment control and of task execution. It never owns
inventory truth, never schedules a shift's workforce, and never drives a PLC —
it decides *when* work enters the floor and *in what order*.

## The three-layer position

| Layer | Horizon | Answers | This service |
|---|---|---|---|
| **WMS** | minutes → days | *What needs to happen, and why* | upstream of us |
| **WES** | seconds → minutes | *Who does it, right now, in what order* | **this service** |
| **WCS** | milliseconds → seconds | *How the machine performs the next step* | downstream of us |

## What you'll find here

| Section | Contents |
|---|---|
| [Overview](./what-it-does.md) | What the service does, its hexagonal layering, how to run it |
| [Business Context](../business-context/index.md) | Domain vision, ubiquitous language, the reasoning behind waveless release and Drum-Buffer-Rope flow balancing |
| [Domain-Driven Design](../ddd/index.md) | Subdomain classification, the ddd-crew artifact pack (core domain chart, bounded context canvas, aggregate design canvas, message flow, EventStorming, UML/ER/sequence diagrams), every domain event, read models |
| [API Reference](../api/index.md) | The REST API generated from the real `apis/openapi.yaml`, the event contract from `apis/asyncapi.yaml`, and the RFC 7807 error model |
| [Ecosystem](../ecosystem/index.md) | The context map, Kafka topics and REST lookups actually wired today, and the sibling services |
| [AI Ecosystem (MCP)](../mcp/governance-charter.md) | The MCP governance charter this service's `cmd/mcp` server follows |
| [Analytics (Data Product)](../analytics/release-throughput-report.md) | The Release Throughput & Backlog Health report contract |
| [ADRs](../adr/index.md) | Thirty-two architecture decision records |

## At a glance

- **Language / runtime**: Go 1.26, module `github.com/claudioed/wes-work-planning`
- **Architecture**: hexagonal (ports & adapters), enforced by executable
  fitness tests ([ADR-0007](../adr/0007-arch-go-fitness-tests.md))
- **Binaries**: `cmd/wes` (OLTP REST + Kafka), `cmd/wes-projector` and
  `cmd/wes-reports` (analytics data product), `cmd/mcp` (MCP server)
- **Inbound adapters**: REST (chi), Kafka consumer, MCP (Streamable HTTP)
- **Outbound adapters**: Postgres (pgx/v5, transactional outbox), in-memory,
  log and Kafka event publishers, process-path catalogue (file or Kafka),
  inventory-storage classification lookup, facility-layout travel-distance
  lookup
- **Aggregates**: `ChargeForecast`, `ShiftPlan`/`PathPlan`, `WorkPool`, `WorkUnit`
- **Read models (projections, not aggregates)**: `LaborPlanObserved` (with
  the ADR-0019 drift outcome), `UsableInventoryObserved`, backlog telemetry,
  rebalance recommendation, and the analytics `throughput_rollup`
- **Integration**: publishes `WorkReleased` (consumed by
  fulfillment-execution) and `PathCapacityChanged` (consumed by
  order-management and network-fulfillment), plus nine further domain events
  (including `PathPlanDriftDetected`) with no consumer today; consumes
  `ShiftPlanCommitted`, `StockReserved`,
  `ReservationRevoked`, `TaskCompleted`, `OrderAllocated`,
  `OrderPartiallyAllocated`, and — with `PATH_CATALOGUE_SOURCE=kafka` —
  process-path-management's catalogue events
