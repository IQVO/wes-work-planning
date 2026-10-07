---
id: index
title: Ecosystem
sidebar_label: Introduction
sidebar_position: 0
slug: /ecosystem/
description: Where this service sits among the warehouse-systems bounded contexts.
---

# Ecosystem

`warehouse-systems` is a fleet of Go services, each a bounded context with its
own model, its own database and its own deployment lifecycle. This one is the
WES tier's core — the conductor. This section covers the seven contexts this
service integrates with directly; the others (labor-performance,
warehouse-planning) have no edge to it, and warehouse-ops-agent and the
warehouse-console shell only call its public REST/MCP API.

| Page | Contents |
|---|---|
| [Context map](./context-map.md) | The ddd-crew context map: every Kafka and REST edge wired today, with U/D roles, patterns and evidence |
| [Integration events](./integration-events.md) | Every topic published and consumed, with real payloads and idempotency behaviour |
| [Sibling services](./sibling-services.md) | What each directly integrated sibling owns |

## The directly integrated services

```mermaid
flowchart TB
    subgraph wms["WMS tier — what and where"]
        INV["inventory-storage<br/><i>Core</i>"]
        OM["order-management<br/><i>Generic/Supporting</i>"]
        PM["product-master<br/><i>Supporting</i>"]
    end
    subgraph wes["WES tier — when and in what order"]
        WP["<b>wes-work-planning</b><br/><i>Core — this service</i>"]
        FE["fulfillment-execution<br/><i>Core</i>"]
    end
    subgraph sup["Supporting and Generic"]
        WM["workforce-management<br/><i>Supporting</i>"]
        FL["facility-layout<br/><i>Generic</i>"]
        PPM["process-path-management<br/><i>Generic</i>"]
    end
    NF["network-fulfillment"]

    INV -->|"StockReserved<br/>ReservationRevoked"| WP
    WM -->|"ShiftPlanCommitted"| WP
    OM -->|"OrderAllocated<br/>OrderPartiallyAllocated"| WP
    PPM -->|"ProcessPath* catalogue<br/>(PATH_CATALOGUE_SOURCE=kafka)"| WP
    WP -->|"WorkReleased"| FE
    WP -->|"PathCapacityChanged"| OM
    WP -->|"PathCapacityChanged"| NF
    FE -->|"TaskCompleted"| WP
    PM -->|"ProductClassified<br/>(local copy, ADR-0035)"| WP
    WP -.->|"GET /distance"| FL

    style WP fill:#2e6da4,color:#ffffff,stroke:#1b4368,stroke-width:3px
```

Solid arrows are Kafka edges, dashed arrows are REST lookups this service
makes; every one of them touches this service. See the
[context map](./context-map.md) for the relationship patterns.
