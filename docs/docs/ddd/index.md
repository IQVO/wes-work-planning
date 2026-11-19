---
id: index
title: Domain-Driven Design
sidebar_label: Introduction
sidebar_position: 0
slug: /ddd/
description: Subdomain classification, the ddd-crew artifact pack, domain events, read models, and context relationships.
---

# Domain-Driven Design

This section is the tactical and strategic DDD record for the **Work Planning &
Release** bounded context. Everything here is derived from the platform's
strategic reference documents and from this repository's own code — no
aspirational modelling.

| Page | Contents |
|---|---|
| [Subdomain classification](/docs/ddd/subdomain-classification) | Core / Supporting / Generic, with the justification |
| [DDD artifacts (ddd-crew)](/docs/ddd/ddd-artifacts) | The artifact pack: core domain chart, bounded context canvas, context map, aggregate design canvas, domain message flow, EventStorming, ubiquitous language, UML class, ER and sequence diagrams |
| [Aggregate design canvas](/docs/ddd/aggregate-design-canvas) | Both aggregates, every invariant, every failing path |
| [Domain events](/docs/ddd/domain-events) | The eight events published, and every event consumed |
| [Read models](/docs/ddd/read-models) | Projections — and why they are never aggregate state |
| [Context relationships](/docs/ddd/context-relationships) | Customer/Supplier, ACL, OHS, Conformist — the strategic patterns |

## One-page summary

```mermaid
classDiagram
    class WorkPool {
        <<AggregateRoot>>
        PathId pathId
        FeedMode mode
        int wipLimit
        int alarmThreshold
        int64 version
        +ReleaseNext() string
        +BacklogDepth() int
        +WIP() int
    }
    class WorkUnit {
        <<AggregateRoot>>
        string id
        PathId pathId
        CPT cpt
        string reference
        State state
        +Release(at)
        +Complete(at)
    }
    class ReleasePolicy {
        <<DomainService>>
        +Apply(pool) string
    }

    ReleasePolicy ..> WorkPool : applies to
    WorkPool ..> WorkUnit : references by id
```

Source: `internal/domain/{release,workunit}`. Omits: value
objects, events and read models — see [Class diagrams](/docs/ddd/class-diagram).
The `ChargeForecast` and `ShiftPlan`/`PathPlan` aggregates moved to
warehouse-planning
([ADR-0049](../adr/0049-charge-forecast-and-shift-plan-move-to-warehouse-planning.md)).

Note the association style between `WorkPool` and `WorkUnit`: the pool holds
**entry records keyed by work unit id**, not `WorkUnit` objects. They are two
separate aggregates with two separate consistency boundaries, referenced by
identity — updated in the same use case and the same transaction, but each
guarding its own invariants.
