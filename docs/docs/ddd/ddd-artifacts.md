---
id: ddd-artifacts
title: DDD artifacts (ddd-crew)
sidebar_label: Artifact pack overview
sidebar_position: 1
description: Index of the ddd-crew DDD artifact pack and UML diagrams for Work Planning & Release, with the tool each follows and its sources of truth.
---

# DDD artifacts (ddd-crew)

The full [ddd-crew](https://github.com/ddd-crew) artifact pack for this
bounded context, plus UML class, ER and sequence diagrams. Every diagram is
Mermaid, derived from the code on `develop`, and carries a **Source:** line
naming the files it came from and what it leaves out.

| Artifact | ddd-crew tool / notation | Page |
|---|---|---|
| Core domain chart | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) | [Core domain chart](./core-domain-chart.md) |
| Bounded context canvas | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) | [Bounded context canvas](./bounded-context-canvas.md) |
| Context map | [Context Mapping](https://github.com/ddd-crew/context-mapping) | [Context map](../ecosystem/context-map.md) (lives in Ecosystem) |
| Aggregate design canvas | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) | [Aggregate design canvas](./aggregate-design-canvas.md) |
| Domain message flow | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) | [Domain message flow](./domain-message-flow.md) |
| EventStorming | [EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) | [EventStorming](./eventstorming.md) |
| Ubiquitous language | glossary | [Ubiquitous language](../business-context/ubiquitous-language.md) (lives in Business Context) |
| UML class diagrams | UML | [Class diagrams](./class-diagram.md) |
| ER diagram | crow's foot ER | [ER diagram](./entity-relationship.md) |
| UML sequence diagrams | UML | [Sequence diagrams](./sequence-diagrams.md) |
| Domain events | event catalogue | [Domain events](./domain-events.md) |

Supporting pages in the same section: [Subdomain classification](./subdomain-classification.md),
[Read models](./read-models.md), [Context relationships](./context-relationships.md).

## Sources of truth

When a page and the code disagree, the code wins. The pages were written from:

- **Domain**: `internal/domain/**` — aggregates, value objects, events, views.
- **Application**: `internal/application/ports/*.go` and
  `internal/application/usecases/*.go` — commands, transactions, retries,
  idempotency.
- **Adapters and wiring**: `internal/adapters/**`, `cmd/wes`, `cmd/mcp`,
  `cmd/wes-projector`, `cmd/wes-reports`.
- **Contracts**: `apis/openapi.yaml` (REST) and `apis/asyncapi.yaml` (events);
  CloudEvents type constants in `internal/adapters/kafka/cloudevents/cloudevents.go`.
- **Schema**: `migrations/*.up.sql` and `migrations/analytics/*.up.sql`.
- **Decisions**: the [ADRs](../adr/index.md).
- **Siblings**: each sibling repository's `origin/develop`, for who really
  consumes what.

Estimates (throughput, aggregate size, chart positions) are labelled as
estimates.
