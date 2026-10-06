---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
sidebar_position: 1
description: Why Work Planning & Release is classified as a Core subdomain, with the justification from the reference model.
---

# Subdomain classification

## Verdict: **Core subdomain**

Work Planning & Release is the **core domain** of the WES tier and one of the
core domains of the platform as a whole.

## The justification, from the reference model

The platform's strategic reference is explicit that the WES tier's
classification is *conditional*:

> **WES** — **Core Domain** *if* operational efficiency is your differentiator
> (you build/tune your own DC); **Supporting/Generic** if you consume a vendor
> WES and integrate at arm's length.

This platform builds its own. There is no vendor WES behind an anti-corruption
layer here — the release policy, the WIP-limit invariant, the Drum-Buffer-Rope
rebalance rule and the CPT priority function are all written, owned and
unit-tested in this repository. That places it squarely on the "Core" side of
the conditional.

The fulfilment reference model reaches the same conclusion from the capability
side, classifying **Fulfilment Orchestration & Optimisation** — "real-time
plan/replan of pick-pack-ship path" — as **Core**, on the grounds that
continuous re-planning to the fastest/cheapest path is precisely where the
differentiator lives.

That model's context map places this capability as the conductor:

> **Work Orchestration (WES core) is downstream** of both Planning and
> Inventory: it consumes the *plan* and the *stock reality* and turns them into
> real-time work.

Which is, to the letter, what this service does: it consumes the labour plan
(`ShiftPlanCommitted` from Workforce), consumes stock reality
(`StockReserved`/`ReservationRevoked` from Inventory), and turns them into
released work (`WorkReleased` to Execution).

## Where this sits among the directly integrated services

| Service | Tier | Subdomain | Why |
|---|---|---|---|
| `inventory-storage` | WMS | **Core** | Owns inventory truth: stock ledger, bin-accurate location, chaotic stow, revocable reservations. Random stow with bin-accurate tracking is a genuine operational differentiator. |
| **`wes-work-planning`** | **WES** | **Core** | **This service.** Real-time orchestration built in-house — release policy, WIP backpressure, flow balancing. |
| `workforce-management` | — | **Supporting** | Allocating workforce to workload is necessary and industry-common, not a differentiator. Deliberately stops at the path boundary. |
| `fulfillment-execution` | WES/WCS-adjacent | **Core** | Pick/Pack/SLAM task lifecycle with pull-based `claimNext` and lease semantics — directly drives throughput and accuracy at scale. |
| `facility-layout` | — | **Generic** | The physical warehouse map. Same bucket as Cartonization and WCS in the reference model: extract it once rather than duplicating it in every context. |
| `order-management` | WMS | **Generic/Supporting** | Order intake, per-line allocation, promise dates and choreographed release — needed, but not this platform's differentiator. |
| `process-path-management` | — | **Generic** | The declared process-path catalogue, extracted once for every context that validates a `pathId`. |
| `network-fulfillment` | — | **Supporting** | Anti-corruption layer to an external retail fulfillment network; consumes our `PathCapacityChanged` into its `CapabilityOffer`. |

The same classification is plotted on the
[Core domain chart](./core-domain-chart.md).

## What "Core" obliges

Classification is not a badge; it is a budget decision. Because this context is
Core, the following are justified investments and are actually present:

- **A hand-written domain model with no framework in it** — no ORM entities, no
  generated CRUD. ([ADR-0001](../adr/0001-hexagonal-ports-and-adapters.md))
- **Invariants enforced in the aggregate, with failing-path tests for each** —
  not validation annotations on a DTO.
  ([Aggregate design canvas](./aggregate-design-canvas.md))
- **A custom release policy as a first-class domain-service object**, so the
  priority function is a thing you can replace rather than a `SORT BY` clause.
  ([ADR-0002](../adr/0002-waveless-continuous-release.md))
- **Executable architecture fitness tests** that fail the build if the
  dependency rule is violated. ([ADR-0007](../adr/0007-arch-go-fitness-tests.md))
- **Mutation testing on the domain packages**, because a Core domain's tests
  should be shown to actually kill defects, not merely to execute lines.

And, symmetrically, what Core *does not* oblige: the read-model projections of
other contexts' data (`LaborPlanObserved`, `UsableInventoryObserved`) are
plain value types with no invariants and no behaviour, because they model
someone else's core, not ours.
