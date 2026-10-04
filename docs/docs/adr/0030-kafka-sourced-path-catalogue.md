---
id: 0030-kafka-sourced-path-catalogue
slug: /adr/0030-kafka-sourced-path-catalogue
title: 0030. Kafka-sourced process-path catalogue (PATH_CATALOGUE_SOURCE=kafka) and boot-time dial retry
sidebar_label: 0030. Kafka path catalogue
description: ADR 0030 — an alternative, per-process, replay-from-earliest source for the process-path catalogue behind ports.PathCatalogue, selected with PATH_CATALOGUE_SOURCE, plus the internal/bootretry helper that guards the first outbound dial.
---

# 0030. Kafka-sourced process-path catalogue and boot-time dial retry

## Status

Accepted. Records a decision that was implemented without an ADR (the
`kafkacatalog` adapter) and amends [ADR-0012](./0012-process-path-catalogue-validation.md),
which described only the file source.

## Context

[ADR-0012](./0012-process-path-catalogue-validation.md) validates every
`path_id` against the fleet's process-path catalogue, loaded once from
`warehouse-infra`'s YAML (`PATH_CATALOGUE_FILE`). `process-path-management` now
owns the catalogue and publishes `ProcessPathCreated`, `ProcessPathUpdated` and
`ProcessPathDeactivated` on `warehouse.process-path-management.events`.
Reading a copy of a YAML file means the service can disagree with the owner
until a redeploy.

## Decision

1. **The catalogue is a port, not a concrete type.** Callers depend on
   `ports.PathCatalogue` (`Lookup(id) (PathDefinition, error)`), which
   `*pathcatalog.Catalogue` already satisfies. `Handlers.Catalogue`, the Kafka
   `Consumer` and `ApplyOrderAllocated` all take the port. (ADR-0012's text that
   named the concrete `*pathcatalog.Catalogue` field is superseded by this.)
2. **Two sources, chosen by `PATH_CATALOGUE_SOURCE`** (read in
   `cmd/wes/main.go`'s `wireCatalogue`):
   - `file` (default) — unchanged ADR-0012 behaviour: `filecatalog.Load`
     reads `PATH_CATALOGUE_FILE` once; a missing/invalid file is fatal.
   - `kafka` — `kafkacatalog.Consumer` replays the topic from the earliest
     offset into an in-memory catalogue and then follows it live; it requires
     `KAFKA_BROKERS`.
3. **Per-process consumer group.** The replay consumer uses a *unique*,
   process-scoped group id (never a shared name), so a restart always replays
   the full history instead of resuming a previous process's offset with an
   empty cache; offsets are committed asynchronously only because they are never
   resumed (enforced by `TestReplayConsumersSetCommitInterval`).
4. **Readiness gate.** Startup blocks until the consumer has seen everything
   that existed in the topic when it started (`WaitReady`, bounded by
   `kafkacatalog.WaitReadyTimeout` = 60 s); exceeding it is fatal, mirroring the
   file loader's "never run against an incomplete catalogue" contract.
5. **Boot-time dial retry (`internal/bootretry`).** The first outbound dial
   (Postgres, and the catalogue topic's offset lookup) is wrapped in
   `bootretry.Retry` — 5 attempts, exponential backoff from 1 s (~31 s total).
   Cause: the fleet's Istio native sidecars reset a pod's first outbound TCP
   connection ~10 s after start; a single attempt turns that into
   CrashLoopBackOff. Retrying is not a weakening of fail-closed: when the budget
   is exhausted the process still refuses to boot and reports the real last
   error. Used by `cmd/wes`, `cmd/wes-projector`, `cmd/wes-reports` and
   `cmd/mcp`.

## Consequences

- The catalogue follows the owning service live; a deactivated path stops
  validating without a redeploy.
- Startup now depends on a replay of the topic (bounded by the 60 s gate) when
  `kafka` is selected. `file` keeps zero new boot dependencies.
- Each replica runs its own replay (safe by design — read-only, rebuilt on
  start; see [ADR-0025](./0025-horizontal-autoscaling-and-pgxpool-tuning.md)).
- Both sources must keep identical matching semantics: the Kafka consumer builds
  a real `pathcatalog.Catalogue` per read rather than reimplementing the match.
