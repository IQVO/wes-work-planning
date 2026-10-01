---
id: 0011-analytical-data-product
title: 11. Per-service analytical data product (report) via a separate analytics topic
sidebar_label: 11. Analytical data product
sidebar_position: 11
description: An analytical read model (the "Release Throughput & Backlog Health" report) built from this service's own domain events on a dedicated warehouse.wes.analytics topic, projected into a separate analytical database and served by a read-only reports binary over REST and MCP — a lightweight data mesh with no central data platform.
---

# 11. Per-service analytical data product (the "report")

## Status

**Accepted.**

## Context

The warehouse-systems estate needs a per-service **report** that supports
analytics while each service stays the **OLTP** system of record for its own
bounded context. The requirement, stated deliberately simply: *follow data-mesh
principles, but without standing up a whole data platform.* No central
warehouse, no lake, no shared ETL team.

Work Planning & Release already has everything the analytical side needs as a
substrate:

- Past-tense **domain events** (`ChargeForecastReceived`, `ShiftPlanCommitted`,
  `WorkUnitCreated`, `WorkReleased`, `BacklogThresholdBreached`,
  `RateDeviationDetected`, `PathThrottled`, `LaborReassignmentFlagged`,
  `WorkUnitCompleted`) raised by the aggregates.
- A Kafka **integration** path (`warehouse.work-planning.events`) with the
  shared `Envelope` and OTel trace propagation established in
  [ADR-0004](./0004-kafka-integration-events.md).
- The `ProcessedEventRepo` port for idempotent at-least-once consumption.
- The dual inbound-adapter pattern (HTTP + MCP) from
  [ADR-0008](./0008-mcp-inbound-adapter.md).

So the event backbone exists; what is missing is the **analytical read side**.
The forces shaping the decision:

- **The integration contract must not become coupled to reporting.** The report
  needs more event types than the integration topic exposes, and they change on
  a different cadence. Widening `warehouse.work-planning.events` with
  analytics-only event types would risk surprising existing consumers and
  entangle two contracts that should evolve separately.
- **Analytics must never contend with OLTP.** A report query load, a long
  aggregation, or a projection rebuild must not touch the transactional
  database that serves `ReleaseNextWork` and `RecordCompletion`.
- **The service still owns its data as a product.** Data-mesh domain ownership
  means the read side lives in this repo, owned by the same team, with a
  contract, an owner, and a freshness SLA — not shipped off to a central team.
- **No new central platform.** Reuse what the estate already runs: Kafka,
  Postgres, chi, the MCP SDK, the Helm chart.

## Decision

**Work Planning & Release owns an analytical data product built solely from its
own domain events, delivered on a dedicated analytics topic, projected into a
separate analytical database, and served read-only over REST and MCP. Three
processes; one writer.**

### 1. Separate analytics topic

A new outbound adapter publishes the report-input event set to
**`warehouse.wes.analytics`**, using the shared **Envelope v1** wrapper
(`event_id`, `event_type`, `occurred_at`, `source`, `schema_version`, `data`;
**superseded by [ADR-0027](./0027-cloudevents-mandatory-event-envelope.md)** —
the analytics topic now carries CloudEvents 1.0 with `dataschema`
`urn:warehouse:wes-work-planning:analytics:<EventName>:v1` instead of
`schema_version`)
with a per-`event_type` snake_case `data` payload. The existing integration
publisher and `warehouse.work-planning.events` are **left untouched**, so no
existing consumer is affected. The composition root fans each domain event to
both publishers via an `events.MultiPublisher` when `EVENT_PUBLISHER=kafka`.
Analytics consumers switch on `event_type`, ignore unknown types, and dedupe on
`event_id`.

Unlike the fulfillment-execution pilot ([ADR-0012 there]), no repo-lookup
enrichment is needed: every event this report is built from already carries its
own `PathId` (the report's key dimension), so the analytics publisher stays
thin.

### 2. Separate analytical database

Projections land in a **separate analytical database** with its own credentials
(`ANALYTICS_DATABASE_URL`), its own migration set (`migrations/analytics/`), and
a **read-only role** for the reader. Baseline is a dedicated analytical database
in the existing Postgres release; the `ANALYTICS_DATABASE_URL` seam allows
promotion to a physically separate instance later without code changes. The OLTP
`DATABASE_URL` database is never opened by the analytical side.

### 3. Three processes, one writer

- **`cmd/wes`** — the OLTP binary. Unchanged, except its composition root
  additionally publishes domain events to the analytics topic via the
  fan-out publisher.
- **`cmd/wes-projector`** — the analytics **writer**. Consumes
  `warehouse.wes.analytics` (consumer group `wes-analytics`), applies
  idempotent projections (via its own `analytics_processed_events` claim and an
  `analytics_consumed_events` consumer gate), and is the **only** writer of the
  analytical database. Runs the analytical migrations on start.
- **`cmd/wes-reports`** — the **read-only reader**. Opens the analytical
  database with a read-only pool (`default_transaction_read_only=on`) and serves
  `GET /reports/throughput` and `GET /reports/throughput/freshness`. Never
  writes, never migrates.

### 4. Served over REST and MCP

The reports binary serves the REST report resource. A curated, read-only MCP
tool (`get_release_throughput_report`) — following the intent-level tool
discipline of [ADR-0008](./0008-mcp-inbound-adapter.md) — calls the reports REST
rather than opening the analytical database itself, so no process touches a
datastore it does not own.

### 5. The report

A **Release Throughput & Backlog Health** read model, keyed per **process path
× hour bucket**: work released, work units completed, backlog-threshold
breaches, path throttles, and rate-deviation detections per interval. It is a
**projection** from events (consistent with the existing "read models are
projections, not aggregate state" rule), eventually consistent to a freshness
SLA (p95 event-to-report lag < 30s), not real-time.

The analytical read model lives in a new `internal/analytics/report/` region
that depends on nothing; the consumer and store adapters depend on it. The OLTP
**domain and application layers are not modified**, and `arch-test` continues to
enforce that they do not import the analytics store.

## Consequences

### Easier

- **The integration contract is untouched**, so widening what analytics consumes
  never risks an integration consumer. Analytics retention is tuned
  independently of the integration topic.
- **Analytics cannot contend with OLTP** — separate database, separate
  connection, read-only reader role. A runaway report query cannot touch
  transactional throughput.
- **The report is rebuilt purely from events** — no dual-write from OLTP, so the
  transactional write path gains no new failure mode. The read model can be
  rebuilt from scratch by replaying the topic (the consumer starts a fresh group
  at the earliest offset).
- **No central platform.** Everything reuses the estate's existing Kafka,
  Postgres, chi, MCP SDK and Helm.
- **Least privilege by construction.** The read-only DB role and read-only pool
  make "a report can never corrupt the analytical store" a hard guarantee, not a
  convention.

### Harder

- **One more topic, two more binaries, and a second database** to operate.
  Mitigated by reusing the OLTP Postgres chart pattern and the existing
  consumer/publisher scaffolding.
- **Eventual consistency.** The report lags the OLTP truth by the freshness SLA;
  it is not a real-time view. This is the correct data-mesh tradeoff but must be
  communicated to report consumers.
- **The analytics publisher is a second producer path** for the same domain
  events. It re-serializes them under Envelope v1 for the analytics topic; the
  event set it publishes must be kept in step with the report's inputs.
- **First deploy has an empty report** until events flow; historical backfill
  requires replaying `warehouse.wes.analytics` from earliest into a fresh
  projector, so Kafka retention must cover the desired backfill window.

## References

- [ADR-0004 — Kafka integration events and envelope](./0004-kafka-integration-events.md)
- [ADR-0008 — MCP inbound adapter](./0008-mcp-inbound-adapter.md)
- Report contract: [Release Throughput & Backlog Health](../analytics/release-throughput-report.md)
