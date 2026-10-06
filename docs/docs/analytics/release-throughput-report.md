---
id: release-throughput-report
title: Release Throughput & Backlog Health Report
sidebar_label: Release throughput report
description: The wes-work-planning analytical data product — a Release Throughput & Backlog Health read model built from the service's own domain events, served read-only over REST and MCP. Contract, grain, inputs, freshness SLA, and versioning.
---

# Release Throughput & Backlog Health Report

The analytical **data product** owned by Work Planning & Release. It is built
entirely from this service's own domain events (never another service's
database) and served read-only. See
[ADR-0011](../adr/0011-analytical-data-product.md) for the decision.

## Name & owner

- **Report:** Release Throughput & Backlog Health.
- **Owner:** the Work Planning & Release service/team (the same team that owns
  the OLTP write model).

## Grain

One row per **(process path × hour bucket)**, where `hourBucket` is the UTC
hour the row aggregates. Metrics per row:

| Metric | Meaning |
|---|---|
| `workReleased` | Count of `WorkReleased` in the bucket — work admitted into the path. |
| `workUnitCompleted` | Count of `WorkUnitCompleted` in the bucket — released work finished. |
| `backlogThresholdBreached` | Count of `BacklogThresholdBreached` — backlog crossed its alarm threshold. |
| `pathThrottled` | Count of `PathThrottled` — flow balancing throttled upstream release. |
| `rateDeviationDetected` | Count of `RateDeviationDetected` — actual throughput diverged from plan. |

## Inputs (analytics topic events)

Consumed from **`warehouse.wes.analytics`** (the dedicated analytics topic,
separate from the integration topic — CloudEvents 1.0, see
[ADR-0027](../adr/0027-cloudevents-mandatory-event-envelope.md)):

| CloudEvents `type` | Contributes |
|---|---|
| `com.warehouse.wes.work-planning.workunit.WorkReleased` | `workReleased` |
| `com.warehouse.wes.work-planning.workunit.WorkUnitCompleted` | `workUnitCompleted` |
| `com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached` | `backlogThresholdBreached` |
| `com.warehouse.wes.work-planning.workpool.PathThrottled` | `pathThrottled` |
| `com.warehouse.wes.work-planning.workpool.RateDeviationDetected` | `rateDeviationDetected` |

Every event carries its own `path_id` (the report's key dimension) directly, so
no repo-lookup enrichment is needed. `WorkUnitCreated`, `ChargeForecastReceived`,
`ShiftPlanCommitted`, `LaborReassignmentFlagged`, `PathCapacityChanged` and
`PathPlanDriftDetected` are published to the topic but do not currently move
this report; the projector acknowledges them without projecting.

Every message is a CloudEvents 1.0 structured-mode event: `specversion` `1.0`,
`id` (UUID), `source` `/warehouse/wes-work-planning`, `type` (above),
`subject` = the aggregate id, `time` (domain occurred-at, UTC),
`datacontenttype` `application/json`, `dataschema`
`urn:warehouse:wes-work-planning:analytics:<EventName>:v1`, and a snake_case
`data`. The Kafka message key is the aggregate id — the `PathId` for
path-scoped events, the work-unit id for work-unit events — and every message
carries `content-type: application/cloudevents+json; charset=UTF-8`. The
projector dispatches on the full `type`, ignores unknowns, dedupes on the
CloudEvents `id` (idempotent projections), and logs-and-skips anything that is
not a valid CloudEvent.

## Interface

### REST (served by `cmd/wes-reports`, read-only)

```
GET /reports/throughput?from=<RFC3339>&to=<RFC3339>&pathId=&granularity=hour
GET /reports/throughput/freshness
GET /healthz
```

- `from`, `to` — **required**, RFC3339, `[from, to)` compared against `hourBucket`.
- `pathId` — optional exact-match filter.
- `granularity` — optional, defaults to `hour`.

Response (`200`):

```json
{
  "rows": [
    {
      "pathId": "pick-zone-a",
      "hourBucket": "2026-08-26T14:00:00Z",
      "workReleased": 42,
      "workUnitCompleted": 39,
      "backlogThresholdBreached": 1,
      "pathThrottled": 0,
      "rateDeviationDetected": 2
    }
  ]
}
```

Freshness (`200`):

```json
{ "lagSeconds": 4.2 }
```

Errors use RFC 7807 `application/problem+json`, consistent with the OLTP API
([ADR-0005](../adr/0005-rfc-7807-problem-details.md)).

### MCP (curated, read-only)

Tool **`get_release_throughput_report`** — same filters as the REST endpoint; it
calls the reports REST rather than opening the analytical database. Exposed by
the existing `cmd/mcp` server (Streamable HTTP) when `REPORTS_BASE_URL` is set,
consistent with [ADR-0008](../adr/0008-mcp-inbound-adapter.md).

## Freshness SLA

- **Definition:** `lagSeconds` = now − age of the most recently applied event.
- **Target:** p95 event-to-report lag **< 30s** under normal load.
- **Exposed:** `GET /reports/throughput/freshness`.
- Breaching the SLA is an operational signal (projector lag / consumer down),
  not a correctness bug — the report catches up when the projector does.
- **Empty-store note:** `max(occurred_at)` over an empty read model is a single
  NULL row; the reader reads that as **zero lag**, not an error.

## Versioning

- Additive fields (new optional row metric, new query filter) are non-breaking.
- A breaking change to a row's shape or meaning is a new endpoint/tool version.
- The analytics event contract versions independently via the CloudEvents
  `dataschema` (`urn:warehouse:wes-work-planning:analytics:<EventName>:v<N>`)
  and the analytics topic name.

## Runbook notes

- **Three processes, one writer.** `cmd/wes-projector` is the only writer of the
  analytical DB; `cmd/wes-reports` connects read-only. The OLTP `cmd/wes` never
  opens the analytical DB.
- **Fresh group reads full history.** The projector's consumer starts a brand-new
  group at the earliest offset (`StartOffset: FirstOffset`), so a fresh projector
  or a backfill sees the whole topic rather than only events produced after it
  joined.
- **Empty on first deploy.** The report is empty until events flow. To backfill
  history, replay `warehouse.wes.analytics` from earliest into a fresh projector;
  Kafka retention must cover the desired window.
- **Eventual consistency.** The report is a projection, not a real-time view; it
  meets the freshness SLA, not transactional consistency.
