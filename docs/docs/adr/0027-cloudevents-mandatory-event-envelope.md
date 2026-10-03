---
id: 0027-cloudevents-mandatory-event-envelope
title: ADR-0027 — CloudEvents 1.0 as the mandatory event envelope
sidebar_label: 0027 · CloudEvents mandatory envelope
sidebar_position: 27
description: Every Kafka message wes-work-planning produces or consumes — integration AND analytics topics — is a CloudEvents 1.0 event in structured content mode, built with the official sdk-go event package. No flat envelope, no dual-write/dual-read, no EVENT_ENVELOPE_MODE. Supersedes ADR-0021 and the envelope parts of ADR-0004. (Accepted)
---

# ADR-0027 — CloudEvents 1.0 as the mandatory event envelope

## Status

**Accepted** (2026-09-30). Fleet-wide standard, recorded identically in
every warehouse-systems service.

Supersedes [ADR-0021](./0021-cloudevents-envelope-migration.md) (the
dual-read/dual-write migration plan) and the envelope sections (§2, §3 and
the "Recorded honestly" caveat) of
[ADR-0004](./0004-kafka-integration-events.md). It also retires the analytics
"Envelope v1" (`schema_version`) described in
[ADR-0011](./0011-analytical-data-product.md).

## Context

ADR-0004 documented CloudEvents as a *target* while the running publishers
wrote a flat envelope (`event_id`/`event_type`/`occurred_at`/`source`/`data`).
ADR-0021 planned a bake period behind an `EVENT_ENVELOPE_MODE`
(`flat`/`cloudevents`/`dual`) toggle and a specversion-sniffing dual-read on
the fulfillment consumer. The analytics topic used yet another shape with a
`schema_version` field. Three shapes, a toggle and a discriminator are three
ways for producers and consumers to disagree, and the toggle never left
`flat` in any environment. The fleet decided to cut over once, together,
with no coexistence.

## Decision

**Every message this service writes to or reads from Kafka is a CloudEvents
1.0 event. There is no other envelope and no switch.**

### Encoding

- CloudEvents Kafka protocol binding, **structured content mode**: the Kafka
  message value is the JSON event format.
- Every produced message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`, alongside the
  existing W3C `traceparent`/`tracestate` headers (trace context is not
  duplicated into CloudEvents extensions).
- Message keys and the `kafkago.Hash{}` balancer are unchanged
  ([ADR-0024](./0024-kafka-hash-balancer-partition-affinity.md)): the
  integration topic is keyed by the event `id`, the analytics topic by the
  aggregate id.
- Events are built, validated and (un)marshalled with
  `github.com/cloudevents/sdk-go/v2/event` (v2.16.2). The sdk-go protocol and
  client packages are **not** used; transport stays `segmentio/kafka-go`.
  No hand-rolled CloudEvent struct exists.
- One helper package: `internal/adapters/kafka/cloudevents`
  (`New`, `Decode`, `ContentTypeHeader`, `Type`, `DataSchema`, topic names
  and the consumed `type` constants). Every publisher and consumer uses it.

### Context attributes (all required)

| attribute | value |
|---|---|
| `specversion` | `1.0` |
| `id` | UUID v4, minted once per domain event inside `Encode` and persisted with the transactional outbox row ([ADR-0014](./0014-transactional-outbox.md)), so a relay retry republishes the same id. |
| `source` | `/warehouse/wes-work-planning` (both topics) |
| `type` | `com.warehouse.wes.work-planning.<entity>.<EventName>` |
| `subject` | aggregate instance id — work unit id for `workunit` events, path id otherwise |
| `time` | domain occurred-at, UTC, RFC 3339 |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:wes-work-planning:<events\|analytics>:<EventName>:v1` |

`data` is byte-for-byte the payload published before this change. The
analytics `schema_version` field is removed; `dataschema` replaces it.

### Published types (same `type` on both topics)

```
com.warehouse.wes.work-planning.charge.ChargeForecastReceived
com.warehouse.wes.work-planning.plan.ShiftPlanCommitted
com.warehouse.wes.work-planning.workunit.WorkUnitCreated
com.warehouse.wes.work-planning.workunit.WorkReleased            -> fulfillment-execution
com.warehouse.wes.work-planning.workunit.WorkUnitCompleted
com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached
com.warehouse.wes.work-planning.workpool.RateDeviationDetected
com.warehouse.wes.work-planning.workpool.PathThrottled
com.warehouse.wes.work-planning.workpool.LaborReassignmentFlagged
com.warehouse.wes.work-planning.workpool.PathCapacityChanged     -> order-management
```

A breaking payload change requires a new `dataschema` version **and** a new
type with a `.v2` suffix, published as a new event; an existing type is never
mutated.

### Consumed types (dispatch on the exact string)

```
com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted
com.warehouse.wms.inventory-storage.reservation.StockReserved
com.warehouse.wms.inventory-storage.reservation.ReservationRevoked
com.warehouse.wes.fulfillment-execution.task.TaskCompleted
com.warehouse.wes.order-management.order.OrderAllocated
com.warehouse.wes.order-management.order.OrderPartiallyAllocated
com.warehouse.wes.process-path-management.processpath.ProcessPathCreated
com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated
com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated
```

The analytics projector (`cmd/wes-projector`) also consumes this service's
own `WorkReleased`, `WorkUnitCompleted`, `BacklogThresholdBreached`,
`PathThrottled` and `RateDeviationDetected` from `warehouse.wes.analytics`.

### Consumer rules

1. Decode with `cloudevents.Decode` (SDK unmarshal + `specversion == 1.0` +
   `Validate()`). Anything else — bad JSON, the retired flat envelope, a
   missing required attribute — is a deterministic poison message: the main
   inbound consumer publishes it raw to `<topic>.dlq`
   ([ADR-0023](./0023-resilience-circuit-breakers-retry-dlq-shutdown.md))
   without retries and commits; the process-path catalogue consumer and the
   analytics projector log WARN with topic/partition/offset and move past it.
   Nothing ever falls back to parsing a legacy shape.
2. Dispatch on the full `type`; unknown types are ignored.
3. Dedupe on the CloudEvents `id` (`processed_events.event_id` and
   `analytics_processed_events.event_id` keep their column names, now
   populated from `id`).
4. `time`/`subject` come from the context attributes; the payload from
   `DataAs`.

## Consequences

### Easier

- One wire shape for every topic, validated by a real SDK on both ends.
- `dataschema` makes the integration vs analytics payload explicit instead
  of implied by topic name.
- Golden exact-JSON tests (`internal/adapters/outbound/kafka/testdata/ce_*`)
  pin every published event on both topics.

### Harder

- **Not backwards compatible.** All fleet PRs must be deployed together.
  Before deploying: drain the outbox (rows were pre-encoded flat), delete and
  recreate `warehouse.*.events` / `warehouse.*.analytics`, and re-seed the
  process-path catalogue, so no flat message is left for a FirstOffset
  replay. See warehouse-infra `docs/cloudevents-cutover.md`.
- `outbox_events.event_type` now stores the full CloudEvents `type`.
- `EVENT_ENVELOPE_MODE` is gone; setting it has no effect.
