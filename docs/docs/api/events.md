---
id: events
title: Events
sidebar_label: Events (AsyncAPI)
sidebar_position: 2
description: The CloudEvents envelope, the type naming convention, and every event this service publishes and consumes.
---

# Events

The asynchronous contract lives in
[`apis/asyncapi.yaml`](https://github.com/claudioed/wes-work-planning/blob/main/apis/asyncapi.yaml)
(AsyncAPI 2.6.0), linted in CI by Spectral against `.spectral.asyncapi.yaml`.
This page is written from that spec.

## The channel

| | |
|---|---|
| **Topic** | `warehouse.work-planning.events` |
| **Protocol** | Kafka (`localhost:9092` locally; one broker shared by every `warehouse-systems` service) |
| **Message key** | the event id |
| **Default content type** | `application/cloudevents+json` |
| **Delivery** | at-least-once — **consumers must deduplicate** |

## The CloudEvents envelope

Every message this service produces or consumes — on the integration topic
**and** the analytics topic — is a **CloudEvents 1.0 structured-mode** event
(mandatory fleet standard,
[ADR-0027](../adr/0027-cloudevents-mandatory-event-envelope.md)). Context
attributes and the event-specific `data` payload travel together in one JSON
body; there is no other envelope, no dual mode and no envelope toggle. Events
are built and decoded with the official
`github.com/cloudevents/sdk-go/v2/event` package through
`internal/adapters/kafka/cloudevents`. Every produced Kafka message carries
the header `content-type: application/cloudevents+json; charset=UTF-8`
alongside the W3C trace headers.

```json
{
  "specversion": "1.0",
  "id": "1d7e4b90-3c58-4d22-9a6f-8b1c0e5d7a23",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-10231",
  "time": "2026-08-21T22:12:30Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": {
    "path_id": "pick-to-tote",
    "work_unit_id": "wu-10231",
    "cpt": "2026-08-22T02:00:00Z",
    "ref": "order-88421-line-3"
  }
}
```

| Attribute | Rule |
|---|---|
| `specversion` | always `"1.0"` |
| `id` | UUID v4 minted once per domain event and persisted with the outbox row (a redelivery carries the same id); **with `source`, this is the de-duplication key** |
| `source` | always `/warehouse/wes-work-planning` on this channel |
| `type` | see the naming convention below |
| `subject` | the aggregate instance the event is about — a `path_id` for charge/plan/work-pool events, a `work_unit_id` for work-unit events |
| `time` | RFC 3339, taken from the **domain clock**, not from publish time |
| `datacontenttype` | always `application/json` |
| `dataschema` | `urn:warehouse:wes-work-planning:events:<EventName>:v1` on this channel; `...:analytics:<EventName>:v1` on `warehouse.wes.analytics` |

All attributes above are required. The same occurrence carries the same
`type` on both topics; `dataschema` tells the integration payload from the
analytics payload. A breaking payload change requires a new `.v2` type and a
new `dataschema` version, published as a new event.

Consumers should **ignore `type` values they do not recognise**: new event
types may be added to this channel without a major version bump.

### The `type` naming convention

Shared by every bounded context in the platform:

```
com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>
```

All segments lowercase except the final PascalCase event name, which matches
the past-tense domain event name used in the code. For this service the
subdomain is `wes` and the bounded context is `work-planning`:

```
com.warehouse.wes.work-planning.charge.ChargeForecastReceived
com.warehouse.wes.work-planning.plan.ShiftPlanCommitted
com.warehouse.wes.work-planning.workunit.WorkReleased
com.warehouse.wes.work-planning.workpool.PathThrottled
```

The `<entity>` segment names the aggregate that raised the event: `charge`
(ChargeForecast), `plan` (ShiftPlan/PathPlan), `workpool` (WorkPool and the
flow-balancing decisions taken against it), `workunit` (WorkUnit).

## Events published

Topic `warehouse.work-planning.events`. Ten event types are catalogued; the
`data` shape of each is below.

| Event (`type` suffix after `com.warehouse.wes.work-planning.`) | `data` fields | Raised when |
|---|---|---|
| `charge.ChargeForecastReceived` | `path_id` | a charge forecast is recorded for a path |
| `plan.ShiftPlanCommitted` | `path_id` | **this** context commits its own rate × heads × hours plan |
| `workunit.WorkUnitCreated` | `path_id`, `work_unit_id` | a work unit is enqueued into a pool |
| **`workunit.WorkReleased`** | `path_id`, `work_unit_id`, `cpt`, `ref` | the release policy admits the earliest-CPT unit |
| `workunit.WorkUnitCompleted` | `path_id`, `work_unit_id` | a released unit completes |
| `workpool.BacklogThresholdBreached` | `path_id` | backlog depth crosses the pool's alarm threshold |
| `workpool.RateDeviationDetected` | `path_id` | *declared in the catalogue; **no use case raises it today*** |
| `workpool.PathThrottled` | `path_id` | flow balancing decides to throttle upstream release |
| `workpool.LaborReassignmentFlagged` | `path_id` | flow balancing recommends moving headcount |
| `workpool.PathCapacityChanged` | `path_id`, `cutoff_at`, `remaining_units`, `known` | `SampleBacklog` is called with a `cutoffAt` query parameter (ADR-0018) |

**`WorkReleased` and `PathCapacityChanged` are the only ones any other
service consumes today** — `fulfillment-execution` turns `WorkReleased` into a
`Task`; `order-management` consumes `PathCapacityChanged` (below). Its payload is enriched at the
adapter with `cpt` and `ref` (read from the `WorkUnit` repository) so the
downstream consumer never has to call back; the domain event itself carries
only the two identifiers.

`WorkReleased.data` also carries two OPTIONAL fields, present only when
there is a hint to give: `required_capabilities` (array, containing
`"hazmat"` when the released unit's SKU is classified `Hazmat` in
inventory-storage) and `fragile` (bool, `true` when the SKU is classified
`Fragile`). See "Product classification propagation" below.

Publication is opt-in at runtime: with the default `EVENT_PUBLISHER=log` these
events are written to the log publisher instead of Kafka. Set
`EVENT_PUBLISHER=kafka` and `KAFKA_BROKERS` to publish.

## Events consumed

These belong to **other** bounded contexts and are documented in their
services' own specs; they are listed here for orientation. Consumption starts
automatically whenever `KAFKA_BROKERS` is set, independent of
`EVENT_PUBLISHER`.

Every consumer decodes with `cloudevents.Decode` and dispatches on the
**full** `type` string below (never a short name or suffix match); unknown
types are ignored. A message that is not a valid CloudEvents 1.0 event —
including the retired flat envelope — is published raw to `<topic>.dlq` and
committed past (no retries), never parsed as a legacy shape. `time` and the
dedupe `id` come from the CloudEvents attributes; the payload from
`DataAs`.

### `warehouse.workforce.events` — from `workforce-management`

| `type` | `data` | Effect here |
|---|---|---|
| `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | `building_id`, `shift_id`, `path_id`, `planned_heads`, `planned_rate`, `planned_hours` | Upserts the `LaborPlanObserved` projection for `path_id` |

Workforce publishes **one message per path line**, which is why the projection
keys cleanly on `path_id`. This is *not* fed into this context's own
`ShiftPlan` aggregate — see
[ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md).

### `warehouse.inventory.events` — from `inventory-storage`

| `type` | `data` | Effect here |
|---|---|---|
| `com.warehouse.wms.inventory-storage.reservation.StockReserved` | `sku`, `quantity`, `demand_ref` | **Decrements** `UsableInventoryObserved` for that SKU |
| `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` | `sku`, `quantity`, `demand_ref` | **Increments** it back |

Keyed by SKU, not by path — Inventory reservations are SKU-scoped.

### `warehouse.fulfillment.events` — from `fulfillment-execution`

| `type` | `data` | Effect here |
|---|---|---|
| `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | `task_id`, `station_id`, `work_unit_id` | `data.work_unit_id` is passed to the existing `RecordCompletion` use case |

This closes the control loop's feedback edge. No new use case was introduced —
the inbound adapter calls exactly the same code path that
`POST /work-units/{id}/complete` does.

### `warehouse.order-management.events` — from `order-management`

| `type` | `data` | Effect here |
|---|---|---|
| `com.warehouse.wes.order-management.order.OrderAllocated` | `order_id`, `promise_date`, `lines[]` (`line_no`, `sku`, `path_id`, `gift_wrap`) | One `EnqueueWorkUnit` call per line |
| `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` | same shape | same effect |

### `warehouse.process-path-management.events` — from `process-path-management`

With `PATH_CATALOGUE_SOURCE=kafka`, the catalogue cache replays
`com.warehouse.wes.process-path-management.processpath.ProcessPathCreated`,
`...ProcessPathUpdated` and `...ProcessPathDeactivated`. Invalid CloudEvents
on this topic are logged at WARN and skipped.

## Product classification propagation (Task 9, synchronous HTTP)

`inventory-storage` owns SKU-level `ProductClassification` master data
(`Hazmat`/`Fragile`/`TemperatureSensitive`/`Oversized`/`HighValue` tags),
exposed synchronously at `GET /products/{sku}/classification`. Downstream,
`fulfillment-execution`'s `Task` benefits from knowing hazmat-capability and
fragile handling **at claim time**, without a live per-task callback into
inventory-storage.

**Integration mechanism: synchronous outbound HTTP read, not a Kafka
projection.** This was a deliberate finding, not a default: inventory-storage
publishes `ProductClassified` in its domain-event catalogue, but its own
outbound Kafka publisher (`internal/adapters/outbound/kafka/publisher.go`
there) explicitly forwards only `StockReserved` and `ReservationRevoked` to
the broker — see that file's package doc comment and
`apis/asyncapi.yaml`'s "Full catalog vs. actually published" note.
`ProductClassified` is not part of the published integration contract, so
there is nothing to consume; building a Kafka projector against it would be
building a consumer for an event that never reaches the broker. See
[ADR-0009](../adr/0009-product-classification-propagation-to-work-released.md)
for the full reasoning.

- New outbound adapter package `internal/adapters/outbound/productclassification/`
  implementing a new port `ports.ProductClassificationLookup`
  (`GetClassification(ctx, sku) (productclassificationview.ProductClassificationView, error)`):
  a plain `net/http` client (`Client`, mirrors inventory-storage's own
  `facilitylayout.Client` HTTP-adapter pattern) calling
  `GET {INVENTORY_STORAGE_BASE_URL}/products/{sku}/classification`, and a
  `PermissiveLookup` no-op that always reports `Known=false`. Selected via
  `PRODUCT_CLASSIFICATION_MODE=http|permissive` (default `permissive`, so
  existing tests/CI/deployments are unaffected).
- New read model `internal/domain/productclassificationview/` —
  `ProductClassificationView{SKU, HandlingTags, TemperatureClass, Known}` — a
  plain value, not persisted, not an aggregate.
- `WorkUnit` gains an optional `SKU` field (`SetSKU`/`SKU()`), threaded
  through from `EnqueueWorkUnitRequest.SKU` (new optional field, empty by
  default so no existing caller breaks).
- `ReleaseNextWork`'s outbound Kafka publisher
  (`internal/adapters/outbound/kafka/publisher.go`) looks up the released
  unit's SKU classification **once**, at publish time, and stamps two new
  OPTIONAL `WorkReleased.data` fields: `required_capabilities` (array,
  appends `"hazmat"` when the SKU carries the `Hazmat` tag) and `fragile`
  (bool, `true` when the SKU carries the `Fragile` tag). Both fields are
  **omitted** — not defaulted to an explicit empty array / `false` — when
  the SKU is unclassified, has no SKU at all, or the lookup is unavailable
  (permissive mode or a lookup error): this is strictly additive and
  backward compatible with `fulfillment-execution`'s existing `WorkReleased`
  consumer, which the sibling repo's parallel PR extends to read these same
  optional fields, defaulting to `false`/empty when absent.
- **Fail-open, not fail-closed.** Unlike inventory-storage's own
  `StowStock` placement check (which blocks a stow when a Hazmat/
  TemperatureSensitive SKU's lookup is unavailable), a classification-lookup
  problem here must never block or delay releasing work — it can only omit
  an optional enrichment. Documented as a known gap in ADR-0009's
  Consequences, in the same spirit as inventory-storage's ADR-0003 "no
  expiry sweeper" gap.

## Remaining path capacity (ADR-0018)

`GET /paths/{pathId}/telemetry` accepts an optional `cutoffAt` (RFC3339)
query parameter. When supplied, `SampleBacklog` additionally computes the
path's current remaining admission capacity from its own `WorkPool`
(`wipLimit - WIP`, always non-negative under the pool's own enforced
invariant) and publishes `PathCapacityChanged` on
this topic, correlated against the given CPT cutoff timestamp:

```json
{
  "specversion": "1.0",
  "id": "5c7d3f92-1b64-4a08-9e73-2f6a8c1d5b40",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workpool.PathCapacityChanged",
  "subject": "pick-to-tote",
  "time": "2026-08-21T22:40:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:PathCapacityChanged:v1",
  "data": {
    "path_id": "pick-to-tote",
    "cutoff_at": "2026-08-22T02:00:00Z",
    "remaining_units": 17,
    "known": true
  }
}
```

`known` is `false` (and `remaining_units` is always `0`) for a `FlowFed`
path — it has no hard admission ceiling, only a backlog alarm threshold,
which is not a capacity figure — or for a `ReleaseFed` path with no WIP
limit provisioned. `order-management` consumes it through its
`kafkapathcapacity` adapter, which backs its `ports.PathCapacity` port when
order-management runs with `PATH_CATALOGUE_SOURCE=kafka` (as deployed; without
it, that port falls back to the `UnknownPathCapacity` placeholder); see
[ADR-0018](../adr/0018-path-capacity-changed.md) for the full design,
including why correlation runs by `cutoff_at` timestamp rather than
process-path-management's `cptId` string.

## Idempotency

Kafka is at-least-once, so **every** consumer path here is idempotent. Before
applying an event's effect, its CloudEvents `id` is inserted into `processed_events`
(Postgres) or a thread-safe set (in-memory). A primary-key collision means
"already processed": the effect is skipped and the message is acked anyway.

Consequences worth knowing:

- Replaying a `StockReserved` does **not** double-decrement usable inventory.
- Replaying a `ShiftPlanCommitted` does **not** re-write the labour projection.
- Replaying a `TaskCompleted` does **not** call `RecordCompletion` twice — so a
  redelivery never surfaces the aggregate's `ErrAlreadyCompleted` as a
  spurious error or a retry loop. The aggregate would reject it anyway; that
  is defence in depth, not the only net.

## Smoke-testing by hand

```sh
# publish a workforce-shaped CloudEvent onto the shared broker (one line)
echo '{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111",
       "type":"com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted",
       "source":"/warehouse/workforce-management","subject":"S1",
       "time":"2026-08-23T09:00:00Z","datacontenttype":"application/json",
       "dataschema":"urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1",
       "data":{"building_id":"BLD1","shift_id":"S1","path_id":"pick-a",
               "planned_heads":7,"planned_rate":95.5,"planned_hours":8}}' | tr -d '\n' \
| kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic warehouse.workforce.events

curl -s localhost:8080/paths/pick-a/labor-plan-view

# watch what this service publishes
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.work-planning.events --from-beginning
```
