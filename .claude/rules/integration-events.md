# Integration & REST reference — wes-work-planning

## REST API (inbound adapter, 12 operationIds in apis/openapi.yaml)

- `GET  /healthz`                          → healthCheck
- `POST /paths/{pathId}/charge`            → receiveChargeForecast
- `POST /paths/{pathId}/plan`              → commitShiftPlan
- `POST /paths/{pathId}/work-units`        → enqueueWorkUnit
- `POST /paths/{pathId}/release`           → releaseNextWork
- `GET  /paths/{pathId}/telemetry`         → sampleBacklog
- `GET  /paths/{pathId}/rebalance`         → rebalanceDecision
- `GET  /paths/{pathId}/labor-plan-view`   → getLaborPlanView
- `GET  /work-units?reference=`            → getWorkUnitsByReference
- `GET  /work-units/{id}`                  → getWorkUnit
- `POST /work-units/{id}/complete`         → recordCompletion
- `GET  /inventory-view/{sku}`             → getInventoryView

`GET /work-units?reference=` is the read side backing the fleet's
cross-service Order Lifecycle console screen — see ADR-0002 in
`warehouse-ops-agent`'s docs and this repo's own adoption-record ADR under
`docs/docs/adr/`. It returns every WorkUnit ever enqueued against a
caller-supplied `reference` (order-management's `OrderId`), array-shaped,
side-effect-free.

`GET /work-units/{id}` is the identity lookup (GetWorkUnit use case, reuses
`WorkUnitRepo.FindById`): one `WorkUnitResponse` (same schema as the other
work-unit endpoints) or `404 not-found`. It exists so a client holding only
a WorkUnitId — e.g. an RF gun resolving a fulfillment-execution PICK task's
`orderRef`, which is this service's deterministic `<orderId>-line-<lineNo>`
WorkUnitId — can read sku/reference/pathId/cpt/state without parsing the id
string. chi routes the static `/complete` suffix separately, so it does not
shadow `POST /work-units/{id}/complete`.

Full request/response schemas, every status code, and the shared `Problem`
error component: [`apis/openapi.yaml`](../../apis/openapi.yaml). The
Docusaurus REST reference (`docs/docs/api/rest/*.api.mdx`, 12 files, one per
operationId + tag pages) is **generated** from this spec by
`docusaurus-plugin-openapi-docs` — regenerate with
`cd docs && npm run gen-api-docs` (or let the `prebuild` script do it as
part of `npm run build`); never hand-edit the generated `.api.mdx`/`.json`
files.

## Kafka integration events

Client library: `github.com/segmentio/kafka-go` (transport) +
`github.com/cloudevents/sdk-go/v2/event` (envelope). **Every** message on
every topic this service produces or consumes is a CloudEvents 1.0 event in
structured content mode (ADR-0027, mandatory fleet-wide). There is no flat
envelope, no dual-write/dual-read and no `EVENT_ENVELOPE_MODE`. Build and
decode only through `internal/adapters/kafka/cloudevents`
(`New` / `Decode` / `ContentTypeHeader`); never hand-roll an envelope struct.

```json
{
  "specversion": "1.0",
  "id": "uuid-v4",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-10231",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": { }
}
```

Kafka header on every produced message:
`content-type: application/cloudevents+json; charset=UTF-8` (plus the W3C
trace headers). `id` is minted once in `Encode` and persisted with the
outbox row. `subject` is the aggregate id. Analytics events use the same
`type` with `dataschema` `urn:warehouse:wes-work-planning:analytics:<Event>:v1`.

### Published

Topic `warehouse.work-planning.events`:

- `com.warehouse.wes.work-planning.workunit.WorkReleased` — published when `ReleaseNextWork` releases a unit.
  `data`: `{"path_id","work_unit_id","cpt","ref"}` (+ optional
  `required_capabilities`/`fragile` from product-classification
  propagation). Consumed downstream by `fulfillment-execution` → creates a
  `Task`.
- `com.warehouse.wes.work-planning.workpool.PathCapacityChanged` — published when `SampleBacklog` is called with an
  optional CPT `cutoffAt` (via `GET /paths/{pathId}/telemetry?cutoffAt=`).
  `data`: `{"path_id","cutoff_at","remaining_units","known"}`. `known` is
  `false` for a FlowFed path (no hard admission ceiling) or a ReleaseFed
  path with no WIP limit provisioned. Consumed by order-management's
  `internal/adapters/outbound/kafkapathcapacity` adapter, which backs its
  `ports.PathCapacity` port (own per-process consumer group; wired when
  order-management runs with `PATH_CATALOGUE_SOURCE=kafka`) (ADR-0018).
- All other domain events are also published to this topic for
  observability; only `WorkReleased` and `PathCapacityChanged` have live
  consumers today.

Topic `warehouse.wes.analytics` (separate, additive — see
`.claude/rules/architecture.md`'s Analytics section): every domain event,
fanned out alongside the integration topic when `EVENT_PUBLISHER=kafka`,
consumed only by `cmd/wes-projector`.

### Consumed

`internal/adapters/inbound/kafka/consumer.go` subscribes to the topics below
and dispatches on the FULL CloudEvents `type` (constants in
`internal/adapters/kafka/cloudevents`); unknown types are ignored. A message
that fails `cloudevents.Decode` (bad JSON, legacy flat envelope, missing
attribute) is published raw to `<topic>.dlq` without retry and committed —
never parsed as a legacy shape.

1. `warehouse.workforce.events`,
   `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` →
   projected into the read-only `LaborPlanObserved` (`internal/domain/laborview/`),
   keyed by `path_id`. **Not** fed into this service's own
   `ShiftPlan`/`PathPlan` aggregate (ADR-0006 — same term, different bounded
   context).
2. `warehouse.inventory.events`,
   `com.warehouse.wms.inventory-storage.reservation.StockReserved` /
   `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` → projected into `UsableInventoryObserved`
   (`internal/domain/inventoryview/`), keyed by SKU.
3. `warehouse.fulfillment.events`,
   `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` → calls the
   existing `RecordCompletion` use case directly (closes the
   Execution → Orchestration feedback loop).
4. `warehouse.order-management.events`,
   `com.warehouse.wes.order-management.order.OrderAllocated` /
   `com.warehouse.wes.order-management.order.OrderPartiallyAllocated`
   (identical payload shape) → fed directly into
   the existing `EnqueueWorkUnit` use case, one call per allocated line.
   This **replaces** order-management's former synchronous HTTP call to
   `POST /paths/{pathId}/work-units` with event choreography — verify
   against `internal/adapters/inbound/kafka/consumer.go`'s doc comment
   before assuming scope, this list grows.

All four run under one consumer group, `KAFKA_CONSUMER_GROUP` (default
`wes-work-planning`, resolved by `consumerGroupID` in `cmd/wes/main.go`).
Any second process on the shared broker (a local `go run`, the e2e harness)
must set a unique value, or the rebalance gives the single partition to one
member and the other silently consumes nothing.

Separately, with `PATH_CATALOGUE_SOURCE=kafka`,
`internal/adapters/outbound/kafkacatalog` replays
`warehouse.process-path-management.events`
(`com.warehouse.wes.process-path-management.processpath.ProcessPathCreated` /
`...ProcessPathUpdated` / `...ProcessPathDeactivated`; invalid CloudEvents are
WARN-logged and skipped) into the in-memory
`pathcatalog.Catalogue` (ADR-0012) under its own per-process consumer group
(NOT `KAFKA_CONSUMER_GROUP`) — startup blocks until the replay catches up,
then it follows live. Default `PATH_CATALOGUE_SOURCE=file` reads
`PATH_CATALOGUE_FILE` instead.

### Idempotency

Every consumer path is idempotent under Kafka's at-least-once delivery via a
`processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ)`
table (Postgres) or a thread-safe map (in-memory adapter): insert the
CloudEvents `id` (into the `event_id` column) before applying an effect; skip
if already present.
`RecordCompletion` additionally rejects double-complete at the domain level
as defense in depth.

### Transactional outbox (ADR-0014)

With `EVENT_PUBLISHER=kafka` **and** `DATABASE_URL` set, events are written
to the `outbox_events` table in the same transaction as the aggregate change
and relayed to both Kafka topics by an in-process relay
(`internal/adapters/outbound/postgres/` UnitOfWork +
`internal/adapters/outbound/kafka/` RelaySink) — not published directly
in-request. Without `DATABASE_URL`, events publish directly (no outbox).

## AsyncAPI contract

`apis/asyncapi.yaml` declares 10 messages on `warehouse.work-planning.events`
and their 10 analytics counterparts on `warehouse.wes.analytics`, each with
its exact `type` const and `dataschema` const; `defaultContentType` is
`application/cloudevents+json` and every CloudEvents attribute is required.
`RateDeviationDetected` is declared but not yet raised by any use case.
Narrative docs (`docs/docs/ddd/domain-events.md`, `docs/docs/api/events.md`,
`docs/docs/ecosystem/integration-events.md`) are hand-written from it —
update them whenever a message or channel changes.
