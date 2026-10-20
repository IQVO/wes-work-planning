---
id: 0042-pick-face-replenishment-work
slug: /adr/0042-pick-face-replenishment-work
title: 0042. Plan pick-face replenishment requests as REPLENISH work
sidebar_label: 0042. Pick-face replenishment work
description: ADR 0042 — wes-work-planning consumes pick-face-replenishment's ReplenishmentRequested and plans ONE REPLENISH work unit per request id on the REPLENISH path; ReplenishmentCancelled withdraws a unit that is still Pending; URGENT requests get the short CPT horizon so they sit in the front band; WorkReleased v1 gains an optional replenishment block that fulfillment-execution's REPLENISH task decodes.
---

# 0042. Plan pick-face replenishment requests as REPLENISH work

## Status

Accepted. Amended by [ADR-0044](./0044-postgres-and-kafka-are-the-only-runtime.md): `REPLENISHMENT_MODE` defaults to `kafka` and `permissive`/`off` are boot errors; the consumer is always on.

Mirrors the RELOCATE chain of [ADR-0040](./0040-slot-plan-relocation-work.md)
for the second floor-work demand source of the pick-face replenishment design
(fleet plan, decision 5, change P3b). The consuming side is
fulfillment-execution's ADR 0050 (REPLENISH task).

## Context

pick-face-replenishment keeps forward pick faces stocked from reserve bins. For
each pick face under its min/max policy it raises a `ReplenishmentRequested`
(`com.warehouse.wms.pick-face-replenishment.replenishment.ReplenishmentRequested`,
dataschema
`urn:warehouse:pick-face-replenishment:events:ReplenishmentRequested:v1`) on
`warehouse.pick-face-replenishment.events`, key and subject = the request id.
Its `data` carries `request_id`, `site_id`, `sku`, `forward_bin` (the pick face
to fill), `source_bin` (the reserve bin), `quantity`, `priority`
(`NORMAL` | `URGENT`: URGENT when the pick face had nothing available at
detection), `policy_min`, `policy_max` and `requested_at`. It also raises
`ReplenishmentCancelled` (`reason`: `OPERATOR`, `ASSIGNMENT_CHANGED`,
`POLICY_REMOVED`) and `ReplenishmentCompleted` on the same topic.

The physical move is the same kind of floor work as a slot-plan move: an
operator carries stock from one bin to another. This context plans and pools
it, process-path-management routes it, fulfillment-execution runs it with
operators, and inventory-storage records the result from fulfillment-execution's
`TaskCompleted`. pick-face-replenishment closes its request from
inventory-storage's `StockRelocated`.

## Decision

1. **A seventh consumed topic, with its own consumer.**
   `internal/adapters/inbound/kafka/replenishment_consumer.go` reads
   `warehouse.pick-face-replenishment.events`
   (`cloudevents.TopicPickFaceReplenishmentEvents`). It is opt-in:
   `REPLENISHMENT_MODE=permissive` (the default) consumes nothing, so a
   deployment that has not opted in plans no REPLENISH work;
   `REPLENISHMENT_MODE=kafka` runs it under the STABLE group
   `REPLENISHMENT_CONSUMER_GROUP`, shared by every replica. `kafka` without the
   group, without `KAFKA_BROKERS` or without `DATABASE_URL` (Postgres: the planned
   work is shared state, so the in-memory repositories are never an option for
   this consumer), and any other mode value, is a boot error.
   The consumer follows the fleet pattern: dispatch on the FULL CloudEvents
   type; `ReplenishmentRequested` and `ReplenishmentCancelled` are handled;
   `ReplenishmentCompleted` and every unknown type are committed past; a message
   that is not a valid CloudEvent (bad JSON, the retired flat envelope) is
   WARN-logged and skipped; `FetchMessage` + `CommitMessages` after success;
   the use case claims the CloudEvents `id` and writes its effect in ONE atomic
   scope ([ADR-0028](./0028-processed-event-mark-atomic-with-handling.md));
   a payload that can never be applied is dead-lettered at once, any other
   failure is retried with jittered backoff (3 attempts) and then the raw message
   goes to `warehouse.pick-face-replenishment.events.dlq`; a message that only
   shutdown interrupted is never dead-lettered.

2. **`ApplyReplenishmentRequested`, beside `ApplySlotPlanApproved`.** Nothing
   leaves except through the transactional outbox
   ([ADR-0014](./0014-transactional-outbox.md)): `WorkUnitCreated` is published
   in the same transaction as the processed-event mark, the pool entry and the
   unit.

3. **ONE REPLENISH work unit per request id.**
   - id `replenish-<request_id>` (deterministic). A redelivery of the same
     CloudEvents `id` is caught by the processed-event mark; a DIFFERENT event
     for an already-planned request id finds the unit and is a no-op, never a
     second unit;
   - `reference = request_id`, so `GET /work-units?reference=<request_id>`
     finds it;
   - path `REPLENISH`, validated against the process-path catalogue
     ([ADR-0012](./0012-process-path-catalogue-validation.md)). The catalogue
     declares no replenishment intake value (it defines `CUSTOMER_ORDER`,
     `STOCK_TRANSFER`, `SLOT_RELOCATION`, `INVENTORY_CONTROL`), so no intake is
     evaluated: like every path reached by explicit id, `REPLENISH` is accepted
     whatever it declares. The path must exist in the catalogue before requests
     arrive, or they are dead-lettered until it does (replay from the DLQ);
   - `work_kind = REPLENISH`, `site_id`, `sku`, `quantity`, plus the
     replenishment value: `request_id`, `from_bin` (the event's `source_bin`),
     `to_bin` (the event's `forward_bin`), `priority`.

4. **Invariants of the value** (`workunit.ReplenishmentMove`): `request_id`,
   `sku` and both bins are present, `quantity > 0`, the bins differ, `priority`
   is `NORMAL` or `URGENT`. `site_id` and `requested_at` are required by the use
   case. A violation is deterministic poison and goes to the DLQ without
   retries.

5. **Priority mapping: URGENT sits in the front band through the CPT.** The
   WorkUnit has no priority field; pools release earliest CPT first, and
   fulfillment-execution's `claimNext` does the same. So the priority is
   expressed in the CPT: `cpt = requested_at + horizon(priority)`, with
   `REPLENISHMENT_CPT_HORIZON` (default `8h`) for NORMAL and
   `REPLENISHMENT_URGENT_CPT_HORIZON` (default `1h`) for URGENT. An urgent
   horizon above the normal one is capped at it, so URGENT can never sort
   behind NORMAL. Malformed or non-positive values fall back to the default and
   never fail the boot. The priority itself travels on the released event.

6. **Release goes through the existing flow.** `ReleaseNextWork` on path
   `REPLENISH`. `WorkReleased` stays v1. For a REPLENISH unit its `data` gains
   ONE optional object, present only on REPLENISH units:

   ```json
   "replenishment": {
     "request_id": "repl-9c2f6b1e-5d3a-4e8b-a7f0-1b2c3d4e5f60",
     "sku": "SKU-2",
     "from_bin": "WH1-RSV-A07-03-02-B",
     "to_bin": "WH1-FWD-A01-01-02-A",
     "quantity": 6,
     "priority": "URGENT"
   }
   ```

   This is exactly the shape fulfillment-execution's `WorkReleased` consumer
   decodes (its `WorkReleasedReplenishment`, ADR 0050); a golden exact-JSON test
   pins the bytes and a consumer-compatibility test decodes them through a
   field-for-field copy of that decoder. REPLENISH work never carries
   `relocation`, and the transfer fields (`work_kind`, `transfer_ref`, `site_id`,
   `sku`, `quantity`) are NOT emitted for it. Every RELOCATE, transfer and order
   `WorkReleased` payload is byte-identical to before.

7. **Cancellation: the same as a superseded RELOCATE move, and no more.**
   `ReplenishmentCancelled` for a unit that is still `Pending` cancels it
   (`Pending → Cancelled`, the ADR-0040 state) and withdraws its pool entry, in
   one atomic scope. Released or completed work runs to completion (it was
   already handed to fulfillment-execution), and a cancellation for a request
   this context never planned is a no-op. No event is published: a Pending unit
   was never released, so no downstream service holds work for it.

8. **Persistence.** Migration `0015` is additive: four `NOT NULL DEFAULT ''`
   columns on `work_units` (`replenishment_request_id`, `replenishment_from_bin`,
   `replenishment_to_bin`, `replenishment_priority`); the sku and the quantity are
   the unit's existing columns. The memory adapter holds the same value; the
   Postgres integration test pins the round trip.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `REPLENISHMENT_MODE` | `permissive` | `kafka` runs the consumer; `permissive` (or unset) consumes nothing; anything else is a boot error |
| `REPLENISHMENT_CONSUMER_GROUP` | *(unset)* | STABLE consumer group shared by every replica; required (boot error if unset or blank) when the mode is `kafka`; also requires `KAFKA_BROKERS` and `DATABASE_URL` |
| `REPLENISHMENT_CPT_HORIZON` | `8h` | CPT of a NORMAL request = `requested_at` + this Go duration |
| `REPLENISHMENT_URGENT_CPT_HORIZON` | `1h` | CPT of an URGENT request = `requested_at` + this Go duration; capped at the normal horizon |

Text for the orchestrator to roll into `.claude/rules/integration-events.md`
(this change could not edit `.claude/**`):

> Separately, with `REPLENISHMENT_MODE=kafka` (ADR-0042),
> `internal/adapters/inbound/kafka/replenishment_consumer.go` reads
> `warehouse.pick-face-replenishment.events` under its own STABLE group
> `REPLENISHMENT_CONSUMER_GROUP` (unset = boot error; so is a missing
> `KAFKA_BROKERS` or a missing `DATABASE_URL`), acts only on
> `com.warehouse.wms.pick-face-replenishment.replenishment.ReplenishmentRequested`
> (`ApplyReplenishmentRequested`: ONE `REPLENISH` work unit per request id, id
> `replenish-<request_id>`) and `...ReplenishmentCancelled`
> (`ApplyReplenishmentCancelled`: cancels the unit only while Pending), and
> ignores `ReplenishmentCompleted` and every other type. CloudEvents `id` claim
> + effect in one UnitOfWork; invalid CloudEvents are WARN-skipped; poison goes to
> `warehouse.pick-face-replenishment.events.dlq`; the offset is committed after
> success. Default `REPLENISHMENT_MODE=permissive` consumes nothing.
> `REPLENISHMENT_CPT_HORIZON` (8h) / `REPLENISHMENT_URGENT_CPT_HORIZON` (1h) set
> the CPT of NORMAL / URGENT work.

## Consequences

- REPLENISH work is staffed, capacity-planned and measured like every other
  path: labour plans, WIP limits, release telemetry and analytics counters apply
  unchanged once the `REPLENISH` path exists in the catalogue.
- The work-unit read side shows a REPLENISH unit with `pathId: REPLENISH`,
  `reference = request_id` and `state`; the OpenAPI `WorkUnit` does not enumerate
  work kinds or paths, so it is unchanged.
- New envs above; no chart value is added in this change because the mode
  defaults to `permissive`.
- warehouse-infra: the `REPLENISH` path must be in process-path-management's
  catalogue (infra config, outside this change), the ensure-topics entry for
  `warehouse.pick-face-replenishment.events` is pick-face-replenishment's own, and
  a deployment opts in by setting `REPLENISHMENT_MODE=kafka` and
  `REPLENISHMENT_CONSUMER_GROUP`.

## Not done / limitations

- **A cancellation that finds no unit is lost.** `ReplenishmentCancelled` for a
  request id this context has not planned is a no-op and is not remembered. The
  topic is keyed by request id, so within one partition a cancellation cannot
  overtake its request; across a consumer-group reset or a replay with the
  processed-event marks pruned ([ADR-0032](./0032-housekeeping-retention-sweeper.md))
  the request could be planned again.
- **Released work cannot be recalled.** As in ADR-0040, there is no
  `WorkUnitCancelled` event and no recall of a unit already released to
  fulfillment-execution; it runs to completion.
- **Completion is not consumed here.** `ReplenishmentCompleted` is ignored: the
  unit completes through fulfillment-execution's `TaskCompleted`, as for every
  other kind.
- `policy_min` and `policy_max` are not read; the REST work-unit read model does
  not expose the replenishment value (unlike `relocation`).

## Alternatives considered

- **A separate replenishment aggregate** — rejected for the ADR-0033 reason: a
  replenishment is the same releasable unit of work with extra descriptive
  context.
- **A priority field on the WorkUnit** — rejected: every pool reader and the
  release ordering would have to learn it. The CPT already orders every queue, and
  the shorter URGENT horizon puts those requests in front without a new concept.
- **Sharing `KAFKA_CONSUMER_GROUP`** — rejected: the consumer is opt-in and its
  effect is shared state in Postgres, so, like the ProductClassified consumer, it
  has its own stable group and its own mode.
