---
id: 0033-transfer-work-demand-choreography
slug: /adr/0033-transfer-work-demand-choreography
title: 0033. Consume NIP WorkDemandReleased into transfer-referenced work units
sidebar_label: 0033. Transfer work demand choreography
description: ADR 0033 — network-inventory-planning releases inter-site transfer legs as WorkDemandReleased; ApplyWorkDemandReleased enqueues one transfer-referenced WorkUnit per demand under the deterministic id demand_id, atomically with the processed-event mark, and WorkReleased carries the transfer context as strictly optional additive fields on the same v1 payload.
---

# 0033. Consume NIP `WorkDemandReleased` into transfer-referenced work units

## Status

Accepted.

## Context

`network-inventory-planning` (NIP) plans and approves inter-site inventory
transfers. Executing a transfer needs real warehouse work — picking at the
origin site, dispatching/loading, receiving at the destination — and that
work is exactly what this bounded context exists to plan, pool and release.
NIP therefore publishes `WorkDemandReleased`
(`com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased`,
dataschema `urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1`)
on `warehouse.network-inventory-planning.events`, carrying
`{demand_id, work_kind, transfer_ref, path_id, site_id, cpt, sku, quantity}`,
where `work_kind` is one of `TRANSFER_PICK | TRANSFER_DISPATCH |
TRANSFER_ARRIVAL`.

This is the same integration shape as order-management's
[ADR-0031](./0031-order-allocated-choreography): an upstream context
releases work by event, and this service enqueues it through the existing
`EnqueueWorkUnit` path rather than gaining a second admission path.

## Decision

1. **A fifth consumed topic.** The inbound `Consumer` also reads
   `warehouse.network-inventory-planning.events`
   (`cloudevents.TopicNetworkDemandEvents`), with its own
   `<topic>.dlq` writer, its own `handleNetworkDemandEvent`, and unknown
   `type`s ignored as usual.

2. **`ApplyWorkDemandReleased`, beside `ApplyOrderAllocated`.** The handler
   dispatches to a new use case with the same skeleton: `onceAtomically`
   brackets the processed-event mark and the enqueue in ONE atomic scope
   ([ADR-0028](./0028-processed-event-mark-atomic-with-handling.md)), the
   `path_id` is validated against the process-path catalogue
   ([ADR-0012](./0012-process-path-catalogue-validation.md)) and the
   `work_kind` against the declared enum BEFORE anything is enqueued, and
   the reuse target is the existing `EnqueueWorkUnit` — no new admission
   path, no new aggregate.

3. **Deterministic id: `work_unit_id = demand_id`**, with
   `reference = demand_id` as the generic operator ref (so the existing
   `GET /work-units?reference=` lookup serves transfer demands unchanged).
   Idempotency at two levels, exactly like ADR-0031: the CloudEvents `id`
   mark catches redelivery, and `release.ErrDuplicateEntry` for the
   deterministic id (a DIFFERENT event carrying the same demand) is a
   benign no-op, never a stall.

4. **Transfer metadata is additive on `WorkUnit`, not a new invariant.**
   `transfer_ref`, `work_kind`, `site_id`, `quantity` follow the
   `SetSKU`/`SetGiftWrap` discipline: optional fields with zero-value
   defaults, threaded through `EnqueueWorkUnitRequest`, persisted by
   migration `0011` (all `NOT NULL DEFAULT` — every existing row and
   writer keeps working), rehydrated by the Postgres and memory repos
   (`work_kind` validated on read like `state`), and never part of the
   aggregate's own lifecycle rules. `sku` already existed.

5. **`WorkReleased` stays v1 — additive optional fields only.**
   `workReleasedData` gains `work_kind`, `transfer_ref`, `site_id`,
   `quantity`, present ONLY on a transfer-referenced unit
   (`work_kind != ""` is the discriminator). The `type`,
   `dataschema urn:warehouse:wes-work-planning:events:WorkReleased:v1`
   and required `{path_id, work_unit_id, cpt, ref}` are unchanged, so the
   payload of every order-driven unit is byte-identical to before this
   feature existed (pinned by a golden byte-compat test), and consumers
   that ignore unknown fields see no difference at all.

6. **Fail-loud contract.** An unknown `path_id` or `work_kind` is
   deterministic poison: the handler returns an error, the message is
   retried and then dead-lettered — never silently enqueued into a pool
   nothing downstream services (the ADR-0017 rationale, applied to a new
   inbound stream).

7. **Fire-and-forget**, like ADR-0031: no reply event to NIP. The
   published `WorkReleased` (now carrying the transfer context) is the
   only observable signal, and it is the same signal every other
   `EnqueueWorkUnit` caller relies on.

## Consequences

- fulfillment-execution (the `WorkReleased` consumer) can recognize
  transfer tasks by `work_kind` without any lookup, or ignore the fields
  entirely — both are safe.
- The AsyncAPI contract documents the consumed message and the optional
  fields; the catalogue fitness rule is unaffected (the NIP type is
  consumed, not declared by this service).
- A future NIP-side payload change rides the same additive discipline:
  optional fields on the same v1 dataschema, byte-compat preserved for
  every pre-existing unit.

## Alternatives considered

- **A dedicated `TransferWorkUnit` aggregate** — rejected: transfers are
  the same releasable unit of work with extra descriptive context; a
  second aggregate would duplicate the pool, release policy and completion
  loop wholesale.
- **Replying to NIP on enqueue** — rejected for the same fire-and-forget
  reasons as ADR-0031; the outboxed `WorkReleased` is the signal.
- **Bumping WorkReleased to v2** — rejected: additive optional fields on
  v1 are backward compatible by construction; a v2 would force every
  consumer to migrate for nothing.
