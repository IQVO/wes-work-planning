---
id: 0031-order-allocated-choreography
slug: /adr/0031-order-allocated-choreography
title: 0031. Consume OrderAllocated / OrderPartiallyAllocated by choreography, fire-and-forget
sidebar_label: 0031. OrderAllocated choreography
description: ADR 0031 — work reaches this context when order-management publishes OrderAllocated/OrderPartiallyAllocated; ApplyOrderAllocated enqueues one work unit per line under a deterministic id, atomically with the processed-event mark, validates path ids in the use case, and publishes nothing back.
---

# 0031. Consume `OrderAllocated` / `OrderPartiallyAllocated` by choreography

## Status

Accepted. Records the order-management → work-planning integration that
replaced a synchronous call; implemented before this record was written.

## Context

`order-management` used to call `POST /paths/{pathId}/work-units`
synchronously to release work — a runtime coupling this service's owners
rejected once order-management became its own bounded context, in favour of the
event choreography already used with `inventory-storage` and
`workforce-management`. order-management now publishes `OrderAllocated` and
`OrderPartiallyAllocated` on `warehouse.order-management.events`; each carries
`order_id`, `promise_date` and a `lines` array (`line_no`, `sku`, `path_id`,
`gift_wrap`).

## Decision

1. **Both event types are consumed identically** by the `Consumer`
   (`handleOrderManagementEvent`) and handed to the `ApplyOrderAllocated`
   use case.
2. **One `WorkUnit` per line, with a deterministic id**
   `"{order_id}-line-{line_no}"`, the order id as `reference`, the promise date
   as CPT, and the line's `sku`/`gift_wrap` carried through. The existing
   `EnqueueWorkUnit` use case is reused per line — no second admission path.
3. **Idempotent at two levels.** The CloudEvents `id` is recorded in the
   processed-event table in the *same* transaction as every enqueue
   ([ADR-0028](./0028-processed-event-mark-atomic-with-handling.md)), so a
   redelivery is a no-op and a failure rolls the mark back for retry. Separately,
   `release.ErrDuplicateEntry` for a deterministic id (a later partial-allocation
   event for the same line, or an operator replay under a new event id) is a
   benign skip, never a reason to stall the partition.
4. **Path validation lives in the use case.** `ApplyOrderAllocated` validates
   every line's `path_id` against `ports.PathCatalogue` *before* enqueueing
   anything ([ADR-0012](./0012-process-path-catalogue-validation.md),
   [ADR-0030](./0030-kafka-sourced-path-catalogue.md)); an unrecognized path
   fails the whole message, which is retried and then dead-lettered
   ([ADR-0023](./0023-resilience-circuit-breakers-retry-dlq-shutdown.md)).
5. **Fire-and-forget.** No reply event is published back to order-management.
   The other direction of the relationship is separate: order-management
   consumes this service's `PathCapacityChanged`
   ([ADR-0018](./0018-path-capacity-changed.md)).

## Consequences

- No synchronous coupling: order-management and this service deploy and fail
  independently.
- order-management cannot learn from a reply whether a line was accepted; a
  rejected message surfaces only on this side (DLQ + ERROR log).
- The deterministic id makes `GET /work-units?reference=<order_id>` the natural
  lookup for "what did this service do for order X".
- Changing the id scheme, or adding a reply, is a cross-service contract change.
