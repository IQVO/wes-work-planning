---
id: 0028-processed-event-mark-atomic-with-handling
title: ADR-0028 — The processed-event mark commits atomically with the handling it guards
sidebar_label: 0028 · Processed-event mark atomic with handling
sidebar_position: 28
description: Every inbound integration-event use case records the CloudEvents id in processed_events inside the SAME UnitOfWork as its effect, so a failed attempt leaves no mark and the consumer's retry/DLQ path actually runs. A TaskCompleted for a work unit this context never planned is an explicit, logged, processed skip, not a DLQ'd failure. (Accepted)
---

# ADR-0028 — The processed-event mark commits atomically with the handling it guards

## Status

Accepted — implemented in the same change that introduced this record.
Refines the idempotency rule of [ADR-0004](./0004-kafka-integration-events.md)
and the consumer retry/DLQ of
[ADR-0023](./0023-resilience-circuit-breakers-retry-dlq-shutdown.md); builds on
the UnitOfWork of [ADR-0014](./0014-transactional-outbox.md).

## Context

### The bug: a retried failure was reported as success

The inbound consumer (`internal/adapters/inbound/kafka/consumer.go`) wraps each
handler in `handleWithRetry` (up to `maxHandlerAttempts = 3`) and dead-letters
the message once every attempt has failed (ADR-0023). Every handler deduplicated
redeliveries through `ProcessedEventRepo.TryMarkProcessed`, but it called it
**first, as a separate, already-committed write**, outside any UnitOfWork, and
only then ran the effect:

| attempt | what happened |
|---|---|
| 1 | `INSERT INTO processed_events` commits on the pool. `RecordCompletion` / `EnqueueWorkUnit` then fails (transient Postgres blip, serialization conflict, a pool `Save` error, or a genuine domain error). |
| 2 | `TryMarkProcessed` reports `alreadyProcessed`; the handler returns `nil`. |
| — | `handleWithRetry` counts that `nil` as success. The DLQ is never reached and the offset is committed. **The event is silently lost.** |

The consequences per path:

- **`TaskCompleted` (fulfillment-execution)**: the `WorkUnit` never becomes
  `Completed` and its pool's WIP slot is never freed. That is exactly the
  failure mode the e2e `soak_backlog_ramp` run already found once (see
  `RecordCompletion`'s doc comment): a release-fed pool's WIP only ever ratchets
  up until `wipLimit` is reached and every `POST /paths/{pathId}/release` 409s
  for the rest of the shift, with zero Kafka lag to show for it. That run's root
  cause was fixed by `WorkPool.Complete`; this bug made the same wedge reachable
  again from **any** transient error.
- **`OrderAllocated` / `OrderPartiallyAllocated` (order-management)**: the order
  lines are never enqueued, so the order is released upstream but never worked.
- **`ShiftPlanCommitted` / `StockReserved` / `ReservationRevoked`**
  (`ObserveLaborPlan`, `ObserveInventoryChange`): these use cases already owned
  their own idempotency check, but with the **same** mark-first shape and no
  UnitOfWork wired, so a failed `Save` / `ApplyDelta` dropped the projection
  update the same way. They had the bug too.

### A related behaviour the bug was hiding

fulfillment-execution's `TaskCompleted` carries `work_unit_id` = the task's
`orderRef`. For PICK tasks released by this service that is a real
`WorkUnitId`. For PACK tasks fulfillment-execution creates itself during rebin
consolidation (its ADR-0016) it is the **order id**, which is not a WES work
unit, and SLAM tasks can differ too. `RecordCompletion` returns
`ports.ErrNotFound` for those. That "worked" only because the bug above
swallowed it on the retry. Fixing the bug alone would send every pack
completion to the DLQ, which is wrong: a completion for a work unit this context
never planned is a legitimate fact on a shared topic, not a poison message.

## Decision

### 1. Idempotency lives in the application layer, inside the use case's UnitOfWork

One application-layer primitive, `onceAtomically`
(`internal/application/usecases/apply_integration_event.go`), is the
idempotent-consumer step for **every** inbound integration event. Inside one
`atomically(ctx, uow, …)` scope it:

1. calls `ProcessedEventRepo.TryMarkProcessed` (the Postgres adapter resolves
   the scope's transaction through `querierFrom(ctx, pool)`);
2. returns `alreadyProcessed` without applying anything for a redelivery;
3. otherwise runs the effect. Any error rolls the whole scope back, **mark
   included**.

The mark therefore commits if and only if the effect commits. A failed attempt
leaves nothing behind, attempt 2 really re-runs the use case, and a failure that
never heals exhausts the retry budget and reaches `<topic>.dlq`. The nested use
cases (`RecordCompletion`, `EnqueueWorkUnit`) open their own scope with the same
`UnitOfWork`, which joins the outer transaction (`UnitOfWork.Execute` /
`beginOrJoin`), so the `processed_events` row, the `work_units` row, the
`work_pool_entries` rows and the outbox rows are one transaction.

This follows the pattern `ObserveLaborPlan` / `ObserveInventoryChange` already
established (idempotency inside the use case, never in the adapter) and fixes it
there too. Two new inbound-event use cases replace the logic that used to live
in the Kafka adapter:

- `ApplyTaskCompleted` wraps `RecordCompletion` and returns a
  `TaskCompletedOutcome` (`Applied`, `AlreadyProcessed`, `UnknownWorkUnit`).
- `ApplyOrderAllocated` wraps `EnqueueWorkUnit`, validates every line's
  `path_id` against the process-path catalogue **before** enqueuing any line,
  and keeps the deterministic `{order_id}-line-{line_no}` id and the benign
  `release.ErrDuplicateEntry` no-op.

The Kafka adapter no longer holds a `ProcessedEventRepo` at all. It only decodes
and dispatches. Opening the UnitOfWork in the consumer instead was rejected:
inbound adapters must not own transactions or business rules
(`.claude/rules/architecture.md`), and it would have left the two existing
projector use cases with their own diverging copy of the check.

### 2. Without a UnitOfWork the mark is compensated, not left behind

The in-memory wiring (`DATABASE_URL` unset) has no transaction to roll back.
The in-memory `ProcessedEventRepo` therefore implements an optional port,
`ports.ProcessedEventReleaser`. When `uow == nil` and the effect fails,
`onceAtomically` releases the mark it just made. A release failure is joined
onto the original error, never swallowed. The Postgres repo does not implement
it and never needs to.

### 3. `TaskCompleted` for an unknown work unit is a logged, processed skip

`ApplyTaskCompleted` converts `ports.ErrNotFound` from `RecordCompletion`
(and **only** that error) into `TaskCompletedUnknownWorkUnit`. The scope
commits, so the event **is** marked processed and redelivery stays cheap. The
consumer logs it at INFO with `event_id`, `work_unit_id`, `task_id` and
`task_type` (`task_type` is now read from fulfillment-execution's payload, where
it is already published; it is used for logging only) and returns `nil`, so it
is neither retried nor dead-lettered. Every other error, including domain errors
such as `workunit.ErrAlreadyCompleted` under a new event id or
`pathcatalog.ErrUnknownPath`, propagates, commits nothing, is retried, and is
dead-lettered once retries are exhausted.

### 4. The published contract is unchanged

No channel, message, payload or `type` changes. `apis/asyncapi.yaml`'s
consumer-semantics prose and the narrative event docs now describe the atomic
mark and the unknown-work-unit skip.

## Consequences

- **Positive**: no inbound event can be lost by a transient failure. It either
  applies (mark included) or reaches the DLQ unmarked, so a later manual replay
  from the DLQ is applied rather than silently skipped. The WIP ratchet from
  the soak run cannot recur through a swallowed completion.
- **Positive**: pack/slam completions on the shared fulfillment topic stop being
  accidental successes and become explicit, observable skips.
- **Cost**: each inbound event is now one transaction (mark + effect) where it
  used to be two independent writes. That is the same cost the REST path already
  pays under ADR-0014.
- **Rule for future consumers**: a new inbound integration event gets an
  application-layer use case that calls `onceAtomically` and is wired
  `.WithUnitOfWork(repos.uow)` in `cmd/wes`. Never call `TryMarkProcessed` from
  an adapter, and never outside the effect's scope.

## Verification

- Unit (`internal/application/usecases/inbound_event_atomicity_test.go`): every
  inbound use case under both wirings (no UnitOfWork and a staging fake that
  only commits the mark if the scope succeeds) proves a failed attempt is
  retried and applied, the mark is never committed for a failure, and a
  non-`ErrNotFound` error is never converted into a skip.
- Consumer (`internal/adapters/inbound/kafka/consumer_retry_test.go`): the real
  `dispatch` → `handleWithRetry` → `dlqPublish` path with a fake DLQ writer
  covers the transient failure (healed, unit `Completed`, no DLQ), the
  persistent failure (exactly `maxHandlerAttempts` real attempts, then DLQ), the
  unknown-work-unit skip (no retry, no DLQ, INFO log, marked processed), and
  the same retry/DLQ contract for `OrderAllocated`.
- Integration (`consumer_atomicity_integration_test.go`, `-tags=integration`):
  Postgres **and** Kafka via testcontainers, with the real postgres UnitOfWork
  and repos. Failures are injected on the in-transaction `WorkPool` save. It
  asserts the healed completion frees the WIP slot, the healed `OrderAllocated`
  enqueues every line, the PACK completion is a processed skip, and the
  persistent failure is dead-lettered with **no** `processed_events` row.
  Reverting `onceAtomically` to the old mark-first shape makes this test and
  12 unit tests fail.
