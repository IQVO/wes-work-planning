---
id: 0043-taskcompleted-already-completed-skip
slug: /adr/0043-taskcompleted-already-completed-skip
title: 0043. A TaskCompleted for an already-completed work unit is a logged skip
sidebar_label: 0043. Already-completed skip
description: ADR 0043 — a TaskCompleted carrying a NEW CloudEvents id for a work unit that is already completed is a stale fact, classified as TaskCompletedAlreadyCompleted, INFO-logged and marked processed; it is never retried and never dead-lettered. Amends the error enumeration of ADR 0028.
---

# 0043. A TaskCompleted for an already-completed work unit is a logged skip

## Status

Accepted (2026-10-17). Amends the error enumeration of
[ADR 0028](./0028-processed-event-mark-atomic-with-handling.md) §3: the
processed-event mark still commits atomically with the handling; what changes
is which deterministic domain facts count as errors.

## Context

`warehouse.fulfillment.events` is a shared topic consumed by
`ApplyTaskCompleted`. The consumer's `dispatch()` retries any handler error
`maxHandlerAttempts` times and then publishes the raw message to
`warehouse.fulfillment.events.dlq`. That is the right treatment for a poison
message — a payload no retry will ever fix — and the wrong treatment for a
deterministic domain fact that is simply no longer applicable.

Two such facts were already classified as no-op outcomes (ADR 0028):

- `TaskCompletedAlreadyProcessed` — a redelivery of an event whose
  CloudEvents `id` is already marked processed.
- `TaskCompletedUnknownWorkUnit` — the event names a work unit this context
  never planned (e.g. a PACK task fulfillment-execution created itself during
  rebin consolidation, whose `work_unit_id` is the order id).

One hole remained: `RecordCompletion` returns
`workunit.ErrAlreadyCompleted` when a SECOND, DISTINCT task completes a work
unit that is already completed — a different CloudEvents id, so the
processed-event mark does not catch it, and a different work-unit lookup
result than `ports.ErrNotFound`, so the unknown-work-unit classification does
not catch it either. The error propagated, `dispatch()` retried it three
times to no effect (the unit was completed before the first attempt), and
the healthy message was dead-lettered.

This is not hypothetical. On 2026-10-14 two e2e-harness events — each a
second, distinct task completing an already-completed work unit — went
through exactly that path and sit in `warehouse.fulfillment.events.dlq`
today. A work unit cannot be completed twice; the second completion fact
does not and cannot apply. It is a stale fact, not a failure, and it is
deterministic: no retry, ever, will change the outcome.

## Decision

`ApplyTaskCompleted` classifies
`errors.Is(err, workunit.ErrAlreadyCompleted)` from `RecordCompletion` as a
third no-op outcome, `TaskCompletedAlreadyCompleted`, inside the same atomic
scope as the processed-event mark (ADR 0028):

- the handler returns `nil`, so `dispatch()` neither retries nor
  dead-letters the event;
- the event **is** marked processed, so a redelivery of the stale event is
  a cheap `TaskCompletedAlreadyProcessed` no-op that does not re-log;
- one INFO line is logged from the use case (`event_id`, `work_unit_id`),
  mirroring the INFO-logged unknown-work-unit skip; the work unit stays
  `Completed` and nothing else changes.

Every other error — e.g. `workunit.ErrNotReleased` on a unit that was never
released, or `pathcatalog.ErrUnknownPath` — still propagates, commits
nothing, and is retried then dead-lettered once retries are exhausted.
The fix is entirely inside the use case; the consumer's dispatch and
dead-letter machinery is untouched.

The two 2026-10-14 DLQ occurrences do not need to be redriven once this
ships: they are e2e fixtures (distinct tasks completing already-completed
harness work units), their effect is already in place — the unit is
completed — and a replay would now be a logged skip anyway.

## Consequences

- A deterministic domain no-op fact no longer occupies the DLQ, hides real
  poison messages, or burns three retries per delivery.
- The DLQ's meaning sharpens: everything on it is either a transient
  failure that outlived its retries or a genuine poison message.
- The use case now logs directly through `log/slog`'s default logger
  (configured by `slog.SetDefault` in every binary) because the INFO log
  for the sibling skip outcome lives in the consumer, which this change
  deliberately does not touch.
- REST callers are unaffected: `POST /work-units/{id}/complete` still
  surfaces `ErrAlreadyCompleted` as a 409 problem detail; only the inbound
  Kafka path classifies it.

## Alternatives rejected

- **Dead-letter and let a human decide.** The operator cannot apply a
  completion twice either; the redrive is a dead end by construction.
- **Classifying in the consumer (`consumer.go`).** Every classification
  added there has to duplicate the atomic processed-mark scope the use case
  already owns, and the consumer is under parallel change (a new
  pick-face-replenishment consumer); the outcome enum is the use case's
  contract.
- **A metric instead of a log line.** The consumer owns metrics wiring;
  emitting one from the use case would need consumer-adjacent changes, so
  the observable signal is the INFO log line (countable at the log layer).
