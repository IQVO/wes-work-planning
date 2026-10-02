---
id: 0029-work-pool-optimistic-concurrency
title: ADR-0029 — Optimistic concurrency for the WorkPool aggregate
sidebar_label: 0029 · WorkPool optimistic concurrency
sidebar_position: 29
description: WorkPoolRepo.Save is guarded by a version column; Enqueue, Release and RecordCompletion re-load and retry on a lost race, and release heals entries a lost update left behind. (Accepted)
---

# ADR-0029 — Optimistic concurrency for the WorkPool aggregate

## Status

**Accepted** (2026-10-01).

## Context

A `WorkPool` is one aggregate per process path. It holds every work unit's
entry (`pending` / `released` / `completed`) and enforces the WIP limit.
`WorkPoolRepo.Save` persisted it by rewriting the whole pool: upsert the
`work_pools` row, `DELETE` every entry, and re-insert them all.

Three use cases read-modify-write the same pool concurrently:

- `EnqueueWorkUnit`, on every `OrderAllocated` line consumed from Kafka.
- `ReleaseNextWork`, on every `POST /paths/{pathId}/release`.
- `RecordCompletion`, on every `TaskCompleted` consumed from Kafka.

With no concurrency control, whichever save committed last silently
reverted every change made since it loaded the pool (a classic lost
update).

This was observed live in the warehouse-day simulation (e2e-tests
`cmd/warehouse-day`):

- Work units were `Completed` in `work_units` but still `pending` in
  `work_pool_entries`.
- `ReleaseNextWork` picked the earliest-CPT pending entry, found its unit
  already released, and failed with `409 work-unit-already-released`. It did
  this forever, so the path was wedged with 223 units stuck behind one
  stale entry.
- `RecordCompletion` also swallowed `pool.Complete`'s `ErrNotReleased` for
  such an entry, so the entry could never heal itself.

This bug was not caught by the earlier soak fix that made completion free
the pool's WIP slot. That fix's tests run single-threaded, where a lost
update cannot happen.

## Decision

1. **Version column.** `work_pools.version BIGINT NOT NULL DEFAULT 1`
   (migration 0007). The aggregate carries the version it was loaded at.
   - The first save inserts with `ON CONFLICT DO NOTHING`.
   - Later saves run `UPDATE … SET version = version + 1 WHERE path_id = $1 AND version = $loaded`.
   - Zero rows affected returns `ports.ErrConcurrentModification` before any
     entry row is touched, so the caller's transaction rolls back.
2. **Retry from a fresh read.** Each of the three use cases wraps "load the
   pool, change it, save it" in `retryOnPoolConflict`: up to 12 attempts,
   with full-jitter exponential backoff from 2ms capped at 100ms.
   - Every attempt re-loads the pool, so no write is ever applied over a
     newer pool.
   - The pool is saved first inside the unit of work, so a lost race fails
     before anything else is written.
   - Exhausting the budget surfaces the conflict: a 409 over HTTP, or a
     retried message on Kafka (ADR-0028's at-least-once handling).
3. **The WorkUnit aggregate is the source of truth.**
   `WorkPool.Reconcile(id, released, completed)` moves an entry forward to
   match its unit and never moves it backwards.
   - `ReleaseNextWork` heals a stale `pending` entry whose unit already moved
     on, then continues to the next candidate instead of failing.
   - `RecordCompletion` reconciles the entry to `completed` rather than
     calling `Complete`, which rejected an entry that missed its release
     transition.
4. **Faithful rehydration.** Repositories rebuild entries with
   `WorkPool.RestoreEntry`, which reproduces the stored state exactly.
   Before, rehydration replayed `Enqueue` + `Release`, which re-checked the
   WIP limit and could reject a pool whose limit had since been lowered.
5. **In-memory adapters behave like the database.** They store and return
   copies, and enforce the same version check. Before, they handed every
   caller the same pointer, which made concurrent tests racy and hid the
   lost update entirely.

## Consequences

- Concurrent enqueue, release and completion on one path is linearizable
  per save. A unit is released exactly once, and every completion frees its
  slot.
- Pools wedged by the old bug heal on the next release; no data migration
  is needed.
- A pool is still a single hot row per path. That is fine at current
  throughput. Splitting entries into their own rows (claim-style
  compare-and-set per entry) is the next step if contention ever exhausts
  the retry budget.

## Verification

- `work_pool_concurrency_test.go`:
  - 40 concurrent enqueues against 6 concurrent releasers on one path:
    every unit released exactly once, and WIP equals the number of units.
  - 30 concurrent completions: WIP ends at 0.
  - A stale save is rejected.
  - All of these fail on the unversioned save (double releases, lost
    completions), and are clean under `-race` across 20 runs.
- `TestReleaseNextWork_StalePendingEntryIsHealedAndSkipped`: the wedge
  scenario.
- `work_pool_version_integration_test.go` (real Postgres): 10 writers save
  the same loaded pool concurrently; exactly one wins and nine get
  `ErrConcurrentModification`, and re-creating an existing pool is a
  conflict.
