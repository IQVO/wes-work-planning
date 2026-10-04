---
id: 0032-housekeeping-retention-sweeper
slug: /adr/0032-housekeeping-retention-sweeper
title: 0032. Retention sweeper for idempotency_keys and outbox_events
sidebar_label: 0032. Retention sweeper
description: ADR 0032 — an in-process, env-configurable housekeeper deletes expired idempotency_keys (default 24h) and long-published outbox_events (default 7d), batched, never touching an unpublished outbox row.
---

# 0032. Retention sweeper for `idempotency_keys` and `outbox_events`

## Status

Accepted. Closes the "known follow-up, explicitly deferred" in
[ADR-0022](./0022-idempotency-key-middleware.md) (no TTL job for
`idempotency_keys`) and the unbounded-growth consequence of
[ADR-0014](./0014-transactional-outbox.md) (published `outbox_events` rows were
kept forever).

## Context

Both tables are append-only bookkeeping:

- `idempotency_keys` gets one row per `Idempotency-Key` ever seen on
  `POST /paths/{pathId}/work-units`. A key only needs to outlive the client's
  retry window; `idx_idempotency_keys_created_at` was created for exactly this
  cleanup.
- `outbox_events` gets two rows per domain event (integration + analytics
  topic). Once the relay has set `published_at`, the row is only useful for
  forensics.

Neither was ever deleted, so both grow without bound.

## Decision

1. **A `postgres.Housekeeper` runs in the `cmd/wes` process** next to the outbox
   relay, whenever Postgres is configured. It sweeps once at startup and then
   every `HOUSEKEEPING_INTERVAL`.
2. **Rules.**
   - `idempotency_keys`: delete rows with `created_at` older than
     `IDEMPOTENCY_KEY_TTL`. A committed row always has its outcome populated
     (ADR-0022 §4), so an old row is never "in progress".
   - `outbox_events`: delete rows with `published_at IS NOT NULL` and
     `published_at` older than `OUTBOX_RETENTION`. **An unpublished row is never
     deleted, however old** — it is an undelivered event.
3. **Configuration** (read in `cmd/wes/main.go`; all optional; `0` disables that
   table's sweep; unset/invalid/negative uses the default):

   | Env var | Default | Meaning |
   |---|---|---|
   | `HOUSEKEEPING_INTERVAL` | `1h` | time between sweeps |
   | `IDEMPOTENCY_KEY_TTL` | `24h` | how long an Idempotency-Key is kept |
   | `OUTBOX_RETENTION` | `168h` (7d) | how long a *published* outbox row is kept |

4. **Batched, idempotent deletes.** Each `DELETE` removes at most 1000 rows
   (`WHERE pk IN (SELECT pk … LIMIT n)`) and loops until a batch comes back
   short, so a large backlog never holds one long lock. Because the statements
   are idempotent, replicas sweeping concurrently is harmless — no leader
   election.
5. **Shutdown.** The sweeper's context is cancelled and awaited before `run`
   returns, so the pgx pool is never closed under a running sweep.

## Consequences

- Both tables stay bounded; `idx_idempotency_keys_created_at` is finally used.
- An `Idempotency-Key` retried *after* the TTL is treated as a fresh request.
  24 h is far beyond any realistic client retry window, and the ADR-0022
  guarantee is explicitly "within the retention window".
- Published outbox rows older than the retention can no longer be inspected;
  raise `OUTBOX_RETENTION` if forensics need longer, or set it to `0` to keep
  everything.
- Verified with testcontainers Postgres
  (`TestHousekeeper_SweepsExpiredRowsAndNeverUnpublishedOutbox`, including the
  batch loop and that an ancient unpublished row survives).
