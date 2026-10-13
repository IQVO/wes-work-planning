---
id: 0039-outbox-relay-attempt-cap
slug: /adr/0039-outbox-relay-attempt-cap
title: 0039. Outbox relay attempt cap
sidebar_label: 0039. Outbox relay attempt cap
description: ADR 0039 — the outbox relay stops claiming a row after OUTBOX_MAX_ATTEMPTS failed sends (default 20, 0 = unlimited), logs it at ERROR and leaves it in outbox_events for an operator to re-arm, so one poison row cannot stop the relay forever; later rows with the same key overtake it (fleet gap fix D12).
---

# 0039. Outbox relay attempt cap

## Status

Accepted (2026-10-12, fleet gap fix D12, the same convention as
inventory-storage's reference implementation of that fix). Amends the
per-key ordering consequence of [ADR-0014](./0014-transactional-outbox.md);
everything else in ADR-0014 (one row per event per topic, written in the use
case's transaction, drained by one in-process relay in `id` order) is
unchanged.

## Context

The relay (`internal/adapters/outbound/postgres/outbox_relay.go`, run only by
`cmd/wes`) sends claimed rows one at a time in `id` order and stops a pass at
the first failed send, recording `attempts` and `last_error` on that row. The
next pass starts at the same row again. That keeps a later event for the same
aggregate from overtaking a failed one, and it is right for a broker outage,
which ends.

It is wrong for a **poison row**: one the broker will never accept (a message
over the broker's size limit, a topic that cannot be created, a value the
sink rejects every time). That row fails every pass, every pass stops at it,
and every row behind it, for every key and both topics
(`warehouse.work-planning.events` and `warehouse.wes.analytics`), waits
forever. `WorkReleased` stops reaching fulfillment-execution and
`PathCapacityChanged` stops reaching order-management. The process stays
healthy; only `wes.outbox.lag_seconds` grows.

The relay had no cap at all, so the fix needs the option and the wiring.

## Decision

1. **The relay gets an attempt cap.** `postgres.WithMaxAttempts(n)` excludes a
   row whose `attempts` has reached `n` from the claim query
   (`AND ($2 <= 0 OR attempts < $2)`). `0`, the option's zero value, means
   unlimited: the behaviour before this record.
2. **`cmd/wes` always passes it.** It reads `OUTBOX_MAX_ATTEMPTS`
   (default `20`, `0` = unlimited; an unparseable or negative value falls back
   to `20`, so a typo cannot remove the cap) and logs the value at boot
   (`outbox relay configured`). The chart exposes it as
   `config.outboxMaxAttempts`. `cmd/mcp` writes outbox rows but runs no relay,
   so it has nothing to cap.
3. **An excluded row is not deleted or moved.** It stays in `outbox_events`
   with `published_at IS NULL`, its `attempts` and its `last_error`. The
   housekeeper ([ADR-0032](./0032-housekeeping-retention-sweeper.md)) deletes
   only published rows, so it never removes one. There is no DLQ topic: a row
   the broker rejects would most likely be rejected on a DLQ topic of the same
   broker, and the table already is the inspectable store.
4. **The failure that reaches the cap is logged once at ERROR**
   (`outbox relay: row reached the attempt cap and is no longer claimed`)
   with `outbox_id`, `topic`, `event_type`, `key`, `attempts`,
   `max_attempts` and the send error, after the attempt is committed.
5. **Re-arming is an operator action**: after fixing the cause,
   `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` makes the row
   claimable on the next pass (runbook, "Re-arm an outbox row that hit the
   attempt cap").
6. **Sensor.** The architecture fitness test `TestOutboxRelaysPassAttemptCap`
   fails when a non-test file under `cmd/` calls `NewOutboxRelay` without a
   `WithMaxAttempts(...)` argument, so the cap is a deliberate setting at the
   composition root and never the unlimited default by omission.

## Consequences

- **Per-key ordering is no longer absolute.** Once a row is excluded, later
  rows overtake it, **including rows with the same key**: a consumer can see
  `WorkUnitCompleted` for a unit whose `WorkReleased` never arrived, or see a
  re-armed row after a newer event for the same aggregate. This is the
  deliberate trade: the cap exists so one poison row cannot stop the whole
  relay, for every key, forever. Consumers already dedupe on the CloudEvents
  `id` and tolerate redelivery (ADR-0014); a re-armed row is a late delivery
  of the same `id`.
- Ordering among rows that are **not** excluded is unchanged: within a pass
  the relay still stops at the first failure, so a transient failure below the
  cap never lets a later row overtake.
- With the defaults (`OUTBOX_RELAY_INTERVAL` 1 s, cap 20) a row that fails
  every send is excluded after roughly 20 s of continuous failure. A broker
  outage longer than that also excludes the rows at the head of the queue, one
  per ~20 s, each logged at ERROR and each needing a re-arm. Raise
  `OUTBOX_MAX_ATTEMPTS` (or set `0`) where long broker outages are expected
  and a stalled relay is preferable to manual re-arming.
- Excluded rows are visible with
  `SELECT id, topic, event_type, key, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= <cap>;`.

## Verification

- `go test -tags=integration ./internal/adapters/outbound/postgres/ -run TestOutboxRelay -count=1`:
  `TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes`,
  `TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain`,
  `TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows` (testcontainers Postgres).
- `go test ./cmd/wes/ -run TestOutboxMaxAttemptsFromEnv`.
- `go test ./internal/architecture/ -run OutboxRelay`.
