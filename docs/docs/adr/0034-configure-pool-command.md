---
id: 0034-configure-pool-command
slug: /adr/0034-configure-pool-command
title: 0034. ConfigurePool — an explicit command to set a path's pool mode and WIP limit
sidebar_label: 0034. ConfigurePool command
description: ADR 0034 — PUT /paths/{pathId}/pool sets a WorkPool's feed mode and WIP limit (creating the pool if absent); lowering a limit below the current WIP never evicts work, releases pause until WIP < limit.
---

# 0034. ConfigurePool — an explicit command to set a path's pool mode and WIP limit

## Status

Accepted. Decision taken 2026-10-06 on the 2026-10-05 audit finding "no code
path creates a flow-fed pool". Builds on
[ADR-0003](./0003-flow-balancing-as-domain-service.md) (release-fed vs flow-fed)
and [ADR-0029](./0029-work-pool-optimistic-concurrency.md) (versioned
`WorkPool`).

## Context

A `WorkPool` has a feed mode (`ReleaseFed`: this service enforces a hard WIP
limit; `FlowFed`: priority only, the limit is merely an alarm) and a WIP limit.
Until now the only code that created a pool was `EnqueueWorkUnit`, which seeds a
missing pool as `ReleaseFed` with a limit of 1000. Nothing could create a
flow-fed pool, and nothing could change a limit, so the flow-fed branch of the
domain was unreachable in production and every path ran with the arbitrary
default.

Operators must be able to configure a path's pool without coupling this context
to process-path-management (which owns the path catalogue, not admission policy).

## Decision

1. **We add an explicit `ConfigurePool` command**, exposed as
   `PUT /paths/{pathId}/pool` with body `{"mode": "ReleaseFed"|"FlowFed",
   "wipLimit": <integer>}`. It sets the pool's mode and WIP limit and **creates
   the pool when none exists**. The response is the pool as configured plus its
   live `wip` and `backlogDepth`.
2. **Validation.** `mode` must be one of the existing `FeedMode` values and
   `wipLimit` must be a positive integer, otherwise `400`
   (`unknown-feed-mode`, `invalid-wip-limit`). `pathId` is validated against the
   process-path catalogue like every other write that can seed an aggregate
   ([ADR-0012](./0012-process-path-catalogue-validation.md)). A rejected command
   changes nothing and creates no pool.
3. **Lowering a limit never evicts or cancels work.** Configuring a limit below
   the pool's current WIP is accepted. No entry changes state; the pool is simply
   saturated, `ReleaseNextWork` answers `409 wip-limit-reached` (the existing
   `WIP >= limit` check) until completions bring WIP strictly below the new
   limit, and `RemainingCapacity` reports `0` (ADR-0018 already clamps at zero).
   **Raising** a limit takes effect on the next release. This is the standard
   WIP semantics; it needs no new release rule.
4. **Idempotent, optimistically concurrent.** `PUT` states the desired
   configuration, so the same body gives the same result, and an identical repeat
   on an existing pool writes nothing (no version bump). The read-modify-write
   runs inside the same `retryOnPoolConflict` as every other pool writer
   (ADR-0029), so it can never overwrite a concurrent enqueue/release/completion
   and vice versa; exhausting the retry budget is the existing
   `409 concurrent-modification`.
5. **Scope.** Only mode and WIP limit are set; the alarm threshold and the pool's
   entries are never touched (a newly created pool takes the existing alarm
   default of 1000). The fallback stays: a path nobody configured still gets
   `ReleaseFed` with limit 1000 on first `EnqueueWorkUnit`.
6. **No event, no auth, no MCP tool.** Configuration is operational state, not a
   business fact other contexts react to, and the fleet CloudEvents standard
   requires no audit event for it, so none is published and the integration
   contract is unchanged. REST/MCP stay unauthenticated (fleet decision, see
   [ADR-0016](./0016-remove-rest-mcp-static-bearer-auth.md)). No MCP tool is added:
   that would need its own decision under the
   [MCP governance charter](../mcp/governance-charter.md).
7. **Contract.** The REST change is purely additive (a new route and two new
   problem types); no existing request, response or event changes.

## Consequences

- Flow-fed pools are now reachable; `RebalanceDecision`'s `ThrottleUpstream`
  branch and the backlog alarm can fire in a running system once an operator
  configures a path `FlowFed`.
- An operator can raise or lower a path's admission ceiling at runtime without a
  deploy and without touching the path catalogue.
- A lowered limit does not make the system "catch up" by cancelling work: the
  overshoot (`wip > wipLimit`) is visible in the response and in telemetry and
  drains only as work completes. This is deliberately conservative and reversible
  — raising the limit again restores throughput at once.
- Switching a pool `ReleaseFed → FlowFed` stops enforcing the limit immediately;
  entries keep their state. Switching back re-enforces it against whatever WIP is
  then outstanding (same pause semantics as lowering).
- Verified by unit tests (domain rule incl. lowering below WIP, use case, HTTP) and
  testcontainers Postgres integration tests (create/update/idempotency, lowering
  below WIP without eviction, configure racing enqueue loses no entry).
- The configuration is not audited as an event. If auditability is later required
  it would be an analytics-topic-only event (a new ADR); nothing here prevents it.
