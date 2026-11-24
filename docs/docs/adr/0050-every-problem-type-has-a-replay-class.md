---
id: 0050-every-problem-type-has-a-replay-class
slug: /adr/0050-every-problem-type-has-a-replay-class
title: "0050. Every problem type has a replay class; a retryable refusal is not stored"
sidebar_label: "50. Replay classes"
sidebar_position: 50
description: "ADR 0050: every RFC 7807 problem type this service can answer declares exactly one idempotency replay class (FINAL, OUTCOME, RETRYABLE) on its catalogue entry. A retried request is answered from the idempotency store only when the first answer is final for that request; a RETRYABLE refusal (concurrent-modification, wip-limit-reached, work-pool-empty, the not-founds of caller- or event-supplied ids, a not-yet-known CASE pack level, a 5xx) rolls the whole request transaction back, key row included, so the same key executes again."
---

# 0050. Every problem type has a replay class; a retryable refusal is not stored

Date: 2026-11-20

## Status

Accepted (2026-11-20). Amends [ADR 0022](./0022-idempotency-key-middleware.md) (the
transactional Idempotency-Key middleware) and the guideline contract of
[ADR 0047](./0047-fleet-rest-api-guideline.md). Implements the fleet decision of
2026-11-18 (`warehouse-harness-template` `.claude/rules/fleet/idempotency-and-outbox.md`,
standard: `docs/idempotency-replay-classes.md`). Wire slugs, titles and statuses are
unchanged; only which refusals are stored changes.

## Context

ADR 0022 stores and replays every answer below 500 — its deliberate v1 simplification —
so a 4xx refusal is replayed verbatim for the 24-hour window of the key. That is wrong
for refusals whose cause is state that can change: a `409 concurrent-modification` (the
optimistic-lock loser after `retryOnPoolConflict`'s budget is spent), a `409
wip-limit-reached` (the WIP drains as completions land), a `409 work-pool-empty` (work
arrives with the next enqueue or integration event), a `404 not-found` for an id the
CALLER chose (the Kafka consumer creates work units from order-management events,
`internal/adapters/inbound/kafka/consumer.go`), a `422 no-active-case-level` (the SKU's
CASE pack level is a product-master fact that arrives by event, ADR 0045). A client
that did the right thing — waited for the state to change and retried the same key —
was answered with the stale refusal pinned by the key row for the window.

The fleet decision (owner, 2026-11-18): a retried request is answered from the store
only when the first answer is FINAL for that request. Each problem type gets exactly one
class, declared on its catalogue entry:

- **FINAL** — the answer is a function of the request alone. Stored and replayed.
- **OUTCOME** — the command COMMITTED state and then answered an error that must be
  remembered. Stored, and its writes commit with the stored answer.
- **RETRYABLE** — nothing committed and the answer depends on state that can change.
  Not stored: the request transaction rolls back, key row included, and a retry
  executes again.

2xx is stored; 5xx and panics are never stored; a replay carries
`Idempotency-Replayed: true`. The middleware reads the class and holds no list of slugs
and no status ranges beyond 2xx and 5xx. A problem type without a class fails a unit
test (`TestEveryProblemTypeDeclaresAReplayClass`); at runtime a refusal that declares no
class is released like RETRYABLE — the safe fallback: a released refusal costs one more
execution, a wrongly stored one pins a stale refusal for the window.

Releasing a RETRYABLE key means only that the same key may be tried again by whoever
sent it: a different body under the same key afterwards is a NEW request, not
`idempotency-key-reused`. No consumer-side or client-side retry behaviour is added by
this ADR.

## The store design this rides on (ADR 0022, unchanged)

Design A, the savepoint design: the middleware opens the request's own transaction,
inserts the `idempotency_keys` row with `ON CONFLICT (key) DO NOTHING`, runs the handler
behind `SAVEPOINT handler_run` with the transaction bound into the request context
(`internal/pgtx`; the repositories join it), then updates the row with the captured
answer and commits. Releasing a RETRYABLE key is exactly the rollback the 5xx branch
already did: `tx.Rollback` discards the key row AND the handler's writes. An OUTCOME
refusal needs no new mechanism either — in this store a 4xx whose savepoint released
cleanly already commits its writes with the key row — but the failed-release path (a
statement died inside the handler and the savepoint had to be rolled back) refuses to
store an OUTCOME whose writes are gone: the request fails with a 500 so the same key
retries the real work.

What a concurrent duplicate sees when the first request ends RETRYABLE: the second
request's `INSERT ... ON CONFLICT` blocks on the first transaction's key row until it
resolves; the rollback removes the row, so the blocked insert SUCCEEDS and the duplicate
executes the work fresh (no replay header). It never observes the released refusal.

## Decision

Every problem type of the service — the `problemCatalog` entries of
`internal/adapters/inbound/http/errors.go` plus the standalone problems written through
`writeProblem` directly (the readiness 503, the reports reader's own answers) — declares exactly one class, on its catalogue entry.
GET and PUT routes take no `Idempotency-Key`; their entries are classified anyway so no
problem type of the package is unclassified, and their classes are nominal.

| Problem type | Status | Class | Why |
|---|---|---|---|
| `malformed-request-body` | 400 | FINAL | the body alone (decodeJSON, rejectNUL runs before the middleware) |
| `reference-required` | 400 | FINAL | a missing query parameter (a GET: never behind the middleware) |
| `idempotency-key-required` | 400 | FINAL | answered by the middleware before the store is touched |
| `idempotency-key-invalid` | 400 | FINAL | the header's own format, before the store |
| `unknown-query-parameter` | 400 | FINAL | the query alone (a GET: never stored) |
| `invalid-limit` | 400 | FINAL | the paging query alone (a GET: never stored) |
| `invalid-cursor` | 400 | FINAL | the paging query alone (a GET: never stored) |
| `idempotency-key-reused` | 409 | FINAL | the store's own answer: no handler ran, no new row |
| `not-found` | 404 | RETRYABLE | caller/event-supplied ids (the K1 consumer creates units from order-management events); the slug is shared with the router's own NUL-path 404 — mixed causes |
| `concurrent-modification` | 409 | RETRYABLE | the optimistic-lock race after `retryOnPoolConflict`'s budget; nothing of this request was written |
| `wip-limit-reached` | 409 | RETRYABLE | the WIP drains as completions land; raised before any write of this request |
| `work-pool-entry-already-released` | 409 | FINAL | a pool entry never un-releases (`Release`/`ReleaseNext` are at-most-once; `reconciledState` is a forward-only merge) — flipped from RETRYABLE on 2026-11-23 (addendum below) |
| `work-unit-already-enqueued` | 409 | FINAL | the pool never removes a released or completed entry (`Withdraw` refuses both), and the duplicate is the caller-supplied id already present — flipped from RETRYABLE on 2026-11-23 |
| `work-pool-empty` | 409 | RETRYABLE | work arrives: a later enqueue or an integration event |
| `work-unit-already-released` | 409 | FINAL | `WorkUnit.Release` is one-way and a released unit never returns to a releasable state — flipped from RETRYABLE on 2026-11-23 |
| `work-unit-already-completed` | 409 | FINAL | `WorkUnit.Complete` is terminal — flipped from RETRYABLE on 2026-11-23 |
| `work-unit-not-released` | 409 | RETRYABLE | the release happens later (an operator or a release request): state not reached yet |
| `work-unit-cancelled` | 409 | FINAL | `WorkUnit.Cancel` is terminal and blocks every later transition — flipped from RETRYABLE on 2026-11-23 |
| `invalid-quantity` | 400 | FINAL | a request field |
| `invalid-path-id` | 400 | FINAL | a request field's format |
| `unknown-path-id` | 400 | RETRYABLE | the process-path catalogue may be Kafka-fed (ADR 0030): a path declared later is a referenced fact not known yet |
| `work-unit-id-required` | 400 | FINAL | a request field |
| `work-unit-reference-required` | 400 | FINAL | a request field |
| `invalid-line-no` | 400 | FINAL | a request field's range |
| `unknown-pick-unit` | 400 | FINAL | a request field's enum |
| `invalid-pick-plan` | 400 | FINAL | the pick plan's own fields (and the pick/case-break exclusion) |
| `invalid-case-break` | 400 | FINAL | the case break's own fields |
| `invalid-pick-quantity` | 400 | FINAL | a request field |
| `no-active-case-level` | 422 | RETRYABLE | the SKU's pack levels are fed by product-master events (ADR 0045): a referenced fact not known yet |
| `quantity-not-case-multiple` | 422 | RETRYABLE | computed against the event-fed active CASE pack level |
| `work-pool-entry-not-found` | 400 | RETRYABLE | the entry may arrive later (an enqueue or an integration event) |
| `invalid-wip-limit` | 400 | FINAL | the ConfigurePool body (a PUT: never behind the middleware) |
| `unknown-feed-mode` | 400 | FINAL | the ConfigurePool body's enum (a PUT: never behind the middleware) |
| `invalid-report-query` | 400 | FINAL | the reports query alone (a read-only binary, never behind the middleware) |
| `dependency-unavailable` | 503 | RETRYABLE | a 5xx is never stored |
| `report-store-error` | 500 | RETRYABLE | a 5xx is never stored |
| `internal-error` | 500 | RETRYABLE | a 5xx is never stored |

`TestTheADRClassTableMatchesTheCatalogue` holds this table equal to the catalogue.

### Judgement calls (most consequential first)

1. **Terminal-state refusals are RETRYABLE, not FINAL.**
   `work-unit-already-released`, `work-unit-already-completed`, `work-unit-cancelled`,
   `work-pool-entry-already-released` and `work-unit-already-enqueued` can look FINAL:
   a completed unit never un-completes, a cancelled one never un-cancels, a pool entry
   never leaves the pool, so no retry could ever pass. They are classified RETRYABLE
   because the answer is a function of the aggregate's state — written by other
   requests and events — not of the request alone, and the fleet's unsure-rule says the
   cost of a released refusal (one more execution) is preferred over the cost of a
   wrongly pinned one (a stale 409 for 24 hours). Splitting the truly-irreversible ones
   out as FINAL is a recorded follow-up, not part of this PR.
2. **`not-found` and `work-pool-entry-not-found` are RETRYABLE.** This service does not
   mint work unit ids (`workUnitId` is caller-supplied on the POST and event-supplied on
   the K1 choreography), the facts are delivered by other requests and consumers, and
   the `not-found` slug is shared with the router's own NUL-path 404 — mixed causes. A
   404 on the command's own target is FINAL only when the service mints the ids, nothing
   is ever deleted AND the slug is unshared; none of the three holds.
3. **`unknown-path-id` is RETRYABLE.** With `PATH_CATALOGUE_SOURCE=file` the catalogue
   is fixed at boot and the answer is request-stable; with `=kafka` (ADR 0030) a path
   declared later arrives by the replayed topic. The slug mixes both causes, so it is
   RETRYABLE.
4. **No OUTCOME entries.** Every keyed use case validates before its first write;
   nothing commits state and then refuses. The savepoint design supports OUTCOME (a
   4xx's handler writes commit — the store does NOT discard them, so OUTCOME can exist
   here), and the middleware branch is probe-tested
   (`TestOutcomeProbeRefusalCommitsItsWritesWithTheStoredAnswer`), but no problem type
   declares it today.
5. **The middleware's own answers (`idempotency-key-required/invalid/reused`) are
   classified FINAL** even though they are answered outside the store: the catalogue is
   the one place every problem type of the package is classified, and the class
   documents the intent (the request alone / the store's own answer).
6. **The 410-moved routes were FINAL** (`charge-forecast-moved`, `shift-plan-moved`): the
   answer was the same for every body of the gone route (ADR 0049). ADR 0049 stage E
   removed both routes (a POST answers `404`), so both problem types left the catalogue
   and the table above.

## Consequences

- A retry under a key whose first answer was RETRYABLE executes the work again; the
  same key with a DIFFERENT body is a new request, not `idempotency-key-reused`.
- A stored FINAL refusal is unchanged: byte-identical replay for the window, same
  slugs, titles and statuses on the wire.
- `apis/openapi.yaml`'s Idempotency-Key description now says which refusals are
  replayed and which release the key (descriptions only; oasdiff breaking gate green).
- The reference test of `warehouse-harness-template`
  (`templates/idempotency/replay_classes_test.go.tmpl`, rendered as
  `idempotency_replay_classes_integration_test.go` with the wiring in
  `idempotency_replay_classes_wiring_integration_test.go`) proves every entry's class
  against the real middleware over a real Postgres, per class; two real-router proofs
  (`wip-limit-reached`, `concurrent-modification`) drive the same rule through the
  shipped chi router.
- Retention is unchanged and enforced: the housekeeper (ADR 0022, 24h TTL) purges
  expired `idempotency_keys` rows in batches.

## Addendum 2026-11-23: the terminal-state refusals are FINAL

Owner decision of 2026-11-23, resolving judgement call 1. The five terminal-state refusals
(`work-unit-already-released`, `work-unit-already-completed`, `work-unit-cancelled`,
`work-pool-entry-already-released`, `work-unit-already-enqueued`) moved from RETRYABLE to FINAL after code
verification: `WorkUnit.Cancel/Release/Complete` are one-way and a Cancelled or Completed unit blocks every later
transition (`work_unit.go`), `reconciledState` is a forward-only merge, and `WorkPool.Release`/`ReleaseNext` are
at-most-once with `Withdraw` refusing released and completed entries. A retry under the same key can therefore never
pass, so the stored FINAL answer is never wrong, and it spares a doomed re-execution of the command. The unsure-rule
that kept them RETRYABLE no longer applies, because the irreversibility is proven, not guessed.
`work-unit-not-released` stays RETRYABLE: a later release clears it.
