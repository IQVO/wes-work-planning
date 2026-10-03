---
id: 0022-idempotency-key-middleware
slug: /adr/0022-idempotency-key-middleware
title: 22. Transactional Idempotency-Key middleware for POST /paths/{pathId}/work-units
sidebar_label: 22. Idempotency-Key middleware
description: "ADR 0022 — POST /paths/{pathId}/work-units requires a caller-supplied Idempotency-Key header. A route-scoped middleware begins the outer Postgres transaction, joins it with EnqueueWorkUnit's own UnitOfWork via a new internal/pgtx package, and lets the database's own unique-index lock do request de-duplication with no polling, no timeout, and no in-progress state. Ported from order-management's reference implementation (order-management PR #105 / its ADR 0023)."
---

# 22. Transactional Idempotency-Key middleware for `POST /paths/{pathId}/work-units`

## Status

Accepted — implemented in the same change that introduced this record. This
is a direct port of order-management's reference pattern (order-management
PR #105, its ADR 0023), which that repo's own ADR names as "the reference
pattern the rest of the warehouse-systems fleet copies for their own
resource-creation endpoints."

## Context

This service has four mutating `POST` routes:

- `POST /paths/{pathId}/charge` (`postChargeForecast` → `ReceiveChargeForecast`)
- `POST /paths/{pathId}/plan` (`postShiftPlan` → `CommitShiftPlan`)
- `POST /paths/{pathId}/work-units` (`postWorkUnit` → `EnqueueWorkUnit`)
- `POST /paths/{pathId}/release` (`postRelease` → `ReleaseNextWork`)

(A fifth, `POST /work-units/{id}/complete`, acts on an existing id and is
out of scope by construction — the same class of route order-management's
ADR rules out for `retry-allocation`/`release`.)

Of these, only `POST /paths/{pathId}/work-units` is a genuine
resource-CREATION endpoint whose retry-without-idempotency-key risk matches
order-management's `POST /orders`: `ChargeRepo.Save` and `PlanRepo.Save`
are both `INSERT ... ON CONFLICT (path_id) DO UPDATE` upserts keyed by the
caller-supplied `path_id` — a lost-response retry re-applies the exact same
upsert and converges to the same row, not a duplicate. `postRelease` does
not create anything at all: it applies the release policy to whichever
pending entry the pool currently ranks highest (earliest CPT) and moves
it to `Released` — a retry after a lost response either re-releases the
same now-already-`Released` unit (rejected by `workunit.ErrAlreadyReleased`,
mapped to `409`) or, worse, releases a *different* unit than the one the
first (successful, response-lost) call actually released, which an
idempotency-key cache would not fix either (a replayed body carries no
`{id}`, there is nothing to compare against a stored request to detect the
"same call" in the first place). `postWorkUnit`, by contrast, creates a
brand-new `work_units` row via `WorkUnitRepo.Save`'s
`INSERT ... ON CONFLICT (id) DO UPDATE` keyed by the CALLER-supplied
`workUnitId` in the request body — a lost-response retry with a
DIFFERENT `workUnitId` (a client that doesn't realize its first call
succeeded and mints a fresh id for "the same" logical unit) produces a
second, duplicate `work_units` row and a second `WorkPool` entry, with no
natural-key upsert to save it. This is exactly order-management's
`POST /orders` problem, and `EnqueueWorkUnit` already wraps its
`WorkUnitRepo.Save` + `WorkPoolRepo.Save` + `EventPublisher.Publish` in
`ports.UnitOfWork`/`atomically()` from the merged outbox rollout — the same
transactional infrastructure this problem needs, already in place, exactly
as order-management's ADR 0023 describes for `ReceiveOrder`.

## Decision

### 1. `Idempotency-Key` header, required on `POST /paths/{pathId}/work-units` only

A request to that route without an `Idempotency-Key` header gets `400
application/problem+json` (`idempotency-key-required`). The other three
mutating `POST` routes are left alone, for the reasons in Context above —
ported unchanged from order-management's v1 scoping decision: require the
header on true resource-creation endpoints rather than making it
optional-but-recommended.

### 2. `idempotency_keys` table (migration `0006_idempotency_keys`)

Identical schema to order-management's migration `0008_idempotency_keys`:

```sql
CREATE TABLE idempotency_keys (
    key              TEXT PRIMARY KEY,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    status_code      INTEGER,
    response_body    BYTEA,
    response_headers JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
```

`request_hash` is `hex(sha256(request body))`. `status_code`,
`response_body`, and `response_headers` start `NULL` and are populated by
exactly one `UPDATE`, in the same transaction that inserted the row,
immediately before that transaction commits (§4).

### 3. `RequireIdempotencyKey` middleware (`internal/adapters/inbound/http/idempotency.go`)

Route-scoped only: `r.With(RequireIdempotencyKey(pool)).Post("/work-units",
h.postWorkUnit)` inside the `/paths/{pathId}` sub-router in `router.go`,
never `r.Use(...)` on the whole router.

Behavior is byte-for-byte identical to order-management's
`RequireIdempotencyKey`: the request body is read fully into memory once,
hashed, and restored via `io.NopCloser(bytes.NewReader(...))`; a
transaction begins directly against the `pgxpool.Pool` (no import of the
`postgres` package needed); `INSERT INTO idempotency_keys (...) VALUES
(...) ON CONFLICT (key) DO NOTHING` runs inside it:

- **1 row inserted** (fresh key): call the real handler with the
  tx-carrying context via an `httptest.ResponseRecorder`, capturing rather
  than writing the response yet (§4).
- **0 rows** (existing key): roll back this empty transaction and fall
  through to a plain `SELECT` (§5 explains why no extra locking is
  needed):
  - `request_hash` mismatch → `422` (`idempotency-key-reused`).
  - `request_hash` match → replay the stored `status_code`/
    `response_headers`/`response_body` VERBATIM, without calling the real
    handler at all.

### 4. The no-null-status-code-ever-committed invariant

Unchanged from order-management's ADR 0023 §4: on the fresh-key path,
after the wrapped handler runs against the recorder, the SAME transaction
that inserted the bare row runs `UPDATE idempotency_keys SET
status_code=$1, response_body=$2, response_headers=$3, completed_at=now()
WHERE key=$4`, then commits — and only after a successful commit does the
middleware copy the recorder's headers/status/body onto the real
`http.ResponseWriter`. A panic is recovered, rolls the transaction back
(so neither the idempotency row nor the `EnqueueWorkUnit` writes it made
ever become visible), and re-panics so chi's `Recoverer` still produces
the normal `500`. A panic's outcome is never cached.

The invariant this buys: a transaction that reads a COMMITTED
`idempotency_keys` row can never observe a NULL `status_code` — there is
no third, "in-progress" state, no client-facing retry-after/409, and no
polling loop or timeout anywhere in this design.

### 5. Concurrency: Postgres' own unique-index lock does the serialization

Unchanged argument from order-management's ADR 0023 §5: two concurrent
requests with the SAME key race on the `INSERT ... ON CONFLICT (key) DO
NOTHING`; Postgres serializes them at the primary-key unique index, so by
the time any transaction observes `rowsAffected() == 0`, the original
inserter has unconditionally resolved (commit or rollback). Combined with
§4's invariant, no polling loop, lock-retry budget, or timeout is needed
anywhere in this code. Proven with a real test, not asserted from theory:
`TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneWorkUnitCreated`
fires five real goroutines at the real router with the same key and body
and asserts, via a direct DB count, that exactly one `work_units` row
exists afterward.

### 6. Reusing, not duplicating, the transaction-join mechanism (`internal/pgtx`)

Before this change, this service's tx-in-context mechanism
(`withTx`/`txFrom`, an unexported `txKey{}`) lived entirely inside
`internal/adapters/outbound/postgres/unit_of_work.go` — same shape as
order-management's pre-idempotency state. The idempotency middleware
lives in `internal/adapters/inbound/http`, and this repo's own
`internal/architecture` fitness tests (`TestHexagonalDependencyRule`)
forbid the inbound HTTP adapter from importing the outbound Postgres
adapter, and vice versa.

The fix, ported unchanged from order-management: extract the bare
key-type-plus-`WithTx`/`TxFrom` pair into a new, tiny, dependency-free
package, `internal/pgtx`, that both adapter packages import.
`postgres.withTx`/`postgres.txFrom` (and therefore `querierFrom`,
`beginOrJoin`, and `UnitOfWork.Execute`) now delegate to
`pgtx.WithTx`/`pgtx.TxFrom` — an internal refactor with no change in
observable behaviour for any existing caller (verified: `go test ./...`
across the whole module, including every other repo, package, and use
case that calls `UnitOfWork.Execute`, is unaffected). The idempotency
middleware calls the exact same `pgtx.WithTx`/`pgtx.TxFrom` functions.

`EnqueueWorkUnit`'s own `atomically()`/`UnitOfWork.Execute` call needed
**zero changes** to pick up the middleware's transaction: `Execute`'s
existing "already inside a transaction? just run `fn(ctx)`" branch
(`if _, ok := txFrom(ctx); ok { return fn(ctx) }`) already does exactly
the right thing once the context it receives carries a `pgtx`-bound
transaction from any source, not just its own — verified with a real
test (`TestIdempotency_FreshKey_CreatesWorkUnitAndRecordsOutcome` and the
replay/concurrency scenarios directly assert both the `idempotency_keys`
row AND the `work_units` row exist/don't-exist exactly as the
atomic-commit argument predicts), not assumed.

`internal/architecture`'s `TestHexagonalDependencyRule` passes unchanged
(`make arch-test`): `internal/pgtx` is imported by both
`internal/adapters/inbound/http` and
`internal/adapters/outbound/postgres`, and neither of those adapter
packages imports the other — the boundary holds.

### 7. Response caching scope: every normal response, including business errors

Unchanged from order-management's ADR 0023 §7: every NORMAL (non-panic)
response is cached, including a `4xx` domain-validation error (e.g. an
empty `reference`, rejected by `workunit.NewWorkUnit` and surfaced as
`400`). A client retrying the exact same key + body deterministically
gets the exact same answer, including a validation error, rather than
re-validating. Proven by
`TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed`.

## Consequences

- `POST /paths/{pathId}/work-units` now requires an `Idempotency-Key`
  header; every existing client of that route needs one on every call.
- The whole request cycle for that route — idempotency bookkeeping, the
  `WorkUnit` Save, the `WorkPool` Save, the outbox insert — is one
  Postgres transaction; a failure anywhere after the idempotency row's
  `INSERT` rolls back the ENTIRE cycle, including the idempotency row
  itself. A retried request after such a failure re-attempts the real
  work from scratch.
- `internal/pgtx` is a new, tiny shared package; every future
  cross-cutting-transaction feature in this service should extend it
  rather than re-invent a parallel tx-in-context mechanism.
- **Known follow-up, explicitly deferred (same as order-management's
  ADR 0023):** no TTL/cleanup job exists yet for old `idempotency_keys`
  rows; `idx_idempotency_keys_created_at` exists so a future scheduled
  job can find old rows without a full table scan. Building that job is
  out of scope here.
- `POST /paths/{pathId}/charge`, `POST /paths/{pathId}/plan`, and
  `POST /paths/{pathId}/release` remain unprotected by this middleware —
  ruled out explicitly in Context above, not simply left for "later" the
  way order-management deferred its `{id}`-scoped routes.

## Alternatives considered

Same two alternatives order-management's ADR 0023 considered and
rejected, for the same reasons: application-level in-memory
de-duplication (does not survive a pod restart or work across replicas),
and a three-state (`pending`/`completed`/`failed`) design with a timeout
and a `409`-retry-later response (unnecessary once Postgres' own
unique-index lock and the no-null-status-code invariant are in place —
see §4/§5).

A third alternative specific to this port was also considered and
rejected: **also protecting `POST /paths/{pathId}/charge` and
`POST /paths/{pathId}/plan`.** Rejected because both are already
idempotent by construction (natural-key upsert on the caller-supplied
`path_id` — see Context) — adding the middleware there would add
mandatory-header friction to every caller of those two routes for zero
additional safety.
