---
id: 0026-migrations-direct-postgres-connection
slug: /adr/0026-migrations-direct-postgres-connection
title: 26. Run golang-migrate against a direct Postgres connection, not PgBouncer
sidebar_label: 26. Migrations bypass PgBouncer
description: "ADR 0026 — fleet-wide fix, ported from order-management's reference PR #115 (ADR-0029): golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43). Two or more wes-work-planning replicas starting concurrently (HPA scale-out enabled by ADR-0025, or an ordinary rolling deploy) would crash-loop until one won the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL, carries a direct (non-pooled) connection string used ONLY for the migration step in cmd/wes's wireRepositories; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer."
---

# 26. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record.
Fleet-wide bug found and fixed first in `order-management` PR #115
([ADR-0029](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0029-migrations-direct-postgres-connection.md)),
which is that PR's reference implementation for the other 8 OLTP services
in the fleet, `wes-work-planning` among them. `warehouse-infra`'s
companion PR #44 already provisions the `MIGRATIONS_DATABASE_URL` secret
key for all 9 services in one pass — no further `warehouse-infra` work is
needed for this service; this PR is the Go-code-and-chart half of the
fan-out.

## Context

`warehouse-infra`'s PgBouncer rollout ([PR #43](https://github.com/claudioed/warehouse-infra/pull/43))
repointed every one of the fleet's 9 OLTP services' `DATABASE_URL` secret
at PgBouncer, in **transaction-pooling** mode
(`pool_mode = "transaction"`). That's the correct mode for this fleet's
steady-state traffic — application code never holds session state across
statements — and PR #43 already carved out one deliberate exception:
analytics DSNs were left pointed directly at Postgres, because a single
low-QPS analytics consumer gets no pooling benefit. `wes-work-planning`
already follows that carve-out itself: `cmd/wes-projector`'s
`connectAnalyticsStore` migrates the analytics store via
`ANALYTICS_DATABASE_URL`, a DSN Terraform never routes through PgBouncer
— that path was never affected by this bug and needs no change here.

What PR #43 did not carve out: the OLTP **migrations**.
`cmd/wes/main.go`'s `wireRepositories` runs golang-migrate's postgres
driver (`github.com/golang-migrate/migrate/v4/database/postgres`) against
the same `DATABASE_URL` at process startup, before serving any traffic.
golang-migrate's postgres driver calls `SELECT pg_advisory_lock($1)` to
serialize concurrent migration runs — by design: if two processes start
at once and both try to run the same migration, whichever loses the lock
should block, not race.

`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it, and is expected to be released by
that same connection (or the session ending). PgBouncer's
transaction-pooling mode does not preserve that mapping — each statement
in a client's logical session can be routed to a different physical
backend connection, because the client's backend connection is returned
to the pool the instant its transaction commits. So:

- Pod A dials PgBouncer, gets backend connection #1, takes the advisory
  lock, runs migrations.
- Pod B dials PgBouncer *concurrently*, gets a **different** backend
  connection, and PgBouncer may freely reuse/rotate backend connections
  for either pod's subsequent statements mid-"session" from the
  application's point of view.
- The advisory lock never behaves as a real mutex across the two pods.
  Whichever pod's statements land on a backend connection with
  unexpected transaction/prepared-statement state gets errors like
  `pq: unnamed prepared statement does not exist` or `pq: canceling
  statement due to statement timeout`, and crash-loops for roughly 1-2
  minutes until the race resolves.

This is a **latent, fleet-wide, production-blocking bug**, not specific
to this service: it fires on any ordinary rolling ArgoCD deploy with more
than one `api` replica, and on every HPA scale-out event for the `api`
workload's `HorizontalPodAutoscaler` (ADR-0025, currently
default-disabled specifically because this bug had not yet been closed
for this service). `order-management`'s companion ADR-0029 documents a
live reproduction of the identical failure signature for that service;
the mechanism is unrelated to any per-service business logic, so it
applies identically here.

## Decision

Give `wes-work-planning` a **second** connection string,
`MIGRATIONS_DATABASE_URL` — a direct (non-pooled, session-mode) Postgres
connection string, same user/password/dbname as `DATABASE_URL`, pointed
at Postgres itself rather than PgBouncer — used **only** for the
golang-migrate startup step in `cmd/wes/main.go`'s `wireRepositories`.
`DATABASE_URL` and the pgxpool built from it are completely unchanged:
every request this service serves still goes through PgBouncer in
transaction-pooling mode, exactly as PR #43 set up.

This mirrors `order-management` ADR-0029's decision exactly, and is
architecturally identical to the `ANALYTICS_DATABASE_URL` carve-out this
service already has for `cmd/wes-projector`/`cmd/wes-reports`, and to
universal Postgres/PgBouncer operational guidance: **migrations need a
direct/session connection; steady-state application traffic goes through
the pooler.** We are not weakening or changing PgBouncer's `pool_mode`
(still `transaction`) — this fix is entirely about routing one specific,
short-lived, startup-only operation around the pooler.

`warehouse-infra` PR #44 already provisions `MIGRATIONS_DATABASE_URL` as
a new key alongside the existing `DATABASE_URL` key in
`wes-work-planning-db` (and the other 8 services' equivalent secrets) —
confirmed live in `terraform/terraform.tfstate`. `cmd/wes/main.go` now
reads it for the migration step only:

```go
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
...
wireRepositories(databaseURL, migrationsDatabaseURL, migrationsPath, logger)
...
postgres.Migrate(migrationsDatabaseURL, migrationsPath)  // migrations only
// the pgxpool opened right after this still uses databaseURL, unchanged
```

`cmd/mcp/main.go` does **not** run migrations itself (it only opens the
pgxpool via its own `wireRepositories`, reusing whatever schema `cmd/wes`
already migrated) — unlike `order-management`, where both `cmd/order`
and `cmd/mcp` independently run `RunMigrations`, this repo's `cmd/mcp`
has no migration step to redirect, so it needs no code or chart change.
`cmd/wes-projector`/`cmd/wes-reports` already migrate the analytics store
via `ANALYTICS_DATABASE_URL`, which was never routed through PgBouncer in
the first place — also out of scope for this fix.

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset
keeps every environment that doesn't provision the split — local dev, CI
integration tests, or any cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte. Nothing about local dev or CI
changes as a result of this PR.

`charts/wes-work-planning`: a new `database.migrationsExistingSecretKey`
value (default `"MIGRATIONS_DATABASE_URL"`) renders a
`MIGRATIONS_DATABASE_URL` env var, sourced from the same
`database.existingSecret`, in the `api` Deployment only (`templates/
deployment.yaml`) — `optional: true` on the `secretKeyRef` so a secret
that predates this chart version still starts the pod. The `mcp`
Deployment's template is deliberately left untouched, per the "no
migration step to redirect" reasoning above.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected, for the same reason order-management ADR-0029 rejected it:
session pooling would fix the advisory-lock problem but throws away the
entire point of PgBouncer for this fleet — transaction pooling is what
lets many short-lived HTTP-request-scoped OLTP connections share a small
number of physical Postgres backends. Switching to session mode
fleet-wide to accommodate a ~1-2 second startup-time lock call is the
tail wagging the dog.

### Why not just remove the advisory lock / skip migrations on non-leader replicas?

Rejected, again mirroring ADR-0029: golang-migrate's advisory lock is
exactly the right mechanism *given a session-scoped connection* — the bug
is the mismatch between that mechanism and the pooling mode we run
migrations through, not the mechanism itself. An init-container Job that
runs migrations exactly once before any replica starts was considered and
rejected: it's a bigger architectural change (a new Kubernetes resource
type, coordination with the Deployment's rollout strategy) for the same
outcome this two-line env-var fallback already achieves, and it would
still need its own direct-vs-pooled connection decision.

## Consequences

- **Fixes** the fleet-wide crash-loop bug for `wes-work-planning`,
  following the same pattern `order-management` PR #115 already shipped
  and verified live.
- **No runtime behavior change**: `DATABASE_URL` is untouched, so
  request-serving connection pooling, `pool_mode`, and PgBouncer's own
  configuration are all unaffected by this PR.
- **No behavior change for environments without the split**: the
  `getenv("MIGRATIONS_DATABASE_URL", databaseURL)` fallback means local
  dev and CI integration tests keep using `DATABASE_URL` for everything,
  exactly as before.
- **Unblocks safely enabling ADR-0025's `api` HorizontalPodAutoscaler**:
  this was the concrete blocker keeping that HPA (currently
  default-disabled) from being safely turned on — an HPA scale-out is
  exactly the "2+ replicas start concurrently" trigger for this bug.
- `cmd/mcp` and the analytics binaries (`cmd/wes-projector`,
  `cmd/wes-reports`) needed no change: neither runs migrations against a
  PgBouncer-fronted DSN today.
- One more secret key to keep in sync going forward; already mechanically
  provisioned by `warehouse-infra` PR #44 from the same `local.services`
  map `DATABASE_URL` is generated from, so there is no new per-service
  manual Terraform step.

## Verification

`make check` and `make check-all` (fmt, vet, build, lint, test, coverage
gate, arch-test, bdd) pass locally. `helm lint charts/wes-work-planning`
passes, and `helm template ... | grep MIGRATIONS_DATABASE_URL` confirms
the new env var renders in the `api` Deployment, sourced from
`wes-work-planning-db`'s `MIGRATIONS_DATABASE_URL` key with
`optional: true`, and does **not** render in the `mcp` Deployment.

Two new regression tests in `cmd/wes/main_test.go` prove the fix
end-to-end, mirroring `order-management`'s `cmd/order/wiring_test.go`
tests for ADR-0029:

- `TestMigrationsDatabaseURLFallback` — proves the `getenv` fallback in
  both directions: falls back to `DATABASE_URL` when
  `MIGRATIONS_DATABASE_URL` is unset; uses `MIGRATIONS_DATABASE_URL`, not
  `DATABASE_URL`, when set.
- `TestWireRepositories_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`
  — proves `wireRepositories` itself threads `migrationsDatabaseURL` into
  the migration step (not `databaseURL`), using a schemeless
  `MIGRATIONS_DATABASE_URL` to get a distinctive parse error
  (`"parse scheme"`) that a dial/"connection refused" error against
  `databaseURL` could never produce.

`warehouse-infra`'s `terraform.tfstate` was inspected directly and
confirms `MIGRATIONS_DATABASE_URL` already exists, live, in the
`wes-work-planning-db` secret's `database_urls`-derived state (PR #44,
already merged) — pointed at
`postgres-postgresql.warehouse-data.svc.cluster.local:5432`, not
PgBouncer. Live cluster re-verification of a real concurrent-replica
scale-out (mirroring `order-management` ADR-0029's `kubectl scale
deployment --replicas=3` reproduction) is deferred to the same rollout
step that turns on ADR-0025's `api` HPA for this service, since that HPA
is the actual trigger this fix protects against and is currently
default-disabled.
