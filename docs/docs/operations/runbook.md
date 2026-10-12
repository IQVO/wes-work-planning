---
id: runbook
title: Runbook
sidebar_label: Runbook
description: How wes-work-planning is deployed and operated — Helm chart, Deployments and ports, probes and boot sequence, migrations, Kafka topics and consumer groups, outbox relay, DLQs, housekeeping, scaling and routine procedures.
---

# Runbook

Everything here is derived from `cmd/*/main.go`, the adapters under
`internal/adapters/` and the Helm chart in `charts/wes-work-planning/`. For
every environment variable see [Configuration](./configuration.md); for
symptoms and fixes see [Troubleshooting](./troubleshooting.md).

## Deployment shape

The Helm chart is **`wes-work-planning`** (`charts/wes-work-planning`,
`Chart.yaml` version `0.1.0`; the CI `release` job packages it with the
release tag on every push to `main`). One image contains all four binaries
(`/app/wes`, `/app/mcp`, `/app/wes-projector`, `/app/wes-reports`) plus
`/app/migrations`; each Deployment picks its binary with `command:`.

| Deployment (component label) | Binary | Enabled by | Container port | Service | Probes |
|---|---|---|---|---|---|
| `<release>` (`api`) | `cmd/wes` (image entrypoint) | always | `http` 8080 (`service.targetPort`) | `<release>` ClusterIP, port 80 → `http` | startup `/healthz` (every 2 s, 30 failures = 60 s), liveness `/healthz` (5 s delay, every 10 s), **readiness `/readyz`** (3 s delay, every 5 s) |
| `<release>-mcp` (`mcp`) | `/app/mcp` | `mcp.enabled` | `http` 8090 | `<release>-mcp` ClusterIP, port 8090 → `http` | startup / liveness / readiness all `/healthz` |
| projector (`analytics-projector`) | `/app/wes-projector` | `analytics.enabled` | `admin` 8091 | none | startup / liveness / readiness all `/healthz` on `admin` |
| reports (`analytics-reports`) | `/app/wes-reports` | `analytics.enabled` | `http` 8092 | ClusterIP, port 80 → `http` | startup / liveness / readiness all `/healthz` |
| frontend (`frontend`) | nginx-unprivileged serving `web/` | `frontend.enabled` | 8080 | ClusterIP, port 80 | — (static assets) |

Only the api Deployment sets `terminationGracePeriodSeconds: 30` and mounts
the process-path catalogue ConfigMap at `/etc/wes-work-planning`
(`pathCatalogue.content` or `pathCatalogue.existingConfigMap`, key
`process-paths.yaml`). `charts/wes-work-planning/tests/test_service_selectors.py`
asserts each Service selects exactly one Deployment.

**Edge.** The chart's own Ingress (`ingress.enabled`) and Gateway API
`HTTPRoute` (`gatewayApi.enabled`) are both off by default. In the kind
cluster warehouse-infra routes Kong `http://localhost:8000/api/wes-work-planning/*`
to the api Service with the prefix stripped, and the Nginx web gateway serves
the operator remote at `http://localhost/mfes/wes-work-planning/`. REST and
MCP are unauthenticated ([ADR-0016](../adr/0016-remove-rest-mcp-static-bearer-auth.md)).

## Boot sequence of `cmd/wes` and what readiness waits for

`cmd/wes` does all of its waiting **before** it opens the HTTP listener, so
the startup probe (60 s budget) is what covers boot; `/readyz` only flips
during shutdown.

1. **Catalogue** (`cmd/wes/catalogue.go`). `file` mode reads
   `PATH_CATALOGUE_FILE` and exits on any error. `kafka` mode dials
   `warehouse.process-path-management.events` with the boot retry (5
   attempts, 1+2+4+8 s backoff), starts the replay goroutine and blocks until
   every partition reached the high-water mark it saw at start, for at most
   60 s.
2. **Telemetry** setup — never blocks (exporters dial lazily).
3. **Repositories** (`cmd/wes/repositories.go`). With `DATABASE_URL`: run
   golang-migrate against `MIGRATIONS_DATABASE_URL` (boot retry inside a 60 s
   context), open the pgxpool against `DATABASE_URL`, then ping it (boot
   retry). Without it: in-memory adapters.
4. **Classification** mode is validated (`kafka` needs a group and brokers).
5. **Event publisher** (`log`, Kafka direct, or Kafka outbox + relay).
6. **Serve**: HTTP listener, outbox relay goroutine, housekeeper goroutine,
   integration consumer (when `KAFKA_BROKERS` is set) and the
   `ProductClassified` consumer (when `PRODUCT_CLASSIFICATION_MODE=kafka`).

`GET /healthz` always answers `200 {"status":"ok"}`. `GET /readyz` answers
`200 {"status":"ready"}` from the moment the listener is up and
`503 {"status":"not_ready"}` once SIGTERM is received; it does **not** check
Postgres or Kafka at request time. Both routes are in `apis/openapi.yaml`
(operations `health-check` and `readiness-check`).

### Graceful shutdown (SIGTERM/SIGINT)

`cmd/wes/serving.go` `gracefulShutdown`, inside one 5 s budget
([ADR-0023](../adr/0023-resilience-circuit-breakers-retry-dlq-shutdown.md)):

1. readiness flips to not-ready;
2. the integration consumer is cancelled and **waited for** (an in-flight
   message finishes or is redelivered), then closed;
3. the `ProductClassified` consumer and the catalogue consumer stop;
4. the HTTP server drains;
5. the outbox relay finishes its in-flight pass;
6. on return the housekeeper is stopped and awaited, then the pool closes and
   telemetry is flushed (another 5 s).

`cmd/mcp`, `cmd/wes-projector` and `cmd/wes-reports` shut their HTTP server
down within 10 s and stop.

## Migrations

| Database | Runner | When | Files | Bookkeeping table |
|---|---|---|---|---|
| OLTP | `cmd/wes` via golang-migrate (`internal/adapters/outbound/postgres/migrate.go`) | every start, before the pool opens; `ErrNoChange` is success | `migrations/0001`–`0013` (`MIGRATIONS_PATH`) | golang-migrate's `schema_migrations` |
| Analytical | `cmd/wes-projector` (`internal/adapters/outbound/analyticsstore/migrate.go`) | every start, before its pool opens | `migrations/analytics/0001_report.up.sql` (`ANALYTICS_MIGRATIONS_PATH`) | `schema_migrations_analytics` (one row per applied file) |

- The OLTP migration step uses `MIGRATIONS_DATABASE_URL` when set — it must be
  a **direct** Postgres DSN, never PgBouncer, because golang-migrate's
  `pg_advisory_lock` is session-scoped
  ([ADR-0026](../adr/0026-migrations-direct-postgres-connection.md)). The chart
  reads it from the same Secret as `DATABASE_URL` (key
  `MIGRATIONS_DATABASE_URL`, `optional: true`).
- Concurrent replicas starting together serialise on that advisory lock;
  the losers wait, then see `ErrNoChange`.
- `cmd/mcp` and `cmd/wes-reports` never migrate. Deploy `cmd/wes` (and the
  projector) first after a schema change.
- `*.down.sql` files exist for every OLTP migration but no binary runs them;
  a rollback is a manual `migrate ... down 1` with the CLI against the direct
  DSN.

OLTP tables: `charge_forecasts`, `shift_plans`, `work_pools`,
`work_pool_entries`, `work_units`, `processed_events`, `labor_plan_view`,
`usable_inventory_view`, `outbox_events`, `idempotency_keys`,
`product_classification_copy`, plus `events`, which `0001_init` creates but
no code reads or writes. Analytical tables: `analytics_processed_events`,
`analytics_consumed_events`, `throughput_rollup`.

## Kafka

The fleet runs **one** broker (in-cluster Bitnami release; `localhost:9092`
from the host). Topics are auto-created on first write
(`AllowAutoTopicCreation` on every writer). Every message is a CloudEvents 1.0
structured-mode event ([ADR-0027](../adr/0027-cloudevents-mandatory-event-envelope.md)).

### Produced

| Topic | Producer | Content | Key |
|---|---|---|---|
| `warehouse.work-planning.events` | `cmd/wes`, `cmd/mcp` (`EVENT_PUBLISHER=kafka`) | all 11 domain events, `dataschema urn:warehouse:wes-work-planning:events:<Event>:v1` | aggregate id (work-unit id or path id), `Hash` balancer ([ADR-0024](../adr/0024-kafka-hash-balancer-partition-affinity.md)) |
| `warehouse.wes.analytics` | same | the same 11 events, analytics payloads ([ADR-0011](../adr/0011-analytical-data-product.md)) | aggregate id |
| `<consumed-topic>.dlq` | `cmd/wes` integration consumer | raw poison message + `x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` headers | original key |

Writers use `RequiredAcks=all` and a 10 ms `BatchTimeout`.

### Consumed

| Topic | Event types handled | Process | Consumer group | Start offset for a new group | Failure handling |
|---|---|---|---|---|---|
| `warehouse.workforce.events` | `ShiftPlanCommitted` | `cmd/wes` | `KAFKA_CONSUMER_GROUP` (default `wes-work-planning`) | earliest (kafka-go default) | 3 attempts (100 ms → 2 s jittered), then `.dlq`; invalid CloudEvent → `.dlq` at once |
| `warehouse.inventory.events` | `StockReserved`, `ReservationRevoked` | `cmd/wes` | same | earliest | same |
| `warehouse.fulfillment.events` | `TaskCompleted` | `cmd/wes` | same | earliest | same; unknown work unit = INFO skip |
| `warehouse.order-management.events` | `OrderAllocated`, `OrderPartiallyAllocated` | `cmd/wes` | same | earliest | same |
| `warehouse.network-inventory-planning.events` | `WorkDemandReleased` | `cmd/wes` | same | earliest | same |
| `warehouse.product-master.events` | `ProductClassified` | `cmd/wes` (`PRODUCT_CLASSIFICATION_MODE=kafka`) | `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | earliest | invalid → WARN + commit past; transient → retried forever (200 ms → 5 s), never committed until applied |
| `warehouse.process-path-management.events` | `ProcessPathCreated`, `ProcessPathUpdated`, `ProcessPathDeactivated` | `cmd/wes` (`PATH_CATALOGUE_SOURCE=kafka`) | per-process `wes-work-planning-process-path-catalogue-<host>-<pid>-<nanos>` | earliest (full replay every boot) | WARN + skip |
| `warehouse.wes.analytics` | `WorkReleased`, `WorkUnitCompleted`, `BacklogThresholdBreached`, `PathThrottled`, `RateDeviationDetected` (others acknowledged) | `cmd/wes-projector` | `wes-analytics` | earliest | `ReadMessage` commits first; an invalid CloudEvent is WARN-skipped, a handling error is logged at ERROR and the loop moves on |

Each `cmd/wes` integration handler records the CloudEvents `id` in
`processed_events` in the **same transaction** as its effect
([ADR-0028](../adr/0028-processed-event-mark-atomic-with-handling.md)), so a
redelivery is a no-op and a failed attempt leaves no mark. A DLQ publish
failure (or an offset commit failure) stops the consume loop for the whole
consumer — the offset is deliberately not committed, so nothing is lost.

## Transactional outbox and relay

With `EVENT_PUBLISHER=kafka` **and** `DATABASE_URL`, every use case writes
one `outbox_events` row per event per topic inside its own transaction
([ADR-0014](../adr/0014-transactional-outbox.md)). The relay
(`internal/adapters/outbound/postgres/outbox_relay.go`) runs **only in
`cmd/wes`** and drains rows written by `cmd/wes` and `cmd/mcp` alike:

- claims up to 100 unpublished rows `ORDER BY id ... FOR UPDATE SKIP LOCKED`
  (overlapping pods in a rolling deploy never claim the same row);
- sends them one at a time in id order through one topic-less writer; marks
  each `published_at = now()`, `attempts + 1`;
- on the first send failure records `attempts`/`last_error` on that row,
  commits what was sent and stops the pass (a later event for the same
  aggregate never overtakes a failed one); the row is retried next pass;
- a full batch loops immediately; otherwise it sleeps `OUTBOX_RELAY_INTERVAL`
  (default 1 s).

Delivery is at-least-once with a stable CloudEvents `id`; every consumer
dedupes on it. Watch `wes.outbox.lag_seconds` ([Observability](./observability.md)).

## Housekeeping

`postgres.Housekeeper` ([ADR-0032](../adr/0032-housekeeping-retention-sweeper.md))
runs in `cmd/wes` when Postgres is configured: one sweep at start, then every
`HOUSEKEEPING_INTERVAL` (1 h). Each sweep deletes, in batches of 1000:

- `idempotency_keys` rows older than `IDEMPOTENCY_KEY_TTL` (24 h) — after that
  a retried `POST /paths/{pathId}/work-units` with the same key is treated as
  new;
- **published** `outbox_events` rows older than `OUTBOX_RETENTION` (7 days).
  Unpublished rows are never touched.

`0` disables a table's sweep. Logs `housekeeping sweep removed rows` (INFO)
or `housekeeping sweep failed` (ERROR). Every statement is idempotent, so N
replicas sweeping is harmless. `processed_events` is **not** swept and grows
with every consumed event.

## Analytics reader routes (not in `apis/openapi.yaml`)

`cmd/wes-reports` serves three routes that the OLTP OpenAPI spec does not
describe (the contract lives on
[Release throughput report](../analytics/release-throughput-report.md)):

| Route | Query | Response |
|---|---|---|
| `GET /reports/throughput` | `from`, `to` (required, RFC3339), `pathId` (optional exact match), `granularity` (optional, only `hour`) | `200 {"rows":[{"pathId","hourBucket","workReleased","workUnitCompleted","backlogThresholdBreached","pathThrottled","rateDeviationDetected"}]}`; `400` problem type `invalid-report-query`; `500` `report-store-error` |
| `GET /reports/throughput/freshness` | — | `200 {"lagSeconds": <float>}` (0 for an empty store) |
| `GET /healthz` | — | `200 {"status":"ok"}` |

`cmd/mcp` likewise serves the MCP endpoint at `/` and `/mcp` plus
`GET /healthz` — see [MCP tools](../mcp/tools.md).

## Scaling

Every HPA block in `values.yaml` is **disabled by default**
([ADR-0025](../adr/0025-horizontal-autoscaling-and-pgxpool-tuning.md)); when
enabled the Deployment omits `replicas:` so the HPA owns it.

| Workload | HPA (min/max, CPU target) | Why it scales (or not) | Pool |
|---|---|---|---|
| api (`cmd/wes`) | 1/4, 70 % | stateless; consumers share `wes-work-planning` (and the classification group) via normal rebalancing; the catalogue consumer is per-process by design; the relay uses `SKIP LOCKED` | `MaxConns=10`, `statement_timeout=5s`; PgBouncer `pool_size=12` is the real server-side cap |
| projector | 1/2, 70 % | stable group `wes-analytics`; projection idempotent on event id | `MaxConns=5`, `10s` |
| reports | 1/3, 70 % | stateless, read-only | `MaxConns=5`, `15s`, read-only transactions |
| frontend | 1/3, 70 % | static assets | — |
| mcp | **none** | the go-sdk Streamable HTTP handler keeps per-process sessions keyed by `Mcp-Session-Id`; a second replica would reject sessions it did not create | shares the OLTP `MaxConns=10` |

With one partition per topic, extra api replicas add HTTP capacity, not
consumer parallelism.

## Routine procedures

### Replay a dead-lettered message

1. Read it with headers:
   `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic warehouse.order-management.events.dlq --from-beginning --property print.headers=true --property print.key=true`.
2. Fix the cause named in `x-dlq-error` (an unknown `path_id` means the
   catalogue must declare it; an invalid CloudEvent must be fixed by its
   producer).
3. Re-produce the **original value** onto the source topic named in
   `x-dlq-source-topic` with the same key. The consumer dedupes on the
   CloudEvents `id`, so a message that was in fact applied is a no-op.

### Re-publish an outbox row

A row's wire bytes (and CloudEvents `id`) are fixed at write time. To send it
again, set `published_at = NULL` on that `id` in `outbox_events`; the next
relay pass resends it and consumers dedupe on the `id`. Rows already removed
by the housekeeper (published more than `OUTBOX_RETENTION` ago) cannot be
re-sent this way.

### Rebuild a read model from Kafka

- **Classification copy**: stop `cmd/wes`, reset the
  `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` offsets to earliest (or deploy with
  a new group id) and start it. The upsert is version-guarded, so stale
  versions never win.
- **Analytics report**: the projector's group is the fixed constant
  `wes-analytics`. Scale the projector to 0, truncate `throughput_rollup`,
  `analytics_processed_events` and `analytics_consumed_events` (otherwise
  every replayed id is deduped), reset `wes-analytics` to earliest with
  `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`, scale
  back. Kafka retention bounds how far back this goes.
- **Labor-plan and inventory views**: resetting `wes-work-planning` alone
  replays nothing new, because every id is already in `processed_events`.

### Rotate database credentials

Update the Secret named by `database.existingSecret` (keys `DATABASE_URL`
and, optionally, `MIGRATIONS_DATABASE_URL`) or
`analytics.database.existingSecret` (`ANALYTICS_DATABASE_URL`,
`ANALYTICS_READER_DATABASE_URL`), then `kubectl rollout restart` the
Deployments that read it (api and mcp for OLTP; projector and reports for
analytics). DSNs are read only at boot.

### Change a path's WIP limit or feed mode

`PUT /paths/{pathId}/pool` with `{"mode":"ReleaseFed"|"FlowFed","wipLimit":N}`
([ADR-0034](../adr/0034-configure-pool-command.md)). Lowering the limit below
current WIP never evicts work; releases pause until WIP drops under it. It
raises no event.

### Run a second consumer against the shared broker

Set a unique `KAFKA_CONSUMER_GROUP` (for example
`wes-work-planning-local-$USER`). A new group starts at the **earliest**
offset, so the process replays each topic's retained history into its own
store.
