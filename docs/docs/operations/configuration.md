---
id: configuration
title: Configuration reference
sidebar_label: Configuration
description: Every environment variable each of the four binaries reads, with its default, whether it is required, what it does and the file that reads it.
---

# Configuration reference

This repository builds **four binaries** from one image (`Dockerfile` copies
them to `/app/wes`, `/app/mcp`, `/app/wes-projector`, `/app/wes-reports`).
Each reads its own environment once at boot; nothing is re-read at runtime.
There is no config file other than the process-path catalogue YAML.

Conventions used below:

- **Default** is what the binary uses when the variable is unset *or empty*
  (every reader uses a `getenv(key, fallback)` helper that treats `""` as
  unset).
- **Required** means the process refuses to start without it. "Conditional"
  means it is required only in the mode named in the meaning column.
- Durations are Go `time.ParseDuration` strings (`500ms`, `30s`, `1h`,
  `168h`).
- Test-only variables (the `*_TEST_*` family read by integration tests) are
  not listed.

REST and MCP are **unauthenticated** fleet-wide
([ADR-0016](../adr/0016-remove-rest-mcp-static-bearer-auth.md)): there is no
API-key, token or scope variable to set.

## `cmd/wes` — OLTP REST API, Kafka consumers, outbox relay, housekeeper

### Process and persistence

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API (and `/healthz`, `/readyz`). | `cmd/wes/config.go:50` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn` or `error` (case-insensitive; anything else is `info`). JSON to stdout. | `cmd/wes/main.go:31` |
| `DATABASE_URL` | *(unset)* | no | OLTP Postgres DSN. **Unset selects the in-memory adapters** (state lost on restart, no idempotency middleware, no outbox, no housekeeper). Set, the process migrates the schema, opens a pgxpool (`MaxConns=10`, `statement_timeout=5s`) and wires the Postgres adapters and `UnitOfWork`. | `cmd/wes/config.go:48` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | DSN used **only** for the golang-migrate startup step. Point it at a direct (non-PgBouncer) connection: golang-migrate takes a session-scoped `pg_advisory_lock`, which PgBouncer's transaction pooling breaks ([ADR-0026](../adr/0026-migrations-direct-postgres-connection.md)). The runtime pool always uses `DATABASE_URL`. | `cmd/wes/config.go:52` |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP `*.up.sql` files applied on start (relative to the working directory; the image's working directory is `/app`, which contains `migrations/`). | `cmd/wes/config.go:53` |

### Events and Kafka

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `EVENT_PUBLISHER` | `log` | no | `log` writes every domain event to the log only. `kafka` publishes to `warehouse.work-planning.events` **and** `warehouse.wes.analytics`: through the transactional outbox and its in-process relay when `DATABASE_URL` is set ([ADR-0014](../adr/0014-transactional-outbox.md)), directly to both topics otherwise. Any value other than `kafka` behaves like `log`. | `cmd/wes/config.go:55` |
| `KAFKA_BROKERS` | *(unset)* | conditional | Comma-separated broker list (`localhost:9092` from the host against the fleet's single broker). **Setting it starts the inbound integration-event consumer** on five topics, independently of `EVENT_PUBLISHER`. Required by `EVENT_PUBLISHER=kafka`, `PATH_CATALOGUE_SOURCE=kafka` and `PRODUCT_CLASSIFICATION_MODE=kafka` (each fails the boot with a named error when it is missing). | `cmd/wes/config.go:54` |
| `KAFKA_CONSUMER_GROUP` | `wes-work-planning` | no | Group id of the integration-event consumer, shared by every replica. Give any *second* process against the shared broker (a developer's `go run`, the e2e harness) a unique value, or the rebalance hands the partition to one member and the other silently consumes nothing. | `cmd/wes/serving.go:193` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Sleep between relay passes that found nothing to publish (a pass that drained a full batch of 100 loops immediately). Only used with `EVENT_PUBLISHER=kafka` **and** `DATABASE_URL`. A malformed or non-positive value falls back to `1s`. | `cmd/wes/publisher.go:74` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | Outbox relay attempt cap ([ADR-0039](../adr/0039-outbox-relay-attempt-cap.md)): a row that has failed this many sends is no longer claimed, logged once at ERROR (`outbox relay: row reached the attempt cap and is no longer claimed`) and left in `outbox_events` for inspection; later rows, including rows with the same key, overtake it. `0` = unlimited (one poison row blocks the relay forever). A malformed or negative value falls back to `20`. Re-arm a row with `UPDATE outbox_events SET attempts = 0 WHERE id = ...` ([Runbook](./runbook.md#re-arm-an-outbox-row-that-hit-the-attempt-cap)). Chart value `config.outboxMaxAttempts`. | `cmd/wes/publisher.go:72` |

### Process-path catalogue

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `PATH_CATALOGUE_SOURCE` | `file` | no | `file` loads `PATH_CATALOGUE_FILE` once at boot. `kafka` replays `warehouse.process-path-management.events` from the beginning under a per-process consumer group and **blocks startup** until the replay catches up (60 s budget, `kafkacatalog.WaitReadyTimeout`) ([ADR-0030](../adr/0030-kafka-sourced-path-catalogue.md)). Any other value behaves like `file`. | `cmd/wes/catalogue.go:24` |
| `PATH_CATALOGUE_FILE` | `/etc/wes-work-planning/process-paths.yaml` | yes, in `file` mode | Catalogue YAML. A missing or invalid file **stops the process from starting** — there is no empty-catalogue fallback ([ADR-0012](../adr/0012-process-path-catalogue-validation.md)). The Helm chart mounts it from a ConfigMap. | `cmd/wes/catalogue.go:81` |

### Sibling lookups

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | no | `permissive` (or empty): `WorkReleased` carries no hazmat/fragile hints. `kafka`: consume product-master's `ProductClassified` into the local `product_classification_copy` table (in memory without `DATABASE_URL`) and read hints from it. `http` — the retired live lookup — and every other value **fail the boot** ([ADR-0035](../adr/0035-product-classification-local-copy.md)). | `cmd/wes/classification.go:33` |
| `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | *(unset)* | yes, in `kafka` mode | Stable consumer group of the `ProductClassified` consumer, shared by every replica (the copy is shared state). warehouse-infra sets `wes-work-planning-product-classification`. | `cmd/wes/classification.go:34` |
| `TRAVEL_DISTANCE_MODE` | `permissive` | no | `http` (case-insensitive) enables the facility-layout travel-distance lookup on `POST /paths/{pathId}/plan`, wrapped in a circuit breaker plus a 3-attempt jittered retry ([ADR-0017](../adr/0017-travel-distance-lookup-on-commit-shift-plan.md), [ADR-0023](../adr/0023-resilience-circuit-breakers-retry-dlq-shutdown.md)). Anything else is the no-op lookup. | `cmd/wes/repositories.go:44` |
| `FACILITY_LAYOUT_BASE_URL` | *(unset)* | yes, in `http` mode | Base URL of facility-layout; the client calls `GET <base>/distance?from=&to=`. Not validated at boot: an empty value makes every lookup fail, which is fail-open (the plan commits without a travel-distance hint). | `cmd/wes/repositories.go:44` |

### Housekeeping (Postgres only)

The retention sweeper ([ADR-0032](../adr/0032-housekeeping-retention-sweeper.md))
runs only when `DATABASE_URL` is set.

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `HOUSEKEEPING_INTERVAL` | `1h` | no | Time between sweeps (the first sweep runs at startup). Malformed or non-positive falls back to `1h`. | `cmd/wes/housekeeper.go:23` |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which `idempotency_keys` rows are deleted. An explicit `0` (or `0s`) **disables** this sweep; malformed or negative falls back to `24h`. | `cmd/wes/housekeeper.go:24` |
| `OUTBOX_RETENTION` | `168h` (7 days) | no | Age, measured from `published_at`, after which **published** `outbox_events` rows are deleted. Unpublished rows are never deleted. `0` disables; malformed or negative falls back to `168h`. | `cmd/wes/housekeeper.go:25` |

### HTTP edge and telemetry

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5183` | no | Comma-separated browser origins allowed to call the REST API directly (the console shell's dev port and this repo's `web/` remote dev port). Methods `GET, POST, PUT, DELETE`; headers `Content-Type`, `Authorization`, `Idempotency-Key`; no credentials. | `internal/adapters/inbound/http/router.go:89` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC Collector endpoint for traces and metrics. Accepts bare `host:port` or a URL (`http://host:4317`). Nothing listening is not an error: exporters dial lazily and drop data. | `cmd/wes/main.go:94` |
| `OTEL_SERVICE_NAME` | `wes-work-planning` | no | `service.name` resource attribute and the otelchi server name. | `cmd/wes/config.go:56` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute (the chart sets the image tag). | `cmd/wes/main.go:93` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go:54` |

## `cmd/mcp` — MCP server (Streamable HTTP)

`cmd/mcp` runs the same `SampleBacklog`, `RebalanceDecision` and
`ReleaseNextWork` use cases as the REST API over the same OLTP database
([ADR-0008](../adr/0008-mcp-inbound-adapter.md)). It never runs migrations,
never starts a Kafka consumer, never runs the outbox relay (the relay in
`cmd/wes` drains every outbox row, whichever process wrote it) and never
writes the classification copy. See [MCP tools](../mcp/tools.md).

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `MCP_ADDR` | `:8090` | no | Listen address. The MCP endpoint is mounted at both `/` and `/mcp`; `GET /healthz` answers `{"status":"ok"}`. | `cmd/mcp/main.go:93` |
| `LOG_LEVEL` | `info` | no | As for `cmd/wes`. | `cmd/mcp/main.go:49` |
| `DATABASE_URL` | *(unset)* | no | OLTP Postgres DSN (the same database as `cmd/wes`, which must have migrated it). Unset selects **its own** in-memory work-pool and work-unit stores — they are not shared with a `cmd/wes` process, so tools then see an empty world. | `cmd/mcp/main.go:73` |
| `EVENT_PUBLISHER` | `log` | no | Same contract as `cmd/wes`: `kafka` + `DATABASE_URL` writes outbox rows; `kafka` without a database publishes directly to both topics. | `cmd/mcp/main.go:84` |
| `KAFKA_BROKERS` | *(unset)* | conditional | Required by `EVENT_PUBLISHER=kafka`. Opens writers only; no consumer. | `cmd/mcp/main.go:84` |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | no | `kafka` with `DATABASE_URL` reads the `product_classification_copy` table `cmd/wes` maintains (read-only); `kafka` without a database falls back to permissive; `http` and unknown values fail the boot. | `cmd/mcp/main.go:79` |
| `REPORTS_BASE_URL` | *(unset)* | no | Base URL of `cmd/wes-reports`. Set, the `get_release_throughput_report` tool is registered (10 s HTTP client timeout); unset, the tool does not exist. The chart points it at the release's reports Service when `analytics.enabled=true`. | `cmd/mcp/main.go:237` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/wes`. | `cmd/mcp/main.go:60` |
| `OTEL_SERVICE_NAME` | `wes-work-planning-mcp` | no | `service.name`. | `cmd/mcp/main.go:55` |
| `SERVICE_VERSION` | `dev` | no | `service.version`. | `cmd/mcp/main.go:59` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name`. | `internal/adapters/outbound/telemetry/telemetry.go:54` |

## `cmd/wes-projector` — analytics writer

Consumes `warehouse.wes.analytics` under the fixed group `wes-analytics` and
projects into the analytical database it owns
([ADR-0011](../adr/0011-analytical-data-product.md)).

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `ANALYTICS_DATABASE_URL` | *(unset)* | **yes** | Read-write DSN of the analytical database. Missing it exits with `ANALYTICS_DATABASE_URL is required`. Pool: `MaxConns=5`, `statement_timeout=10s`. | `cmd/wes-projector/main.go:70` |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical `*.up.sql` files, applied at start and recorded in `schema_migrations_analytics`. | `cmd/wes-projector/main.go:75` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Broker list. Note the default: unlike `cmd/wes`, the projector always consumes. | `cmd/wes-projector/main.go:74` |
| `ADMIN_ADDR` | `:8091` | no | Admin listener serving only `GET /healthz`. | `cmd/wes-projector/main.go:69` |
| `LOG_LEVEL` | `info` | no | Also accepts `warning` as a synonym of `warn`. | `cmd/wes-projector/main.go:47` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As above. | `cmd/wes-projector/main.go:56` |
| `OTEL_SERVICE_NAME` | `wes-projector` | no | `service.name`. | `cmd/wes-projector/main.go:51` |
| `SERVICE_VERSION` | `dev` | no | `service.version`. | `cmd/wes-projector/main.go:55` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name`. | `internal/adapters/outbound/telemetry/telemetry.go:54` |

## `cmd/wes-reports` — analytics reader

Serves `GET /reports/throughput` and `GET /reports/throughput/freshness`
read-only (see [Release throughput report](../analytics/release-throughput-report.md)).

| Variable | Default | Required | Meaning | Read in |
|---|---|---|---|---|
| `ANALYTICS_DATABASE_URL` | *(unset)* | **yes** | DSN of the analytical database — use a read-only role. Every connection is additionally pinned to `default_transaction_read_only=on`; `MaxConns=5`, `statement_timeout=15s`. The chart feeds it the `ANALYTICS_READER_DATABASE_URL` secret key. | `cmd/wes-reports/main.go:63` |
| `HTTP_ADDR` | `:8092` | no | Listen address. | `cmd/wes-reports/main.go:62` |
| `LOG_LEVEL` | `info` | no | Also accepts `warning`. | `cmd/wes-reports/main.go:40` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As above. | `cmd/wes-reports/main.go:49` |
| `OTEL_SERVICE_NAME` | `wes-reports` | no | `service.name`. | `cmd/wes-reports/main.go:44` |
| `SERVICE_VERSION` | `dev` | no | `service.version`. | `cmd/wes-reports/main.go:48` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name`. | `internal/adapters/outbound/telemetry/telemetry.go:54` |

## Where the Helm chart sets them

`charts/wes-work-planning` renders only part of this list natively; the rest
reaches a container through `extraEnv` (api) or `mcp.extraEnv` (MCP).

| Chart value | Env var(s) | Container |
|---|---|---|
| `config.httpAddr`, `config.eventPublisher`, `config.logLevel` | `HTTP_ADDR`, `EVENT_PUBLISHER`, `LOG_LEVEL` | api (`EVENT_PUBLISHER`/`LOG_LEVEL` also MCP) |
| `config.outboxMaxAttempts` (default `20`) | `OUTBOX_MAX_ATTEMPTS` | api |
| `database.url` / `database.existingSecret` + `existingSecretKey` | `DATABASE_URL` | api, MCP |
| `database.migrationsExistingSecretKey` (`optional: true`) | `MIGRATIONS_DATABASE_URL` | api |
| `kafka.enabled` + `kafka.brokers` | `KAFKA_BROKERS` | api, MCP; the projector always gets `kafka.brokers` |
| `pathCatalogue.enabled` + `pathCatalogue.mountPath` | `PATH_CATALOGUE_FILE` | api |
| `otel.enabled`, `otel.endpoint`, `otel.serviceName`, `environment`, image tag | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `ENVIRONMENT`, `SERVICE_VERSION` | all four (service names fixed per Deployment) |
| `mcp.httpAddr`, `mcp.eventPublisher`, `mcp.reportsBaseUrl` | `MCP_ADDR`, `EVENT_PUBLISHER`, `REPORTS_BASE_URL` | MCP |
| `analytics.projector.adminAddr`, `analytics.migrationsPath` | `ADMIN_ADDR`, `ANALYTICS_MIGRATIONS_PATH` | projector |
| `analytics.reports.httpAddr` | `HTTP_ADDR` | reports |
| `analytics.database.*` (secret keys `ANALYTICS_DATABASE_URL`, `ANALYTICS_READER_DATABASE_URL`) | `ANALYTICS_DATABASE_URL` | projector (writer key), reports (reader key) |
| `extraEnv` | everything else (`PATH_CATALOGUE_SOURCE`, `PRODUCT_CLASSIFICATION_*`, `TRAVEL_DISTANCE_MODE`, `FACILITY_LAYOUT_BASE_URL`, `KAFKA_CONSUMER_GROUP`, `OUTBOX_RELAY_INTERVAL`, housekeeping, `CORS_ALLOWED_ORIGINS`) | api |

In the kind cluster, warehouse-infra's `terraform/locals.tf` adds
`PRODUCT_CLASSIFICATION_MODE=kafka`,
`PRODUCT_CLASSIFICATION_CONSUMER_GROUP=wes-work-planning-product-classification`,
`TRAVEL_DISTANCE_MODE=http` and `FACILITY_LAYOUT_BASE_URL` to the api
container, and `PRODUCT_CLASSIFICATION_MODE=kafka` to the MCP container.

## Related pages

- [Runbook](./runbook.md) — what each setting means for a deployment.
- [Running locally](../overview/running-locally.md) — the minimal set for a laptop.
- [Observability](./observability.md) — what the telemetry variables produce.
