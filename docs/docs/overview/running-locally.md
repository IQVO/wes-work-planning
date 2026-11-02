---
id: running-locally
title: Running locally
sidebar_label: Running locally
sidebar_position: 3
description: Run the service against a real Postgres and a real Kafka (docker compose), exercise the API with curl, and smoke-test the consumer.
---

# Running locally

## Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| Go | `1.26.9` (the `go` directive in `go.mod`) | every binary and test |
| Docker | any recent | `docker compose` (Postgres 16 and a single-node Kafka) and the testcontainers integration tests |
| `golangci-lint` | v2.13.1 (pinned in `Makefile`) | `make lint` and the lefthook pre-commit hook |
| Node.js | `>=20.0` (`docs/package.json` engines) | only for `docs/` and `web/` |

## Build and test with make

`make help` lists every target. There is no `run` or database target: run
the binaries with `go run` and their infrastructure with
`docker compose up -d` (`postgres:16`, user and database `wes`, port `5432`,
plus `confluentinc/confluent-local:7.6.1` as a single-node Kafka that advertises
`localhost:9092`).

| Target | Does |
|---|---|
| `make build` | `go build ./...` (all four binaries) |
| `make test` | unit + httptest + BDD + MCP evals, `-race` |
| `make check` | `fmt-check vet build lint test` (what the pre-push hook runs) |
| `make integration` | testcontainers suites (needs Docker only) |

Every target, test suite and CI job is on [Testing](../development/testing.md).

## The process-path catalogue comes first

Every `pathId` is validated against the declared process-path catalogue
([ADR-0012](../adr/0012-process-path-catalogue-validation.md)); an unknown one
is rejected with `400 unknown-path-id`. With the default
`PATH_CATALOGUE_SOURCE=file` the service reads the YAML at
`PATH_CATALOGUE_FILE` (default `/etc/wes-work-planning/process-paths.yaml`) and
**refuses to start** if that file is missing or invalid. Locally, point it at
`warehouse-infra`'s catalogue, which declares the `pick`, `pack`, `rebin` and
`slam` prefixes (so `pick-a` below is a known path):

```sh
export PATH_CATALOGUE_FILE=../warehouse-infra/config/process-paths/sortable-fc.yaml
```

## Run it: real Postgres and real Kafka

There is no in-memory mode, no log-only publisher and no optional consumer
([ADR-0044](../adr/0044-postgres-and-kafka-are-the-only-runtime.md)). `cmd/wes`
exits at boot, with an error that names the variable, when `DATABASE_URL`,
`KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP`,
`PRODUCT_CLASSIFICATION_CONSUMER_GROUP` or `REPLENISHMENT_CONSUMER_GROUP` is
missing.

```sh
docker compose up -d --wait   # Postgres 16 + Kafka on localhost:9092

DATABASE_URL="postgres://wes:***@localhost:5432/wes?sslmode=disable" \
KAFKA_BROKERS=localhost:9092 \
KAFKA_CONSUMER_GROUP=wes-work-planning-local \
PRODUCT_CLASSIFICATION_CONSUMER_GROUP=wes-work-planning-product-classification-local \
REPLENISHMENT_CONSUMER_GROUP=wes-work-planning-replenishment-local \
go run ./cmd/wes
```

`cmd/wes` applies the OLTP migrations (`MIGRATIONS_PATH`, default
`migrations`) itself before opening the pool — there is no separate
`migrate` step. `GET /readyz` answers `200` once Postgres answers a ping and a
Kafka broker answers a metadata request; `GET /healthz` is liveness only.
If the outbox relay or a consumer dies on its own, the process exits non-zero.
Stop the stack with `docker compose down -v`.

The warehouse kind cluster maps its own Kafka to `localhost:9092`. If that is
running, start the compose Kafka on another port and point `KAFKA_BROKERS` at it:
`KAFKA_HOST_PORT=19092 docker compose up -d --wait`, then
`KAFKA_BROKERS=localhost:19092`.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Address the HTTP server listens on |
| `DATABASE_URL` | **required** | Postgres DSN. Boot error `DATABASE_URL is required` when unset |
| `EVENT_PUBLISHER` | `kafka` | Kept because the chart and warehouse-infra set it; `kafka` is the only value (events go through the outbox, drained by the always-on relay). `log` and any other value are a boot error |
| `KAFKA_BROKERS` | **required** | Comma-separated brokers. Boot error `KAFKA_BROKERS is required` when unset |
| `KAFKA_CONSUMER_GROUP` | **required** | Consumer group of the integration-event consumer. Use a unique value when running locally against a shared broker, or the deployed pod keeps the partition and your process consumes nothing |
| `PATH_CATALOGUE_SOURCE` | `file` | `file` reads `PATH_CATALOGUE_FILE` at boot; `kafka` replays `warehouse.process-path-management.events` (own per-process consumer group) and blocks startup until the replay completes |
| `PATH_CATALOGUE_FILE` | `/etc/wes-work-planning/process-paths.yaml` | Catalogue YAML for `PATH_CATALOGUE_SOURCE=file` |
| `MIGRATIONS_PATH` | `migrations` | OLTP migrations applied on start |
| `PRODUCT_CLASSIFICATION_MODE` / `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | `kafka` / **required** | The local copy of product-master's `ProductClassified` events is always kept and read for the hazmat/fragile hints; `permissive`, `off`, `http` and any other mode are rejected at boot ([ADR-0035](../adr/0035-product-classification-local-copy.md), [ADR-0044](../adr/0044-postgres-and-kafka-are-the-only-runtime.md)) |
| `REPLENISHMENT_MODE` / `REPLENISHMENT_CONSUMER_GROUP` | `kafka` / **required** | The pick-face-replenishment consumer is always on; `permissive`, `off` and any other mode are rejected at boot ([ADR-0042](../adr/0042-pick-face-replenishment-work.md)) |
| `TRAVEL_DISTANCE_MODE` / `FACILITY_LAYOUT_BASE_URL` | `permissive` / *(unset)* | REST sync edge to facility-layout (not changed by ADR-0044): `http` enables the travel-distance lookup ([ADR-0017](../adr/0017-travel-distance-lookup-on-commit-shift-plan.md)) |

Every variable of all four binaries, with defaults and source files, is on
[Configuration](../operations/configuration.md).

## A full loop with curl

```sh
# 1 — the charge: 900 units due by 18:00, 500 more by 21:00
curl -s -X POST localhost:8080/paths/pick-a/charge \
  -H 'Content-Type: application/json' \
  -d '{"buckets":[{"cpt":"2026-08-23T18:00:00Z","quantity":900},
                  {"cpt":"2026-08-23T21:00:00Z","quantity":500}]}'

# 2 — the plan: 6 heads on 8 installed stations, 95 units/h, 8 h
curl -s -X POST localhost:8080/paths/pick-a/plan \
  -H 'Content-Type: application/json' \
  -d '{"plannedHeads":6,"installedStations":8,"rateUnitsPerHour":95,"hours":8}'

# 3 — enqueue two work units with different CPTs
#     (the Idempotency-Key header is required: a fresh UUID per request)
curl -s -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"workUnitId":"wu-1","cpt":"2026-08-23T21:00:00Z","reference":"order-77/line-1"}'
curl -s -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"workUnitId":"wu-2","cpt":"2026-08-23T18:00:00Z","reference":"order-78/line-1"}'

# 4 — release: returns wu-2, the earlier CPT, regardless of insert order
curl -s -X POST localhost:8080/paths/pick-a/work-releases -H "Idempotency-Key: $(uuidgen)"

# 5 — telemetry, then the rebalance recommendation
curl -s localhost:8080/paths/pick-a/telemetry
curl -s localhost:8080/paths/pick-a/rebalance

# 6 — complete it
curl -s -X POST localhost:8080/work-units/wu-2/completions -H "Idempotency-Key: $(uuidgen)"
```

Every endpoint, with full schemas and status codes, is in the
[REST API Reference](../api/rest-overview.md).

## Pointing at another broker

`KAFKA_BROKERS` can name any real broker, for example the shared one in the
`warehouse` kind cluster at `localhost:9092`. A shared broker keeps consumer-group
offsets, so use unique `*_CONSUMER_GROUP` values for a local run, or the
deployed pod keeps the partition and your process consumes nothing.

Smoke-test the consumer by hand — publish a `ShiftPlanCommitted` CloudEvent
(the only envelope the consumer accepts, see
[ADR-0027](../adr/0027-cloudevents-mandatory-event-envelope.md)) onto the
workforce topic and read back the projection:

```sh
echo '{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111",
       "type":"com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted",
       "source":"/warehouse/workforce-management","subject":"S1",
       "time":"2026-08-23T09:00:00Z","datacontenttype":"application/json",
       "dataschema":"urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1",
       "data":{"building_id":"BLD1","shift_id":"S1","path_id":"pick-a",
               "planned_heads":7,"planned_rate":95.5,"planned_hours":8}}' | tr -d '\n' \
| docker compose exec -T kafka kafka-console-producer \
    --bootstrap-server localhost:29092 --topic warehouse.workforce.events

curl -s localhost:8080/paths/pick-a/labor-plan-view
# {"pathId":"pick-a","plannedHeads":7,"plannedRate":95.5,"plannedHours":8,"observedAt":"..."}
```

Replaying the exact same message is a no-op — see
[Idempotency](../ecosystem/integration-events.md#idempotency).

## Test suites

```sh
go build ./...
go vet ./...
go test ./...            # unit + httptest + godog acceptance
go test ./... -race
go test -tags=integration ./...   # needs Docker only (testcontainers boots its own Postgres/Kafka)
```

CI (`.github/workflows/ci.yml`) runs these as separate jobs on every push and
pull request — `lint`, `guide-lint`, `complexity`, `test`, `bdd`, `contract`,
`evals-tests`, `integration`, `mutation-fast`, `vuln`, `api-lint`,
`arch-test`, `docs-api-drift` and `web`. `helm-lint` and `trivy-scan` run
only on pull requests into `main`; `mutation` and `drift` are
weekly/dispatch-only; `docker-publish` and `release` run on pushes to `main`.
