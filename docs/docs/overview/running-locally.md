---
id: running-locally
title: Running locally
sidebar_label: Running locally
sidebar_position: 3
description: Run the service in-memory or against Postgres, exercise the API with curl, and connect it to the shared Kafka broker.
---

# Running locally

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

## Option 1 — in-memory, no infrastructure

```sh
go run ./cmd/wes
```

The server listens on `:8080` with in-memory repositories. State is lost on
restart; this is the fastest way to try the API.

## Option 2 — Postgres

```sh
docker compose up -d   # Postgres only

DATABASE_URL="postgres://wes:wes@localhost:5432/wes?sslmode=disable" go run ./cmd/wes
```

`cmd/wes` applies the OLTP migrations (`MIGRATIONS_PATH`, default
`migrations`) itself before opening the pool — there is no separate
`migrate` step.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Address the HTTP server listens on |
| `DATABASE_URL` | *(unset)* | Postgres DSN; falls back to in-memory repositories if unset |
| `EVENT_PUBLISHER` | `log` | `log` or `kafka` — where domain events get published |
| `KAFKA_BROKERS` | *(unset)* | Comma-separated brokers. Required for `EVENT_PUBLISHER=kafka`, and setting it also starts the inbound integration-event consumer |
| `KAFKA_CONSUMER_GROUP` | `wes-work-planning` | Consumer group of the integration-event consumer. Set a unique value when running locally against the shared cluster broker, or the deployed pod keeps the partition and your process consumes nothing |
| `PATH_CATALOGUE_SOURCE` | `file` | `file` reads `PATH_CATALOGUE_FILE` at boot; `kafka` replays `warehouse.process-path-management.events` (own per-process consumer group) and blocks startup until the replay completes |
| `PATH_CATALOGUE_FILE` | `/etc/wes-work-planning/process-paths.yaml` | Catalogue YAML for `PATH_CATALOGUE_SOURCE=file` |
| `MIGRATIONS_PATH` | `migrations` | OLTP migrations applied on start when `DATABASE_URL` is set |
| `PRODUCT_CLASSIFICATION_MODE` / `INVENTORY_STORAGE_BASE_URL` | `permissive` / *(unset)* | `http` enables the inventory-storage classification lookup ([ADR-0009](../adr/0009-product-classification-propagation-to-work-released.md)) |
| `TRAVEL_DISTANCE_MODE` / `FACILITY_LAYOUT_BASE_URL` | `permissive` / *(unset)* | `http` enables the facility-layout travel-distance lookup ([ADR-0017](../adr/0017-travel-distance-lookup-on-commit-shift-plan.md)) |

The full list, including observability and analytics variables, is in the
repository `README.md`.

Note that `KAFKA_BROKERS` and `EVENT_PUBLISHER` are independent: setting
`KAFKA_BROKERS` alone starts *consuming* without switching the publisher away
from `log`.

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
#     (in-memory mode; with DATABASE_URL set, also send -H 'Idempotency-Key: <unique>')
curl -s -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Content-Type: application/json' \
  -d '{"workUnitId":"wu-1","cpt":"2026-08-23T21:00:00Z","reference":"order-77/line-1"}'
curl -s -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Content-Type: application/json' \
  -d '{"workUnitId":"wu-2","cpt":"2026-08-23T18:00:00Z","reference":"order-78/line-1"}'

# 4 — release: returns wu-2, the earlier CPT, regardless of insert order
curl -s -X POST localhost:8080/paths/pick-a/release

# 5 — telemetry, then the rebalance recommendation
curl -s localhost:8080/paths/pick-a/telemetry
curl -s localhost:8080/paths/pick-a/rebalance

# 6 — complete it
curl -s -X POST localhost:8080/work-units/wu-2/complete
```

Every endpoint, with full schemas and status codes, is in the
[REST API Reference](../api/rest-overview.md).

## Connecting to the shared Kafka broker

A single broker is shared by every `warehouse-systems` service: the
in-cluster Kafka release in the `warehouse` kind cluster, reachable from the
host at `localhost:9092`. This repo's `docker-compose.yml` runs Postgres only.

```sh
KAFKA_BROKERS=localhost:9092 EVENT_PUBLISHER=kafka \
  KAFKA_CONSUMER_GROUP=wes-work-planning-local-$USER go run ./cmd/wes
```

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
| kubectl --context kind-warehouse -n warehouse-systems exec -i kafka-controller-0 -c kafka -- \
    kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic warehouse.workforce.events

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
go test -tags=integration ./...   # needs Docker (testcontainers); the older Postgres repo suite also needs DATABASE_URL
```

CI (`.github/workflows/ci.yml`) runs these as separate jobs on every push and
pull request — `lint`, `guide-lint`, `complexity`, `test`, `bdd`, `contract`,
`evals-tests`, `integration`, `mutation-fast`, `vuln`, `api-lint`,
`arch-test`, `docs-api-drift` and `web`. `helm-lint` and `trivy-scan` run
only on pull requests into `main`; `mutation` and `drift` are
weekly/dispatch-only; `docker-publish` and `release` run on pushes to `main`.
