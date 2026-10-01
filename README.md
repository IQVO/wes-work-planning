# WES — Work Planning & Release

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> waveless release, CloudEvents, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world company**.

The core bounded context of a Warehouse Execution System. It turns a shift's
**charge** (volume due by each CPT) into a **plan** (rate x heads per process
path), releases work continuously in priority order (waveless), and performs
flow balancing off live backlog telemetry — Drum-Buffer-Rope with CPT as the
drum.

## Architecture

Hexagonal / Ports & Adapters, with a strict dependency rule: **domain depends
on nothing; application depends on domain; adapters depend on
application/domain.**

```
cmd/wes/                     main.go — wiring/composition root (OLTP service)
cmd/wes-projector/           analytics writer (ADR-0011)
cmd/wes-reports/             analytics read-only reports API (ADR-0011)
cmd/mcp/                     MCP server (ADR-0008)
internal/
  domain/                    pure Go: aggregates, value objects, domain events
    charge/                  ChargeForecast aggregate
    plan/                    ShiftPlan, PathPlan aggregates
    release/                 WorkPool aggregate, ReleasePolicy domain service
    workunit/                WorkUnit aggregate
    shared/                  value objects (CPT, Rate, PathId, Quantity, ...)
  application/                use cases (ports IN) + port interfaces (ports OUT)
    ports/                   driven-port interfaces
    usecases/                one struct per use case
  adapters/
    inbound/http/            REST handlers, DTOs, chi router
    inbound/kafka/           integration-event consumer + analytics consumer (projector)
    inbound/mcp/             MCP tools, resources, prompts
    outbound/postgres/       pgx repositories + migrations, UnitOfWork, transactional outbox + relay (ADR-0014)
    outbound/memory/         in-memory repositories (tests/local)
    outbound/events/         log-based event publisher + MultiPublisher (direct Kafka fan-out when no Postgres)
    outbound/kafka/          integration + analytics publishers (also the outbox's Encoders) and the relay's RelaySink
    outbound/filecatalog/    process-path catalogue from a YAML file (PATH_CATALOGUE_SOURCE=file, ADR-0012)
    outbound/kafkacatalog/   process-path catalogue replayed from process-path-management's topic (PATH_CATALOGUE_SOURCE=kafka)
    outbound/productclassification/  inventory-storage classification lookup (ADR-0009)
    outbound/traveldistance/ facility-layout travel-distance lookup (ADR-0017)
    outbound/analyticsstore/ analytics Postgres projection + report reader
    outbound/telemetry/      OpenTelemetry setup
migrations/                  golang-migrate SQL files (OLTP; migrations/analytics/ for the projector)
```

The domain package imports nothing outside the Go standard library. The
application layer depends only on domain types and the `ports` interfaces it
defines. Adapters are the only layer allowed to import a framework, a SQL
driver, or an HTTP router.

## Running

Every run needs a **process-path catalogue** (ADR-0012). With the default
`PATH_CATALOGUE_SOURCE=file` the service reads the YAML at
`PATH_CATALOGUE_FILE` (default `/etc/wes-work-planning/process-paths.yaml`) and
refuses to start if it is missing or invalid. Locally, point it at
`warehouse-infra`'s `config/process-paths/sortable-fc.yaml` (it declares the
`pick`, `pack`, `rebin` and `slam` prefixes, so `pick-a` below is a known path;
an unknown path id is rejected with `400 unknown-path-id`):

```sh
export PATH_CATALOGUE_FILE=../warehouse-infra/config/process-paths/sortable-fc.yaml
```

### Option 1 — in-memory (no database)

```sh
go run ./cmd/wes
```

The server starts on `:8080` using in-memory repositories (state is lost on
restart) — good for trying the API without any infrastructure.

### Option 2 — Postgres

```sh
docker compose up -d   # Postgres only

DATABASE_URL="postgres://wes:wes@localhost:5432/wes?sslmode=disable" go run ./cmd/wes
```

`cmd/wes` applies the OLTP migrations in `MIGRATIONS_PATH` itself before it
opens the pool, so there is no separate `migrate` step.

### Config

| Env var          | Default | Purpose                                                                 |
|------------------|---------|--------------------------------------------------------------------------|
| `HTTP_ADDR`      | `:8080` | Address the HTTP server listens on                                     |
| `DATABASE_URL`   | (unset) | Postgres DSN; falls back to in-memory if unset                         |
| `MIGRATIONS_PATH` | `migrations` | Directory of OLTP migrations applied on start when `DATABASE_URL` is set |
| `EVENT_PUBLISHER`| `log`   | `log` (default) or `kafka` — where domain events get published. With `kafka` **and** `DATABASE_URL` set, events are written to the `outbox_events` table in the same transaction as the aggregate and relayed to both Kafka topics by an in-process relay (transactional outbox, [ADR-0014](docs/docs/adr/0014-transactional-outbox.md)); with `kafka` but no `DATABASE_URL` they are published directly |
| `KAFKA_BROKERS`  | (unset) | Comma-separated Kafka brokers; required for `EVENT_PUBLISHER=kafka` and enables the inbound integration-event consumer whenever set |
| `KAFKA_CONSUMER_GROUP` | `wes-work-planning` | Kafka consumer group id. Leave unset in a deployment so replicas share one group and split the partitions. Set a unique value for any process run alongside a deployed instance against the SAME broker (the e2e-tests harness, a local `go run`) — otherwise both join the same group, Kafka awards the single partition to one of them, and the other silently consumes nothing |
| `OUTBOX_RELAY_INTERVAL` | `1s` | How long the outbox relay sleeps between passes that found nothing to publish (Go duration, e.g. `500ms`). Only used in outbox mode |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | `permissive` (default, no-op, always omits hazmat/fragile hints) or `http` — synchronous lookup of a released unit's SKU classification from inventory-storage |
| `INVENTORY_STORAGE_BASE_URL` | (unset) | Base URL for inventory-storage's REST API; required when `PRODUCT_CLASSIFICATION_MODE=http` |
| `TRAVEL_DISTANCE_MODE` | `permissive` | `permissive` (default, no-op, always omits the travel-distance hint) or `http` — synchronous lookup of the real travel distance between two facility-layout LocationCodes at shift-plan-commit time (ADR-0017) |
| `FACILITY_LAYOUT_BASE_URL` | (unset) | Base URL for facility-layout's REST API; required when `TRAVEL_DISTANCE_MODE=http` |
| `PATH_CATALOGUE_SOURCE` | `file` | `file` (default) reads `PATH_CATALOGUE_FILE` once at boot; `kafka` (requires `KAFKA_BROKERS`) replays process-path-management's `warehouse.process-path-management.events` topic into an in-memory catalogue, blocks startup until the replay completes, then keeps it current live. The replay uses its own per-process consumer group, independent of `KAFKA_CONSUMER_GROUP` |
| `PATH_CATALOGUE_FILE` | `/etc/wes-work-planning/process-paths.yaml` | Path to the declared process-path catalogue YAML (see `warehouse-infra`'s `config/process-paths/sortable-fc.yaml`, the same file `fulfillment-execution` reads). Used only when `PATH_CATALOGUE_SOURCE=file`; loaded once at startup, and a missing or invalid file is a fatal boot-time error — see [ADR-0012](docs/docs/adr/0012-process-path-catalogue-validation.md) |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5183` | Comma-separated browser origins allowed by the REST CORS middleware |

## Analytics data product (Release Throughput & Backlog Health)

Alongside the OLTP service, this repo owns a per-service **analytical data
product** built entirely from its own domain events — a lightweight data mesh
with no central data platform. See
[ADR-0011](./docs/docs/adr/0011-analytical-data-product.md) and the
[report contract](./docs/docs/analytics/release-throughput-report.md).

Three processes, one writer:

- `cmd/wes` — OLTP (unchanged). With `EVENT_PUBLISHER=kafka` it fans every
  domain event to both the integration topic (`warehouse.work-planning.events`)
  and the separate analytics topic (`warehouse.wes.analytics`).
- `cmd/wes-projector` — the **only** writer of the analytical database. Consumes
  `warehouse.wes.analytics` (from the earliest offset), applies idempotent
  projections, and runs the analytical migrations on start.
- `cmd/wes-reports` — read-only reader. Serves `GET /reports/throughput` and
  `GET /reports/throughput/freshness` over a read-only pool.

```sh
docker compose up -d   # Postgres only; Kafka is the shared broker on localhost:9092

# Create a separate analytical database (baseline: same Postgres release), then:
export ANALYTICS_DATABASE_URL="postgres://wes:***@localhost:5432/wes_analytics?sslmode=disable"
export KAFKA_BROKERS="localhost:9092"

# 1) OLTP service, publishing to both topics
EVENT_PUBLISHER=kafka DATABASE_URL="postgres://wes:***@localhost:5432/wes?sslmode=disable" \
  go run ./cmd/wes

# 2) The writer: consumes the analytics topic, migrates + projects (admin :8091)
go run ./cmd/wes-projector

# 3) The reader: serves the report read-only (:8092)
go run ./cmd/wes-reports

# Query it
curl "localhost:8092/reports/throughput?from=2026-01-01T00:00:00Z&to=2027-01-01T00:00:00Z"
curl "localhost:8092/reports/throughput/freshness"
```

The MCP server (`cmd/mcp`) exposes the read-only `get_release_throughput_report`
tool when `REPORTS_BASE_URL` (e.g. `http://localhost:8092`) is set; it calls the
reports REST rather than opening the analytical database.

### Running the MCP server in Kubernetes

The MCP server ([ADR-0008](docs/docs/adr/0008-mcp-inbound-adapter.md)) ships in
the same image as the OLTP service (`/app/mcp`, built from `cmd/mcp`) and is
deployed by the Helm chart as a separate Deployment + ClusterIP Service
(`<release>-mcp`, port `8090`) when `mcp.enabled=true`. It is **off by default**.
The binary reuses the OLTP `DATABASE_URL` secret, and — when
`analytics.enabled=true` — is pointed at this release's reports Service so the
report tool is registered. The Streamable HTTP endpoint is mounted at both `/`
and `/mcp` (warehouse-ops-agent's `*_MCP_ENDPOINT` convention is
`http://<release>-mcp.<ns>.svc.cluster.local:8090/mcp`); `GET /healthz`
backs the liveness/readiness probes.

```sh
helm upgrade --install wes charts/wes-work-planning \
  --set database.url="postgres://..." \
  --set mcp.enabled=true
```

### Analytics config

| Env var | Default | Purpose |
|---|---|---|
| `ANALYTICS_DATABASE_URL` | (unset) | Analytical DB DSN. Required by `cmd/wes-projector` (read-write) and `cmd/wes-reports` (read-only role recommended). |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | Directory of analytical `*.up.sql` migrations the projector applies on start. |
| `ADMIN_ADDR` | `:8091` | `cmd/wes-projector` health endpoint address. |
| `HTTP_ADDR` (reports) | `:8092` | `cmd/wes-reports` REST address. |
| `REPORTS_BASE_URL` | (unset) | When set on `cmd/mcp`, enables the `get_release_throughput_report` MCP tool pointed at the reports REST. |

## API

All bodies/responses are JSON. Timestamps are RFC3339. Every endpoint is also
documented exhaustively (full request/response schemas, every status code, a
`Problem` component reused across every error response) in
[`apis/openapi.yaml`](./apis/openapi.yaml).

## Errors

Every error response (4xx/5xx) is [RFC 7807](https://www.rfc-editor.org/rfc/rfc7807)
`application/problem+json`, not a bespoke shape:

```sh
curl -sD - localhost:8080/paths/does-not-exist/telemetry
```

```
HTTP/1.1 404 Not Found
Content-Type: application/problem+json

{
  "type": "https://errors.wes-work-planning.warehouse-systems.dev/not-found",
  "title": "Resource not found",
  "status": 404,
  "detail": "resource not found",
  "instance": "/paths/does-not-exist/telemetry"
}
```

`type` is a stable per-category identifier (does not need to resolve to a
real page), `title` is a fixed category summary, `status` duplicates the HTTP
status code, `detail` is the specific error message for this occurrence, and
`instance` is the request path that produced it.

### `POST /paths/{pathId}/charge` — ReceiveChargeForecast

```sh
curl -X POST localhost:8080/paths/pick-a/charge \
  -H 'Content-Type: application/json' \
  -d '{"buckets":[{"cpt":"2026-08-21T12:00:00Z","quantity":100}]}'
```

### `POST /paths/{pathId}/plan` — CommitShiftPlan

```sh
curl -X POST localhost:8080/paths/pick-a/plan \
  -H 'Content-Type: application/json' \
  -d '{"plannedHeads":3,"installedStations":5,"rateUnitsPerHour":50,"hours":8}'
```

Fails with `400` if `plannedHeads > installedStations`.

### `POST /paths/{pathId}/work-units` — EnqueueWorkUnit

Requires an `Idempotency-Key` header (ADR-0022): this is the one mutating
endpoint that creates a genuinely new resource, so a retried request with
the same key and the same body is guaranteed to return the identical
response and create exactly one work unit, never a duplicate. A retry
with the same key but a *different* body gets `422`
(`idempotency-key-reused`); a missing header gets `400`
(`idempotency-key-required`).

```sh
curl -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: wu-1-attempt-1' \
  -d '{"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-line-1"}'
```

### `POST /paths/{pathId}/release` — ReleaseNextWork

Applies the release policy: admits the pending work unit with the earliest
(most urgent) CPT.

```sh
curl -X POST localhost:8080/paths/pick-a/release
```

Fails with `409` if the pool is empty or a release-fed pool is at its WIP
limit.

### `POST /work-units/{id}/complete` — RecordCompletion

```sh
curl -X POST localhost:8080/work-units/wu-1/complete
```

Fails with `409` if the unit was never released, or already completed.

### `GET /paths/{pathId}/telemetry` — SampleBacklog

```sh
curl localhost:8080/paths/pick-a/telemetry
curl "localhost:8080/paths/pick-a/telemetry?cutoffAt=2026-08-21T12:00:00Z"
```

Returns backlog depth, WIP, feed mode, and whether the path is over its
alarm threshold. Publishes `BacklogThresholdBreached` when over threshold.
With the optional `cutoffAt` (RFC3339) it also returns
`remainingCapacityKnown`/`remainingCapacityUnits` and publishes
`PathCapacityChanged` for that (path, CPT cutoff) pair — see
[ADR-0018](docs/docs/adr/0018-path-capacity-changed.md).

### `GET /paths/{pathId}/rebalance` — RebalanceDecision

```sh
curl localhost:8080/paths/pick-a/rebalance
```

Recommends `ThrottleUpstream` (flow-fed path over its alarm threshold) or
`ReassignLabor` (release-fed path saturated at its WIP limit with backlog
remaining), or `NoActionNeeded`.

### `GET /paths/{pathId}/labor-plan-view` — LaborPlanView (read model)

```sh
curl localhost:8080/paths/pick-a/labor-plan-view
```

The latest labor plan Workforce Management reported for this path, projected
from its `ShiftPlanCommitted` integration event. See **Integration** below —
this is a separate read model from this service's own `ShiftPlan` aggregate.
`404` if nothing has been observed yet.

### `GET /work-units?reference=` — GetWorkUnitsByReference

```sh
curl "localhost:8080/work-units?reference=order-line-1"
```

Every work unit ever enqueued with that `reference` (order-management's order
id), as an array. Read-only; an unknown reference returns `200` with `[]`.

### `GET /inventory-view/{sku}` — InventoryView (read model)

```sh
curl localhost:8080/inventory-view/sku-1
```

The latest usable-quantity projection for a SKU, from Inventory's
`StockReserved`/`ReservationRevoked` integration events. `404` if nothing
has been observed yet.

### `GET /healthz`

```sh
curl localhost:8080/healthz
```

## Local development / quality gate

A `Makefile` mirrors the CI sensors so they can be run locally, before pushing.
`make help` lists every target.

```sh
make check        # fast pre-commit loop: fmt-check, vet, build, lint, test -race
make check-all    # check + 90% coverage gate + arch-test + bdd (pre-push gate)
make vuln         # govulncheck ./... — known CVEs in deps and the Go stdlib
make mutation     # fast blocking mutation subset CI enforces (./internal/domain/release)
make contract     # Schemathesis property-based contract tests vs apis/openapi.yaml
```

`make integration` and `make mutation-all` are excluded from both bundles: the
first needs a running Postgres (`DATABASE_URL`), the second is the slow,
exhaustive mutation run that CI keeps on a weekly schedule. `make contract`
boots the service with its in-memory adapters (plus a baked-in process-path
catalogue) and needs `schemathesis==4.28.0` (`st`) installed — see
`scripts/contract-test.sh`.

Git hooks are managed with [lefthook](https://github.com/evilmartians/lefthook)
via `lefthook.yml` — `pre-commit` runs `make fmt-check`, `make vet` and
`make lint`; `pre-push` runs `make check`. Activate them once per clone:

```sh
brew install lefthook          # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

## Testing

```sh
go build ./...
go vet ./...
go test ./...
```

Integration tests are build-tagged (`-tags=integration`). The Kafka suites
(`internal/adapters/inbound/kafka`, `internal/adapters/outbound/kafkacatalog`)
and the Postgres outbox/migration suites start their own brokers and databases
with testcontainers, so they need Docker but no environment variables:

```sh
go test -tags=integration ./...
```

The older Postgres repository suite (`integration_test.go`,
`tracing_integration_test.go`) is still skipped unless `DATABASE_URL` is set:

```sh
DATABASE_URL="postgres://wes:wes@localhost:5432/wes?sslmode=disable" \
  go test -tags=integration ./internal/adapters/outbound/postgres/...
```

### BDD / Acceptance tests

Executable specifications live in `features/` as Gherkin `.feature` files and
are run with [godog](https://github.com/cucumber/godog), the official Cucumber
BDD framework for Go. The suite is black-box: it starts the real chi router
over the in-memory adapters in an `httptest` server and drives the REST API
with real HTTP calls (`features_test.go` at the repo root).

```sh
go test ./... -run TestFeatures -v
```

Scenarios cover committing a ShiftPlan against installed-station capacity,
CPT-priority Release with at-most-once handout, RecordCompletion plus the
backlog telemetry read model, and flow-balancing rebalance decisions. CI runs
them in the `bdd` job.

## Integration

This service both publishes and consumes integration events over Kafka
(`github.com/segmentio/kafka-go`), on the shared broker other warehouse-systems
services also use — the in-cluster Kafka release in the `warehouse` kind
cluster, reachable from the host at `localhost:9092`. This repo's
`docker-compose.yml` runs Postgres only. Every message on every topic
(integration and analytics, produced and consumed) is a **CloudEvents 1.0**
event in structured content mode — mandatory, with no flat envelope and no
dual mode ([ADR-0027](./docs/docs/adr/0027-cloudevents-mandatory-event-envelope.md)).
Produced messages carry the Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`; consumers
dispatch on the full `type` and dead-letter anything that is not a valid
CloudEvent:

```json
{
  "specversion": "1.0",
  "id": "uuid-v4",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-10231",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": { }
}
```

### Publishes

Topic `warehouse.work-planning.events`:

- **`WorkReleased`** — published when `ReleaseNextWork` releases a unit.
  `data`: `{"path_id","work_unit_id","cpt","ref"}`. Consumed downstream by
  fulfillment-execution, which turns it into a Task.

  Additive: `data` also carries two OPTIONAL fields when there is a hint to
  give — `required_capabilities` (array, containing `"hazmat"` when the
  released unit's SKU is classified `Hazmat` in inventory-storage) and
  `fragile` (bool, `true` when the SKU is classified `Fragile`). Looked up
  once, synchronously, from inventory-storage's
  `GET /products/{sku}/classification` at publish time — see
  [ADR-0009](./docs/docs/adr/0009-product-classification-propagation-to-work-released.md).
  Both fields are omitted, not defaulted to empty/false, when unavailable.
- **`PathCapacityChanged`** — published when `SampleBacklog` is called with a
  `cutoffAt` (`GET /paths/{pathId}/telemetry?cutoffAt=`). `data`:
  `{"path_id","cutoff_at","remaining_units","known"}` — remaining admission
  capacity per (path, CPT cutoff). Consumed by order-management's
  `kafkapathcapacity` adapter (its `PathCapacity` port, keyed by path and
  cutoff instant) — see [ADR-0018](./docs/docs/adr/0018-path-capacity-changed.md).

The other domain events are published to the same topic with a
`{"path_id", ...}` payload; no other service consumes them today.

Set `EVENT_PUBLISHER=kafka` (and `KAFKA_BROKERS`) to publish here instead of
the default `log` publisher; `internal/adapters/outbound/kafka` implements the
same `ports.EventPublisher` interface the log publisher does.

### Consumes

- Topic `warehouse.workforce.events`, event type `ShiftPlanCommitted` —
  `data`: `{"building_id","shift_id","path_id","planned_heads","planned_rate","planned_hours"}`.
  Projected into the `LaborPlanObserved` read model
  (`GET /paths/{pathId}/labor-plan-view`) — **not** fed into this service's own
  `ShiftPlan`/`CommitShiftPlan`, which is a different model in a different
  bounded context that happens to share the term "ShiftPlan".
- Topic `warehouse.inventory.events`, event types `StockReserved` and
  `ReservationRevoked` — `data` for both: `{"sku","quantity","demand_ref"}`.
  `StockReserved` decrements, `ReservationRevoked` increments, the
  `UsableInventoryObserved` read model, keyed by SKU
  (`GET /inventory-view/{sku}`).
- Topic `warehouse.fulfillment.events`, event type `TaskCompleted` — `data`:
  `{"task_id","station_id","work_unit_id"}`. Closes the control loop's
  feedback edge from fulfillment-execution back to this service:
  `data.work_unit_id` is fed directly into the existing `RecordCompletion` use
  case (`RecordCompletionRequest.WorkUnitId`), transitioning that work unit
  from Released to Completed exactly as `POST /work-units/{id}/complete`
  would. No new use case — this is additive wiring only.
- Topic `warehouse.order-management.events`, event types `OrderAllocated` and
  `OrderPartiallyAllocated` — `data` (identical shape for both):
  `{"order_id","promise_date","lines":[{"line_no","sku","path_id","gift_wrap"}]}`.
  Replaces order-management's former synchronous call to
  `POST /paths/{pathId}/work-units` with event choreography: order-management
  publishes here once it has allocated stock and locally marked an order line
  Released. For each line, `data.order_id`/`line.line_no` derive a
  deterministic `work_unit_id` (`"{order_id}-line-{line_no}"`), and the
  existing `EnqueueWorkUnit` use case is called directly — no new use case.
  Deliberately fire-and-forget: there is no reply event back to
  order-management; the existing `WorkUnitCreated`/`WorkReleased` events on
  `warehouse.work-planning.events` remain the only observable signal of
  downstream progress, same as for every other `EnqueueWorkUnit` caller.

Setting `KAFKA_BROKERS` starts this consumer automatically, independent of
`EVENT_PUBLISHER`, under the consumer group `KAFKA_CONSUMER_GROUP` (default
`wes-work-planning`).

With `PATH_CATALOGUE_SOURCE=kafka`, a separate consumer also replays
process-path-management's `warehouse.process-path-management.events`
(`ProcessPathCreated`/`ProcessPathUpdated`/`ProcessPathDeactivated`) into the
in-memory process-path catalogue that validates every `pathId`. It uses its own
per-process consumer group so every process replays the full history.

### Idempotency

This section covers Kafka consumer idempotency. For the `Idempotency-Key`
HTTP header on `POST /paths/{pathId}/work-units`, see ADR-0022 and that
endpoint's own section above — a separate, request/response-level
mechanism, not related to the Kafka de-duplication described here.

Kafka is at-least-once. Every consumed event's `event_id` is recorded in
`processed_events` (Postgres) / an in-memory set before its effect is
applied; a redelivered `event_id` is skipped (and still acked) rather than
re-applied. See `internal/application/usecases/observe_labor_plan.go` and
`observe_inventory_change.go`. `TaskCompleted` reuses this same mechanism from
the inbound Kafka adapter itself (`internal/adapters/inbound/kafka/consumer.go`)
since it calls `RecordCompletion` directly rather than through a projector use
case; `RecordCompletion`'s own domain-level double-complete rejection is a
second, independent safety net, not a substitute for it.

### Smoke-testing against the shared broker

With the `warehouse` kind cluster running and this service running locally
with `KAFKA_BROKERS=localhost:9092` and a unique `KAFKA_CONSUMER_GROUP` (so it
does not compete with the deployed pod for the partition):

```sh
kubectl --context kind-warehouse -n warehouse-systems exec -i kafka-controller-0 -c kafka -- \
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic warehouse.workforce.events <<'EOF'
{"specversion":"1.0","id":"evt-1","type":"com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted","source":"/warehouse/workforce-management","subject":"shift-1","time":"2026-08-21T20:00:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1","data":{"building_id":"bldg-1","shift_id":"shift-1","path_id":"pick-a","planned_heads":7,"planned_rate":95.5,"planned_hours":8}}
EOF

curl localhost:8080/paths/pick-a/labor-plan-view
# {"pathId":"pick-a","plannedHeads":7,"plannedRate":95.5,"plannedHours":8,"observedAt":"..."}
```

Same pattern for `warehouse.inventory.events` /
`com.warehouse.wms.inventory-storage.reservation.StockReserved` /
`GET /inventory-view/{sku}`.

For `TaskCompleted`, first get a work unit into Released state (enqueue then
release it), then publish the completion event and re-query the unit:

```sh
curl -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Idempotency-Key: wu-1-attempt-1' \
  -d '{"workUnitId":"wu-1","cpt":"2026-08-21T23:00:00Z","reference":"ref-1"}'
curl -X POST localhost:8080/paths/pick-a/release

kubectl --context kind-warehouse -n warehouse-systems exec -i kafka-controller-0 -c kafka -- \
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic warehouse.fulfillment.events <<'EOF'
{"specversion":"1.0","id":"evt-task-1","type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted","source":"/warehouse/fulfillment-execution","subject":"task-1","time":"2026-08-21T23:05:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1","data":{"task_id":"task-1","station_id":"station-1","work_unit_id":"wu-1"}}
EOF

curl localhost:8080/paths/pick-a/telemetry
# work unit wu-1 is now Completed
```

## Observability

The service is instrumented with OpenTelemetry: traces and metrics are pushed
over **OTLP/gRPC** to a Collector, and logs are structured JSON on `log/slog`
carrying the active span's `trace_id`/`span_id`.

### Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`, case-insensitive. JSON to stdout. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | Collector's OTLP/gRPC receiver. Accepts the spec URL form (`http://host:4317`) or bare `host:port`. |
| `OTEL_SERVICE_NAME` | `wes-work-planning` | `service.name` resource attribute. |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute. |
| `ENVIRONMENT` | `local` | `deployment.environment.name` resource attribute. |

A Collector is *expected* at `OTEL_EXPORTER_OTLP_ENDPOINT` but never
*required*: the OTLP exporters dial lazily, so with nothing listening the
telemetry is silently dropped and the service starts and serves normally. In
the `warehouse-infra` kind cluster the Helm chart points this at the
in-cluster Collector Service (`charts/wes-work-planning/values.yaml`, the
`otel` block).

### What gets exported

**Traces**

- One server span per HTTP request (`github.com/riandyrn/otelchi`), named
  after the chi **route pattern** (`/paths/{pathId}/release`) rather than the
  raw path, so span names stay low-cardinality.
- A child span per database call (`github.com/exaring/otelpgx` as the pgx v5
  pool tracer), carrying the normalized SQL — parameter placeholders, never
  literal values.
- `kafka.publish <topic>` on the producer side and `kafka.consume <topic>` on
  the consumer side, per the OTel messaging semantic conventions. Trace
  context crosses the broker in the message's `traceparent` header
  (`internal/adapters/kafka/otelkafka`), so a `WorkReleased` published here
  and consumed by `fulfillment-execution` is one distributed trace — and a
  `ShiftPlanCommitted` published by `workforce-management` continues its
  trace into this service's projector.

**Metrics**

- `http.server.request.duration` (histogram, seconds) and
  `http.server.active_requests`, from otelchi's metric middleware.
- `wes.work_units.released` — the business metric: a counter incremented in
  the `ReleaseNextWork` use case (not in the HTTP handler, so it tracks the
  real domain event), attributed by `path_id`.
- Go runtime metrics — goroutines, GC, memory —
  (`go.opentelemetry.io/contrib/instrumentation/runtime`).

**Logs**

Every record is JSON on stdout. Records emitted while a span is active carry
that span's IDs, so logs join traces:

```json
{"time":"...","level":"INFO","msg":"http request","method":"POST","route":"/paths/{pathId}/release","status":200,"trace_id":"21da85e3a34ce1fabc425b63dfb148c6","span_id":"3af54929e6a0094c"}
```

The OTel SDK's own diagnostics are bridged onto the same logger at `debug`,
so nothing escapes as plain text.

### Smoke-testing trace propagation

With the shared broker running, publish a `WorkReleased` by releasing work,
then read the message back and compare its `traceparent` with the `trace_id`
of the request that produced it:

```sh
KAFKA_BROKERS=localhost:9092 EVENT_PUBLISHER=kafka LOG_LEVEL=info go run ./cmd/wes

curl -X POST localhost:8080/paths/pick-a/work-units \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: wu-1-attempt-1' \
  -d '{"workUnitId":"wu-1","cpt":"2026-08-23T23:00:00Z","reference":"order-1"}'
curl -X POST localhost:8080/paths/pick-a/release

kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.work-planning.events --property print.headers=true --from-beginning
```

The reverse direction works the same way: publish a `ShiftPlanCommitted` with
a `traceparent` header onto `warehouse.workforce.events` and the projector's
log line comes out carrying that same `trace_id`.

## Invariants

Three aggregate invariants are enforced in the domain layer and each has a
failing-path unit test:

- **ShiftPlan/PathPlan**: `plannedHeads <= installedStations`
  (`internal/domain/plan/path_plan_test.go`).
- **WorkUnit**: assigned/released at most once, cannot complete twice, and
  cannot complete before release (`internal/domain/workunit/work_unit_test.go`).
- **WorkPool**: at-most-once handout per entry, and a release-fed pool's WIP
  limit is a hard invariant on release (`internal/domain/release/work_pool_test.go`).

## Documentation

Full documentation site: **<https://claudioed.github.io/wes-work-planning/>**

It covers the business context and ubiquitous language, the DDD model
(subdomain classification, every aggregate and invariant, all ten domain
events), an API reference generated from `apis/openapi.yaml` plus an Events
page from `apis/asyncapi.yaml`, the ecosystem context map, the MCP governance
charter, the analytics report contract, and eighteen architecture decision
records. Source lives in [`docs/`](./docs) (Docusaurus);
it is built and deployed to GitHub Pages by
[`.github/workflows/docs.yml`](./.github/workflows/docs.yml).

## Operator micro-frontend (`web/`)

`web/` is `planning_mfe`, this context's Module Federation remote. It talks only to
this service's own REST API and is never part of `make check`.

**Standalone development** is unchanged:

```bash
cd web && npm install && npm run dev     # http://localhost:5183
```

**Deployed to the kind cluster**, it is built into a static bundle and served
by its own `nginx-unprivileged` pod:

```bash
cd web
docker build --build-context uikit=../../warehouse-ui-kit \
  -t warehouse/wes-work-planning-frontend:local .
```

The cluster's localhost topology separates the two kinds of traffic onto two
independent entrypoints, and neither proxies to the other:

| URL | Served by | Carries |
|---|---|---|
| `http://localhost/mfes/wes-work-planning/` | Nginx web gateway → this remote's nginx pod | HTML, JS, CSS, fonts, `remoteEntry.js` |
| `http://localhost:8000/api/wes-work-planning/` | Kong | this service's REST API |

Kong never serves frontend assets, and the Nginx gateway never proxies an API.
Enable the workload with `frontend.enabled=true` in the Helm chart; the Service
is deliberately `ClusterIP` with no Ingress/HTTPRoute, because frontend path
routing belongs to the Nginx web gateway in `warehouse-infra`.

Because one image must work in more than one environment, the remote reads its
API origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (published
by the console shell) rather than baking a hostname in at build time. A
production build with no runtime config **fails loudly** instead of silently
falling back to a developer port; standalone `npm run dev` still uses
`http://localhost:8083`. See `web/src/config.ts`.

Chart invariants are asserted by:

```bash
python3 charts/wes-work-planning/tests/test_service_selectors.py
```

which proves every Service selects exactly one Deployment — the OLTP Service
must never select the frontend, analytics or MCP pods.
