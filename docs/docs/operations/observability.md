---
id: observability
title: Observability
sidebar_label: Observability
description: Every OpenTelemetry metric instrument, span and log field the four binaries emit, how they are exported, the warehouse-infra dashboard that reads them, and suggested alerts.
---

# Observability

All four binaries call the same `telemetry.Setup`
(`internal/adapters/outbound/telemetry/telemetry.go`): traces and metrics are
**pushed over OTLP/gRPC** (insecure) to `OTEL_EXPORTER_OTLP_ENDPOINT`
(default `localhost:4317`); there is **no `/metrics` scrape endpoint** in
any binary. Exporters dial lazily, so with no Collector listening the data
is dropped and export errors go to the log at `debug` — the service never
fails or blocks because of telemetry. Metrics use the SDK's periodic reader;
traces use a batching span processor.

Every signal carries the resource attributes `service.name`
(`OTEL_SERVICE_NAME`; defaults `wes-work-planning`, `wes-work-planning-mcp`,
`wes-projector`, `wes-reports`), `service.version` (`SERVICE_VERSION`) and
`deployment.environment.name` (`ENVIRONMENT`). Propagation is W3C
`traceparent` plus `baggage`, over HTTP and across Kafka headers.

In the kind cluster the Collector's Prometheus exporter turns instrument
names into series names (dots become underscores, plus unit and `_total`
suffixes — for example `wes.work_units.released` is queried as
`wes_work_units_released_total`).

## Metrics

### Service-owned instruments

| Instrument | Type | Unit | Attributes | Emitted by | Source |
|---|---|---|---|---|---|
| `wes.work_units.released` | Int64 counter | `{work_unit}` | `path.id` | `cmd/wes` and `cmd/mcp`, once per work unit actually released by `ReleaseNextWork` (recorded in the use case after the commit, not in the handler) ([ADR-0013](../adr/0013-standard-metrics-convention.md)) | `internal/adapters/outbound/telemetry/release_metrics.go` |
| `wes.outbox.lag_seconds` | Float64 observable gauge | `s` | — | `cmd/wes`, only with `EVENT_PUBLISHER=kafka` and `DATABASE_URL`. Age of the oldest unpublished `outbox_events` row, 0 when drained; sampled by a query at each collection | `internal/adapters/outbound/postgres/outbox_metrics.go` |
| `circuit_breaker.state` | Int64 gauge | `{state}` | `dependency` (`facility-layout`) | `cmd/wes` with `TRAVEL_DISTANCE_MODE=http`. `0` closed, `1` half-open, `2` open. Recorded **only on a state transition**, so no series exists until the breaker first changes state | `internal/adapters/outbound/telemetry/circuit_breaker_metrics.go`, `internal/adapters/outbound/traveldistance/breaker.go` |

### Library instruments

| Instrument | Type | Unit | Attributes | Emitted by | Library |
|---|---|---|---|---|---|
| `http.server.request.duration` | Float64 histogram | `s` | `http.method`, `http.scheme`, `http.route` (chi pattern); scope attribute `service.name` | `cmd/wes`, `cmd/mcp`, `cmd/wes-reports` (the projector's admin mux is not instrumented) | `github.com/riandyrn/otelchi/metric` v0.12.3 |
| `http.server.active_requests` | Int64 up-down counter | — | same | same | same |
| `db.client.operation.duration` | histogram | `s` | `db.system.name=postgresql`, `pgx.operation.type` | every binary with a Postgres pool | `github.com/exaring/otelpgx` v0.12.0 (pool tracer) |
| `db.client.operation.errors` | Int64 counter | — | same | same | same |
| `pgxpool.acquires`, `pgxpool.acquire_duration`, `pgxpool.acquired_connections`, `pgxpool.canceled_acquires`, `pgxpool.constructing_connections`, `pgxpool.empty_acquire`, `pgxpool.empty_acquire_wait_time`, `pgxpool.idle_connections`, `pgxpool.max_connections`, `pgxpool.max_idle_destroys`, `pgxpool.max_lifetime_destroys`, `pgxpool.new_connections`, `pgxpool.total_connections` | observable counters/gauges | — | `db.system.name`, `db.client.connection.pool.name` (`host:port/db`) | **`cmd/wes-projector` and `cmd/wes-reports` only** (`analyticsstore.RecordPoolStats`); the OLTP pool of `cmd/wes`/`cmd/mcp` registers no pool-stats instruments | `otelpgx.RecordStats` |
| `go.memory.used`, `go.memory.limit`, `go.memory.allocated`, `go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`, `go.processor.limit`, `go.config.gogc` | runtime | various | — | all four binaries | `go.opentelemetry.io/contrib/instrumentation/runtime` v0.71.0 |

The `http.route` attribute is the chi route pattern (`/paths/{pathId}/release`),
never the raw path, so cardinality is bounded by the route table.

## Traces

| Span name | Kind | Where | Notable attributes |
|---|---|---|---|
| chi route pattern, e.g. `/paths/{pathId}/release`, `/reports/throughput`, `/mcp` | server | otelchi middleware on the `cmd/wes`, `cmd/wes-reports` and `cmd/mcp` routers | HTTP semconv |
| `kafka.publish warehouse.work-planning.events`, `kafka.publish warehouse.wes.analytics` | producer | `Publisher.Publish` / `AnalyticsPublisher.Publish` — **direct** publishing only (no `DATABASE_URL`) | `messaging.system=kafka`, `messaging.operation.name=publish`, `messaging.destination.name`, `messaging.batch.message_count`, `messaging.event_types` |
| `kafka.consume <topic>` | consumer | integration consumer (5 topics), `ProductClassified` consumer, analytics projector | `messaging.kafka.offset`, partition, `messaging.message.id` (CloudEvents `id`), `cloudevents.event_type`, `cloudevents.event_source`; `product.sku` on the classification consumer |
| `mcp.tool <name>` | internal | every MCP tool call | `mcp.tool.name` |
| `query …`, `prepare …`, `batch query …`, connect/acquire | client | otelpgx tracer on every pool | normalized SQL, never parameter values |

Trace context crosses Kafka in the message headers
(`internal/adapters/kafka/otelkafka`). Under the outbox the headers are
captured when the row is **encoded inside the request's transaction**, so a
consumer's `kafka.consume` span continues the HTTP request (or consumer)
span that produced the event; the relay's later send adds no span of its own.

The process-path catalogue consumer (`kafkacatalog`) creates no spans.

## Logs

Every binary logs JSON to stdout through `log/slog`, at `LOG_LEVEL`. Records
emitted inside a span carry `trace_id` and `span_id`
(`telemetry.NewTraceHandler`). The OTel SDK's own diagnostics are bridged
onto the same logger.

The per-request access log (`internal/adapters/inbound/http/logging.go`,
`cmd/wes` and `cmd/wes-reports`; message `http request`, `ERROR` for 5xx,
`INFO` otherwise) has these fields — captured from a real run:

```json
{"time":"2026-10-12T14:02:56.3234-03:00","level":"INFO","msg":"http request","method":"POST","path":"/paths/pick-a/release","route":"/paths/{pathId}/release","status":200,"duration_ms":0,"bytes":168,"request_id":"host/Ae6N1jNENb-000008","trace_id":"9b39de83f1dd803926e4b1a52e44b03c","span_id":"fbf313e269e1bec3"}
```

`path` has CR/LF stripped (log-injection guard); `request_id` comes from
chi's `RequestID` middleware. With `EVENT_PUBLISHER=log` every domain event is
logged as `domain event published` with `event` and `occurred_at`.

Messages worth alerting or searching on:

| Message | Level | Meaning |
|---|---|---|
| `invalid CloudEvent, sending to dead-letter topic` | ERROR | a consumed message failed CloudEvents validation; fields `topic`, `partition`, `offset`, `dlq_topic` |
| `exhausted retries, sending to dead-letter topic` | ERROR | a handler failed 3 times; fields `id`, `type`, `attempts`, `error` |
| `kafka consumer stopped` | ERROR | the integration consumer's loop exited (a DLQ publish or commit failed) — nothing is consumed until restart |
| `TaskCompleted for a work unit this context never planned; skipping` | INFO | expected for PACK tasks fulfillment-execution created itself |
| `applying ProductClassified failed; retrying the same message` | ERROR | the classification consumer is stuck retrying one message |
| `skipping invalid CloudEvent on product-master topic` / `... on analytics topic` / `... on process-path catalogue topic` | WARN | poison message skipped |
| `analytics message handling failed` | ERROR | the projector failed to apply an already-committed event (it is not retried) |
| `outbox relay pass failed` | ERROR | relay could not claim or send; rows stay unpublished |
| `outbox relay: row reached the attempt cap and is no longer claimed` | ERROR | one row failed `OUTBOX_MAX_ATTEMPTS` sends and is excluded from claims (ADR-0039); fields `outbox_id`, `topic`, `event_type`, `key`, `attempts`, `max_attempts`, `error`. Alert on it: an event was not delivered |
| `outbox relay configured` | INFO | boot: the relay's `outbox_max_attempts` |
| `housekeeping sweep failed` / `housekeeping sweep removed rows` | ERROR / INFO | retention sweeper outcome |
| `retrying` / `succeeded after retry` | WARN / INFO | boot-time dial retry (`op` = `run migrations`, `ping postgres`, `connect to the process-path catalogue topic`, ...) |
| `kafka consumer did not stop before the shutdown deadline` (and the same for `outbox relay`, `product classification consumer`) | WARN | drain exceeded the 5 s shutdown budget |
| `circuit breaker metrics not registered; continuing without them`, `outbox lag gauge unavailable` | WARN | an instrument could not be created; the service runs without it |

## Dashboard

warehouse-infra ships a Grafana dashboard for this context,
`terraform/dashboards/contexts/wes-work-planning.json` (uid
`warehouse-wes-work-planning`, generated by
`scripts/gen-context-dashboards.py`). Its rows:

- **API Gateway (Kong)** — request rate, 5xx rate, p95 request vs upstream
  latency for the `httproute.warehouse-systems.wes-work-planning.*` service;
- **Service HTTP RED** — request rate by route, 5xx by process and p95 by
  process from `http_server_request_duration_seconds_*`, filtered to
  `service_name=~"wes-(work-planning|projector|reports).*"`;
- **Business metrics** — `wes_work_units_released_total`;
- **Go runtime** — goroutines and memory used;
- **Logs** (Loki, `app="wes-work-planning"`) — live stream, volume by level,
  errors and warnings.

The outbox lag, circuit-breaker and pgxpool instruments are exported but not
on that dashboard.

## Suggested alerts

These are suggestions derived from the instruments above; none are
provisioned in this repository.

| Alert | Condition (PromQL-style) | Why |
|---|---|---|
| Outbox stuck | `wes_outbox_lag_seconds > 60` for 5 m | relay down, Kafka unreachable, or one row failing repeatedly (check `outbox_events.last_error`) |
| Outbox row dead | any log line `outbox relay: row reached the attempt cap and is no longer claimed` | an event was excluded after `OUTBOX_MAX_ATTEMPTS` failed sends and will not be delivered until re-armed ([Runbook](./runbook.md#re-arm-an-outbox-row-that-hit-the-attempt-cap)) |
| Facility-layout breaker open | `circuit_breaker_state{dependency="facility-layout"} == 2` for 5 m | travel-distance hints are being skipped (fail-open) |
| REST 5xx ratio | 5xx share of `http_server_request_duration_seconds_count` for `service_name="wes-work-planning"` > 1 % for 10 m | Postgres/pool problems surface as `internal-error` |
| Release stalled | `rate(wes_work_units_released_total[15m]) == 0` while backlog exists (`GET /paths/{pathId}/telemetry` `backlogDepth > 0`) | WIP limit reached, pool wedged, or no release caller |
| DLQ traffic | any log line `sending to dead-letter topic`, or the `<topic>.dlq` end offset growing | poison or unprocessable upstream events |
| Report stale | `GET /reports/throughput/freshness` `lagSeconds > 30` (the freshness SLA) | projector down or lagging |
| Consumer dead | log `kafka consumer stopped` | integration consumption halted until the pod restarts |
