---
paths:
  - "cmd/**"
  - "internal/**"
  - "web/**"
  - "charts/**"
  - "migrations/**"
---

# Architecture reference — wes-work-planning

## Layer map

```
cmd/wes/                     main.go — wiring/composition root for the OLTP service
cmd/wes-projector/           analytics data-product writer (the ONLY writer of the analytics DB)
cmd/wes-reports/             analytics data-product reader (GET /reports/...)
cmd/mcp/                     MCP inbound adapter binary (ADR-0008)

internal/
  domain/                    pure Go: aggregates, value objects, domain events, domain errors
    charge/                  ChargeForecast aggregate
    plan/                    ShiftPlan, PathPlan aggregates
    release/                 WorkPool aggregate, release policy (domain service)
    workunit/                WorkUnit aggregate
    shared/                  value objects: CPT, Rate, PathId, Quantity, domain events
    laborview/                read-only LaborPlanObserved projection (NOT an aggregate — see ADR-0006)
    inventoryview/           read-only UsableInventoryObserved projection, keyed by SKU
    pathcatalog/             process-path catalogue value type (ADR-0012)
    productclassificationview/  SKU -> ProductClassification view value (ADR-0009; local copy since ADR-0035)
    traveldistanceview/      TravelDistance read-model value from facility-layout (ADR-0017)

  application/
    ports/                   driven-port interfaces (repositories, event publisher, clock, lookups)
    usecases/                one struct per use case, orchestrates domain

  adapters/
    inbound/http/            REST handlers (chi), DTOs, RFC 7807 error mapping
    inbound/kafka/           consumer for warehouse.workforce.events, warehouse.inventory.events,
                              warehouse.fulfillment.events, warehouse.order-management.events,
                              warehouse.network-inventory-planning.events; plus the
                              ProductClassified consumer of warehouse.product-master.events
                              (ADR-0035, own group PRODUCT_CLASSIFICATION_CONSUMER_GROUP)
    inbound/mcp/             MCP tool registrations (ADR-0008)
    outbound/postgres/       repository impls (pgx), transactional outbox + relay (ADR-0014)
    outbound/memory/         in-memory repo impls for tests/local
    outbound/events/         log-based publisher + MultiPublisher (fan-out when no Postgres)
    outbound/kafka/          integration + analytics Kafka publishers, outbox Encoders, RelaySink
    outbound/kafkacatalog/   Kafka-sourced path-catalogue adapter (PATH_CATALOGUE_SOURCE=kafka;
                              replays warehouse.process-path-management.events, own consumer group)
    outbound/filecatalog/    file-sourced path-catalogue adapter (default; PATH_CATALOGUE_FILE)
    outbound/productclassificationcopy/  local copy of product-master's classification (Postgres
                              Store, MemoryStore, PermissiveLookup; PRODUCT_CLASSIFICATION_MODE=
                              kafka|permissive, ADR-0035) — no live lookup to inventory-storage
    outbound/traveldistance/ synchronous HTTP client to facility-layout GET /distance (ADR-0017)
    outbound/analyticsstore/ analytics Postgres reader/writer
    outbound/telemetry/      OpenTelemetry wiring / metrics adapters

migrations/                  golang-migrate SQL files (OLTP schema)
migrations/analytics/        separate analytical schema, applied only by cmd/wes-projector
```

The domain package imports nothing outside the Go standard library. The
application layer depends only on domain types and the `ports` interfaces it
defines. Adapters are the only layer allowed to import a framework, a SQL
driver, or an HTTP router. `internal/architecture/architecture_test.go` is a
Go fitness test (ADR-0007) that enforces this dependency rule — run it via
`make arch-test`.

## Analytics data product (ADR-0011)

Additive read side built from this service's OWN domain events. The OLTP
domain/application layers are NOT modified and must NOT import the analytics
store (the arch-test above enforces this).

- Events are fanned to a SEPARATE topic `warehouse.wes.analytics` by
  `internal/adapters/outbound/kafka/analytics_publisher.go`; the integration
  topic/publisher (`warehouse.work-planning.events`) are untouched. Selected
  by `EVENT_PUBLISHER=kafka` (fan-out alongside the integration publisher via
  `MultiPublisher`).
- Separate analytical Postgres (`ANALYTICS_DATABASE_URL`), own migrations
  (`migrations/analytics/`), intended as a read-only reader role for
  `cmd/wes-reports`.
- Three processes, one writer: `cmd/wes` (OLTP), `cmd/wes-projector` (the
  ONLY writer; consumes the analytics topic from FirstOffset, idempotent on
  `event_id`), `cmd/wes-reports` (read-only reader, `GET /reports/...`).
- Report: **Release Throughput & Backlog Health**, keyed per path × hour
  (work released/completed, backlog breaches, path throttles, rate
  deviations). Contract: `docs/docs/analytics/release-throughput-report.md`.
- `GET /reports/throughput/freshness` reports projection lag.
- The MCP server exposes a read-only `get_release_throughput_report` tool
  when `REPORTS_BASE_URL` is set — it calls the reports REST API, never the
  analytical database directly.

## Frontend micro-frontend remote (`web/`)

This repo also owns `web/`: `planning-mfe`, a Vite + React Module Federation
**remote** consumed by the separate `warehouse-console` shell repo. It is a
plain browser client of this service's own REST API (path
telemetry/rebalance dashboard, work-unit-by-reference search) — nothing in
`web/` talks to any other bounded context, and nothing in `internal/` knows
`web/` exists. `web/` has its own `package.json`, build, and dev server
(`:5183`); it does not participate in this repo's Go quality gate and is not
part of the Go module.

CORS middleware (`go-chi/cors`) is enabled on every REST route, allowing
`CORS_ALLOWED_ORIGINS` (env, default
`http://localhost:5173,http://localhost:5183` — the `warehouse-console`
shell and this service's own `planning-mfe` remote).

JSON DTOs live in the http adapter; never leak domain structs directly across
the wire.

## Deployment

Helm chart under `charts/wes-work-planning/`. The MCP server ships in the
same image as the OLTP service (`/app/mcp`, built from `cmd/mcp`) and is
deployed as a separate Deployment + ClusterIP Service (`<release>-mcp`, port
`8090`) only when `mcp.enabled=true` (off by default). It reuses the OLTP
`DATABASE_URL` secret and, when `analytics.enabled=true`, is pointed at the
reports Service.
