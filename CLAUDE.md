# Project: WES — Work Planning & Release (Core Bounded Context)

> **Study project.** Educational DDD exercise using real WMS/WES/WCS
> terminology (waveless release, CloudEvents, RFC 7807, hexagonal
> architecture). Not a production system, not affiliated with any real-world
> company.

This service is the **core domain** of a Warehouse Execution System: it turns
a shift's **charge** (volume due by each deadline) into a **plan** (rate ×
heads per process path), **releases work continuously** (waveless), and
performs **flow balancing** using live buffer telemetry. It is the fleet's
"conductor" — downstream of WMS planning/inventory, upstream of WCS equipment
control.

Source of truth for the domain model: the DDD reference at
`/Users/claudioed/docs/amazon-fulfillment-ddd.md` and
`/Users/claudioed/warehouse-systems-ddd.md`. Honor the ubiquitous language
defined there and in `.claude/rules/domain-model.md`.

## Project Overview

- Module: `github.com/claudioed/wes-work-planning`, Go 1.26.6.
- Three deployable Go binaries (`cmd/wes`, `cmd/wes-projector`,
  `cmd/wes-reports`) plus an MCP server (`cmd/mcp`) and a standalone frontend
  micro-frontend (`web/`, Vite + React Module Federation remote).
- GitFlow: `develop` is the default working branch, `main` is release-only.
- Contracts are spec-first: `apis/openapi.yaml` (REST, Spectral-linted) and
  `apis/asyncapi.yaml` (Kafka events, Spectral-linted) are the sources of
  truth — the Docusaurus site under `docs/` **generates** its REST reference
  pages from `apis/openapi.yaml` at build time (`docs/docs/api/rest/*.api.mdx`,
  via the `prebuild` npm script); never hand-edit those generated files.

## Architecture (NON-NEGOTIABLE)

Hexagonal / Ports & Adapters. Strict dependency rule: **domain depends on
nothing; application depends on domain; adapters depend on
application/domain.** No framework or SQL types in the domain layer.

Full layer-by-layer directory map, the additive analytics data product
(ADR-0011), and the `web/` micro-frontend boundary are documented in
**`.claude/rules/architecture.md`** — read it before touching any adapter or
adding a new bounded-context boundary.

## Key Commands

```sh
# Every run needs the process-path catalogue (ADR-0012) — default
# PATH_CATALOGUE_SOURCE=file refuses to start without PATH_CATALOGUE_FILE
export PATH_CATALOGUE_FILE=../warehouse-infra/config/process-paths/sortable-fc.yaml

# Run in-memory (no infra)
go run ./cmd/wes                       # :8080

# Run against Postgres (cmd/wes applies migrations/ itself on start)
docker compose up -d                   # Postgres only; Kafka is the shared cluster broker
DATABASE_URL="postgres://wes:wes@localhost:5432/wes?sslmode=disable" go run ./cmd/wes

# Analytics data product (separate DB + Kafka fan-out)
go run ./cmd/wes-projector              # :8091 — the ONLY writer
go run ./cmd/wes-reports                # :8092 — read-only reports API

# Docs site (Docusaurus) — regenerates REST reference from apis/openapi.yaml
cd docs && npm ci && npm run build      # prebuild hook runs clean-api-docs + gen-api-docs
```

Local quality gate — **run before every commit**:

```sh
make check       # fmt-check, vet, build, lint, test -race — fast, run every change
make check-all   # + coverage gate (90%), arch-test, bdd — run before pushing
make vuln        # govulncheck — run when touching go.mod
make mutation    # fast blocking mutation subset (CI-enforced thresholds in .gremlins.yaml)
```

`lefthook install` wires these into git hooks (pre-commit: fmt-check/vet/lint;
pre-push: `make check`), but proactively run `make check` yourself rather than
relying on the hook firing.

## Code Standards / Testing

- Router: `chi` (`go-chi/chi/v5`). DB: `pgx/v5` + `pgxpool`. Migrations:
  `golang-migrate` SQL files under `migrations/` (OLTP) and
  `migrations/analytics/` (analytics store, applied only by `cmd/wes-projector`).
- Errors: domain returns typed errors; the HTTP adapter maps them to RFC 7807
  `application/problem+json` responses (ADR-0005) — never a bespoke error
  shape.
- Tests: table-driven. Domain + application unit tests use the in-memory
  adapter. At least one httptest integration test per endpoint. The Postgres
  repo has a build-tagged (`//go:build integration`) test, skipped without
  `DATABASE_URL`. Kafka-touching integration tests MUST use testcontainers
  (`github.com/testcontainers/testcontainers-go/modules/kafka`), never a
  skip-gated `KAFKA_BROKERS` check against an external broker — CI's
  `integration` job has no Kafka service, so a skip-gated test silently
  proves nothing there.
- `gofmt`/`go vet` clean. Every package has a short doc comment.
- Definition of done for any change: `go build ./...`, `go test ./...`
  (unit + httptest), `go vet ./...` all green; README updated if the change
  touches run instructions, env vars, or the API surface; any new/changed
  aggregate invariant has a failing-path test.

## Domain Model

Ubiquitous language, the four aggregates and their enforced invariants, the
ten domain events, and the seven core use cases are documented in
**`.claude/rules/domain-model.md`** — read it before writing any use case or
domain logic. Use those exact terms; do not invent synonyms.

## REST API & Cross-Service Integration

- REST surface (11 operations across health/charge/plan/work-units/release/
  telemetry/rebalance/labor-plan-view/inventory-view), the Kafka integration
  event contract (published + consumed topics, envelope, idempotency), and
  the read-only projections built from consumed events (`LaborPlanObserved`,
  `UsableInventoryObserved`), the `KAFKA_CONSUMER_GROUP` override, and the
  `PATH_CATALOGUE_SOURCE=file|kafka` process-path catalogue are documented in
  **`.claude/rules/integration-events.md`**.
- Full request/response schemas, every status code, and the shared `Problem`
  error component live in [`apis/openapi.yaml`](./apis/openapi.yaml) — this
  is the spec of record; the Docusaurus REST reference is generated from it,
  never edited by hand.
- Async contract of record is [`apis/asyncapi.yaml`](./apis/asyncapi.yaml)
  (CloudEvents 1.0 over Kafka — mandatory, see the Events section below);
  narrative pages in `docs/docs/api/events.md`
  and `docs/docs/ecosystem/integration-events.md` are written from it and
  should be updated by hand whenever a message/channel changes there (this
  fleet documents AsyncAPI narratively per-service; there is no generated
  AsyncAPI static site in this repo — that only exists in the separate
  fleet-wide docs aggregator).

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/`; transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/wes-work-planning`, `type`, `subject` (aggregate id), `time`
  (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:wes-work-planning:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.work-planning.<entity>.<EventName>`. Breaking payload
  change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

Full standard and the fleet's cross-service type catalogue: ADR-0027
(`docs/docs/adr/`).

## Architecture Decision Records

18 ADRs under `docs/docs/adr/` cover hexagonal layering (0001), waveless
release (0002), flow balancing (0003), Kafka integration events (0004),
RFC 7807 (0005), the Labor Plan View vs. Workforce's own ShiftPlan model
distinction (0006), Go architecture fitness tests (0007), the MCP inbound
adapter (0008), product classification propagation (0009), gift-wrap as a
`WorkReleased` characteristic (0010), the analytics data product (0011),
process-path catalogue validation (0012), the standard metrics convention
(0013), the transactional outbox (0014), and the two REST-identity /
static-bearer-auth ADRs (0015 added it, 0016 records its fleet-wide removal),
the facility-layout travel-distance lookup on CommitShiftPlan (0017), the
`PathCapacityChanged` integration event consumed by order-management (0018),
and — among the later records (0019–0026) — the now-superseded CloudEvents
dual-mode migration plan (0021). ADR-0027 makes CloudEvents 1.0 the mandatory
envelope and supersedes 0021 and the envelope parts of 0004.
Read the relevant ADR before reversing a documented decision.

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `internal/adapters/**/kafka/**`, `internal/adapters/outbound/events/**`, `apis/asyncapi*` | `.claude/rules/integration-events.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->
