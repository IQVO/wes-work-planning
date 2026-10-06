---
paths:
  - "cmd/**"
  - "internal/**"
  - "migrations/**"
  - "features/**"
  - "docker-compose.yml"
  - "Makefile"
  - ".golangci.yml"
  - ".gremlins.yaml"
---

# Running locally, code standards and testing (detail)

## Run

```sh
# Every run needs the process-path catalogue (ADR-0012) — default
# PATH_CATALOGUE_SOURCE=file refuses to start without PATH_CATALOGUE_FILE
export PATH_CATALOGUE_FILE=../warehouse-infra/config/process-paths/sortable-fc.yaml

# Run in-memory (no infra)
go run ./cmd/wes                       # :8080

# Run against Postgres (cmd/wes applies migrations/ itself on start)
docker compose up -d                   # Postgres only; Kafka is the shared cluster broker
DATABASE_URL="postgres://wes:***@localhost:5432/wes?sslmode=disable" go run ./cmd/wes

# Analytics data product (separate DB + Kafka fan-out)
go run ./cmd/wes-projector              # :8091 — the ONLY writer
go run ./cmd/wes-reports                # :8092 — read-only reports API

# Docs site (Docusaurus) — regenerates REST reference from apis/openapi.yaml
cd docs && npm ci && npm run build      # prebuild hook runs clean-api-docs + gen-api-docs
```

Other gates: `make vuln` (govulncheck, run when touching `go.mod`);
`make mutation` (fast blocking mutation subset, CI-enforced thresholds in
`.gremlins.yaml`). `lefthook install` wires pre-commit (fmt-check/vet/lint) and
pre-push (`make check`) hooks, but proactively run `make check` yourself
rather than relying on the hook firing.

## Code standards

- Module `github.com/claudioed/wes-work-planning`, Go 1.26.6. GitFlow:
  `develop` is the default working branch, `main` is release-only.
- Three binaries (`cmd/wes`, `cmd/wes-projector`, `cmd/wes-reports`) plus the
  MCP server (`cmd/mcp`).
- Router: `chi` (`go-chi/chi/v5`). DB: `pgx/v5` + `pgxpool`. Migrations:
  `golang-migrate` SQL files under `migrations/` (OLTP) and
  `migrations/analytics/` (analytics store, applied only by `cmd/wes-projector`).
- Errors: domain returns typed errors; the HTTP adapter maps them to RFC 7807
  `application/problem+json` responses (ADR-0005) — never a bespoke error shape.
- `gofmt`/`go vet` clean. Every package has a short doc comment.

## Testing

- Table-driven. Domain + application unit tests use the in-memory adapter. At
  least one httptest integration test per endpoint. The Postgres repo has a
  build-tagged (`//go:build integration`) test, skipped without `DATABASE_URL`.
- Kafka-touching integration tests MUST use testcontainers
  (`github.com/testcontainers/testcontainers-go/modules/kafka`), never a
  skip-gated `KAFKA_BROKERS` check against an external broker — CI's
  `integration` job has no Kafka service, so a skip-gated test silently proves
  nothing there.
- Definition of done: `go build ./...`, `go test ./...` (unit + httptest),
  `go vet ./...` all green; README updated if the change touches run
  instructions, env vars, or the API surface; any new/changed aggregate
  invariant has a failing-path test.
