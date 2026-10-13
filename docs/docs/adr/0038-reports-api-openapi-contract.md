---
id: 0038-reports-api-openapi-contract
slug: /adr/0038-reports-api-openapi-contract
title: 0038. The reports API has its own OpenAPI contract
sidebar_label: 0038. Reports API OpenAPI contract
description: ADR 0038 — the three routes of cmd/wes-reports are specified in apis/openapi-reports.yaml (a separate spec from the OLTP apis/openapi.yaml), linted by Spectral, rendered into the docs and contract-tested against the real router (fleet gap fix D18).
---

# 0038. The reports API has its own OpenAPI contract

## Status

Accepted (2026-10-12, fleet gap fix D18). Adds a contract to
[ADR-0011](./0011-analytical-data-product.md) (the data product) without
changing its behaviour. Follows the fleet convention labor-performance
started (a `*-reports` binary's routes live in `apis/openapi-reports.yaml`).

## Context

`cmd/wes-reports` is the reader of the "Release Throughput & Backlog Health"
data product: a separate binary, on its own port (`:8092`), over a read-only
pool against the analytical database. It serves three routes:
`GET /reports/throughput`, `GET /reports/throughput/freshness` and
`GET /healthz`. The MCP report tool and warehouse-console call them.

None of the three was in a machine-readable contract. `apis/openapi.yaml`
describes `cmd/wes` only, so the reports routes were described by hand in the
runbook and the analytics page, were not linted, were not rendered in the API
reference, and were not contract-tested: a renamed field or a new status code
would reach a consumer unnoticed.

Adding the routes to `apis/openapi.yaml` would be wrong: that spec's server is
`cmd/wes` on `:8080`, the Schemathesis contract job boots `cmd/wes` in memory
against it, and a client generated from it would call routes that service
does not have.

## Decision

1. **A separate spec, `apis/openapi-reports.yaml`** (OpenAPI 3.0.3), with the
   reports binary as its server (`http://localhost:8092`). It documents the
   three routes, their parameters (`from`/`to` required RFC 3339, the
   half-open window `[from, to)`; `pathId` and `granularity=hour` optional),
   the `200` shapes and the RFC 7807 `400 invalid-report-query` and
   `500 report-store-error` problems. Field names and types are what the
   handlers serialise today; nothing in the API changes.
2. **Linted like the OLTP spec.** The `api-lint` CI job runs Spectral with the
   same `.spectral.yaml` on both specs.
3. **Rendered like the OLTP spec.** A second `docusaurus-plugin-openapi-docs`
   config (`wesReports`) generates `docs/docs/api/rest-reports/`; `prebuild`
   and the `docs-api-drift` CI job regenerate both (`clean-api-docs all`,
   `gen-api-docs all`) and fail on any diff or uncommitted page.
4. **Contract-tested in Go**, in
   `internal/adapters/inbound/http/reports_contract_test.go`, without a
   database: `TestReportsContract_RoutesMatchSpec` fails when the router
   serves a route the spec lacks or the spec lists a route the router does
   not serve; `TestReportsContract_ResponsesMatchSpec` drives every
   documented status of every operation through the real router with a fake
   store and checks each body against the declared schema (required
   properties present, no undocumented property). Schemathesis is not used
   here: it would need the analytical database and a projector to produce
   data, and the Go test already covers the same drift.

## Consequences

- **Easier:** a consumer (the MCP report tool, warehouse-console, the ops
  agent) has a spec to read and generate from, and a change to a report
  field fails a test in this repository before it reaches them.
- **Harder:** a new reports route or field must be added to the spec and the
  generated pages must be regenerated in the same change, or CI fails. That
  is the point.
- `rateDeviationDetected` is documented as reserved (always `0`), the same
  rule as the AsyncAPI contract: no ADR defines when RateDeviationDetected is
  raised.
- No behaviour change, no new route, no auth (the reports API stays
  unauthenticated, [ADR-0016](./0016-remove-rest-mcp-static-bearer-auth.md)).
