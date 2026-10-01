---
id: index
title: API Reference
sidebar_label: Introduction
sidebar_position: 0
slug: /api/
description: The REST API generated from the real openapi.yaml, the Kafka event contract, and the RFC 7807 error model.
---

# API Reference

This service publishes two machine-readable contracts, both linted in CI by
Spectral on every push and pull request:

| Contract | File | Rendered here |
|---|---|---|
| **REST** — OpenAPI 3.0.3 | [`apis/openapi.yaml`](https://github.com/claudioed/wes-work-planning/blob/main/apis/openapi.yaml) | [REST API](./rest-overview.md) — **generated from the spec**, not hand-transcribed |
| **Events** — AsyncAPI 2.6.0 | [`apis/asyncapi.yaml`](https://github.com/claudioed/wes-work-planning/blob/main/apis/asyncapi.yaml) | [Events](./events.md) |

## Endpoint coverage: 12 / 12

Every route registered in `internal/adapters/inbound/http/router.go` is
documented in `apis/openapi.yaml`, and therefore appears in the generated
reference.

| # | Method | Path | Use case | OpenAPI `operationId` |
|---:|---|---|---|---|
| 1 | `POST` | `/paths/{pathId}/charge` | ReceiveChargeForecast | `receiveChargeForecast` |
| 2 | `POST` | `/paths/{pathId}/plan` | CommitShiftPlan | `commitShiftPlan` |
| 3 | `POST` | `/paths/{pathId}/work-units` | EnqueueWorkUnit | `enqueueWorkUnit` |
| 4 | `POST` | `/paths/{pathId}/release` | ReleaseNextWork | `releaseNextWork` |
| 5 | `POST` | `/work-units/{id}/complete` | RecordCompletion | `recordCompletion` |
| 6 | `GET` | `/paths/{pathId}/telemetry` | SampleBacklog | `sampleBacklog` |
| 7 | `GET` | `/paths/{pathId}/rebalance` | RebalanceDecision | `rebalanceDecision` |
| 8 | `GET` | `/paths/{pathId}/labor-plan-view` | LaborPlanView (projection) | `getLaborPlanView` |
| 9 | `GET` | `/inventory-view/{sku}` | InventoryView (projection) | `getInventoryView` |
| 10 | `GET` | `/work-units?reference=` | GetWorkUnitsByReference | `getWorkUnitsByReference` |
| 11 | `GET` | `/work-units/{id}` | GetWorkUnit | `getWorkUnit` |
| 12 | `GET` | `/healthz` | — | `healthCheck` |

## Design constraints

- **Richardson Maturity Level 2, deliberately.** Resource nouns, correct verbs,
  correct status codes — and **no hypermedia controls**. Level 3 was not
  attempted; saying so in the spec's own description is more useful than
  leaving readers to infer it.
- **All errors are RFC 7807** `application/problem+json`. See
  [Error model](./errors.md).
- **Domain structs never leak.** Every request and response is a DTO defined in
  the HTTP adapter (`dto.go`). Field names are `camelCase` on the wire;
  timestamps are RFC 3339.
- **`POST /paths/{pathId}/release` is a POST on a sub-resource**, not
  `GET .../next`, because releasing mutates the pool: an entry transitions to
  `Released` and can never be handed out again.

## Two POSTs that are not creations

`/paths/{pathId}/release` and `/work-units/{id}/complete` are `POST` because
they are non-idempotent state transitions, not because they create a resource.
Both return the affected representation with `200`, not `201` — there is no new
URI to point a `Location` header at.

Creation endpoints (`/charge`, `/plan`, `/work-units`) do return `201` with a
`Location` header.
