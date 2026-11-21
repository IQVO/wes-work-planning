---
id: rest-overview
title: REST API
sidebar_label: REST API
sidebar_position: 1
description: How the REST reference is generated, and how to read it.
---

# REST API

The pages nested under this one are **generated directly from
[`apis/openapi.yaml`](https://github.com/IQVO/wes-work-planning/blob/develop/apis/openapi.yaml)**
by `docusaurus-plugin-openapi-docs`, regenerated on every site build. Nothing
below is hand-transcribed, so the reference cannot drift from the spec that CI
validates.

That spec is linted by [Spectral](https://stoplight.io/open-source/spectral) in
the `api-lint` job of `.github/workflows/ci.yml` on every push and pull
request, against the ruleset in `.spectral.yaml`.

## How the reference is organised

Operations are grouped by their OpenAPI `tag`, and each tag maps to one concept
in the [ubiquitous language](../business-context/ubiquitous-language.md):

| Tag | Concept |
|---|---|
| **Charge Forecast** | Moved: the route was removed in stage E (it answered `410` for one release), the forecast is warehouse-planning's `PUT /workload-forecasts/{site_id}/{path_id}` ([ADR-0049](../adr/0049-charge-forecast-and-shift-plan-move-to-warehouse-planning.md)) |
| **Shift Plan** | Moved: the route was removed in stage E (it answered `410` for one release), the plan is warehouse-planning's `PUT /labour-plans/{site_id}/{shift_id}/{path_id}` (ADR-0049) |
| **Work Units** | Enqueue a releasable unit; read one by id or look units up by reference; record its completion |
| **Release** | Continuous, priority-ordered admission (waveless) |
| **Telemetry** | Live buffer read model — backlog depth, WIP, feed mode |
| **Rebalance** | Flow-balancing recommendation (Drum-Buffer-Rope) |
| **Labor Plan View** | Read-only projection of Workforce Management's plan |
| **Inventory View** | Read-only projection of Inventory's usable quantity, by SKU |
| **Health** | Operational health check |

**Shift Plan** and **Labor Plan View** were separate tags on purpose: one was
our aggregate, the other is a projection of someone else's (see
[ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md)). Only the
projection is left here; the plan itself moved to warehouse-planning.

## Server

```
http://localhost:8080
```

There is no authentication (`security: []` in the spec) — the static-bearer
layer was removed fleet-wide
([ADR-0016](../adr/0016-remove-rest-mcp-static-bearer-auth.md)). This
service is deployed inside the cluster and fronted by the platform gateway
(Kong on `:8000` locally); it does not authenticate callers itself.

## Downloading the spec

The generated introduction page carries an **Export** button that links to the
raw spec on `main`. You can also fetch it directly:

```sh
curl -sO https://raw.githubusercontent.com/IQVO/wes-work-planning/main/apis/openapi.yaml
```
