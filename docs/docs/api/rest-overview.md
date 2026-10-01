---
id: rest-overview
title: REST API
sidebar_label: REST API
sidebar_position: 1
description: How the REST reference is generated, and how to read it.
---

# REST API

The pages nested under this one are **generated directly from
[`apis/openapi.yaml`](https://github.com/claudioed/wes-work-planning/blob/main/apis/openapi.yaml)**
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
| **Charge Forecast** | The volume that must clear a path, bucketed by CPT |
| **Shift Plan** | *This* context's committed rate × heads × hours split |
| **Work Units** | Enqueue a releasable unit; read one by id or look units up by reference; record its completion |
| **Release** | Continuous, priority-ordered admission (waveless) |
| **Telemetry** | Live buffer read model — backlog depth, WIP, feed mode |
| **Rebalance** | Flow-balancing recommendation (Drum-Buffer-Rope) |
| **Labor Plan View** | Read-only projection of Workforce Management's plan |
| **Inventory View** | Read-only projection of Inventory's usable quantity, by SKU |
| **Health** | Operational health check |

Note that **Shift Plan** and **Labor Plan View** are separate tags on purpose.
They look similar and are not: one is our aggregate, the other is a projection
of someone else's. See
[ADR-0006](../adr/0006-labor-plan-view-not-shift-plan.md).

## Server

```
http://localhost:8080
```

There is no authentication (`security: []` in the spec). This service is
deployed inside the cluster and fronted by the platform gateway; it does not
authenticate callers itself.

## Downloading the spec

The generated introduction page carries an **Export** button that links to the
raw spec on `main`. You can also fetch it directly:

```sh
curl -sO https://raw.githubusercontent.com/claudioed/wes-work-planning/main/apis/openapi.yaml
```
