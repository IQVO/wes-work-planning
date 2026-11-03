---
id: 0047-fleet-rest-api-guideline
slug: /adr/0047-fleet-rest-api-guideline
title: 0047. Adopt the fleet REST API guideline
sidebar_label: 0047. Fleet REST API guideline
description: "ADR 0047 — noun routes, idempotent POST/PATCH behind a global Idempotency-Key, 409 on key reuse, Idempotency-Replayed, keyset-paged collections, enforced by the rest-guideline Spectral ruleset."
---

# 0047. Adopt the fleet REST API guideline

## Status

**Accepted** (supersedes the key-reuse status and the scope paragraph of
[ADR-0022](./0022-idempotency-key-middleware.md)).

## Context

The fleet standardised its REST surface (see the warehouse-docs REST guideline):
URLs name things, every POST/PATCH is idempotent, key reuse is `409`, every
collection is keyset-paged. This service had two verb routes, four POSTs outside
the idempotency middleware, key reuse answered `422`, and one unpaged collection.

## Decision

- **Noun routes.** `POST /paths/{pathId}/release` becomes
  `POST /paths/{pathId}/work-releases`; `POST /work-units/{id}/complete` becomes
  `POST /work-units/{id}/completions`. Status codes and bodies are unchanged;
  `operationId`s (`releaseNextWork`, `recordCompletion`) are unchanged so MCP/A2A
  mappings do not move. Hard cutover, no aliases (`TestRetiredRoutesAreGone`).
- **Idempotency on every POST.** `charge`, `plan`, `work-units`, `work-releases`
  and `completions` all sit behind `RequireIdempotencyKey` (route-scoped). The key
  is global (looked up by key alone); the fingerprint is method + path + body hash.
  Same key + different fingerprint is `409 idempotency-key-reused` (was 422). A key
  that is not 1-128 printable ASCII is `400 idempotency-key-invalid`. A replay carries
  `Idempotency-Replayed: true`. The key row still commits in the same transaction as
  the aggregate change and the outbox rows (ADR-0014/0022). 2xx and 4xx answers are
  stored; a 5xx or a panic rolls everything back and is never stored. The only PUT
  (`/paths/{pathId}/pool`) is a snapshot replace, idempotent by method, and takes no key.
- **Keyset paging.** `GET /work-units?reference=` answers `{items,nextCursor}`, with
  `limit` (1-500, default 100) and an opaque `cursor`, ordered by `(cpt, id)`; the
  `internal/adapters/inbound/http/page` helper is the fleet reference. Bad `limit` is
  `400 invalid-limit`, a bad cursor `400 invalid-cursor`, any undeclared query
  parameter `400 unknown-query-parameter`.
- **Enforcement.** `.spectral.yaml` + `.spectral/functions/restGuideline.js`
  (`spectral lint ... --fail-severity=warn`, CI job `api-lint`).

## Exemptions

None. (`GET /paths/{pathId}/telemetry|labor-plan-view` are single-path
views addressed by id and are not collections; the Spectral rule does not flag them.)

## Consumers changed

This repo's `web/` remote (`fetchWorkUnitsByReference` follows `nextCursor`).
Outside this repo: e2e-tests (`warehouse-day` floor client), warehouse-ops-agent
(`GET /work-units?reference=`), and any caller of the two renamed routes.

## Amendment: `GET /paths/{pathId}/rebalance` becomes a keyed POST

The rebalance route carried a verb in its path and published `PathThrottled` /
`LaborReassignmentFlagged` as a side effect of a GET (unsafe for caches,
prefetchers and polling). It is replaced, with no alias, by
`POST /paths/{pathId}/rebalance-decisions` (guideline 1.3 e, a decision noun;
operationId `rebalanceDecision` unchanged):

- `Idempotency-Key` required (400 `idempotency-key-required` / `-invalid`), no
  request body, 409 `idempotency-key-reused` on a different request.
- Answers `201` with the same decision JSON, no `Location` (the decision is
  computed, not stored), as pricing's `POST /price-quotes` answers a computed
  result with `201`.
- The first execution runs `RebalanceDecision` and publishes the events. A replay
  of the same key returns the stored decision with `Idempotency-Replayed: true`
  and publishes nothing. Callers (including pollers) must mint a new key per
  decision: reusing one freezes the decision and silences the events.
- `GET /paths/{pathId}/telemetry` still publishes events from a GET; it is a noun
  route and out of scope here, recorded as a known concern.

## Consequences

Breaking by design; deploys together with the rest of the fleet's REST cutover set.
