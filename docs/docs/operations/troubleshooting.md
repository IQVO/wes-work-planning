---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
description: Symptom, cause, check and fix for the failure modes visible in the wes-work-planning code — boot failures, migrations, readiness, Kafka consumption and DLQs, outbox lag, HTTP problem types, idempotency keys, circuit breaker, MCP and analytics.
---

# Troubleshooting

Each row is a failure mode the code actually has, with the log line or
problem `type` it produces. Problem types are relative to
`https://errors.wes-work-planning.warehouse-systems.dev/` (see
[Errors](../api/errors.md)). Settings are on
[Configuration](./configuration.md); procedures on the [Runbook](./runbook.md).

## Boot and readiness

| Symptom | Cause | Check | Fix |
|---|---|---|---|
| Pod exits at once: `failed to load the process-path catalogue` | `PATH_CATALOGUE_FILE` missing or invalid — there is no empty-catalogue fallback ([ADR-0012](../adr/0012-process-path-catalogue-validation.md)) | `kubectl exec ... ls /etc/wes-work-planning`; chart `pathCatalogue.content` / `existingConfigMap` | supply the catalogue (warehouse-infra's `config/process-paths/*.yaml`) |
| Pod exits: `process-path catalogue did not become ready within 60s` | `PATH_CATALOGUE_SOURCE=kafka` and the replay of `warehouse.process-path-management.events` did not catch up | is the topic present and readable from the pod? | fix broker reachability; or fall back to `PATH_CATALOGUE_SOURCE=file` |
| Pod exits: `DATABASE_URL is required`, `KAFKA_BROKERS is required`, `KAFKA_CONSUMER_GROUP is required`, `PRODUCT_CLASSIFICATION_CONSUMER_GROUP is required` or `REPLENISHMENT_CONSUMER_GROUP is required` | a mandatory variable is unset or blank; Postgres and Kafka are the only runtime ([ADR-0044](../adr/0044-postgres-and-kafka-are-the-only-runtime.md)), there is no in-memory or log-only mode | env of the container; chart `database.*`, `kafka.brokers`, `config.*ConsumerGroup` (the chart itself refuses to render without them) | set the variable named in the error |
| Pod exits: `EVENT_PUBLISHER="log" is not supported`, `PRODUCT_CLASSIFICATION_MODE=permissive is not supported`, `REPLENISHMENT_MODE=permissive is not supported` (same for `off`) | stale config from before ADR-0044: `log`, `permissive` and `off` no longer exist | env (and `extraEnv`) | unset the variable or set it to `kafka` |
| Pod exits: `PRODUCT_CLASSIFICATION_MODE=http was removed (ADR-0035)` | stale config from before [ADR-0035](../adr/0035-product-classification-local-copy.md) | env | unset it or use `kafka` |
| Logs `retrying` with `op` `run migrations` / `ping postgres` / `connect to the process-path catalogue topic`, then success | the fleet's Istio sidecar resets each pod's first outbound dial ~10 s after start | `succeeded after retry` follows | none — the boot retry (5 attempts, ~15 s of backoff) absorbs it |
| Same, ending in `(after 5 attempts)` and exit 1 | the dependency really is unreachable | DSN host, Postgres/Kafka health | fix the dependency; the pod restarts |
| Pod killed by the startup probe while still logging boot steps | boot exceeded the probe's 60 s (2 s × 30): catalogue replay (up to 60 s) plus migrations plus retries | timestamps of the boot log lines | raise `startupProbe.failureThreshold`, or speed up the slow step |
| Pod never becomes Ready | `cmd/wes` opens its listener only after boot; `/readyz` is `200` only while Postgres answers a ping **and** a Kafka broker answers a metadata request (cached 3 s, 2 s bound) | logs stop before `http server listening`, or `/readyz` returns `503` with problem `dependency-unavailable` and a `detail` naming `postgres` or `kafka` | the blocking step is in the logs (catalogue, migrations, ping); restore the dependency |
| `/readyz` returns `503 {"status":"not_ready"}` | SIGTERM received — readiness flips first on shutdown, by design | pod is terminating | expected; `/healthz` stays 200 so liveness does not kill the drain |
| `/readyz` returns `503` `dependency-unavailable` | Postgres does not answer a ping, or no Kafka broker answers a metadata request (a missing topic is **not** a failure: the writers auto-create theirs) | `detail` names the dependency | restore the dependency; the pod is taken out of rotation until it answers |
| Pod restarts with `outbox relay stopped on its own` or `... consumer stopped on its own` | the relay or a consumer died (Kafka down long enough, a DLQ publish or an offset commit failed); the process deliberately exits non-zero so the pod restarts instead of staying alive with dead consumers | the ERROR line before the exit | fix the broker; the restarted pod resumes from the committed offsets |
| `cmd/wes-projector` or `cmd/wes-reports` exits: `ANALYTICS_DATABASE_URL is required` | analytics enabled without a DSN | `analytics.database.*` values / Secret | set `projectorUrl`/`reportsUrl` or `existingSecret` |

## Migrations

| Symptom | Cause | Check | Fix |
|---|---|---|---|
| Boot fails in `run migrations` with `pq: unnamed prepared statement does not exist` or `canceling statement due to statement timeout` | golang-migrate ran through PgBouncer's transaction pooling, which breaks its session advisory lock | is `MIGRATIONS_DATABASE_URL` set to a direct DSN? | provide the Secret key `MIGRATIONS_DATABASE_URL` ([ADR-0026](../adr/0026-migrations-direct-postgres-connection.md)) |
| Boot fails: `Dirty database version N` | a previous migration failed half-way | `SELECT * FROM schema_migrations` | repair the schema by hand, then `migrate -path migrations -database <direct DSN> force N` |
| `cmd/mcp` tool calls fail with `relation "work_pools" does not exist` | `cmd/mcp` never migrates; `cmd/wes` has not run against this database yet | `\dt` on the OLTP database | deploy/start `cmd/wes` first |
| Projector boot fails in `run analytics migrations` | analytical database unreachable or a `*.up.sql` failed | `schema_migrations_analytics` rows | fix the DSN or the SQL; each file applies in its own transaction |

## Kafka consumption

| Symptom | Cause | Check | Fix |
|---|---|---|---|
| Upstream events never reach the read models; no `kafka consumer group` / `consuming product classifications` boot log | the pod never got past boot (every consumer starts unconditionally), or a second process shares the group | boot logs; `kafka-consumer-groups.sh --describe --group wes-work-planning` | fix the boot error; see the next row |
| A second process (local `go run`, e2e harness) consumes nothing while the deployed pod works, or vice versa | both share `KAFKA_CONSUMER_GROUP=wes-work-planning`; with one partition only one member gets it | `kafka-consumer-groups.sh --describe --group wes-work-planning` | give the extra process a unique `KAFKA_CONSUMER_GROUP` |
| Lag grows on every consumed topic; log `kafka consumer stopped on its own` and the pod restarts | the loop exits when a DLQ publish or an offset commit fails (on purpose: the offset is not committed, nothing is lost) and the process exits non-zero | ERROR lines before it, e.g. `publish to dead-letter topic` | fix the broker; the restarted pod continues |
| `<topic>.dlq` grows; `invalid CloudEvent, sending to dead-letter topic` | the producer sent something that is not CloudEvents 1.0 (e.g. the retired flat envelope) | `x-dlq-error` header | fix the producer; the message is never retried |
| `<topic>.dlq` grows; `exhausted retries, sending to dead-letter topic` | a handler failed 3 times: unknown `path_id` (catalogue), undeclared `work_kind` on `WorkDemandReleased`, a broken `SlotPlanApproved` (unknown move kind, missing slot) or a RELOCATE path missing from the catalogue / not admitting `SLOT_RELOCATION` ([ADR-0040](../adr/0040-slot-plan-relocation-work.md)), or a persistent Postgres error | `x-dlq-error`, `id`, `type` in the log | declare the path / fix the upstream payload / fix the database, then [replay](./runbook.md#replay-a-dead-lettered-message) |
| A RELOCATE work unit stays `Pending` and is never released | it is blocked: `relocation.blockedBy` names the move that vacates its target slot, which has not completed yet | `GET /work-units/{id}` | complete the blocking unit; see [Runbook](./runbook.md#slot-plan-relocation-work-relocate) |
| RELOCATE work units turned `Cancelled` | a newer `SlotPlanApproved` for the same site superseded their plan before they were released (by design) | `SELECT * FROM slot_plans WHERE site_id = '...' ORDER BY approved_at DESC` | none |
| `SlotPlanApproved older than the site's current plan; recorded, nothing planned` | a plan arrived after a newer one for the same site | INFO only | none |
| `TaskCompleted for a work unit this context never planned; skipping` | a PACK task fulfillment-execution created itself carries the order id | INFO only | none — expected ([ADR-0028](../adr/0028-processed-event-mark-atomic-with-handling.md)) |
| Release-fed paths answer `409 wip-limit-reached` for good although fulfillment finishes tasks | completions are not arriving, so WIP slots are never freed (`RecordCompletion` frees them) | is the `warehouse.fulfillment.events` consumer running? lag on that topic? | restore consumption, or complete via `POST /work-units/{id}/completions` |
| `applying ProductClassified failed; retrying the same message` repeating | a transient error the classification consumer retries forever (it never skips a decodable event) | the `error` field | fix Postgres; the consumer continues by itself |
| `WorkReleased` never carries `required_capabilities`/`fragile` | the copy has no row for the SKU yet (the `ProductClassified` consumer has not seen it, or the SKU is not hazmat/fragile) | `SELECT * FROM product_classification_copy WHERE sku = '...'` | check the consumer lag on `warehouse.product-master.events`; the MCP server reads the same table |

## Outbox and publishing

| Symptom | Cause | Check | Fix |
|---|---|---|---|
| Nothing reaches Kafka although requests succeed | the always-on relay cannot publish: see the next two rows | `outbox relay pass failed`, `wes.outbox.lag_seconds` | restore Kafka; there is no log-only publisher any more |
| `wes.outbox.lag_seconds` keeps rising; `outbox relay pass failed` | Kafka unreachable, or one row failing every pass (later rows wait behind it until it reaches `OUTBOX_MAX_ATTEMPTS`) | `SELECT id, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5` | fix the cause in `last_error`; the relay resumes on its own |
| `outbox relay: row reached the attempt cap and is no longer claimed` (ERROR); an event never arrived | that row failed `OUTBOX_MAX_ATTEMPTS` (default 20) sends and is excluded; later rows overtook it ([ADR-0039](../adr/0039-outbox-relay-attempt-cap.md)) | `SELECT id, topic, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= 20` | fix the cause, then `UPDATE outbox_events SET attempts = 0 WHERE id = ...` ([Runbook](./runbook.md#re-arm-an-outbox-row-that-hit-the-attempt-cap)) |
| Outbox rows written by MCP never published | the relay runs only in `cmd/wes`; the api Deployment is down or crash-looping | api pods and logs | restore the api; it drains every row whichever process wrote it |
| `idempotency_keys` / `outbox_events` grow without bound | housekeeper disabled (`0`) or failing (`housekeeping sweep failed`) | env, logs | fix; note `processed_events` is never swept by design |

## HTTP problem types

| Status / `type` | Cause | Fix |
|---|---|---|
| `400 unknown-path-id` | `pathId` not in the process-path catalogue. Checked on the routes that can create an aggregate: `POST .../charge`, `.../plan`, `.../work-units`, `PUT .../pool` | use a declared path or add it to the catalogue |
| `404 not-found` on `release`, `telemetry`, `rebalance` | no `WorkPool` exists for that path yet (created by the first enqueue or by `PUT /paths/{pathId}/pool`) | enqueue or configure first |
| `404 not-found` on `labor-plan-view` / `inventory-view` | no event observed yet for that path/SKU | wait for the upstream event or check consumption |
| `400 heads-exceed-installed-stations` | `plannedHeads > installedStations` | fix the plan |
| `400 malformed-request-body` | invalid JSON, a missing required field (`cpt`, `plannedHeads`, `installedStations`, `wipLimit`, bucket `cpt`/`quantity`) or an empty/invalid `cutoffAt` | fix the request |
| `400 invalid-line-no` | `lineNo` given but outside 1..2147483647 | omit it or send a valid line |
| `409 wip-limit-reached` | release-fed pool at its WIP limit | complete work, or raise the limit with `PUT /paths/{pathId}/pool` |
| `409 work-pool-empty` | nothing pending | enqueue work |
| `409 concurrent-modification` | 12 optimistic-concurrency retries (jittered, ≤ ~1 s) all lost the race on the same pool ([ADR-0029](../adr/0029-work-pool-optimistic-concurrency.md)) | retry the request; sustained contention on one path is the real problem |
| `409 work-unit-already-enqueued` | the `workUnitId` is already in the pool | use a new id (or treat as success for a retry) |
| `409 work-unit-already-released` / `work-unit-already-completed` / `work-unit-not-released` / `work-unit-cancelled` / `work-pool-entry-already-released` | lifecycle invariant of `WorkUnit` / `WorkPool` | inspect `GET /work-units/{id}` |
| `400 idempotency-key-required` | `POST /paths/{pathId}/work-units` without `Idempotency-Key` (always enforced: Postgres is the only runtime) | send a unique key per logical request ([ADR-0022](../adr/0022-idempotency-key-middleware.md)) |
| `400 malformed-request-body` for a body or query with U+0000, `404` for a path with U+0000 | Postgres text cannot hold NUL; the guard answers before any idempotency key is recorded | the request bytes | remove the NUL character |
| `400 invalid-wip-limit` for a huge `wipLimit` | `wipLimit` above 2147483647 (the Postgres `INTEGER` maximum) | the request body | send a smaller limit |
| `409 idempotency-key-reused` | same key, different body (SHA-256 of the body differs) | new key for a different request |
| Retry with same key and body returns the first response, no new unit | cached replay, by design; after `IDEMPOTENCY_KEY_TTL` (24 h) the key is forgotten and the call runs again | — |
| `500 internal-error` | unmapped error, usually Postgres (pool exhausted, `statement_timeout` 5 s) | ERROR access log with `trace_id`; `db.client.operation.errors` |

There is no `412` in this service: no endpoint uses `ETag`/`If-Match`
preconditions; lost updates on `WorkPool` are handled server-side by the
version column and retry.

## Sibling lookups

| Symptom | Cause | Check | Fix |
|---|---|---|---|
| Committed plans lack `travelDistanceM` | `TRAVEL_DISTANCE_MODE` not `http`, no `fromLocationCode`/`toLocationCode` in the request, an unknown or cross-zone pair, or facility-layout failing — all fail-open | `circuit_breaker_state{dependency="facility-layout"}` | fix facility-layout or `FACILITY_LAYOUT_BASE_URL` |
| `circuit_breaker_state{dependency="facility-layout"} == 2` | 5 consecutive failures, or more than half of at least 10 requests failed in a 30 s window | facility-layout health | the breaker half-opens after 30 s and lets one probe through |

## MCP and analytics

| Symptom | Cause | Check | Fix |
|---|---|---|---|
| Every tool returns `not found` | the path has no pool yet (`cmd/mcp` reads the same Postgres as `cmd/wes`, whose schema it never migrates) | `SELECT * FROM work_pools` | configure the pool with `PUT /paths/{pathId}/pool` |
| `cmd/mcp` exits: `DATABASE_URL is required` / `KAFKA_BROKERS is required` | mandatory variable missing (ADR-0044) | MCP env | set it |
| `get_release_throughput_report` rejected: `missing properties: ["pathId" "granularity"]` | the SDK marks every input field required (see [MCP tools](../mcp/tools.md)) | — | send `"pathId": ""` and `"granularity": ""` (or `"hour"`) |
| `get_release_throughput_report` not listed | `REPORTS_BASE_URL` unset | MCP env / `analytics.enabled` | set it |
| MCP client gets session errors after scaling MCP to 2+ replicas | sessions live in one process's memory | replica count | keep `mcp.replicaCount: 1` |
| `400 invalid-report-query` from `/reports/throughput` | `from`/`to` missing or not RFC3339, or `granularity` not `hour` | the `detail` | fix the query |
| Report rows missing although events were published | the projector commits before handling; a failed apply is logged (`analytics message handling failed`) and **not** retried | projector ERROR logs | rebuild the report ([runbook](./runbook.md#rebuild-a-read-model-from-kafka)) |
| `lagSeconds` above 30 | projector down, lagging, or no events produced recently | consumer group `wes-analytics` lag | restart/scale the projector |

## Browser

| Symptom | Cause | Fix |
|---|---|---|
| CORS error calling the REST API from a browser origin | origin not in `CORS_ALLOWED_ORIGINS` (default `http://localhost:5173,http://localhost:5183`) | add the origin; in the cluster go through Kong on `:8000` |
