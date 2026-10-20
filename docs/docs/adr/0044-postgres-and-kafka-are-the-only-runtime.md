---
id: 0044-postgres-and-kafka-are-the-only-runtime
slug: /adr/0044-postgres-and-kafka-are-the-only-runtime
title: 0044. Postgres and Kafka are the only runtime
sidebar_label: 0044. Only real Postgres and Kafka
description: ADR 0044 — cmd/wes, cmd/mcp and cmd/wes-projector run only on a real Postgres and a real Kafka. No in-memory repositories, no log event sink, no direct-to-Kafka publisher, no optional relay or consumer; permissive/off modes are boot errors. The memory adapters remain test doubles.
---

# 0044. Postgres and Kafka are the only runtime

## Status

Accepted (2026-10-19). Amends the "in-memory when `DATABASE_URL` is unset",
"`EVENT_PUBLISHER=log` is the default", "`KAFKA_BROKERS` enables the consumer"
and "`permissive` is the default" passages of
[ADR 0014](./0014-transactional-outbox.md),
[ADR 0035](./0035-product-classification-local-copy.md) and
[ADR 0042](./0042-pick-face-replenishment-work.md); their decisions otherwise
stand. Mirrors product-master ADR 0010, workforce-management ADR 0038 and
facility-layout ADR 0039.

## Context

The user's rule is final: **everything must use a real database and a real
broker.** Until this change the shipped binaries still had an in-memory path:

- `DATABASE_URL` unset selected the in-memory repositories (no idempotency
  middleware, no outbox, no housekeeper).
- `EVENT_PUBLISHER` defaulted to `log`; `kafka` without a database published
  directly to Kafka, bypassing the outbox.
- `KAFKA_BROKERS` was optional and switched the integration-event consumer on.
- `PRODUCT_CLASSIFICATION_MODE` and `REPLENISHMENT_MODE` defaulted to
  `permissive`, which consumed nothing; `cmd/wes-projector` defaulted its
  broker to `localhost:9092`.
- `cmd/mcp` had its own in-memory work-pool and work-unit stores, so an MCP
  tool saw an empty world unless `DATABASE_URL` was set.

That is exactly the kind of configuration that hid defects. The first
Schemathesis run on a real Postgres found two (below) that the in-memory boot
could never produce.

## Decision

1. **Mandatory environment, checked before anything is dialled or bound.**
   `cmd/wes` refuses to boot, with an error that names the variable, when
   `DATABASE_URL`, `KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP`,
   `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` or `REPLENISHMENT_CONSUMER_GROUP`
   is missing or blank. `cmd/mcp` requires `DATABASE_URL` and `KAFKA_BROKERS`;
   `cmd/wes-projector` requires `KAFKA_BROKERS` (it already required
   `ANALYTICS_DATABASE_URL`).
2. **Kept variable names, narrowed values.** `EVENT_PUBLISHER` (the chart and
   warehouse-infra set it) accepts only `kafka`; `log` and anything else is a
   boot error. `PRODUCT_CLASSIFICATION_MODE` and `REPLENISHMENT_MODE` default to
   `kafka`; `permissive`, `off`, the retired `http` and any other value are boot
   errors naming the variable.
3. **The outbox relay and every consumer are always on.** Events leave only
   through the transactional outbox (ADR 0014); there is no log sink and no
   direct-to-Kafka path. A relay or consumer that dies on its own (not
   through shutdown) takes the process down with a non-zero exit, so the pod
   restarts instead of staying alive with dead consumers.
4. **`/readyz` is real.** It is `503` while draining, while Postgres does not
   answer a ping, or while no Kafka broker answers a metadata request. It never
   requires a topic (the writers auto-create theirs). Results are cached for 3s
   with a 2s bound per probe. `/healthz` stays liveness.
5. **Memory adapters are test doubles.** `internal/adapters/outbound/memory`
   remains for unit tests; the wall clock the binaries need moved to
   `internal/adapters/outbound/system`. The architecture test
   `TestRuntimeBinaryHasNoInMemoryAdapters` (with a bites test) fails when a
   non-test file under `cmd/**` imports the memory or log-events adapter
   packages or names the in-memory/permissive copies.
6. **NUL and integer bounds are client errors.** Postgres text cannot hold
   U+0000: a `rejectNUL` middleware answers `404` for a NUL in the path and
   `400 malformed-request-body` for a NUL in the body or query, before the
   idempotency middleware can record the key. `PUT /paths/{pathId}/pool` bounds
   `wipLimit` to `MaxWIPLimit` (2147483647, the Postgres `INTEGER` maximum);
   a larger value is `400 invalid-wip-limit`.
7. **The idempotency check is always on**, so `Idempotency-Key` is `required:
   true` in `apis/openapi.yaml` on the resource-creating POST.
8. **CI and the chart follow.** The `contract` job runs Schemathesis against
   the real `cmd/wes` on `postgres:16` and `confluentinc/confluent-local:7.6.1`
   service containers (the Kafka container advertises
   `PLAINTEXT_HOST://localhost:<port>`); a before-call hook gives every request
   a fresh `Idempotency-Key`; `--generation-allow-x00 false`. The chart refuses
   to render without `database.*`, `kafka.brokers` and every consumer group,
   and refuses `EVENT_PUBLISHER`, `PRODUCT_CLASSIFICATION_MODE` or
   `REPLENISHMENT_MODE` overrides other than `kafka`. `docker-compose.yml` now
   brings up Postgres and a single-node Kafka for local runs.

## Consequences

- A local run needs `docker compose up -d` (Postgres + Kafka) and four
  environment variables set; there is no "try it with nothing" mode. The unit,
  BDD and architecture tests are unaffected (they use the memory doubles
  directly); the end-to-end test
  (`cmd/wes/e2e_integration_test.go`) boots the real `run(ctx)` against
  testcontainers Postgres and Kafka and drives it only through REST and Kafka.
- Two defects that the in-memory boot hid are fixed: a fuzzed NUL was a `500`
  (SQLSTATE 22021, then 25P02), and a `wipLimit` above the `INTEGER` range was
  a `500` from the Postgres write.
- A deployment must provide every mandatory variable. warehouse-infra's current
  values for this service already do; the chart tests render them to prove it.
- Unknown limitations carried over from the fleet: a kafka-go group consumer
  that starts before its topic exists may not pick the topic up when it appears
  (see inbound-receiving ADR 0011), and the first outbox write to a
  not-yet-created output topic can fail once with `Unknown Topic Or Partition`
  and succeed on the next pass. This ADR does not try to solve either.

## Out of scope (follow-up)

The REST sync edges to sibling contexts keep their `*_MODE`:
`TRAVEL_DISTANCE_MODE=permissive|http` (facility-layout, ADR 0017) is
unchanged. `PATH_CATALOGUE_SOURCE=file|kafka` is a read-only configuration
cache, not state, and is unchanged. Neither is a database or a broker.
