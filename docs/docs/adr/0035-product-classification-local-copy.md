---
id: 0035-product-classification-local-copy
slug: /adr/0035-product-classification-local-copy
title: 0035. Product classification from a local copy of product-master events
sidebar_label: 0035. Product classification local copy
description: ADR 0035 — the WorkReleased hazmat/fragile hints are read from a local Postgres copy fed by product-master's ProductClassified events on warehouse.product-master.events; the live HTTP lookup against inventory-storage is removed and PRODUCT_CLASSIFICATION_MODE becomes kafka|permissive.
---

# 0035. Product classification from a local copy of product-master events

## Status

Accepted (2026-10-06). **Supersedes the live-lookup part of
[ADR-0009](./0009-product-classification-propagation-to-work-released.md)**
(its Decision §1 "synchronous outbound HTTP" and §2 "un-persisted read
model"); ADR-0009 §3–§5 (SKU on `WorkUnit`, the stamp in the outbound Kafka
adapter, fail-open) stay in force. Also retires the product-classification
breaker of [ADR-0023](./0023-resilience-circuit-breakers-retry-dlq-shutdown.md)
(the travel-distance breaker is unchanged).

Companion decisions: product-master ADR 0001 (product-master owns SKU-level
product master data) and ADR 0003 stage D (downstream readers move to local
copies), inventory-storage ADR 0034 (hand-over of classification to
product-master).

## Context

ADR-0009 read a released unit's classification once, synchronously, from
inventory-storage's `GET /products/{sku}/classification`, because
inventory-storage did not publish `ProductClassified`. Two things changed:

- **The source of truth moved.** Classification now belongs to the new
  `product-master` bounded context (product-master ADR 0001). inventory-storage
  hands it over (its ADR 0034): its write endpoint is retired and its `GET`
  is deprecated until it is removed (product-master ADR 0003, stage E).
- **The fact is now on the wire.** product-master publishes
  `com.warehouse.wms.product-master.product.ProductClassified` on
  `warehouse.product-master.events` through its transactional outbox: Kafka
  key and CloudEvents `subject` = the SKU, `data` is a full-state replacement
  of the classification carrying the aggregate `version`
  (`{"sku","handling_tags","temperature_class"?,"dot_hazard_class"?,"classification_source","version"}`).
  product-master ADR 0001 forbids service-to-service lookups against it: the
  REST `GET` endpoints exist for operators, the console and agents.

The reason ADR-0009 gave for not projecting ("there is nothing to consume")
no longer holds, and the HTTP client would keep a runtime dependency on an
endpoint that is being decommissioned.

## Decision

We replace the HTTP adapter behind the unchanged
`ports.ProductClassificationLookup` port with a **local copy** of
product-master's classification, fed by Kafka. The domain, the use cases
that release work, and the WorkReleased payload are unchanged.

1. **Local copy table.** Migration `0012` adds
   `product_classification_copy (sku TEXT PRIMARY KEY, handling_tags TEXT[],
   temperature_class TEXT NULL, dot_hazard_class SMALLINT NULL,
   version BIGINT, updated_at TIMESTAMPTZ)`. One row per SKU; there is no
   "unclassify" event in v1, so rows are never deleted.
2. **Consumer.** A dedicated inbound consumer
   (`internal/adapters/inbound/kafka/product_classification_consumer.go`)
   reads `warehouse.product-master.events` under a **stable consumer group
   read from `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`** (never a literal; the
   copy is shared state in Postgres, so every replica shares the group and
   splits the partitions; a new group starts from the earliest offset so the
   copy fills from history). It acts on the full type
   `com.warehouse.wms.product-master.product.ProductClassified` only and
   ignores every other type on the topic (`ProductRegistered`,
   `ProductDescriptionChanged`, `ProductDimensionsDeclared`,
   `ProductMeasured`).
3. **Exactly-once effect, at-least-once delivery.** A new
   `ObserveProductClassification` use case claims the CloudEvents `id` in the
   existing `processed_events` table and applies the version-guarded upsert
   in **one** `UnitOfWork` through the same `onceAtomically` primitive every
   other inbound event uses ([ADR-0028](./0028-processed-event-mark-atomic-with-handling.md)).
   The upsert inserts a missing SKU and overwrites an existing row **only
   when the incoming `version` is greater than the stored one**; an equal or
   older version (replay, out-of-order redelivery) is a no-op that still
   records the `id`.
4. **Error handling.** A message that is not a valid CloudEvent, or a
   `ProductClassified` whose `data` cannot be decoded or lacks `sku` or a
   `version` ≥ 1, is deterministic poison: logged at WARN with
   topic/partition/offset and committed past (no DLQ: nothing about it can
   ever succeed). Any other failure (database, transaction) is transient: the
   **same message is retried** with capped exponential backoff until it
   succeeds or the process stops, and the offset is committed only after
   success (`FetchMessage` + `CommitMessages`). A stalled partition is loud
   (ERROR per attempt) rather than silently skipping a classification.
5. **Lookup.** `GetClassification` reads the copy and maps it to exactly what
   the HTTP client returned: `Known=true` with `HandlingTags` and
   `TemperatureClass` for a stored SKU, `Known=false` for an unknown SKU, and
   **`Known=false` with a nil error for any read failure** (ADR-0009 §5,
   fail-open: classification never blocks or delays a release). Because the
   outbox encoder calls it inside the release transaction, the read runs in a
   savepoint, so a failed read can never abort the release transaction.
   `dot_hazard_class` is stored but not surfaced: the view and the
   `WorkReleased` mapping (`Hazmat` → `"hazmat"`, `Fragile` → `fragile`) are
   unchanged.
6. **Modes.** `PRODUCT_CLASSIFICATION_MODE` becomes `kafka|permissive`
   (default `permissive`, unchanged). `http` is **removed and rejected at
   boot** with an error naming the allowed values, as is any other value, so
   a stale deployment fails loudly instead of silently degrading.
   - `cmd/wes`, mode `kafka`: requires `KAFKA_BROKERS` and
     `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (either missing is a boot error);
     wires the Postgres copy (or an in-memory copy when `DATABASE_URL` is
     unset) as the lookup and starts the consumer. On shutdown it is stopped
     and drained right after the main integration consumer, before the HTTP
     server and the pool close.
   - `cmd/mcp`, mode `kafka`: **never starts a consumer** and does not read
     the group variable. With `DATABASE_URL` it reads the same
     `product_classification_copy` table read-only (cmd/wes owns the
     migration and the writes); without a database it uses the permissive
     lookup and logs that it did.
   - `permissive` in either binary: unchanged no-op, `Known=false`.
7. **Removed.** The `internal/adapters/outbound/productclassification`
   HTTP client, its circuit breaker and retry, their tests, and every use of
   `INVENTORY_STORAGE_BASE_URL` (classification was its only use in this
   repo). The permissive lookup moves to the new
   `internal/adapters/outbound/productclassificationcopy` package, next to
   the Postgres and in-memory copies.

## Consequences

- **No synchronous cross-service call on the release path.** The latency and
  availability coupling ADR-0009 accepted ("harder", third bullet) is gone;
  the lookup is a primary-key read in this service's own database.
- **Eventual consistency.** A classification reaches this context after
  product-master's outbox relay and this consumer have both run (normally
  well under a second; longer while either is down or lagging). A SKU
  classified moments before its first release can be released without its
  hints: the same outcome as an unknown SKU today (fail-open, no hints),
  which ADR-0009 already accepted. Released work is still not re-stamped
  when a classification changes later (ADR-0009, "harder", first bullet).
- **Empty copy until the backfill.** Until product-master has imported and
  re-published the legacy classifications (product-master ADR 0003 stages A
  and B), older SKUs are unknown here and release without hints. The
  runbook runs the backfill right after product-master is ready.
- **Deployment change required.** An environment still setting
  `PRODUCT_CLASSIFICATION_MODE=http` (warehouse-infra did, for this service)
  no longer boots: it must switch to `kafka` and set
  `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`, or drop the variable to get
  `permissive`. `INVENTORY_STORAGE_BASE_URL` is no longer read.
- **In-memory runs.** Without `DATABASE_URL` the copy lives only as long as
  the process. With a stable group a restart resumes after the committed
  offsets and the copy starts empty, so a local in-memory run should use a
  fresh `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` per run to replay the topic.
- **One more consumer to operate.** Its lag is the staleness of the copy;
  the existing Kafka consumer-lag dashboards apply to the new group.
- **Contract.** `apis/asyncapi.yaml` declares the consumed channel
  `warehouse.product-master.events` with the `ProductClassified` message;
  [ADR-0027](./0027-cloudevents-mandatory-event-envelope.md)'s consumed-type
  list gains the type. No published event, REST endpoint or MCP tool
  changes.
