---
id: 0045-pack-aware-work-units
slug: /adr/0045-pack-aware-work-units
title: 0045. Pack-aware work units
sidebar_label: 0045. Pack-aware work units
description: ADR 0045 — work units carry a pack-aware kind (CASE_PICK, EACH_PICK, BREAK_CASE) released as the pick and case_break objects fulfillment-execution already consumes; pack facts come from a local copy of product-master's pack-level events; estimates use the case cube and weight.
---

# 0045. Pack-aware work units

## Status

Accepted (2026-10-24). Extends [ADR 0035](./0035-product-classification-local-copy.md)
(local copy of product-master facts) and respects
[ADR 0044](./0044-postgres-and-kafka-are-the-only-runtime.md) (real Postgres and
Kafka only). Part of the mastercase / GTIN programme (contract P11-2).

## Context

Sellers now ship master cases and the warehouse stores them as sealed
containers. A pick of one case and a pick of eaches differ in handles, cube and
weight, and a case that must be opened first is its own unit of work.
fulfillment-execution already consumes `BREAK_CASE` tasks and a nested
`case_break` object on released work; this context must emit exactly that shape
and must not invent a competing one. Quantities stay in eaches everywhere.

## Decision

1. A work unit may carry a pack-aware kind: `CASE_PICK`, `EACH_PICK` or
   `BREAK_CASE`. Released work carries the `pick` object (quantity, unit,
   source bin) or the `case_break` object (LPN, loose bin, pick task id) that
   fulfillment-execution consumes. A unit without a pack-aware kind renders and
   releases byte-for-byte as before.
2. Pack facts are never looked up live. A local copy of product-master's
   `ProductPackLevelDeclared` / `ProductPackLevelRetired` events is kept in
   Postgres by a dedicated consumer on its own STABLE group,
   `PACK_VIEW_CONSUMER_GROUP` (chart key `config.packViewConsumerGroup`, default
   `wes-work-planning-pack-view`). It must differ from
   `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`: both read the same topic, and a group
   that already committed past the pack events could never replay them. The
   variable is mandatory and a collision is a boot error.
3. Poison messages (a pack level missing a dimension, an invalid GTIN) are
   dead-lettered, never coerced. The consumer is idempotent by CloudEvents id.
4. Charge and travel estimates may be requested per SKU and pick unit. The
   request `quantity` stays in eaches; the response adds `handles` (cases for
   pick unit `CASE`), `cubeMm3` and `weightG`, the last two omitted when the
   pack level has no size on record. A plain bucket is unchanged.

## Consequences

- One new mandatory env var, rendered by the chart with a default so the
  Application does not break when infra values lag; warehouse-infra may still
  pin the group explicitly.
- The pack copy is shared state in Postgres, so every replica reads the same
  facts; replay after a retirement is handled by the event version.
- Estimates for SKUs with no active CASE level fall back to the each-based
  behaviour; nothing is rounded here (rounding to case multiples belongs to
  inter-site planning).
