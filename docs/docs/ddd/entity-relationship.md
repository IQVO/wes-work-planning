---
id: entity-relationship
title: Entity-relationship diagram
sidebar_label: ER diagram
sidebar_position: 8
description: The final Postgres schema after migrations 0001 to 0016 and the separate analytics database, with the table-to-aggregate mapping.
---

# Entity-relationship diagram

The schema below is the **final state** after applying `migrations/0001` to
`migrations/0016` in order (golang-migrate, run on start-up by
`cmd/wes` (`DATABASE_URL` is mandatory), over `MIGRATIONS_DATABASE_URL` if given —
[ADR-0026](../adr/0026-migrations-direct-postgres-connection.md)), plus the
separate **analytical** database built from `migrations/analytics/`.

There is no database-less mode: `DATABASE_URL` is a boot requirement ([ADR-0044](../adr/0044-postgres-and-kafka-are-the-only-runtime.md)). The
in-memory repositories (`internal/adapters/outbound/memory`) are unit-test
doubles only.

## OLTP database

```mermaid
erDiagram
    work_pools ||--o{ work_pool_entries : "has entries, ON DELETE CASCADE"

    charge_forecasts {
        text path_id PK
        timestamptz received_at
        jsonb buckets "array of cpt and quantity"
    }
    shift_plans {
        text path_id PK
        integer planned_heads
        integer installed_stations
        double_precision rate_units_per_hr
        double_precision hours
        double_precision travel_distance_m "nullable, 0010"
        boolean travel_distance_estimated "nullable, 0010"
    }
    work_pools {
        text path_id PK
        text mode "ReleaseFed or FlowFed"
        integer wip_limit
        integer alarm_threshold
        bigint version "optimistic concurrency, 0007"
    }
    work_pool_entries {
        text path_id PK,FK
        text work_unit_id PK
        timestamptz cpt
        text state "pending, released, completed"
    }
    work_units {
        text id PK
        text path_id "indexed"
        timestamptz cpt
        text reference "indexed, 0004"
        text state
        timestamptz released_at
        timestamptz completed_at
        text sku "0003"
        boolean gift_wrap "0008"
        integer line_no "nullable, 0013"
    }
    labor_plan_view {
        text path_id PK
        integer planned_heads
        double_precision planned_rate
        double_precision planned_hours
        timestamptz observed_at
        integer drift_heads "nullable, 0009"
        timestamptz drift_detected_at "nullable, 0009"
    }
    usable_inventory_view {
        text sku PK
        integer usable_quantity
        timestamptz observed_at
    }
    product_classification_copy {
        text sku PK
        text_array handling_tags
        text temperature_class "nullable"
        smallint dot_hazard_class "nullable"
        bigint version "producer aggregate version"
        timestamptz updated_at
    }
    slot_plans {
        text plan_id PK
        text site_id "indexed with approved_at"
        timestamptz approved_at
        text supersedes_plan_id
        integer move_count
        timestamptz recorded_at
    }
    product_pack_level_copy {
        text gtin PK "14-digit"
        text sku "indexed with level where active"
        text level "EACH, INNER, CASE, PALLET"
        text child_gtin "nullable, 14-digit"
        integer qty_of_child "nullable"
        integer units_per_pack
        integer length_mm "nullable"
        integer width_mm "nullable"
        integer height_mm "nullable"
        integer weight_g "nullable"
        integer measured_length_mm "nullable"
        integer measured_width_mm "nullable"
        integer measured_height_mm "nullable"
        integer measured_weight_g "nullable"
        boolean is_orderable
        boolean is_despatch_unit
        timestamptz effective_from
        timestamptz effective_to "nullable"
        bigint version "producer aggregate version"
        timestamptz updated_at
    }
    processed_events {
        text event_id PK
        timestamptz processed_at
    }
    outbox_events {
        bigserial id PK
        text topic
        text event_type
        bytea key
        bytea value
        jsonb headers
        timestamptz created_at
        timestamptz published_at "partial index where NULL"
        integer attempts
        text last_error
    }
    idempotency_keys {
        text key PK
        text method
        text path
        text request_hash
        integer status_code
        bytea response_body
        jsonb response_headers
        timestamptz created_at "indexed"
        timestamptz completed_at
    }
    events {
        bigserial id PK
        text event_name
        timestamptz occurred_at
        jsonb payload
    }
```

Source: `migrations/0001_init.up.sql` to `migrations/0016_pack_aware_work.up.sql`.
Omits: golang-migrate's own `schema_migrations` bookkeeping table, column
defaults and `NOT NULL` flags (every column above is `NOT NULL` except
`released_at`, `completed_at`, `drift_heads`, `drift_detected_at`,
`travel_distance_m`, `travel_distance_estimated`, `key`,
`published_at`, `last_error`, `temperature_class`, `dot_hazard_class`,
`child_gtin`, `qty_of_child`, the pack-level dimension and measured columns
(`length_mm`, `width_mm`, `height_mm`, `weight_g`, `measured_length_mm`,
`measured_width_mm`, `measured_height_mm`, `measured_weight_g`),
`effective_to` and the idempotency outcome columns).

**The only foreign key in the schema** is
`work_pool_entries.path_id → work_pools.path_id`. Everything else is linked
logically, by value, with no constraint — on purpose:

- `work_pool_entries.work_unit_id` ↔ `work_units.id` — two aggregates
  (`WorkPool`, `WorkUnit`) referenced by identity. A foreign key would couple
  their consistency boundaries; the use cases keep them aligned inside one
  transaction, and `WorkPool.Reconcile` heals a stale entry.
- `work_units.path_id`, `shift_plans.path_id`, `charge_forecasts.path_id`,
  `labor_plan_view.path_id` ↔ `work_pools.path_id` — each is a separate
  aggregate or projection keyed by the same process-path id, whose catalogue
  is owned by process-path-management.
- `usable_inventory_view.sku` ↔ `work_units.sku` — different contexts' keys;
  inventory-storage owns the SKU.
- `product_classification_copy.sku` ↔ `work_units.sku` — product-master owns
  the classification; the copy is fed by its `ProductClassified` events
  ([ADR-0035](../adr/0035-product-classification-local-copy.md)).
- `slot_plans.plan_id` ↔ slotting-optimization's approved plans — the plan is
  owned by slotting-optimization; the copy records every plan this context
  applied (or found stale) so a newer plan supersedes an older one whatever
  order the events arrive in
  ([ADR-0040](../adr/0040-slot-plan-relocation-work.md)).
- `product_pack_level_copy.gtin` / `.sku` ↔ product-master's pack levels —
  the copy is fed by `ProductPackLevelDeclared` / `Retired` / `Measured`
  events, keyed by 14-digit GTIN
  ([ADR-0045](../adr/0045-pack-aware-work-units.md)). Neither new table has a
  foreign key.

## Analytics database

A separate database (`ANALYTICS_DATABASE_URL`), written only by
`cmd/wes-projector` and read only by `cmd/wes-reports`
([ADR-0011](../adr/0011-analytical-data-product.md)).

```mermaid
erDiagram
    analytics_processed_events {
        text event_id PK
        timestamptz occurred_at "indexed desc"
        timestamptz applied_at
    }
    analytics_consumed_events {
        text event_id PK
        timestamptz processed_at
    }
    throughput_rollup {
        text path_id PK
        timestamptz hour_bucket PK
        bigint work_released
        bigint work_unit_completed
        bigint backlog_threshold_breached
        bigint path_throttled
        bigint rate_deviation_detected
    }
    schema_migrations_analytics {
        text filename PK
        timestamptz applied_at
    }
```

Source: `migrations/analytics/0001_report.up.sql`,
`internal/adapters/outbound/analyticsstore/migrate.go`. No foreign keys.
`schema_migrations_analytics` is the bookkeeping table of the projector's own
migrator (`CREATE TABLE IF NOT EXISTS` in `migrate.go`), not a migration file.

## Table ≠ aggregate

| Table | What it is | Owner in code |
|---|---|---|
| `charge_forecasts` | `ChargeForecast` aggregate (buckets as JSONB) | `postgres.ChargeRepo` |
| `shift_plans` | `ShiftPlan` aggregate, one `PathPlan` row per path | `postgres.PlanRepo` |
| `work_pools` + `work_pool_entries` | `WorkPool` aggregate root + its entries | `postgres.WorkPoolRepo` (rewrites every entry on save, guarded by `version`; the `work_pools` row's `mode` / `wip_limit` are set by `ConfigurePool`, [ADR-0034](../adr/0034-configure-pool-command.md), or seeded `ReleaseFed`/1000 by the first enqueue) |
| `work_units` | `WorkUnit` aggregate | `postgres.WorkUnitRepo` |
| `labor_plan_view` | **Read model** `LaborPlanObserved` + ADR-0019 drift | `postgres.LaborPlanViewRepo` |
| `usable_inventory_view` | **Read model** `UsableInventoryObserved` | `postgres.InventoryViewRepo` |
| `product_classification_copy` | **Local copy** of product-master's classification, read as `ProductClassificationView` ([ADR-0035](../adr/0035-product-classification-local-copy.md)) | `productclassificationcopy.Store` |
| `slot_plans` | **Local record** of slotting-optimization's approved plans this context applied or superseded ([ADR-0040](../adr/0040-slot-plan-relocation-work.md)) | `postgres.SlotPlanRepo` |
| `product_pack_level_copy` | **Local copy** of product-master's pack levels, keyed by GTIN ([ADR-0045](../adr/0045-pack-aware-work-units.md)) | `productclassificationcopy` (pack store) |
| `processed_events` | **Infrastructure**: consumer inbox / dedupe on CloudEvents `id` ([ADR-0028](../adr/0028-processed-event-mark-atomic-with-handling.md)) | `postgres.ProcessedEventRepo` |
| `outbox_events` | **Infrastructure**: transactional outbox, one row per topic per event ([ADR-0014](../adr/0014-transactional-outbox.md)); published rows swept after `OUTBOX_RETENTION` | `postgres.OutboxPublisher`, `postgres.OutboxRelay` |
| `idempotency_keys` | **Infrastructure**: `Idempotency-Key` replay store ([ADR-0022](../adr/0022-idempotency-key-middleware.md)); swept after `IDEMPOTENCY_KEY_TTL` | `RequireIdempotencyKey`, `postgres` housekeeper |
| `events` | **Legacy, unused**: created by `0001_init`, no code reads or writes it. Decided 2026-10-06: retained, additive migrations only (never dropped without explicit approval) | — |
| `throughput_rollup` | **Analytics projection** (not a source of truth) | `analyticsstore.PostgresProjection` / `PostgresReport` |
| `analytics_processed_events`, `analytics_consumed_events` | **Infrastructure**: analytics idempotency layers | `analyticsstore` |

`travelDistanceM` / `travelDistanceEstimated` on `PathPlan` (the optional
ADR-0017 hint) are stored in the nullable `shift_plans.travel_distance_m` /
`travel_distance_estimated` columns (migration `0010`); `NULL`
`travel_distance_m` means no hint was recorded.
