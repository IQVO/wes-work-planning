---
paths:
  - "docs/docs/adr/**"
---

# ADR index (0001–0035)

Full records live in `docs/docs/adr/` (index: `docs/docs/adr/index.md`).
Read the relevant ADR before reversing a documented decision.

- 0001 hexagonal ports and adapters
- 0002 waveless continuous release
- 0003 flow balancing as a domain service
- 0004 Kafka integration events (envelope parts superseded by 0027)
- 0005 RFC 7807 problem details
- 0006 Labor Plan View vs. Workforce's own ShiftPlan model
- 0007 arch-go fitness tests
- 0008 MCP inbound adapter (auth section superseded by 0016)
- 0009 product classification propagation to `WorkReleased` (live HTTP lookup superseded by 0035)
- 0010 gift-wrap as a `WorkReleased` characteristic
- 0011 analytics data product
- 0012 process-path catalogue validation (amended by 0030)
- 0013 standard metrics convention
- 0014 transactional outbox
- 0015 REST identity / static bearer scopes (added auth; SUPERSEDED by 0016)
- 0016 removes REST/MCP static-bearer auth fleet-wide
- 0017 travel-distance lookup on `CommitShiftPlan`
- 0018 `PathCapacityChanged` integration event (consumed by order-management and network-fulfillment)
- 0019 reconcile committed `PathPlan` against `LaborPlanObserved`
- 0020 FlowFed paths stay `Known=false`; observed-throughput signal proposed
- 0021 CloudEvents dual-mode migration (SUPERSEDED by 0027)
- 0022 Idempotency-Key middleware for `POST /paths/{pathId}/work-units`
- 0023 circuit breakers, read-only retry, Kafka DLQ, graceful shutdown
- 0024 Kafka Hash balancer (partition affinity) on every outbound writer
- 0025 per-workload HPA and pgxpool tuning
- 0026 golang-migrate over a direct Postgres connection, not PgBouncer
- 0027 CloudEvents 1.0 is the mandatory envelope (supersedes 0021 and the envelope parts of 0004)
- 0028 processed-event mark commits atomically with the handling it guards
- 0029 optimistic concurrency for the `WorkPool` aggregate
- 0030 Kafka-sourced process-path catalogue (`PATH_CATALOGUE_SOURCE=kafka`) and boot-time dial retry (amends 0012)
- 0031 consume `OrderAllocated`/`OrderPartiallyAllocated` by choreography, fire-and-forget
- 0032 retention sweeper for `idempotency_keys` and published `outbox_events`
- 0033 consume network-inventory-planning's `WorkDemandReleased` into transfer-referenced work units (choreography; `WorkReleased` carries optional transfer context)
- 0034 `ConfigurePool` command (`PUT /paths/{pathId}/pool`): sets a pool's feed mode and WIP limit; lowering below current WIP never evicts, releases pause until WIP < limit
- 0035 product classification from a local Postgres copy of product-master's `ProductClassified` (`warehouse.product-master.events`, group `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`); `PRODUCT_CLASSIFICATION_MODE=kafka|permissive`, `http` rejected at boot; supersedes 0009's live lookup
