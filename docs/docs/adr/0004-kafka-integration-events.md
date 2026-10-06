---
id: 0004-kafka-integration-events
title: ADR-0004 — Kafka integration events with a shared envelope and a CloudEvents type convention
sidebar_label: 0004 · Kafka + event envelope
sidebar_position: 4
description: Why cross-context integration is asynchronous over Kafka, why the envelope is duplicated rather than shared, and how idempotency is guaranteed.
---

# ADR-0004 — Integrate over Kafka with a shared envelope convention

## Status

**Superseded by [ADR-0027](./0027-cloudevents-mandatory-event-envelope.md)**
for the wire envelope (§2, §3 and the caution below). The rest — Kafka as the
integration transport (§1), publishing behind the port (§4), mandatory
consumer idempotency (§5) and consuming without publishing (§6) — remains in
force; idempotency now keys on the CloudEvents `id`. Originally accepted
platform-wide.

:::warning[Superseded envelope]
The flat envelope shown in §2 is retired. Every message is now a CloudEvents
1.0 structured-mode event — see ADR-0027. The text below is kept unchanged
as the historical record.
:::

## Context

Five bounded contexts have to exchange facts. This one alone needs stock reality
from Inventory, labour plans from Workforce, and a closed loop with Execution.

The alternative is synchronous HTTP between services. It fails here for
specific, not generic, reasons:

- **It would put another context on the critical path of release.** Calling
  Execution from `ReleaseNextWork` makes releasing work fail when Execution is
  down — coupling an availability decision to an unrelated deployment.
- **It inverts the ownership of facts.** "Stock was reserved" is a fact
  Inventory owns and announces. Polling Inventory for it makes us responsible
  for knowing when to ask.
- **It would tempt shared aggregates.** The strategic reference is unambiguous:
  *"No shared aggregates across contexts … All cross-context communication is
  via integration events/published APIs — enforce this with an explicit
  Anti-Corruption Layer at each boundary."*

The remaining questions were the wire format and how to survive at-least-once
delivery.

## Decision

**We will integrate asynchronously over Kafka, using a fixed envelope
duplicated by agreement in every service, with a CloudEvents-style `type`
naming convention as the documented target contract, and idempotency enforced
by an `event_id` de-duplication table at every consumer.**

### 1. Kafka, `segmentio/kafka-go`

Pure Go, no cgo — so the service builds and cross-compiles without a C
toolchain. One shared broker for all five services; this repository does not
add its own broker to its `docker-compose.yml`.

### 2. One envelope, duplicated by agreement

```json
{
  "event_id": "uuid-v4",
  "event_type": "WorkReleased",
  "occurred_at": "2026-08-21T22:00:00Z",
  "source": "wes-work-planning",
  "data": { }
}
```

`event_id` is a UUID v4 minted at publish time and is also the Kafka message
key. `occurred_at` comes from the **domain clock**, not from publish time.

The struct lives in each service's own `internal/adapters/kafka/envelope`
package and is **deliberately not extracted into a shared library**. A shared
library would make the envelope a versioned dependency, and bumping it would
force coordinated redeploys — reintroducing exactly the coupling the async
boundary was chosen to avoid. The cost is that the definition is copied five
times and could drift; the mitigation is that it is tiny, stable, and each
service's contract is published and linted.

### 3. `type` naming convention

The published contract (`apis/asyncapi.yaml`) uses CloudEvents 1.0 structured
mode with a reverse-DNS type shared platform-wide:

```
com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>
com.warehouse.wes.work-planning.workunit.WorkReleased
```

All segments lowercase except the final PascalCase event name, which matches
the past-tense domain event name in the code.

:::caution[Recorded honestly]
The **running adapters still write the simpler envelope** in §2; the CloudEvents
migration is documented but not implemented, and the sibling consumers expect
the simpler shape. Both are described on the [Events](../api/events.md) page,
and this ADR is where the gap is acknowledged rather than hidden.
:::

### 4. Publishing is opt-in and behind the existing port

The Kafka publisher implements the **same `ports.EventPublisher` interface** as
the log publisher. `EVENT_PUBLISHER=kafka|log` selects one; the default stays
`log` so the existing test suite runs with no broker. Use cases cannot tell
which is wired.

The outbound adapter enriches `WorkReleased` with `cpt` and `ref` by reading the
work-unit repository, so the published contract is self-sufficient and no
consumer has to call back. That enrichment lives in the **adapter** — the domain
event stays ignorant of downstream needs.

### 5. Idempotency is mandatory at every consumer

Kafka is at-least-once, so redelivery is normal. Before applying any consumed
event's effect, its `event_id` is inserted into `processed_events (event_id TEXT
PRIMARY KEY, processed_at TIMESTAMPTZ)`. A primary-key violation means "already
processed": skip the effect, **ack anyway**. The in-memory adapter uses a
mutex-guarded set with identical semantics. Both sit behind one port,
`ProcessedEventRepo`.

Using the primary-key violation *as* the check — rather than read-then-write —
means there is no race between two consumers processing the same redelivery.

### 6. Consuming does not require publishing

Setting `KAFKA_BROKERS` starts the inbound consumer independently of
`EVENT_PUBLISHER`, so a service can observe the platform without emitting to it.

## Consequences

### Easier

- **Availability is decoupled.** Releasing work does not fail because Execution
  is deploying.
- **The ACL has an obvious home.** Foreign payload structs are unexported inside
  the inbound adapter; a sibling changing its wire format touches no domain file.
- **Redelivery is boring.** Replaying a `StockReserved` does not double-decrement;
  replaying a `TaskCompleted` does not surface a spurious `ErrAlreadyCompleted`.
  Each is a unit test.
- **Tests need no broker.** Defaults keep the whole suite infrastructure-free;
  the real-broker test is `//go:build integration` and skipped without
  `KAFKA_BROKERS`.
- **Adding a consumer is additive.** `TaskCompleted` was added later as a third
  topic subscription calling the *existing* `RecordCompletion` use case — no
  domain change at all.

### Harder

- **Eventual consistency is now visible in the API.** `LaborPlanObserved` and
  `UsableInventoryObserved` are as fresh as the last message. Both carry
  `ObservedAt` and both return `404` when nothing has been seen, so staleness is
  explicit rather than disguised.
- **Ordering is per-partition only.** Keying by `event_id` distributes messages
  across partitions, which gives no ordering guarantee between two events about
  the same path or SKU. Tolerable today because the inventory projection applies
  commutative deltas and the labour projection is a last-writer-wins upsert —
  but it is a real constraint, and an event type needing strict per-key ordering
  would require re-keying the topic.
- **`processed_events` grows without bound.** No retention or pruning is
  implemented. It will need a TTL or partitioning before it is a production
  concern.
- **Debugging spans services.** "Why is usable inventory wrong?" now involves
  two services and a broker. Mitigated by the `event_id` appearing in the
  envelope, the message key and `processed_events`.
- **Two documented envelopes.** Until the CloudEvents migration lands, the spec
  and the code disagree, and a reader must know which to trust. Documented in
  three places rather than left to be discovered.

## Addendum (2026-09-30)

Superseded for the envelope by
[ADR-0027](./0027-cloudevents-mandatory-event-envelope.md): CloudEvents 1.0
is mandatory on every topic, with no flat envelope and no dual mode.

## Addendum (2026-09-26)

The CloudEvents migration referenced above as a future intent now has a
concrete plan: [ADR-0021](./0021-cloudevents-envelope-migration.md)
(Proposed at the time; since **Superseded** by ADR-0027), a companion to fulfillment-execution's own envelope-migration
ADR. It implements the CloudEvents envelope this ADR documented but never
shipped, via a non-breaking dual-read/dual-write bake period. See ADR-0021
for the schema, phase sequence, and rollback plan.
