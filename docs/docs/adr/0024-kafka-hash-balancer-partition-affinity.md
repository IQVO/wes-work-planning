---
id: 0024-kafka-hash-balancer-partition-affinity
slug: /adr/0024-kafka-hash-balancer-partition-affinity
title: 24. Switch every outbound Kafka writer's Balancer from LeastBytes to Hash
sidebar_label: 24. Kafka Hash balancer for partition affinity
description: "ADR 0024 — every kafka-go Writer this service constructs (Publisher, AnalyticsPublisher, RelaySink) used &kafkago.LeastBytes{}, a balancer that ignores Message.Key entirely and balances purely by cumulative byte volume. Every message these writers produce already carries a correct per-aggregate Key, but LeastBytes silently discarded it for partition routing — invisible while every business topic ran at 1 partition, and a real per-aggregate-ordering gap since warehouse-infra PR #42 scaled them to 8. Fix: switch every writer's Balancer to &kafkago.Hash{} (FNV-1a over Key), proven with a real-broker Testcontainers test on an 8-partition topic. Mirrors order-management's ADR-0027 (PR #111) and the same fix already merged in inventory-storage and workforce-management."
---

# 24. Switch every outbound Kafka writer's Balancer from LeastBytes to Hash

## Status

Accepted — implemented in the same change that introduces this record.

## Context

`warehouse-infra` PR #42 (already merged into `develop`) took every
business topic — including this service's own
`warehouse.work-planning.events` and `warehouse.wes.analytics` — from 1
partition to 8, to raise consumer-side throughput headroom fleet-wide.

Every message this service's outbound Kafka adapters build was meant to
carry a per-aggregate `Message.Key`: `AnalyticsPublisher.marshalAnalyticsData`
keys by the raising aggregate's own id (`PathId` or `WorkUnitId`). The
integration `Publisher` (`encodeCloudEvent`), however, keyed by the
**CloudEvents event id** — a fresh UUID per event — so even with a
key-aware balancer `WorkUnitCreated`, `WorkReleased` and `WorkUnitCompleted`
of ONE work unit landed on different partitions (see "Follow-up" below).
By inspection, the analytics side looks like it
already provides per-aggregate partition affinity.

It does not. Every `*kafkago.Writer` this package constructs
(`Publisher.NewPublisher`, `AnalyticsPublisher.NewAnalyticsPublisher`,
`RelaySink.NewRelaySink`) was configured with `&kafkago.LeastBytes{}`.
`LeastBytes.Balance` picks the partition with the least cumulative bytes
written so far, and only reads `len(msg.Key) + len(msg.Value)` to update
that running total — it never hashes or otherwise routes on the key's
*content*. Setting a correct, non-nil `Message.Key` achieves nothing for
partition placement under this balancer: two messages with an identical
key can still land on different partitions, because `LeastBytes` is a
pure load-balancing strategy, not a key-routing one. `kafka-go`'s
`Writer` does not pick a key-aware balancer automatically just because a
message happens to carry a key — `Balancer` is a separate, independent
configuration knob.

This was invisible for as long as every topic ran at 1 partition (every
message lands on the only partition regardless of balancer), and remains
invisible to a fake-writer unit test, because a fake `Writer.WriteMessages`
never computes a partition at all — it can only assert the `Key` bytes are
set, which is necessary but not sufficient.

This exact bug — `Message.Key` set correctly, `Balancer` left as
`LeastBytes` — was found and fixed identically in order-management
(ADR-0027, PR #111), and mirrored into inventory-storage and
workforce-management. This ADR ports the same fix to wes-work-planning's
three outbound writers.

## Decision

1. **Every `*kafkago.Writer` this package constructs
   (`Publisher.NewPublisher`, `AnalyticsPublisher.NewAnalyticsPublisher`,
   `RelaySink.NewRelaySink`) switches its `Balancer` from
   `&kafkago.LeastBytes{}` to `&kafkago.Hash{}`.** `Hash` is the balancer
   that actually keys partition placement off `Message.Key`'s bytes
   (FNV-1a hash mod partition count) — the same algorithm Sarama's hash
   partitioner uses, per `kafka-go`'s own documentation.
2. ~~No change to what key is set~~ — **amended (follow-up below):** the
   analytics `AnalyticsPublisher` already keyed every message by aggregate
   id; the integration `Publisher` is changed to do the same.
3. No custom `Balancer` implementation, no producer-side partition
   pinning, and no consumer-side reordering buffer were introduced —
   `kafka-go`'s stock `Hash` balancer plus the existing non-nil `Key` is
   sufficient: this service does not need a specific partition number,
   only that the same aggregate's events always land on the same
   partition as each other, regardless of partition count.
4. Verified with a real-Kafka Testcontainers integration test
   (`balancer_partition_affinity_integration_test.go`,
   `TestAnalyticsPublisherKeysMessagesForSameAggregateOntoTheSamePartition`)
   against an 8-partition topic: 3 events for one work unit plus 1 for a
   different work unit are published, every partition is read back, and
   the test asserts all 3 same-aggregate messages land on one partition
   while the other aggregate's event lands independently. This test was
   confirmed to fail (1 of 3 on the right partition) when the writer's
   `Balancer` is reverted to `LeastBytes`, and pass (3 of 3) with `Hash` —
   proving a fake-writer unit test alone would not catch this class of
   bug.

## Consequences

- Every event `Publisher` or `AnalyticsPublisher` emits for the same
  aggregate (`PathId` or `WorkUnitId`) now deterministically lands on the
  same partition of its topic, for any partition count. A consumer
  reading with multiple instances across many partitions observes a
  correct relative order for any single aggregate's own event history.
  Ordering across different aggregates is still not guaranteed and was
  never a requirement.
- `RelaySink`'s writer relays already-`Encoded` messages built by either
  publisher; it must use the same key-aware balancer as those publishers
  or the guarantee breaks again on the relay's own hop. This ADR switches
  it too.
- No wire-format change: `Balancer` is `kafka-go` client-side routing
  configuration, not part of the JSON envelope/payload a consumer
  decodes. No consumer of either topic needs any change.
- Mirrors order-management's ADR-0027 (PR #111) and the same fix already
  applied in inventory-storage and workforce-management — all four
  services' outbound Kafka writers now use `Hash`, closing this gap
  fleet-wide for every repo audited so far.

## Follow-up: the integration topic is keyed by the aggregate id

The 2026-10-04 ADR audit found that the `Hash` balancer alone did not give
per-aggregate affinity on `warehouse.work-planning.events`: `encodeCloudEvent`
keyed each message by its CloudEvents `id`, which is unique per event, so one
work unit's `WorkUnitCreated`/`WorkReleased`/`WorkUnitCompleted` could land on
different partitions. (The `Publisher.encodeFlat` mentioned in the original
text no longer exists — the flat envelope was removed by ADR-0027.)

Fix: `encodeCloudEvent` now keys by the aggregate id — the same value as the
CloudEvents `subject` (work unit id for WorkUnit events, path id for
everything else); the event id is used only as a fallback if an event type has
no aggregate id. The outbox persists the key with the row, so relayed messages
are keyed identically. Proven by
`TestPublisherKeysEveryEventOfOneWorkUnitOntoTheSamePartition` (8-partition
Testcontainers topic, distinct event ids per message).

Consumer impact: none. fulfillment-execution (which consumes `WorkReleased`)
and the other consumers dedupe on the CloudEvents `id`, not the Kafka key, and
never relied on the key's value; they now additionally observe
per-work-unit ordering.
