---
id: 0020-flowfed-path-observed-throughput-signal
slug: /adr/0020-flowfed-path-observed-throughput-signal
title: 0020. FlowFed paths stay Known=false permanently; an observed-throughput signal is proposed alongside, not instead
sidebar_label: 0020. FlowFed throughput signal
description: ADR 0020 (Accepted) — answers ADR-0018's deferred "different signal for flow-fed admission" question. Concludes a hard admission ceiling is structurally impossible for FlowFed paths regardless of new data, evaluates facility-layout geometry, process-path-management's cycle-time/CPT data, and fulfillment-execution's completion history as candidate inputs, and proposes an additive, honestly-caveated observed-throughput signal built from data this repo already has.
---

# 0020. FlowFed paths stay `Known=false` permanently; an observed-throughput signal is proposed alongside, not instead

## Status

**Accepted** (2026-09-26). Documentation-only decision (Option B: reuse
the existing `GET /reports/throughput` report) — no code follows.

## Context

[ADR-0018](./0018-path-capacity-changed.md) built `PathCapacityChanged`
and, for a `FlowFed` `WorkPool`, deliberately reports `Known=false,
RemainingUnits=0` **always** — never deriving a figure from
`alarmThreshold`, because that would misrepresent a backlog alarm as a
hard admission ceiling. That ADR named the gap and explicitly left it
open:

> `Known=false` on a FlowFed path is a real, permanent gap in v1, not a
> temporary placeholder... until/unless a later ADR introduces a
> different signal for flow-fed admission (out of scope here).

This ADR is that later ADR. The question it has to answer honestly is not
"how do we make `Known=true` work for FlowFed" — that question has
already been answered, correctly, by ADR-0018 and by this repo's own
[ADR-0003](./0003-flow-balancing-as-domain-service.md), which states the
domain fact `PathCapacityChanged` merely reports: *"A flow-fed pool
cannot refuse arrivals — a conveyor does not ask permission — so the only
lever is upstream admission."* A pool with no refusal lever has no
admission ceiling to report, by construction, independent of what data
exists anywhere in the fleet. No new fact changes that: this is a
structural property of the domain, not a data-availability gap. Forcing a
number into `RemainingUnits`/`Known` here would repeat exactly the
mistake ADR-0018 already rejected for the `alarmThreshold` case.

The real, answerable question is narrower: **is there ANY signal —
distinct from, and not pretending to be, a hard admission ceiling — that
could help a caller (order-management's promise engine, per its own
ADR-0014) make a better decision about a FlowFed path than "no
information at all"?**

### What data already exists in the fleet, checked directly

**facility-layout's travel-graph/geometry (its own ADR-0017, "Geometry &
Travel Graph").**
Ships aisle centrelines, cross-aisles, and a shortest-path travel
**distance** in metres between two `LocationCode`s, with a `FixedStructure`
aggregate that includes a `Conveyor` kind. Checked directly: `Conveyor` is
a footprint and a label only — no speed, no belt rate, no capacity
attribute of any kind is modelled anywhere in that service. Distance
alone says nothing about units/hour: two conveyors of identical length
can move product at wildly different rates. **This input cannot produce a
throughput or capacity figure**, and facility-layout's own ADR states its
boundary explicitly: "FL publishes distances and adjacency (facts). Travel
TIME, congestion, route choice = wes-work-planning." Distance is real, but
it is the wrong kind of fact for this question.

**process-path-management's cycle-time/CPT schedule (its own ADR-0010,
"Process paths publish a fulfillment capability contract").**
`ProcessPath.cycleTimeP95` is a **declared, operator-set** end-to-end
duration — a capability, not a measurement. That ADR draws this exact
line itself: *"Capability — facts an operator declares... Capacity — how
many more units a path can absorb... operational state... this service
must not become a mirror of WES state."* `cycleTimeP95` could, in
principle, be combined with a path's physical length and a flow rate to
back into a theoretical maximum throughput — but that arithmetic needs
data neither this fact nor facility-layout's geometry supplies (a
conveyor's actual speed and minimum unit spacing), and it is explicitly
not process-path-management's job to declare it: that ADR names WES
(this repo) as the eventual owner of the capacity half of this picture.
**This input is a real capability fact but cannot alone produce a
capacity or throughput figure**, and is not the right layer to add one
to.

**fulfillment-execution's task completion rates.** This is the one input
that is actually shaped like the answer — and, checked directly against
this repo's own code, **it already arrives here**. `fulfillment-execution`
publishes `TaskCompleted` on `warehouse.fulfillment.events`; this
repo's own Kafka consumer already feeds every one of those messages into
the existing `RecordCompletion` use case
(`internal/adapters/inbound/kafka/consumer.go`'s `handleFulfillmentEvent`),
which calls `WorkPool.Complete` and publishes `WorkUnitCompleted`. **No
new cross-repo dependency is needed to get completion facts for a FlowFed
path — this service already consumes them, today, for every path,
FlowFed or not.** The gap is not data availability; it is that nothing
in this repo currently aggregates those already-flowing completions into
a rate.

Two things this repo already has, that are directly relevant:

1. `internal/domain/shared/events.go` declares `RateDeviationDetected`
   — but grep confirms it is **never raised anywhere** in the codebase.
   [ADR-0003](./0003-flow-balancing-as-domain-service.md) names this
   directly: *"Richer balancing would need actual-rate projections that
   are not built (which is also why `RateDeviationDetected` is declared
   but never raised)."* An actual-rate signal has been an acknowledged,
   unbuilt gap in this repo since before this ADR.
2. The analytics data product
   ([ADR-0011](./0011-analytical-data-product.md)) **already computes and
   stores** exactly the raw ingredient this signal needs:
   `report.Row.WorkUnitCompleted`, a real count of `WorkUnitCompleted`
   events per `(PathId, hourBucket)`, served today at `GET
   /reports/throughput`. This is, right now, an hourly, eventually-
   consistent observed-completions-per-path series — a rate signal, just
   never framed, documented, or offered as one for admission/promise
   purposes.

## Decision

**A hard admission ceiling for FlowFed paths is not proposed and is not
achievable with any data this fleet has or could plausibly add — `Known`
stays permanently `false` on `PathCapacityChanged` for FlowFed pools,
exactly as ADR-0018 already decided, and this ADR does not reopen that.
Separately and additively, we propose exposing an observed-throughput
signal, distinct from the capacity contract, built from data this repo
already receives, with an honest v1 recommendation to reuse what is
already shipped rather than build something new.**

### 1. Why no data source closes the `Known=true` gap

Restated for the record, because it is the load-bearing conclusion: a
`FlowFed` pool has, by this repo's own domain model, no enforced ceiling
— `alarmThreshold` is a backlog alarm, not an admission control, and
nothing about facility-layout's distances, process-path-management's
declared cycle time, or fulfillment-execution's completion facts changes
that a conveyor does not refuse arrivals. **This is honestly a "cannot be
solved" answer to the literal question ADR-0018 deferred**, and this ADR
does not force a solution that doesn't hold up.

### 2. What CAN be offered instead: an observed-throughput signal, kept structurally separate from capacity

Two ways to produce it were compared:

**Option A — a new domain field, live and synchronous.** Extend
`poolEntry` with a `completedAt time.Time`, and `WorkPool` with a method
computing a trailing-window observed-completions-per-hour figure from
already-completed entries, refreshed at `Complete()` time — same
"evaluated at the moment of the triggering call, no scheduler" discipline
as `RemainingCapacity()` (ADR-0018) and `RebalanceDecision` (ADR-0003).
Would ship as a new event, e.g. `PathThroughputObserved{PathId,
WindowStart, WindowEnd, ObservedUnitsPerHour, SampleCount}`, and/or a new
field on the existing telemetry read
(`GET /paths/{pathId}/telemetry`).

- Pro: real-time (as fresh as the last completion), no dependency on the
  analytics pipeline's freshness SLA.
- Con: new domain state (`poolEntry` currently retains no timestamp once
  released/completed — this is a real, non-trivial aggregate change, not
  a read-only addition), a design choice for the trailing window's size
  that has no obvious right answer yet, and a second "rate" concept
  alongside the already-declared-but-unused `RateDeviationDetected` that
  would need reconciling with it rather than adding a third one.

**Option B — reuse the existing analytics report as-is.** Document
`GET /reports/throughput`'s existing `workUnitCompleted` count per
`(pathId, hourBucket)` (ADR-0011, already shipped) as the fleet's answer
to "what is this FlowFed path's observed throughput" — zero new domain
code, zero new event, zero new table.

- Pro: nothing to build. The data has existed since ADR-0011 shipped;
  this ADR's contribution is naming it as usable for this purpose and
  documenting its caveats honestly for a caller that might reach for it
  expecting a capacity guarantee.
- Con: hourly granularity, eventually consistent to the report's
  documented freshness SLA (p95 < 30s event-to-report lag, but the report
  itself buckets by hour, not by a caller's specific decision moment), and
  reachable only via the read-only analytics reader
  (`cmd/wes-reports`), a separate process from the OLTP path a caller
  might expect a capacity signal to come from.

**We propose Option B for v1: no new code, an explicit recommendation
that a caller wanting any signal for a FlowFed path's historical
completion behavior reads `GET /reports/throughput`, filtered to that
`pathId`, and treats it as a coarse, non-binding, historical trend — never
an admission guarantee, never wired into `PathCapacityChanged` at all.**
Option A is named as a possible future refinement, deferred until a real
consumer demonstrates it needs sub-hour freshness or a synchronous read
the analytics reader cannot provide — the same "deferred, not designed
speculatively" posture ADR-0018 itself took toward
order-management's own consuming adapter.

### 3. Recommended interim caller-side handling (the honest answer this ADR gives)

For order-management, or any caller evaluating a FlowFed path for a
promise or admission decision:

1. **Treat `PathCapacityChanged.Known=false` on a FlowFed path exactly as
   ADR-0018 already specifies** — a real, permanent, expected answer, not
   a placeholder to work around. `UnknownPathCapacity`'s existing
   `known=false` fallback already treats this as first-class; nothing
   changes here.
2. **Optionally, off the hot path, consult `GET /reports/throughput`**
   for that `pathId` as a soft, historical input to the caller's OWN risk
   buffer or lead-time padding for FlowFed-fed orders — never as a
   replacement for a real admission signal, because none exists, and
   never synchronously in the promise-calculation path itself (the
   analytics reader is a separate, eventually-consistent process; calling
   it inline would reintroduce exactly the cross-context hot-path coupling
   this fleet's architecture (ADR-0004, this repo's own outbox
   discipline) avoids elsewhere).
3. **Do not attempt to reconstruct a ceiling from `alarmThreshold` or any
   other FlowFed-side figure.** ADR-0018 already tried and rejected this;
   this ADR's research confirms no additional fleet data changes that
   conclusion.

## What is IN v1 and what is explicitly deferred

**In v1:**

- No change to `PathCapacityChanged`'s contract. `Known=false` for
  FlowFed remains permanent, exactly as ADR-0018 decided.
- Documentation only: `GET /reports/throughput` is named, in this record
  and in the `docs/docs/ecosystem/` sibling-service documentation, as the
  fleet's answer to "observed throughput for a FlowFed path," with the
  caveats in the decision above stated explicitly wherever it is
  referenced for this purpose.
- No new domain event, no new field, no new table, no new consumer
  wiring.

**Deferred, and named as such rather than silently dropped:**

- **Option A** (a real-time, sub-hour observed-throughput figure computed
  from `poolEntry.completedAt` and a new `PathThroughputObserved`
  event) — deferred until a real consumer states a freshness or
  synchronicity requirement the existing analytics report cannot meet.
- **Actually raising `RateDeviationDetected`** — this ADR does not
  attempt to close that separate, longer-standing gap (ADR-0003's own
  "richer balancing... actual-rate projections that are not built"); a
  future ADR building Option A would need to reconcile the two rather
  than ship a third, unrelated "rate" concept.
- Any change to `RemainingCapacity()`/`WorkPool`'s domain model — none is
  proposed here.
- Any caller-side promise-engine algorithm that consumes the observed-
  throughput signal — this ADR documents that the signal exists and how
  to reach it; how order-management (or any caller) weights it into a
  promise decision is that caller's own future ADR, exactly as ADR-0018
  left order-management's `PathCapacity` adapter to order-management's
  own PR.

## Alternatives considered

- **Derive `RemainingUnits` from `alarmThreshold` for FlowFed paths.**
  Already considered and rejected by ADR-0018 itself; this ADR's research
  found no new fact that overturns that reasoning, and does not
  reconsider it.
- **A new cross-repo dependency on fulfillment-execution specifically for
  this signal.** Rejected as unnecessary — completion facts for every
  path, FlowFed included, already arrive via the existing
  `warehouse.fulfillment.events` consumer; no new subscription is needed.
- **A new cross-repo dependency on facility-layout or
  process-path-management for a derived throughput figure.** Rejected —
  neither service models the missing ingredient (conveyor speed/minimum
  spacing); combining their existing facts cannot produce a real
  throughput number, only a fabricated-looking one, which would repeat
  ADR-0018's own rejected `alarmThreshold`-as-capacity mistake in a
  different guise.
- **Build Option A (live, sub-hour signal) now, in this ADR.** Rejected
  for v1 — it is a real, non-trivial aggregate change (new state on
  `poolEntry`, an unresolved window-size design question, and an
  unreconciled overlap with the already-declared, unused
  `RateDeviationDetected`) proposed with no concrete consumer need yet
  stated; Option B answers the same near-term need with code that is
  already shipped.

## References

- [ADR-0018 — `PathCapacityChanged`](./0018-path-capacity-changed.md) —
  the ADR whose "a later ADR introduces a different signal for flow-fed
  admission" this record answers, and whose `Known=false`-for-FlowFed
  decision this record leaves unchanged.
- [ADR-0003 — Flow balancing as a domain service, not a scheduled
  job](./0003-flow-balancing-as-domain-service.md) — the domain fact that
  a FlowFed pool has no refusal lever, and the prior naming of
  `RateDeviationDetected` as declared-but-unraised.
- [ADR-0011 — Per-service analytical data product](./0011-analytical-data-product.md) —
  the existing `GET /reports/throughput` this ADR recommends reusing.
- facility-layout ADR-0017 (Geometry & travel graph) — evaluated and
  found to supply distance, not throughput/capacity data.
- process-path-management ADR-0010 (Fulfillment capability contract) —
  evaluated and found to supply a declared capability constant, not a
  capacity/throughput measurement, and the ADR that itself names WES as
  the future owner of the capacity half of this picture.
