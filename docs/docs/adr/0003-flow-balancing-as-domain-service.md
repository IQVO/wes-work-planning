---
id: 0003-flow-balancing-as-domain-service
title: ADR-0003 — Flow balancing as a domain service, not a scheduled batch job
sidebar_label: 0003 · Flow balancing as a domain service
sidebar_position: 3
description: Why the rebalance decision is evaluated synchronously on read over a pool snapshot instead of by a periodic sweep.
---

# ADR-0003 — Model flow balancing as a domain service, not a scheduled job

## Status

**Accepted.**

## Context

Once work is on the floor, paths drift from plan: a path staffed for 95
units/hour runs 70, a buffer holds ten times what it should, an upstream path
outruns a downstream one. Detecting the drift and naming the correction is
[flow balancing](../business-context/flow-balancing.md) — Drum-Buffer-Rope with
CPT as the drum.

The conventional implementation is a **scheduled sweep**: a job runs every
minute, walks every path, compares telemetry to plan, emits alerts. It is the
first design most people reach for.

Two things make it a poor fit.

**First, the shape of the logic.** The decision needs a cross-aggregate,
near-real-time view — pool state, feed mode, WIP limit, alarm threshold — held
simultaneously. The platform's strategic reference makes exactly this argument
about the analogous `AssignmentOptimizer`:

> **Domain Service, not Aggregate method**: the actual matching logic … is best
> modelled as a **Domain Service** … rather than a method on `Assignment`
> itself, because it needs a cross-aggregate, near-real-time view of the whole
> `TaskPool` and `ResourcePool` simultaneously — a single `Assignment` instance
> can't compute its own optimal match in isolation.

The same reasoning applies here. A `WorkPool` can report its own backlog and
WIP, but the *recommendation* is a judgement about a pool in the context of its
feed mode and its plan — it does not belong to the pool aggregate any more than
an optimal assignment belongs to a single `Assignment`.

**Second, the shape of the timing.** A sweep has a staleness window equal to its
period: an operator asking "what should I do about `pick-a` right now?" gets an
answer computed up to a minute ago, about a pool whose backlog moves every few
seconds.

And a scheduled job is hard to test in the way that matters. Asserting "a
release-fed pool at its WIP limit with backlog remaining recommends
`ReassignLabor`" through a scheduler means asserting through timing, and you end
up testing the scheduler.

## Decision

**We will model flow balancing as `RebalanceDecision`, a synchronous decision
evaluated on read over a pool snapshot, and it will recommend rather than act.**

1. **Evaluated on demand.** `POST /paths/{pathId}/rebalance-decisions` (formerly `GET .../rebalance`, see [ADR-0047](./0047-fleet-rest-api-guideline.md)) computes the
   recommendation at the moment it is asked. No scheduler, no background
   goroutine, no staleness window.
2. **Pure function of a snapshot.** Inputs: feed mode, backlog depth, WIP, WIP
   limit, alarm threshold. No hidden state.
3. **The lever depends on the controllable input** — the substantive rule:

   | Feed mode | Condition | Recommendation | Event |
   |---|---|---|---|
   | Flow-fed | `backlogDepth > alarmThreshold` | `ThrottleUpstream` | `PathThrottled` |
   | Release-fed | `WIP ≥ wipLimit` **and** `backlogDepth > 0` | `ReassignLabor` | `LaborReassignmentFlagged` |
   | either | otherwise | `NoActionNeeded` | — |

   A flow-fed pool cannot refuse arrivals — a conveyor does not ask permission —
   so the only lever is upstream admission. A release-fed pool at its WIP limit
   is *already* exercising its lever fully; further throttling would push on a
   control already fully pressed, so the constraint is capacity, not admission.

4. **It recommends; it does not act.** No headcount is moved, no release is
   stopped. The output is a named decision plus the telemetry that justified it,
   and an event.
5. **`NoActionNeeded` is a real answer.** A recommender that always recommends
   something trains people to ignore it.
6. **Observed labour is context, not input.** The response may carry the
   `LaborPlanObserved` projection, but the decision table above does not consult
   it.

## Consequences

### Easier

- **No staleness.** The answer is as fresh as the request.
- **Every branch is a two-line test.** Construct a pool in a given state, call
  `Execute`, assert the action and the event. No clock manipulation, no
  scheduler, no flakiness. The rebalance rules are also covered end-to-end by a
  godog acceptance feature (`features/rebalance.feature`).
- **The two-lever rule is legible.** Reading the switch statement tells you the
  whole policy, including why release-fed and flow-fed differ.
- **No control-loop oscillation.** Because nothing acts automatically, the
  system cannot enter the classic feedback-loop pathology of throttling on a
  single noisy sample, over-correcting, and throttling again.
- **No coupling to another context's cadence.** Keeping `LaborPlanObserved` out
  of the decision means our flow balancing does not change behaviour when
  Workforce changes how often it publishes.

### Harder

- **Nothing is detected unless someone asks.** A path can sit over its alarm
  threshold indefinitely with no recommendation computed, because the trigger is
  a request. Continuous monitoring therefore has to come from *outside* — a
  poller, a dashboard, an operator. That is a genuine gap, accepted knowingly:
  the alternative embeds a scheduler in the core domain.
- **`PathThrottled` and `LaborReassignmentFlagged` fire per evaluation.** Two
  reads of a saturated path emit two events. Consumers must deduplicate or
  debounce; the events are facts about an evaluation, not edge-triggered state
  changes.
- **Recommendations require a human or an external policy to act.** Correct for
  labour — moving people belongs to `workforce-management` — but it does mean
  the loop is not closed automatically.
- **The rule is deliberately coarse.** Two thresholds and a feed mode. It does
  not model arrival rate versus service rate, or trend. Richer balancing would
  need actual-rate projections that are not built (which is also why
  `RateDeviationDetected` is declared but never raised).

### Reversibility

High. A scheduled sweep could be added later as an *inbound adapter* that calls
the same `RebalanceDecision` use case on a timer — the decision logic would not
change at all. That is precisely the shape this design leaves open.
