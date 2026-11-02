---
id: 0002-waveless-continuous-release
title: ADR-0002 — Waveless continuous release over wave-based batching
sidebar_label: 0002 · Waveless release
sidebar_position: 2
description: Why work is admitted one unit at a time in CPT order, with a WIP limit as backpressure, instead of being batched into waves.
---

# ADR-0002 — Release work continuously (waveless), not in waves

## Status

**Accepted.** This is the defining decision of the bounded context.

## Context

Work has to be admitted onto the floor somehow. The industry offers two shapes,
and the reference model treats "wave / waveless" as a first-class WES concern —
*how work is batched and released to the floor*.

**Wave-based** release composes work into discrete groups, opens a wave, and
does not open the next until the current one closes. It is the legacy default
and it has real properties: it batches work that shares travel, and it gives
operations a natural unit to talk about ("we're on wave 3").

It also has structural problems that this domain cannot tolerate:

1. **Priority freezes at composition.** Ordering is fixed when the wave is
   built. This context's entire priority function is CPT — a *physical truck
   departure*. If a truck is re-timed, a path goes down, or hot volume arrives,
   a composed wave cannot re-sort.
2. **The wave boundary is an unasked-for synchronisation barrier.** The
   reference model calls out the legacy behaviour directly: a wave-based system
   closes the whole wave before opening packing — *"slow and bursty"*. That
   barrier couples parcels that share nothing except having been batched
   together.
3. **Burst-then-starve loading.** A wave floods the floor at open and starves it
   at close. Average utilisation looks acceptable while instantaneous
   utilisation oscillates between congestion and idle.
4. **Wave size is a poor backpressure signal.** It is chosen ahead of time,
   from an estimate, and cannot respond to what the floor is actually doing.

Against that, the reference model describes the target behaviour as making an
initial plan and then **refining it at every step** as new information arrives —
"the fastest/cheapest path is frequently recomputed." A frozen batch cannot do
that.

## Decision

**We will release work continuously and waveless-ly: one unit at a time, in
earliest-CPT order, on demand, bounded by a per-pool WIP limit.**

1. **No schedule.** Release happens when the floor asks (`POST
   /paths/{pathId}/work-releases`). There is no timer, batch window, wave identifier
   or cron entry anywhere in the service.
2. **Priority is re-evaluated at every call.** `WorkPool.nextPendingIndex()`
   scans pending entries for the earliest CPT each time. A CPT that changes
   affects the very next handout.
3. **CPT is the only sort key.** No age factor, no weighting, no secondary sort.
   CPT is the deadline that physically exists; any other key would be a proxy
   for it.
4. **Backpressure replaces batch size.** On a **release-fed** pool,
   `ReleaseNext` refuses with `ErrWIPLimitReached` when `WIP ≥ wipLimit`. "How
   much work should be on the floor" becomes an enforceable invariant rather
   than an estimate.
5. **The release decision is a policy object**, `release.ReleasePolicy`, not a
   method buried in a handler or a `SORT BY` in a repository.

```go
func (ReleasePolicy) Apply(pool *WorkPool) (string, error) {
    return pool.ReleaseNext()
}
```

The policy is deliberately thin today. Naming it as a separate domain service
is the point: admission is the rule most likely to change, and changing it
should mean replacing one object.

## Consequences

### Easier

- **Reacting to change is immediate.** A re-timed truck, a new hot order or a
  down path affects the next release call, not the next wave.
- **Floor loading is smooth and bounded.** The WIP limit holds outstanding work
  at a known ceiling instead of oscillating.
- **Unrelated work is decoupled.** There is no batch, so nothing waits on
  anything it does not depend on.
- **The rule is trivially testable.** Release is pure in-memory domain logic
  over a pool snapshot; every branch is a short unit test with no clock and no
  I/O.
- **Backpressure composes with flow balancing.** Because the WIP limit is
  enforced, a release-fed pool sitting *at* its limit with backlog remaining is
  an unambiguous signal — see
  [ADR-0003](./0003-flow-balancing-as-domain-service.md).

### Harder

- **No natural travel batching.** A wave can deliberately group picks in the
  same aisle; pure CPT ordering cannot. That optimisation now has to happen
  downstream in `fulfillment-execution`, where travel and station capability are
  actually known. This is a real capability given up, not a wash.
- **Priority is single-dimensional.** Earliest-CPT-first will not, on its own,
  express customer tiering or cold-chain handling. Adding those means changing
  the release policy — cheap because it is a separate object, but still a
  change.
- **Operations loses the "wave" vocabulary.** There is no batch to name in a
  stand-up. Backlog depth, WIP and CPT burn-down replace it, which is a
  retraining cost.
- **More release calls.** One decision per unit instead of one per wave. Cheap
  here, but a real difference in call volume against the HTTP surface.

### Reversibility

Fair. Wave semantics could be reintroduced as an alternative `ReleasePolicy`
implementation without touching the pool aggregate, the use case or the API —
which is exactly why the policy is a separate object. What would *not* survive
is the WIP-limit invariant, which is meaningful only for continuous admission.
