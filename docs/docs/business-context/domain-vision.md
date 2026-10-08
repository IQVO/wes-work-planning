---
id: domain-vision
title: Domain vision
sidebar_label: Domain vision
sidebar_position: 1
description: The vision statement for the Work Planning & Release bounded context and the boundaries it deliberately refuses to cross.
---

# Domain vision

> **Turn a shift's charge into a committed plan, then admit work onto the
> floor continuously in deadline order, correcting the flow from live buffer
> telemetry — so that every parcel makes its truck without the floor ever
> being starved or flooded.**

That is the whole job. The platform-level vision it serves, from the reference
model, is:

> Fulfil customer orders from many disparate SKUs at massive scale by
> receiving goods, storing them under chaotic storage, and reliably picking,
> packing, and shipping them along the fastest/cheapest path — continuously
> re-optimising physical work in real time.

The phrase that matters for *this* context is **"continuously re-optimising
physical work in real time."** In the reference model that is the heartbeat of
the core domain, and it is exactly the slice this service owns.

## The conductor metaphor

The industry framing that this platform follows describes a WES as the
*conductor* that decides which activities happen when, while the WCS is the
*specialist* that ensures the equipment performs them, and the WMS is the
system of record that says what must happen and why.

Concretely for this service:

- **WMS says**: "900 units for the 18:00 truck, 500 more for the 21:00 truck,
  all on the pick path." → arrives here as a **charge forecast**.
- **This service decides**: 6 heads on `pick-a` at 95 units/hour for 8 hours;
  release `wu-2` next because its CPT is 18:00 and `wu-1`'s is 21:00; the
  pool is at its WIP limit with backlog remaining, so flag a headcount move.
- **Downstream execution does**: turn the released unit into a claimable task
  and put it in front of an associate or a machine.

## Three boundaries this context refuses to cross

The value of the boundary is what it *keeps out*. Each of these is a decision,
not an omission.

### It does not own inventory truth

Whether a SKU is physically available is `inventory-storage`'s job. This
context keeps a **read-only, SKU-keyed projection** of what Inventory last
reported (`UsableInventoryObserved`) and nothing more. If it tried to own
usable quantity, two systems would disagree about stock during every network
partition, and the disagreement would be invisible until a picker found an
empty bin.

Nor does it own product classification: `product-master` does. This context
keeps a version-guarded local copy of `product-master`'s `ProductClassified`
events, used only to stamp fail-open `hazmat`/`fragile` hints on released
work ([ADR-0035](../adr/0035-product-classification-local-copy.md)).

### It does not assign individual people to individual tasks

Headcount *planning* per process path lives in `workforce-management`;
individual *task dispatch* lives in `fulfillment-execution`. This service
stops at the **path boundary**: it says "this path needs more heads", never
"Maria goes to station 3."

The reference model is explicit about why: if the planning tier acquires
knowledge of individual workers, real-time location and travel distance, then
a supporting concern (labour orchestration) has leaked into a system that must
stay stable and auditable, and every shift-pattern change forces a regression
in the wrong place.

### It does not talk to equipment

No PLC, no conveyor, no robot. Releasing work is an *admission decision*, and
admission decisions are made in units of work, not in units of actuator
commands.

## Why "charge", not "demand"

The word is chosen deliberately. **Charge** is the volume that must *clear*,
bucketed by CPT. "Demand" or "orders due today" flattens the staircase into a
single number, and the moment you flatten it you can no longer tell whether
you are on track: a path that has cleared 60 % of the day's volume may be
perfectly on plan or may have already missed the 18:00 truck, and only the
CPT bucketing distinguishes those two worlds.

## Why a process path is a queue, not a step

A **process path** in this model is *a named station that owns a queue* —
unit-in → unit-out, a service rate, a staffed capacity — not a stage in a
workflow. This matters because the interesting questions are queueing
questions: how deep is the backlog, is arrival rate above service rate, is
this buffer starving or flooding? A workflow-step model cannot express any of
those; a queue model expresses all of them, which is what makes
[flow balancing](./flow-balancing.md) possible at all.
