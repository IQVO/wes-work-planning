---
id: 0048-site-and-forward-pick-face-on-released-work
slug: /adr/0048-site-and-forward-pick-face-on-released-work
title: "0048. Released work carries its site; pick plans carry a forward-pick face"
sidebar_label: "48. Site + forward pick face on released work"
sidebar_position: 48
description: "ADR 0048 — WorkReleased publishes top-level site_id for every work kind that has one, and pick plans gain from_bin derived from the local slot-plan copy."
---

# 0048. Released work carries its site; pick plans carry a forward pick face

Date: 2026-11-06
Status: Accepted

## Context

Two consumers need to know *where* released work happens, and neither can
get it today:

- `task-interleaving` ranks hint candidates by travel distance between
  locations, so a task without a site and a from-location can never be
  announced to the pool (`TaskAvailable` requires a placement).
- The pick-batching design (plan `pick-batching-2026-11-05`) forms a
  `WorkBatch` from hinted PICK candidates of one site and one CPT band;
  with neither field the pool stays unannounced and every batch is 1.

The Wave 1 spike of that plan (`.hermes/plans/pick-batching-2026-11-05/
spike-prerequisites.md`) proved the gap is upstream of this context: 100 %
of order-driven PICK work is released with no site and no `from_bin`, and
neither is sourceable from the events this context consumed at the time.
`WorkUnit.siteId` existed but was populated only for transfer, RELOCATE and
REPLENISH work, and `WorkReleased` published it only for transfer legs
(ADR-0033). `Order.fulfillmentSite` has existed in order-management since
ADR 0044 but was frozen out of the allocation events.

## Decision

1. **Order work gets a site.** The `OrderAllocated` and
   `OrderPartiallyAllocated` payloads gain a top-level `site_id`
   (omitempty — order-management ADR 0050), and the order-demand intake
   stores it on `WorkUnit.siteId` exactly as the transfer, RELOCATE and
   REPLENISH paths already do. Absent/empty site behaves exactly as
   before.
2. **`WorkReleased` publishes `site_id` for EVERY work kind whose unit
   carries one** — the ADR-0033 transfer-only gate is dropped. A unit with
   no site omits the field (never an empty string), so every pre-0048
   payload of a site-less unit stays byte-identical.
3. **Pick plans gain `from_bin` derived at release from the local
   `SlotPlanApproved` copy** (already consumed for RELOCATE, ADR-0040):
   the SKU's forward-pick face. This is a deliberate v1 approximation —
   real cluster picking walks forward-pick faces, so it is also the
   semantically right first source. A SKU with no forward-pick face omits
   `from_bin` and its unit completes exactly as before.
4. **The pick object repeats `site_id`** so a pick consumer reads one
   object.

Phase 2 (not built here): propagate the *reserved* bin chosen by
inventory-storage's allocation instead of the slot-plan face, and split a
line across bins when allocation reserves more than one. Recorded as the
known approximation of this ADR.

## Consequences

- `fulfillment-execution` can announce PICK tasks to the open-task pool
  (its own follow-up PR) once it consumes `pick.site_id` and
  `pick.from_bin` into its `PickPlan`.
- Golden fixtures for RELOCATE/REPLENISH releases change (they now carry
  `site_id`); every site-less payload is unchanged. Consumers that treat
  absent as "unknown" are unaffected.
- `OrderPartiallyAllocated` and `OrderAllocated` share one payload struct
  in this repo, so both carry the field from one change.
- The REST enqueue path is unchanged: a caller-given `fromBin` still wins
  over the derived face.
