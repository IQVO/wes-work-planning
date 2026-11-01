---
id: 0040-slot-plan-relocation-work
slug: /adr/0040-slot-plan-relocation-work
title: 0040. Plan approved slot plans as sequenced RELOCATE work
sidebar_label: 0040. Slot-plan relocation work
description: ADR 0040 — wes-work-planning consumes slotting-optimization's SlotPlanApproved and plans one RELOCATE work unit per move on the RELOCATE path; a move into a slot another move of the same plan vacates waits for that move's TaskCompleted; a newer plan for the site cancels the older plans' not-yet-released units; WorkReleased v1 gains an optional relocation block that fulfillment-execution's RELOCATE task decodes.
---

# 0040. Plan approved slot plans as sequenced RELOCATE work

## Status

Accepted.

Implements this context's part of fleet decision E1 (slotting-optimization
ADR 0010, "Approved slot plans are executed as WES relocation work", which
supersedes slotting-optimization ADR 0007) and follows decision E5 (the
process-path catalogue declares which demand sources may route work onto a
path; process-path-management ADR 0034).

## Context

slotting-optimization publishes `SlotPlanApproved`
(`com.warehouse.wms.slotting-optimization.slotplan.SlotPlanApproved`, dataschema
`urn:warehouse:slotting-optimization:events:SlotPlanApproved:v1`) on
`warehouse.slotting-optimization.events`, key and subject = the plan id. Its
`data` carries `plan_id`, `site_id`, `approved_at`, an optional
`supersedes_plan_id`, the full `assignments` map and the `moves` that change
the site's forward-slot picture. Each move is `{sku, from_slot?, to_slot?,
kind}` where `kind` is `Assign` (no `from_slot`), `Relocate` (both) or
`Vacate` (no `to_slot`).

A move is physical floor work: an operator carries the SKU's stock from one
bin to another. The fleet already has one conductor for floor work. This
context plans and pools it, process-path-management routes it,
fulfillment-execution runs it with operators (its ADR 0047, task type
`RELOCATE`), and inventory-storage records the result on the ledger from
fulfillment-execution's `TaskCompleted`. Decision E1 makes approved slot
plans a second demand source next to orders, beside the network transfer
legs of [ADR-0033](./0033-transfer-work-demand-choreography.md).

ADR 0007's open questions are answered by decision E1; the parts this context
owns are:

- **Sequencing (answer 4).** Inside one plan, a move INTO a slot that another
  move of the same plan vacates must wait until that vacating move's task has
  completed. Otherwise the operator finds the destination slot still full.
- **Supersession (answer 3).** When a newer plan for the site is approved, the
  superseded plan's work that has not been released yet is cancelled and the
  new plan's moves are planned. Released or in-progress work runs to
  completion.
- **Quantity (answer 5).** v1 moves the whole on-hand quantity of the SKU at
  the source bin, counted and confirmed by the operator at execution, so the
  plan carries no quantity and neither does the work unit. The open side of
  an `Assign` (source) or a `Vacate` (destination) is chosen and scanned by
  the operator at execution.

## Decision

1. **A sixth consumed topic.** The inbound `Consumer` also reads
   `warehouse.slotting-optimization.events`
   (`cloudevents.TopicSlottingEvents`) under the same consumer group, with its
   own `<topic>.dlq` writer (`RequiredAcks: RequireAll`, like every DLQ
   writer). Only `SlotPlanApproved` is handled; `SlotPlanGenerated`,
   `SlotPlanRejected` and any unknown type are ignored.

2. **`ApplySlotPlanApproved`, beside `ApplyWorkDemandReleased`.** One use case
   applies one plan, with the processed-event mark (the CloudEvents `id`) and
   every write in ONE atomic scope
   ([ADR-0028](./0028-processed-event-mark-atomic-with-handling.md)). Nothing
   leaves except through the transactional outbox
   ([ADR-0014](./0014-transactional-outbox.md)).

3. **One RELOCATE work unit per move.**
   - id `<plan_id>-move-<move_seq>`, where `move_seq` is the move's 0-based
     index in `moves` (deterministic, so a replay under a new event id is a
     no-op);
   - `reference = plan_id`, so `GET /work-units?reference=<plan_id>` lists
     every unit of a plan;
   - path `RELOCATE`, validated against the process-path catalogue
     ([ADR-0012](./0012-process-path-catalogue-validation.md)). When the
     catalogue entry declares an `intake` (process-path-management ADR 0034)
     it must contain `SLOT_RELOCATION`; an entry without an intake is
     accepted, because this route is chosen by explicit path id;
   - `work_kind = RELOCATE`, `site_id = site_id`, `sku = sku`, plus the
     relocation value: `plan_id`, `move_seq`, `kind`, `from_slot`, `to_slot`;
   - `cpt = approved_at + RELOCATION_CPT_HORIZON` (default `24h`): a slot
     plan has no customer cutoff, but every pooled unit needs a CPT, and
     fulfillment-execution's CPT-miss sweep must not flag a fresh move as
     already late.

4. **Sequencing: a blocked unit stays out of the pool.** Move B waits on move
   A when `A.from_slot == B.to_slot` (A is not B, same plan). B is created
   `Pending` with `blocked_by = A's work unit id` and is NOT admitted to the
   RELOCATE work pool, so no release (REST, MCP) can hand it out. When A's
   completion is recorded (`TaskCompleted` from fulfillment-execution, already
   consumed by `ApplyTaskCompleted`, or `POST .../completions`), `RecordCompletion`
   admits every `Pending` unit blocked by A into the pool in the SAME
   transaction and clears its `blocked_by`. Two rules keep this total:
   - when several moves vacate the same slot (a valid plan has at most one),
     B waits on the one with the lowest `move_seq`;
   - a dependency cycle (a swap: X→Y and Y→X) is broken at its lowest
     `move_seq`: that move is planned unblocked. The floor holds two SKUs in
     one bin for the duration of the swap, which the ledger allows.

5. **Supersession is decided per site by approval time.** A `slot_plans` row
   records every plan this context has applied (`plan_id`, `site_id`,
   `approved_at`, `supersedes_plan_id`, `move_count`). For an incoming plan P
   at site S:
   - P already recorded: no-op (the same plan under another event id);
   - the latest recorded plan of S was approved AFTER P (ties: the greater
     `plan_id` wins): P is stale. Plan ids are Kafka keys, so two plans of
     one site may arrive out of order. P is recorded and plans nothing;
   - otherwise P supersedes: every `Pending` RELOCATE unit of S that belongs
     to another plan (blocked or pooled) is cancelled. The unit moves to the
     new terminal state `Cancelled` and its pool entry is withdrawn
     (`WorkPool.Withdraw`). Then P's units are planned. `Released` and
     `Completed` units are never touched. `supersedes_plan_id` is recorded
     for audit only; the rule does not depend on it, so a missed
     intermediate plan cannot leave work behind.

6. **Release goes through the existing flow.** RELOCATE units are released by
   `ReleaseNextWork` on path `RELOCATE`, exactly like every other pool. A
   pooled entry whose unit was cancelled is withdrawn on the way past, never
   released. `WorkReleased` stays v1. For a RELOCATE unit its `data` gains
   ONE optional object, present only on RELOCATE units:

   ```json
   "relocation": {
     "plan_id": "plan-3b0e8d2a",
     "move_seq": 1,
     "kind": "Relocate",
     "sku": "SKU-2",
     "from_bin": "WH1-FWD-A02-03-01-A",
     "to_bin": "WH1-FWD-A01-01-02-A"
   }
   ```

   `from_bin` is omitted for an `Assign` and `to_bin` for a `Vacate` (the
   operator scans the open side). `kind` keeps the slotting spelling. This is
   the shape fulfillment-execution's `WorkReleased` consumer turns into its
   `RelocationMove` (ADR 0047 there); its completion reports
   `relocation_ref = "<plan_id>:<move_seq>"`. The transfer fields of
   ADR-0033 (`work_kind`, `transfer_ref`, `site_id`, `quantity`) remain the
   transfer discriminator and are NOT emitted for RELOCATE units, so
   fulfillment-execution's transfer branch never sees a relocation.

7. **Fail loud on a broken contract.** A `SlotPlanApproved` with no `plan_id`,
   `site_id` or `approved_at`, a move with no `sku`, an unknown `kind`, a
   `kind` missing the slot it requires, a `Relocate` whose two slots are
   equal, an unknown `RELOCATE` path or an intake that excludes
   `SLOT_RELOCATION` is deterministic poison: the handler returns an error,
   the message is retried and then dead-lettered to
   `warehouse.slotting-optimization.events.dlq`. A message interrupted only by
   shutdown cancellation is never dead-lettered (the consumer's existing
   `ctx.Err()` guard).

8. **No cancellation event in v1.** A cancelled unit was never released, so
   no downstream service holds work for it. Its cancellation is visible on
   the work-unit read API (`state: Cancelled`, `cancelledAt`). A
   `WorkUnitCancelled` event can be added later if a consumer needs one.

## Consequences

- RELOCATE work is staffed, capacity-planned and measured like every other
  path: labour plans, WIP limits, release telemetry and the analytics
  counters apply unchanged.
- The work-unit read side (`GET /work-units/{id}`,
  `GET /work-units?reference=`) shows `Cancelled`, `cancelledAt`, the
  `relocation` value and `blockedBy`. The `WorkUnitState` enum gains
  `Cancelled`. `Cancelled` is terminal: `Release` and `Complete` refuse it.
- Migration `0014` is additive: relocation columns on `work_units` (all
  `NOT NULL DEFAULT` except the nullable `cancelled_at`), two partial
  indexes, and the `slot_plans` table.
- New env `RELOCATION_CPT_HORIZON` (Go duration, default `24h`).
- warehouse-infra: no change is required for this context. The consumer joins
  the existing `KAFKA_CONSUMER_GROUP`, and the topic is
  slotting-optimization's own publish topic, so its ensure-topics entry
  already exists wherever slotting-optimization runs. The `RELOCATE` path
  must be in the process-path catalogue (process-path-management's seed)
  before plans arrive, or they are dead-lettered until it is (replay from
  the DLQ; see the runbook).

## Alternatives considered

- **A blocked flag inside the pool entry** (a `held` entry state) —
  rejected: every pool reader (backlog depth, WIP, remaining capacity) would
  need to learn a state that means "not really backlog". Keeping blocked
  units out of the pool keeps the pool's meaning "releasable work".
- **Sequencing in fulfillment-execution** — rejected: release order is this
  context's job; fulfillment-execution would have to hold tasks it was told
  to run.
- **Supersession driven only by `supersedes_plan_id`** — rejected: one lost
  or out-of-order event would leave a superseded plan's work in the pool
  forever. Approval time per site is total and order-independent.
- **A separate relocation aggregate** — rejected for the ADR-0033 reason: a
  move is the same releasable unit of work with extra descriptive context.
