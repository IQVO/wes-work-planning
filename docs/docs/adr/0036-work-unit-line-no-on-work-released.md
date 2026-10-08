---
id: 0036-work-unit-line-no-on-work-released
slug: /adr/0036-work-unit-line-no-on-work-released
title: 0036. WorkUnit stores line_no; WorkReleased carries it
sidebar_label: 0036. WorkUnit line_no on WorkReleased
description: ADR 0036 — the order line number the work-unit id already embeds becomes an explicit, optional WorkUnit.line_no, carried as an optional line_no on WorkReleased v1 on both the integration and the analytics topics (fleet decision 18, per-line confirm-pick, hops 3–4).
---

# 0036. WorkUnit stores line_no; WorkReleased carries it

## Status

Accepted (2026-10-07). Amends nothing: it is **additive** on top of
[ADR-0031](./0031-order-allocated-choreography.md) (the deterministic
`<order>-line-<n>` ids), [ADR-0010](./0010-gift-wrap-as-a-work-released-characteristic.md)
and [ADR-0033](./0033-transfer-work-demand-choreography.md) (the other optional
`WorkReleased` fields) and [ADR-0011](./0011-analytical-data-product.md) (the
analytics stream).

This is hops 3 and 4 of fleet decision 18 (per-line confirm-pick), recorded in
the architect's 2026-10-05 audit decision log. The other hops live in
order-management (sends `lineNo` when it reserves), inventory-storage
(`Reservation.line_no`, and the per-line confirm in its `TaskCompleted`
consumer — its ADR 0035 is the fallback this keeps) and fulfillment-execution
(`Task.source_line_no`, `TaskCompleted.line_no`).

## Context

`OrderAllocated` / `OrderPartiallyAllocated` carry a `line_no` per allocated
line, and `ApplyOrderAllocated` already receives it as
`OrderAllocatedLine.LineNo`. It uses it for one thing: building the work unit
id `"<order>-line-<n>"`. After that the number exists **only inside an id
string** — the `WorkUnit` has no `line_no`, and `WorkReleased` has no
`line_no` (it carries `ref`, which is the order id, not the line).

Downstream, that loses the information the confirm-pick flow needs: a
`TaskCompleted` for an order can name the order but not which line was picked,
so inventory-storage has to count picks and confirm everything on the last
one (its ADR 0035), which leaves a "one pick early" edge and a window in which
a reservation is confirmed before its line is picked. The fix is to carry the
line explicitly end to end. Parsing it back out of the id string is rejected:
the id format is this service's private business (REST callers supply
arbitrary ids, transfer units use demand ids), and an explicit, validated
field is something a consumer can rely on.

## Decision

We store the line number on the `WorkUnit` and publish it on `WorkReleased`,
as an **optional, additive** field everywhere. Nothing becomes required, no
event version changes, and no existing id changes.

1. **Aggregate.** `WorkUnit` gains an optional `lineNo` (`LineNo()` /
   `SetLineNo()`, the same additive-setter style as `SKU`, `GiftWrap` and the
   transfer fields). `0` means *unknown*. It plays no part in any invariant.
2. **Enqueue.** `EnqueueWorkUnitRequest` gains an optional `LineNo`. A
   negative value is rejected with `workunit.ErrInvalidLineNo`; `0` is
   *unknown*. `ApplyOrderAllocated` passes `line.LineNo` for each line; a
   non-positive value from upstream is treated as unknown (the id is still
   built exactly as before), so a malformed line is **never newly
   dead-lettered** by this change. `WorkUnitId` is untouched.
3. **Persistence.** Migration `0013` adds `work_units.line_no INTEGER`,
   **nullable, no default**. An unknown line is written as `NULL`, never `0`;
   `NULL` rehydrates as unknown. Every row created before this ADR is
   therefore `NULL` and every existing `INSERT` keeps working. The in-memory
   repo mirrors it.
4. **REST.** `POST /paths/{pathId}/work-units` accepts an optional `lineNo`
   (integer ≥ 1; `null` ≡ omitted). An explicit `0` or negative value is
   `400` with the new problem type `invalid-line-no` — it is rejected, not
   silently coerced to "unknown". The work-unit response echoes `lineNo`,
   omitted when unknown. The `Idempotency-Key` behaviour
   ([ADR-0022](./0022-idempotency-key-middleware.md)) is unchanged: the body
   hash simply covers the new field.
5. **`WorkReleased` v1 `data`, integration topic** (`warehouse.work-planning.events`):
   gains an optional integer `line_no`, **read off the `WorkUnit` at encode
   time exactly like `ref`/`cpt`** (inside the release transaction, so the
   outbox sees the just-saved row), and **omitted when unknown**. Same `type`,
   same `dataschema` (`…:events:WorkReleased:v1`).
6. **`WorkReleased` v1 `data`, analytics topic** (`warehouse.wes.analytics`):
   gains the same optional `line_no` under the same rule. The analytics
   publisher used to be repo-free; it now takes an optional work-unit repo
   (`AnalyticsPublisher.WithWorkUnits`), wired in `cmd/wes` and `cmd/mcp`
   with the same repository the integration publisher reads. Without a repo
   (or on a failed read) the field is simply omitted — fail-open, like the
   classification hints (ADR-0009): a publishing hiccup never blocks a release.
7. **Transfer units** (ADR-0033) never carry a line. A unit created before
   this ADR publishes no `line_no`. In both cases a consumer sees exactly the
   payload it saw before.
8. **Contract rule for consumers.** Absent `line_no` means *line unknown* and
   every consumer must keep working without it. This is what lets the four
   repos of decision 18 ship in any order.

## Consequences

- **Easier:** fulfillment-execution can stamp the line on its `Task` from
  `WorkReleased.line_no` and echo it on `TaskCompleted`, which lets
  inventory-storage confirm exactly one line's reservation instead of
  counting picks. No consumer parses an id.
- **Backward compatible by construction.** Optional field, same v1 identity,
  `omit` when unknown, nullable column. The pre-existing publisher goldens
  are byte-identical (a legacy unit has no line); new goldens pin the
  with-line shape on both topics. Verified against the fleet's
  `origin/develop`: fulfillment-execution's `WorkReleased` consumer decodes
  into a plain struct (no `DisallowUnknownFields` anywhere in that repo), and
  this repo's own analytics consumer and projector read only `path_id` /
  `work_unit_id`; no other repo consumes the payload.
- **Harder:** the analytics publisher is no longer repo-free, so its `Encode`
  now performs one indexed read per `WorkReleased` (the integration publisher
  already did). Under the outbox this happens inside the release transaction.
- **Not retroactive:** units already in the pool when this ships have no
  `line_no`, so their `WorkReleased` carries none and downstream keeps using
  the last-pick fallback for them until they drain. There is deliberately no
  backfill that parses `<order>-line-<n>` out of existing ids.
- **Hard to reverse:** once a consumer relies on `line_no`, removing it would
  reintroduce the premature-confirm window. The column itself is additive and
  can stay.
- **Open edge:** the nullable column is `INTEGER` without a `CHECK (line_no >= 1)`.
  The aggregate's callers validate (REST rejects `< 1`; the order consumer
  maps non-positive to unknown); a constraint was left out to keep the
  migration trivially safe on a populated table.
