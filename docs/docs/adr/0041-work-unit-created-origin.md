---
id: 0041-work-unit-created-origin
slug: /adr/0041-work-unit-created-origin
title: 0041. WorkUnitCreated carries the originating reference and line number
sidebar_label: 0041. WorkUnitCreated origin
description: ADR 0041 — the integration-stream WorkUnitCreated payload gains the optional reference (the order id for order work) and line_no, so order-management can confirm that a released order line was admitted into a work pool by matching explicit attributes instead of parsing the work-unit id. Strictly additive, v1 stays v1, analytics stream unchanged. Fleet gap OM-0005, companion to order-management ADR 0043.
---

# 0041. WorkUnitCreated carries the originating reference and line number

## Status

Accepted (2026-10-17). Companion to order-management ADR 0043, which closes the
"no release-confirmation loop" gap recorded in order-management ADR 0005. It
extends the `WorkUnitCreated` payload under the CloudEvents rules of [ADR 0027](./0027-cloudevents-mandatory-event-envelope.md) (additive
fields, `v1` stays `v1`) and follows the same additive pattern as
[ADR 0036](./0036-work-unit-line-no-on-work-released.md).

## Context

order-management releases an order line and publishes `OrderAllocated`; this
context consumes it (`ApplyOrderAllocated`) and enqueues one work unit per line
under the id `{order_id}-line-{line_no}`, with `Reference` = the order id and
`LineNo` stored explicitly (ADR 0036). It then raises `WorkUnitCreated` on
`warehouse.work-planning.events`. That event is exactly the confirmation
order-management has been missing: the work was admitted into a pool.

Its `data` carries only `path_id` and `work_unit_id`. To use it, order-management
would have to rebuild the id from its own copy of the `{order_id}-line-{line_no}`
format and compare strings, or parse ours. The format is built independently on
both sides today. The fleet rule for this situation (the explicit-attribute rule of
the 2026-10 decision log) is to carry the correlation as an attribute of the
fact, never to derive it from a name.

## Decision

The integration-stream `WorkUnitCreated.data` gains two optional fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `reference` | string | The work unit's originating reference exactly as stored: the order id for order work (ADR 0005), the demand id for transfer work (ADR 0033), the plan id for RELOCATE work (ADR 0040). Omitted when empty. |
| `line_no` | integer, at least 1 | The order line number (ADR 0036). Omitted when unknown (0). |

- **Strictly additive.** Type, subject, key, `dataschema` (`...:WorkUnitCreated:v1`)
  and the two existing fields do not change. A unit with no origin publishes the
  exact payload it always did; the existing golden files are untouched.
- **Both raise sites.** `EnqueueWorkUnit` (REST, `ApplyOrderAllocated`,
  `ApplyWorkDemandReleased`) and `ApplySlotPlanApproved` attach the unit's
  `Reference()` and `LineNo()` with `WorkUnitCreated.WithOrigin`. The domain event
  is self-contained; the encoder does not read the work-unit repository (unlike
  `WorkReleased`, whose payload is enriched at encode time).
- **Integration stream only.** The analytics stream
  (`warehouse.wes.analytics`) is a separate data product (ADR 0011) and keeps its
  two-field payload; a test pins that.
- **A consumer treats an unknown `reference` as "not mine".** The field is shared
  by order, transfer and relocation work, so a consumer matches it against its own
  aggregates and ignores everything else.

No migration, no new event type, no new topic.

## Consequences

- order-management can confirm a release by `(reference, line_no)` (its ADR 0043),
  with its consumer off by default. Until it is enabled nothing changes for any
  existing consumer: unknown optional fields are ignored.
- `WorkUnitCreated` is no longer unconsumed once that consumer is enabled.
- The `{order_id}-line-{line_no}` work-unit id format stays frozen, but nothing
  new depends on it.

## Alternatives rejected

- **A new `WorkEnqueued` reply event.** It would restate a fact `WorkUnitCreated`
  already is, and add a second event to keep in step.
- **Match on the work-unit id.** Couples two repositories through a string format
  neither owns as a contract.
- **Enrich at encode time from the work-unit repository** (the `WorkReleased`
  pattern). An extra read per event for data the event can carry.
