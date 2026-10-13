---
id: 0037-mcp-annotations-for-event-raising-tools
slug: /adr/0037-mcp-annotations-for-event-raising-tools
title: 0037. MCP tools that publish events are not annotated read-only
sidebar_label: 0037. MCP annotations tell the truth
description: ADR 0037 — get_backlog_telemetry and get_rebalance_recommendation publish domain events through the outbox, so they are annotated readOnlyHint false, destructiveHint true, idempotentHint false, and their descriptions name the event (fleet gap fix D17).
---

# 0037. MCP tools that publish events are not annotated read-only

## Status

Accepted (2026-10-12). Amends [ADR-0008](./0008-mcp-inbound-adapter.md), which
listed `get_backlog_telemetry` and `get_rebalance_recommendation` as "(read)"
tools. Applies the [MCP governance charter](../mcp/governance-charter.md) §4
unchanged. Fleet gap fix D17 (2026-10-12 decision log).

## Context

The MCP tool annotations are the only signal a model host has, before a
call, about whether the call changes anything. Hosts and the fleet's ops
agent use `readOnlyHint` to decide whether a call needs a human
confirmation.

Two of this server's tools were annotated `readOnlyHint: true`, but the use
cases behind them publish domain events:

| Tool | Use case | Event published | When |
|---|---|---|---|
| `get_backlog_telemetry` | `SampleBacklog` | `BacklogThresholdBreached` | the pool is over its alarm threshold |
| `get_rebalance_recommendation` | `RebalanceDecision` | `PathThrottled` | a flow-fed pool is over its alarm threshold (`ThrottleUpstream`) |
| `get_rebalance_recommendation` | `RebalanceDecision` | `LaborReassignmentFlagged` | a release-fed pool is at its WIP limit with work backlogged (`ReassignLabor`) |

With Postgres and `EVENT_PUBLISHER=kafka` each event becomes outbox rows that
the relay sends to `warehouse.work-planning.events` and
`warehouse.wes.analytics` ([ADR-0014](./0014-transactional-outbox.md)). Other
contexts and the analytics report count them, and a published event cannot be
recalled. Every qualifying call publishes again, so a model that polls the
telemetry tool in a loop publishes one alarm event per call. The REST
endpoints behind the same use cases (`GET /paths/{pathId}/telemetry`,
`GET /paths/{pathId}/rebalance`) behave the same way; this record is about the
MCP annotations only.

The governance charter (§4) has two classes: a read tool **MUST** be
read-only (no state change) and a write tool **MUST** be annotated
destructive. Its rule 4 requires the description to state side effects
plainly.

## Decision

We annotate both tools as what they are under the charter: **write tools**.

1. `get_backlog_telemetry` and `get_rebalance_recommendation` carry
   `readOnlyHint: false`, `destructiveHint: true` and `idempotentHint: false`,
   the same annotations as `release_next_work`.
2. Each description ends with a "Side effect:" sentence that names the event
   it publishes and when.
3. `testdata/tool_registry.golden` pins the new annotations, and
   `TestTools_EventRaisingToolsAreNotAnnotatedReadOnly` fails if either tool
   goes back to read-only, loses `destructiveHint`, claims idempotence, or
   stops naming its event.
4. The use cases do not change. The tools still return the same read
   models, and the REST routes are untouched.

`destructiveHint: true` is the charter's rule for a non-read-only tool. In
MCP terms it is also the specification's default for a non-read-only tool.
A tool that only appends events could arguably be `destructiveHint: false`
("additive"), but the charter has no such class, and adding one is a
fleet-wide charter change, not a decision for this repository.

## Consequences

- **Easier:** a host that confirms non-read-only calls now confirms these
  two, and a model reading the description knows a telemetry poll can raise
  an alarm event.
- **Harder:** an agent that used to call telemetry freely now gets a
  confirmation prompt when its host gates destructive tools. For a read with
  no side effect, the `telemetry://{pathId}/backlog` resource is NOT an
  alternative: it runs the same `SampleBacklog`. A truly side-effect-free
  telemetry read needs a use case that does not publish, which is out of
  scope here.
- **Fleet:** the charter stays as it is. If the fleet later adds an
  "additive write" class (`readOnlyHint: false`, `destructiveHint: false`),
  these two tools are the first candidates; that change would supersede this
  record.
