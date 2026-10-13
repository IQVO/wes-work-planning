---
id: tools
title: MCP tools, resources and prompts
sidebar_label: MCP tools
description: Every tool, resource template and prompt the cmd/mcp server exposes, with input fields, read-only/destructive annotations and output shapes, captured from a live tools/list.
---

# MCP tools, resources and prompts

`cmd/mcp` is a separate binary serving the Model Context Protocol over
**Streamable HTTP** only (`github.com/modelcontextprotocol/go-sdk` v1.8.0),
as an inbound adapter over the same use cases as the REST API
([ADR-0008](../adr/0008-mcp-inbound-adapter.md)). It listens on `MCP_ADDR`
(default `:8090`) and mounts the MCP endpoint at **both `/` and `/mcp`**;
`GET /healthz` answers `{"status":"ok"}` for the probes. The surface is
**unauthenticated** ([ADR-0016](../adr/0016-remove-rest-mcp-static-bearer-auth.md));
keep it in-cluster. Server name `wes-work-planning-mcp`, version `1.0.0`.

The tool list below was captured from a live `tools/list` against a local
build (`MCP_ADDR=127.0.0.1:18190`, `REPORTS_BASE_URL` set); the registry is
also pinned by `internal/adapters/inbound/mcp/testdata/tool_registry.golden`.

## Tools

| Tool | Annotations | Input (all listed as required by the schema) | Output | Use case |
|---|---|---|---|---|
| `get_backlog_telemetry` | `readOnlyHint: false`, **`destructiveHint: true`**, `idempotentHint: false` (publishes `BacklogThresholdBreached`, [ADR-0037](../adr/0037-mcp-annotations-for-event-raising-tools.md)) | `pathId` (string, non-empty) | `{pathId, backlogDepth, wip, mode, overAlarmThreshold}` | `SampleBacklog` |
| `get_rebalance_recommendation` | `readOnlyHint: false`, **`destructiveHint: true`**, `idempotentHint: false` (publishes `PathThrottled` / `LaborReassignmentFlagged`, ADR-0037) | `pathId` | `{pathId, action, backlogDepth, wip}`; `action` is `NoActionNeeded`, `ThrottleUpstream` or `ReassignLabor` | `RebalanceDecision` |
| `release_next_work` | `readOnlyHint: false`, **`destructiveHint: true`**, `idempotentHint: false` | `pathId` | `{workUnitId, cpt, ref}` of the released unit | `ReleaseNextWork` |
| `get_release_throughput_report` | `readOnlyHint: true`, `idempotentHint: false` | `from`, `to` (RFC3339, `[from, to)`), `pathId`, `granularity` | `{rows: [{pathId, hourBucket, workReleased, workUnitCompleted, backlogThresholdBreached, pathThrottled, rateDeviationDetected}]}` | calls `GET /reports/throughput` on `cmd/wes-reports` |

Behaviour worth knowing:

- **Registered conditionally**: `get_release_throughput_report` exists only
  when `REPORTS_BASE_URL` is set; it never opens the analytical database
  ([ADR-0011](../adr/0011-analytical-data-product.md)). Its HTTP client has a
  10 s timeout; a non-2xx from the reports service becomes a tool error.
- **All four input fields of the report tool are required by the generated
  schema** (the SDK marks every struct field without `omitempty` as
  required), although the handler treats `pathId` and `granularity` as
  optional. A call without them is rejected with
  `validating root: required: missing properties: ["pathId" "granularity"]`;
  send `"pathId": ""` and `"granularity": ""` (or `"hour"`) to mean "all
  paths, hourly". `from` and `to` must be non-empty.
- **The two telemetry tools publish events, so they are not read-only**
  ([ADR-0037](../adr/0037-mcp-annotations-for-event-raising-tools.md)):
  `get_backlog_telemetry` (and the resource below) run `SampleBacklog`,
  which publishes `BacklogThresholdBreached` when the pool is over its alarm
  threshold — the same as `GET /paths/{pathId}/telemetry`.
  `get_rebalance_recommendation` publishes `PathThrottled` /
  `LaborReassignmentFlagged` on a `ThrottleUpstream` / `ReassignLabor`
  answer. Both are annotated `readOnlyHint: false`, `destructiveHint: true`
  (governance charter §4.2: a non-read-only tool is destructive) and
  `idempotentHint: false`, and their descriptions name the event, so a host
  that asks for confirmation before a non-read-only call asks here too.
- **`release_next_work`** applies the release policy: earliest CPT first,
  rejected when the path has no pool, the pool is empty or a `ReleaseFed`
  pool is at its WIP limit. The domain error text is returned as the tool
  error (`not found`, `no pending work in this pool`,
  `release-fed pool WIP limit reached`). `WorkReleased` goes through
  the same publisher contract as REST (outbox rows with `DATABASE_URL` +
  `EVENT_PUBLISHER=kafka`, drained by `cmd/wes`), and the
  `wes.work_units.released` counter is incremented.
- An empty `pathId` is rejected before any use case runs:
  `invalid pathId "": must be a non-empty process path id`.
- Every call runs inside an OTel span `mcp.tool <name>` with attribute
  `mcp.tool.name`.

## Resource template

| URI template | MIME type | Content |
|---|---|---|
| `telemetry://{pathId}/backlog` | `application/json` | the same JSON as `get_backlog_telemetry` for that path |

A URI that does not match `telemetry://<one segment>/backlog` is rejected.

## Prompt

| Prompt | Purpose |
|---|---|
| `balance_flow` | standard operating procedure: read telemetry, apply the rebalance recommendation, release only on a healthy or under-fed path, and escalate when a path stays over threshold or keeps hitting the WIP limit |

## Trying it

The server is plain JSON-RPC over HTTP: `initialize`, then reuse the
returned `Mcp-Session-Id` header.

```sh
go run ./cmd/mcp     # in-memory, :8090

curl -si localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
# -> 200, header Mcp-Session-Id: <id>, an SSE "data:" line with the result

curl -s localhost:8090/mcp -H 'Mcp-Session-Id: <id>' \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'   # 202

curl -s localhost:8090/mcp -H 'Mcp-Session-Id: <id>' \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
```

Without `DATABASE_URL`, `cmd/mcp` uses **its own** in-memory stores — they
are not shared with a `cmd/wes` you started separately, so
`get_backlog_telemetry` answers `not found` until work exists in *that*
process. Point both binaries at the same Postgres to see the same pools.

## Deployment notes

- Chart: `mcp.enabled=true` deploys `<release>-mcp` (Service port 8090,
  probes on `/healthz`). `mcp.eventPublisher` (empty = inherit
  `config.eventPublisher`) and `mcp.reportsBaseUrl` (empty = this release's
  reports Service when `analytics.enabled`) feed the container.
- **One replica only**: Streamable HTTP sessions live in the process's
  memory, so the chart deliberately has no HPA for MCP
  ([ADR-0025](../adr/0025-horizontal-autoscaling-and-pgxpool-tuning.md)).
- Configuration: [MCP variables](../operations/configuration.md#cmdmcp--mcp-server-streamable-http).
- Governance rules every fleet MCP server follows:
  [MCP governance charter](./governance-charter.md).
