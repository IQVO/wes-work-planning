---
id: flowfed-observed-throughput
title: Observed throughput for FlowFed paths
sidebar_label: FlowFed observed throughput
sidebar_position: 4
description: Where a caller finds an observed-completions signal for a FlowFed path — GET /reports/throughput — and what it is not (not an admission ceiling).
---

# Observed throughput for FlowFed paths

[ADR-0020](../adr/0020-flowfed-path-observed-throughput-signal.md) decided that
a `FlowFed` path **never** gets a hard admission ceiling:
`PathCapacityChanged` carries `Known=false` for FlowFed pools permanently
([ADR-0018](../adr/0018-path-capacity-changed.md)). The only signal this
service offers for such a path is **observed throughput**, and it is the
existing report endpoint:

```
GET /reports/throughput?from=<RFC3339>&to=<RFC3339>&pathId=<flow-fed-path>&granularity=hour
```

Served read-only by `cmd/wes-reports`
([report definition](../analytics/release-throughput-report.md)). The
`workUnitCompleted` field of each row is the number of `WorkUnitCompleted`
events for that `(pathId, hourBucket)` — an observed completions-per-hour
series.

## Caveats (read before using it)

- **Not an admission guarantee.** It is a historical count of what already
  completed. It says nothing about what the path will accept next. Never derive
  a ceiling from it, and never wire it into `PathCapacityChanged`.
- **Coarse.** One row per UTC hour per path; a caller's decision moment does
  not align to the bucket.
- **Eventually consistent.** Served from the analytics read model; target
  p95 event-to-report lag is under 30 seconds, observable at
  `GET /reports/throughput/freshness`.
- **Separate process.** It is served by the read-only reports reader, not the
  OLTP service. Do **not** call it synchronously on a promise or admission hot
  path; use it off-path as a soft input to your own risk buffer or lead-time
  padding.
- **Not a rate-deviation signal.** `RateDeviationDetected` is declared but
  not raised; this report does not replace it
  ([ADR-0003](../adr/0003-flow-balancing-as-domain-service.md)).

## When this will change

A sub-hour, synchronous signal (ADR-0020 "Option A", a
`PathThroughputObserved` event computed in the `WorkPool`) is deferred until a
consumer demonstrates it needs freshness the report cannot give.
