---
id: 0023-resilience-circuit-breakers-retry-dlq-shutdown
slug: /adr/0023-resilience-circuit-breakers-retry-dlq-shutdown
title: 23. Per-dependency circuit breakers, read-only retry, Kafka DLQ, and graceful shutdown hardening
sidebar_label: 23. Circuit breakers, retry, DLQ, shutdown
description: "ADR 0023 — Phase 2 resilience for wes-work-planning, ported verbatim from order-management's ADR-0025 (PR #107): sony/gobreaker/v2 circuit breakers per outbound dependency (never one global breaker) that reuse each client's EXISTING fail-open permissive behaviour as the OPEN-state fallback rather than inventing a new one; cenkalti/backoff/v4 jittered retry on productclassification's and traveldistance's read-only GETs; a dead-letter topic per consumed Kafka topic so one poison message cannot block its partition; and a readiness-flip-first graceful shutdown sequence."
---

# 23. Per-dependency circuit breakers, read-only retry, Kafka DLQ, and graceful shutdown hardening

## Status

Accepted — implemented in the same change that introduced this record.
The product-classification breaker and retry described here were retired
with the HTTP client by
[ADR-0035](./0035-product-classification-local-copy.md); the
travel-distance parts stand.
This is Phase 2 (resilience) of the fleet production-readiness plan,
ported verbatim from order-management's reference implementation
(ADR-0025 there, PR #107) as the copy-verbatim pattern for this phase.

## Context

Before this change, wes-work-planning had two sync cross-context HTTP
clients — `productclassification.Client` (`GET
/products/{sku}/classification` against inventory-storage) and
`traveldistance.Client` (`GET /distance?from=&to=` against
facility-layout) — each with a `permissive`/`http` mode switch
(`PRODUCT_CLASSIFICATION_MODE`, `TRAVEL_DISTANCE_MODE`) but **no
circuit breaker, no bounded retry, and no context-deadline
propagation**: a slow or failing downstream just hung every caller
until Go's own zero-value (no) client timeout, and a failing
dependency was retried on every single request forever, with no
mechanism to stop hammering it. Its inbound `Consumer` (four topics:
`warehouse.workforce.events`, `warehouse.inventory.events`, the
fulfillment topic, `warehouse.order-management.events`) had **no
dead-letter handling at all** — every handler error was logged and the
offset committed anyway, silently dropping the event rather than
retrying it or preserving it for replay; there was no bounded
in-process retry for a genuinely transient failure either. Confirmed
fleet-wide: no DLQ existed anywhere in this repo before this phase.
Graceful shutdown already existed (`signal.Notify` +
`server.Shutdown` + consumer/relay context-cancel, `cmd/wes/main.go`)
but had no readiness-flip step and no `terminationGracePeriodSeconds`
in the Helm chart to give the drain room to run before Kubernetes
SIGKILLs the pod.

## Decision

### 1. One circuit breaker PER downstream dependency, never one global breaker

`internal/resilience` (a new, tiny, dependency-free top-level package,
ported byte-for-byte from order-management aside from its module
path) holds the tuning EVERY breaker in this service shares:

```go
const (
    DefaultMaxRequests = 1                // 1 probe per half-open cycle
    DefaultInterval    = 30 * time.Second // closed-state rolling-Counts reset window
    DefaultTimeout     = 30 * time.Second // open-state cooldown before a half-open probe
)

func ReadyToTrip(counts gobreaker.Counts) bool {
    if counts.ConsecutiveFailures >= 5 {
        return true
    }
    if counts.Requests < 10 {
        return false
    }
    return float64(counts.TotalFailures)/float64(counts.Requests) > 0.5
}
```

`productclassification.NewBreakerClient` and
`traveldistance.NewBreakerClient` each construct their OWN
`*gobreaker.CircuitBreaker[...]` instance using this shared tuning — a
slow or failing `inventory-storage` deployment tripping its breaker
can never affect `facility-layout`'s breaker (or vice versa), and each
keeps its own independent `*http.Client`/connection pool (bulkhead,
§4).

### 2. The breaker's OPEN-state fallback REUSES each client's existing fail-open semantics — it does not invent a new one

Both of this repo's cross-context reads are soft routing/enrichment
inputs, not mutations, so BOTH already had a fail-open contract before
this change:

- `productclassification.BreakerClient`, while OPEN, calls
  `PermissiveLookup.GetClassification` — the SAME fail-open behaviour
  (`Known=false, nil error`) this client already had for a transport
  error or a 500 (ADR-0009).
- `traveldistance.BreakerClient`, while OPEN, calls
  `PermissiveLookup.GetDistance` — the SAME fail-open behaviour
  (`Known=false, nil error`) this client already had for a 404, a 422
  (cross-zone refusal), a transport error, or a 500 (ADR-0017).

`isBreakerRejection(err)` (duplicated, unexported, in each package —
adapters never depend on each other per the hexagonal fitness test)
distinguishes gobreaker refusing to even ATTEMPT the call
(`gobreaker.ErrOpenState`/`ErrTooManyRequests`) from a real error a
call gobreaker DID let through; only the former routes to the
fallback — a real error from an attempted call propagates unchanged,
exactly as before this breaker existed.

### 3. Context deadline propagation: `resilience.CallTimeout`

```go
func CallTimeout(ctx context.Context, maxPerCall time.Duration) (context.Context, context.CancelFunc)
```

Every outbound call derives its timeout from the inbound request's OWN
remaining `ctx.Deadline()`, capped at `maxPerCall` (`DefaultTimeout`,
30s) when that remaining budget is larger or absent. Both
`BreakerClient.GetClassification` and `BreakerClient.GetDistance` call
this before `breaker.Execute`.

### 4. Bulkhead: confirmed, not newly built

Both `productclassification.NewClient` and `traveldistance.NewClient`
already each construct their own `*http.Client` (defaulting when a
nil `HTTPDoer` is passed) — there was never a shared client across the
two dependencies to begin with. This ADR only confirms that
invariant; no code change was needed for it specifically.

### 5. Retry (`cenkalti/backoff/v4`, jittered, max 3 attempts) on BOTH reads — both are genuinely read-only

Unlike order-management (where only `productclassification`'s read got
retry, and `inventorystorage`'s mutating `Reserve`/`RevokeReservation`
deliberately did not), BOTH of this repo's outbound clients are pure
GET/read calls with no mutating counterpart — `GetClassification` and
`GetDistance` each retry their newly-split-out raw `fetch` call up to
3 total attempts (`backoff.WithMaxRetries(policy, 2)`), jittered
exponential backoff (50ms–500ms), bounded by the SAME `callCtx`
`CallTimeout` derived. A 404/422 (a legitimate `Known=false` answer,
not a failure) returns on the FIRST attempt, exactly like a 200 does —
it never consumes retry budget. Each retry loop runs INSIDE one
`breaker.Execute` call, so a retry storm against an already-degraded
dependency still only ever counts as ONE success/failure toward that
breaker's trip condition, not three.

#### 5a. Both clients split into a fail-open, port-facing method and a raw, retry-facing `fetch`

Before this change, `GetClassification`/`GetDistance` each swallowed
every error into `Known=false, nil` at the SAME call site that made
the HTTP request — there was no way for a wrapping retrier to tell
"this SKU/location pair has no answer" (404/422, a real answer) apart
from "the request itself failed" (a transport error, an unexpected
status — a candidate for retry). `fetch` is the new, un-swallowed raw
call in each package's `client.go`; the public method is now a
one-line fail-open wrapper around it, preserving its exact
pre-existing external behaviour and its full existing test suite
unchanged (ADR-0009, ADR-0017). `BreakerClient.retryingFetch` calls
`fetch` directly in both packages.

### 6. Dead-letter queue for the inbound Kafka consumer

`Consumer.handleMessage` (via `dispatch`) retries the
per-topic handler (`handleWorkforceEvent`, `handleInventoryEvent`,
`handleFulfillmentEvent`, `handleOrderManagementEvent`) in-process, with jittered backoff
(`cenkalti/backoff/v4`, 100ms–2s), up to `maxHandlerAttempts` (3) total
attempts, via `handleWithRetry`. Once all 3 attempts are exhausted,
the raw message is published — byte-for-byte, plus
`x-dlq-source-topic`/`x-dlq-error`/`x-dlq-failed-at` headers carrying
replay/debugging context — to `<source-topic>.dlq` via a
`*kafkago.Writer` the consumer now owns per topic
(`Consumer.dlqWriters`, keyed by source topic, closed alongside every
reader in `Close`), and **the offset is committed anyway**: one poison
message must never permanently block every other event behind it on
the same partition. This is logged at ERROR level (`Consumer.logError` →
`slog.ErrorContext`; it was WARN until the 2026-10 audit fix) with enough context
(topic, dlq_topic, event_id, event_type, attempts, error) to be an
alert-worthy signal and support a manual replay tool, not a silent
drop — replacing this repo's previous behaviour of silently dropping
any handler error after a single attempt.

Each of the four consumed topics
(`cloudevents.TopicWorkforceEvents`, `cloudevents.TopicInventoryEvents`,
`cloudevents.TopicFulfillmentEvents`, `cloudevents.TopicOrderManagementEvents`
in `internal/adapters/kafka/cloudevents`) gets its OWN
`<topic>.dlq` writer, derived from its own source topic (never a fixed
constant) — mirroring `RepromiseConsumer`'s `NewRepromiseConsumerForTopic`
pattern so an isolated integration-test topic automatically gets its
own isolated DLQ topic for free.

Proven end to end with a real testcontainers Kafka
(`consumer_dlq_integration_test.go`,
`TestConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition`):
a message whose handler is made to always fail lands on the `.dlq`
topic after exactly 3 attempts, with the raw original JSON payload and
the error-context headers intact, and — published right after the
poison message on the SAME topic — a well-formed message is processed
without delay, proving the partition was never blocked.

### 7. Circuit breaker state as a Prometheus gauge

`telemetry.CircuitBreakerMetrics` (new,
`internal/adapters/outbound/telemetry/circuit_breaker_metrics.go`)
registers ONE OTel `Int64Gauge`, `circuit_breaker.state` — re-exported
by this fleet's OTel Collector prometheus exporter as
`circuit_breaker_state{dependency="product-classification"|"facility-layout"}`
(0=closed, 1=half-open, 2=open, gobreaker's own numbering verbatim, no
translation table) — on the SAME global `otel.Meter` this service's
other metrics use, not a second, parallel registry.
`resilience.RecordStateChange(dependency, recorder)` adapts a
`resilience.StateRecorder` (the interface `CircuitBreakerMetrics`
implements) into `gobreaker.Settings.OnStateChange`'s signature; a
`nil` recorder is a documented no-op, so a test that does not care
about the metric never needs to construct one.

### 8. Graceful shutdown hardening

`cmd/wes/main.go`'s pre-existing signal-handling + `server.Shutdown`
sequence is extended, not rewritten, into this order:

1. **Flip readiness to not-ready FIRST**
   (`inboundhttp.Readiness.SetNotReady`, backing a new `GET /readyz`,
   distinct from the pre-existing `GET /healthz` which stays a pure
   liveness signal and is never flipped by shutdown) — before anything
   else stops, so a Kubernetes `readinessProbe` polling `/readyz` has
   a window to observe the flip and stop routing NEW traffic to this
   pod before step 2 ever closes the listener.
2. **Stop accepting new HTTP connections and drain in-flight
   requests** — `server.Shutdown(shutdownCtx)`, unchanged from before.
3. **Stop the outbox relay and the Kafka consumer loop cleanly** —
   cancel each one's own context (no NEW work is picked up after this)
   and wait, bounded by the same shutdown deadline, for each goroutine
   to actually finish in-flight work — for the Kafka consumer, this
   means a message already being handled runs to completion INCLUDING
   its offset commit before `Run` returns. In `cmd/wes/main.go` this is
   a `consumerDone` channel closed when `consumer.Run` returns;
   `gracefulShutdown` cancels the consumer context, waits on
   `consumerDone` (bounded by the 5s shutdown deadline, logging a WARN on
   expiry), and only then closes the consumer, the catalogue consumer and
   the HTTP server (the outbox relay uses the same pattern with `relayDone`).
4. **Close the pgx pool LAST** — deferred near the TOP of `run()`, so
   by `defer`'s LIFO order it runs AFTER every consumer/relay goroutine
   has already stopped touching it.

`Readiness`'s zero value (and a `nil *Readiness`) is always ready —
every existing test and any caller that predates this type behaves
exactly as before.

`charts/wes-work-planning/values.yaml`'s `readinessProbe` now points
at `/readyz` (was `/healthz`) — `startupProbe`/`livenessProbe` are
UNCHANGED, still `/healthz`, because liveness must never be flipped by
a graceful drain or Kubernetes would SIGKILL the pod mid-drain instead
of letting it finish. A new `terminationGracePeriodSeconds: 30` was
added (previously unset, a confirmed gap).

## Consequences

- Every outbound call from `productclassification`/`traveldistance`
  now derives its timeout from the caller's remaining budget rather
  than an unbounded/default one — a caller with a short deadline gets
  a short-lived outbound call, not one that outlives its own patience.
- A failing `inventory-storage` or `facility-layout` deployment now
  trips its OWN breaker after 5 consecutive failures (or a sustained
  >50% error rate with enough volume) and stops sending real traffic
  to it for `DefaultTimeout` (30s) before probing again.
- The Kafka consumer can no longer silently drop an event on a single
  transient handler error, and can no longer be permanently wedged by
  one poison message; every other message on the partition keeps
  flowing. Each `.dlq` topic is a new operational surface: it needs
  monitoring/alerting (out of scope for this change — the ERROR-level
  log line is the interim signal) and a manual replay tool (also out
  of scope).
- `GET /readyz` is a new, distinct endpoint fleet operators/SRE
  tooling should point `readinessProbe`s at going forward.
- `productclassification.Client.GetClassification`'s and
  `traveldistance.Client.GetDistance`'s OBSERVABLE behaviour (their
  port contracts: always `Known=false, nil` on any problem) is
  unchanged; only their internal implementation split (`fetch` vs. the
  wrapper) changed, to make retry possible without breaking that
  contract.
- `sony/gobreaker/v2` and `cenkalti/backoff/v4` (promoted from
  indirect to direct) are new direct dependencies.

## Alternatives considered

- **One global circuit breaker for all outbound calls:** rejected for
  the same isolation reasoning order-management's ADR-0025 gives — a
  `facility-layout` outage tripping the SAME breaker that guards
  `inventory-storage` calls would incorrectly degrade one input for a
  problem confined to the other.
- **A brand-new fallback behaviour when a breaker opens (e.g. a cached
  last-known-good answer):** rejected — the breaker only decides WHEN
  to fall back, not WHAT the fallback is; inventing new fallback
  semantics here would diverge from the pre-existing, already-
  understood permissive-mode contracts (ADR-0009, ADR-0017) this
  service's operators already reason about.
- **Retrying the mutating use cases this consumer eventually calls
  (`RecordCompletion`, `EnqueueWorkUnit`) via the SAME blind
  handler-level retry as a genuinely-idempotent read:** not applicable
  here — `handleWithRetry` retries the WHOLE per-topic handler
  function, which is itself built on this repo's existing
  `ProcessedEventRepo` dedup/idempotency machinery, so a retried
  handler invocation is already safe to redeliver; this mirrors how
  order-management's `RepromiseConsumer` retry wraps
  `RepromiseOrder.Execute` rather than bypassing its own concurrency
  guards.
- **Dropping a DLQ message instead of publishing it:** rejected — an
  alert-worthy signal with full replay context (raw payload + error)
  is strictly more operationally useful than a silent drop, matching
  order-management's ADR-0025 decision on the same question.

## References

- order-management PR #107, ADR-0025 — the reference design this
  record ports verbatim.
- ADR-0009 — `productclassification`'s original fail-open contract,
  reused unchanged as the breaker's OPEN-state fallback here.
- ADR-0017 — `traveldistance`'s original fail-open contract, reused
  unchanged as the breaker's OPEN-state fallback here.
