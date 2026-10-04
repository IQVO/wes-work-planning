---
id: 0014-transactional-outbox
slug: /adr/0014-transactional-outbox
title: 0014. Transactional outbox feeding both the integration and the analytics topic
sidebar_label: 0014. Transactional outbox
description: ADR 0014 — why this service stopped publishing to Kafka from inside its use cases and now commits every event's wire form, for BOTH topics, into one outbox table in the same transaction as the aggregate, with an in-process relay draining it onto Kafka.
---

# 0014. Transactional outbox feeding both the integration and the analytics topic

## Status

Accepted — implemented in the same change that introduced this record.

## Context

Every publishing use case in this service had the same shape:

```go
if err := uc.workUnits.Save(ctx, unit); err != nil { return nil, err }
if err := uc.pools.Save(ctx, pool); err != nil { return nil, err }
if err := uc.publisher.Publish(ctx, event); err != nil { return nil, err }
```

Two or three independent Postgres writes, then a Kafka write, no
compensation. A crash, a broker timeout, or a pod eviction between them
leaves a WorkUnit that was enqueued (or released, or completed) in the
store but never announced on `warehouse.work-planning.events` — so
fulfillment-execution never creates the task — and never announced on
`warehouse.wes.analytics` — so the Release Throughput report silently
undercounts. The reverse failure (Publish succeeds, then the HTTP response
is lost) already existed too: with `EVENT_PUBLISHER=kafka` a broker
outage turned every `POST /paths/{id}/release` into a 500 *after* the
row had been committed, which is the worst of both worlds.

process-path-management fixed the same problem first
(its ADR 0003) and that design is the template here. Three things make
this service different enough that a literal copy would not work:

1. `ports.EventPublisher.Publish` is **variadic** here.
2. There are **two topics**. The integration `Publisher` writes the
   fleet contract to `warehouse.work-planning.events`; the
   `AnalyticsPublisher` writes the analytics envelope to
   `warehouse.wes.analytics` (ADR 0011). Both were fanned out by
   `events.MultiPublisher` and both must be fed from the outbox.
3. The integration publisher **reads a repository at publish time**:
   `Publisher.dataFor` looks up the released WorkUnit to stamp `cpt`,
   `ref`, `sku`-derived hazmat/fragile hints (ADR 0009) and `gift_wrap`
   (ADR 0010) onto the `WorkReleased` payload. Under an outbox the
   encoding therefore cannot happen in a relay that runs later — it must
   happen **inside the use case's transaction**, where that read sees the
   row the use case just wrote.

## Decision

Adopt the **transactional outbox**, in its **fan-out** form:

1. **`outbox_events` table** (migration `0005_outbox`). One row is one
   already-encoded Kafka message for one topic: `topic`, `event_type`,
   `key`, `value` (the envelope bytes, exactly what the topic will carry),
   `headers` (JSON `[{"key","value"}]` — the W3C `traceparent` injected at
   encode time), plus `published_at`, `attempts`, `last_error` for the
   relay. A domain event produces **two rows** (one per topic), inserted
   back to back. A partial index on `published_at IS NULL` keeps the
   relay's scan tiny.

2. **`kafka.Encoder` — Encode split from Send.** Both `Publisher` and
   `AnalyticsPublisher` gained
   `Encode(ctx, events...) ([]Encoded, error)`: the envelope build, the
   repo-reading `dataFor`, the trace-header injection, and the
   analytics publisher's "skip events outside the contract" rule all
   moved there. `Publish` is now `Encode` + `WriteMessages` with the
   existing producer span, so the direct path and the outbox path can
   never disagree on wire format. `Encoded.Topic` is carried separately
   from `kafkago.Message.Topic` because kafka-go rejects a message that
   sets Topic when its Writer also pins one; the two fixed-topic writers
   leave it empty and only the relay's topic-less `RelaySink` applies it.

3. **`ports.UnitOfWork`** — `Execute(ctx, fn func(ctx) error) error`. Use
   cases wrap **all** their `Save`s and their `Publish` in one
   `atomically(ctx, uow, fn)` scope; reads that only decide whether to
   act stay outside. The port is optional (`nil` = run back to back),
   which is exactly the in-memory / log-publisher / direct-Kafka
   configuration. Constructors are unchanged; each publishing use case
   gained a chainable `WithUnitOfWork(u)` setter. `SampleBacklog` and
   `RebalanceDecision` save nothing but still wrap their `Publish`, so
   every use case is uniform. The domain layer is untouched and the
   arch-go fitness tests pass unchanged.

4. **`postgres.UnitOfWork`** opens a `pgx.Tx`, binds it to the context,
   and commits or rolls back around `fn`. Every repo resolves its querier
   from the context (`querierFrom`): the pool standalone, the bound
   transaction inside a scope. `WorkPoolRepo.Save`, which already ran
   its own multi-statement transaction, now uses `beginOrJoin` so it
   **joins** the enclosing transaction instead of committing on its own
   — without that, a rolled-back `EnqueueWorkUnit` would still leave a
   `work_pools` row behind (the integration test asserts it does not).

5. **`postgres.OutboxPublisher`** implements the variadic
   `ports.EventPublisher`: for each configured `Encoder` (integration,
   then analytics) it calls `Encode` **inside the transaction** and
   `INSERT`s one row per `Encoded`. It never touches the broker.

6. **`postgres.OutboxRelay`** runs as a goroutine inside `cmd/wes`, next
   to the HTTP server. Each pass claims up to 100 pending rows with
   `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE
   SKIP LOCKED`, sends them to the `Sink` **one at a time in id order**,
   and marks each `published_at`. On a send failure it stops the pass at
   that row, records `attempts+1` / `last_error`, commits what was
   already sent, and retries on the next tick. Sleep between empty passes
   is `OUTBOX_RELAY_INTERVAL` (default `1s`); a full batch is followed
   immediately by another pass. The production Sink is
   `kafka.RelaySink`: one kafka-go Writer with no fixed topic that routes
   every message by its own `Encoded.Topic`, so one relay serves both
   topics.

7. **Composition root** (`cmd/wes/main.go` only; `wes-projector`,
   `wes-reports` and `mcp` are untouched):

   | `DATABASE_URL` | `EVENT_PUBLISHER` | Publisher wired                                   | Relay   |
   |----------------|-------------------|---------------------------------------------------|---------|
   | unset          | `log` (default)   | log                                               | none    |
   | unset          | `kafka`           | `MultiPublisher(integration, analytics)` — direct | none    |
   | set            | `log`             | log                                               | none    |
   | set            | `kafka`           | **`OutboxPublisher(pool, integration, analytics)`** | **yes** |

   The mode is logged at startup
   (`event publisher configured publisher=kafka mode=outbox|direct`).
   Graceful shutdown stops the HTTP server first, then cancels the relay
   and waits for its in-flight pass (bounded by the shutdown deadline),
   so an event committed by a request that finished a moment before
   SIGTERM is not stranded until the next pod boots.

### Delivery semantics (what consumers may now rely on)

- **Atomicity**: an aggregate change and its events — on *both* topics —
  commit together or not at all. Verified by an integration test that
  makes the outbox insert fail and asserts that `work_units`,
  `work_pools` and `work_pool_entries` are all absent afterwards.
- **At-least-once**: a crash between a successful `Send` and the row's
  `UPDATE` republishes that row on the next pass. Both consumers already
  tolerate this: the analytics projector is idempotent on `event_id`, and
  integration consumers keep a `processed_events` table. The republished
  message carries the **same `event_id`** and bytes because the wire form
  was fixed at encode time.
- **Per-key ordering**: preserved. Rows are drained in insertion order
  within one relay; the analytics key is the aggregate id and a failed
  row blocks everything behind it rather than being skipped. Two relays
  (a rolling deploy's overlapping pods) never claim the same row thanks
  to `SKIP LOCKED`, but global order across the two is not guaranteed —
  per-aggregate order is what consumers depend on and that holds because
  one aggregate's events for one request are a contiguous id range.
- **Latency**: events reach the topics within one relay interval
  (≤1s in the cluster) of the HTTP response, versus "before the response"
  under the direct publish.

## Consequences

**Positive**
- No code path can persist a WorkUnit/pool/plan/forecast change without
  also persisting its events for both topics.
- No broker dependency on the request path: `POST …/release` succeeds
  while Kafka is down; the events go out once it returns.
- The direct and outbox paths share one `Encode`, so the analytics
  report and the integration consumers see byte-identical envelopes
  either way.
- The pattern (Encoder split + `beginOrJoin` + variadic OutboxPublisher)
  is reusable by fulfillment-execution, workforce-management and
  labor-performance, which have the same three differences from PPM.

**Negative / accepted**
- One more table, one more goroutine, one more failure mode to observe.
  The relay logs every failed pass at ERROR with row id and broker error;
  `outbox_events.attempts`/`last_error` are queryable. An outbox-lag
  metric was a follow-up and is now built: the `wes.outbox.lag_seconds`
  observable gauge (`postgres.RegisterOutboxLagGauge`, registered in
  `cmd/wes` next to the relay) reports the age of the oldest unpublished
  `outbox_events` row, 0 when drained.
- Events are no longer synchronous with the HTTP response (documented
  above; acceptable for this domain).
- Two rows per event doubles outbox volume versus a single-topic outbox.
  The alternative — one row, fan out in the relay — would have forced the
  repo-reading `dataFor` to run after commit, reintroducing the
  read-your-own-write hazard this ADR exists to remove.
- `EVENT_PUBLISHER=kafka` without `DATABASE_URL` still publishes
  directly. That mode exists only for in-memory local runs and is logged
  as `mode=direct`.

## Alternatives considered

- **Retry around `Publish` / `MultiPublisher`.** Does not fix a crash
  between the writes, and keeps the broker's latency on the request
  path. Rejected.
- **Publish-then-Save.** Inverts the failure: a `WorkReleased` on the
  topic for a unit the store never released. Strictly worse for
  fulfillment-execution. Rejected.
- **One outbox row per event, fan out to topics in the relay.** Smaller
  table, but encoding would move out of the transaction — see the
  negative consequence above. Rejected for this service; fine for
  services whose encoders read nothing.
- **Change-data-capture (Debezium).** Adds Kafka Connect to a kind
  cluster already running Istio, Kong, Kafka, Postgres and the
  observability stack, and moves envelope encoding out of the service.
  Deferred.
- **A separate relay binary** (like `wes-projector`). Cleaner isolation,
  but the relay is a single `SELECT`/`UPDATE` loop and the graceful
  shutdown handling keeps it proportionate in-process. It can be lifted
  into `cmd/wes-relay` later without touching the adapters.

## Verification actually performed

- Unit, `internal/application/usecases/unit_of_work_test.go` (28 tests):
  for each of the seven publishing use cases — every Save and the
  Publish run inside exactly one scope, a publish failure rolls the scope
  back, a rejected/no-op invocation opens no scope, and a nil UnitOfWork
  still saves and publishes. `RecordCompletion` additionally proves a
  pool-save failure rolls back before anything is published.
- Unit, `internal/adapters/outbound/kafka/encoded_test.go`: `Encode`
  for both publishers (topic, key, event_type, envelope, `traceparent`
  header present when a span is active, none otherwise, analytics skips
  unknown types), `Publish` leaves `Message.Topic` empty for the
  fixed-topic writer, `RelaySink` routes per message in one write.
- Integration, `-tags=integration`, **testcontainers Postgres**
  (`postgres:16-alpine`, the real migrations — never an external
  `DATABASE_URL`, never a skip):
  `outbox_integration_test.go` — commit-together for both topics with
  the `WorkReleased` payload enriched from the in-transaction row;
  rollback of every aggregate table on a failed encode (including the
  `beginOrJoin`ed pool save) and a clean retry afterwards; relay
  publishes 3 events × 2 topics in id order and marks them, second pass
  no-op; relay stops at a failed row (`attempts=1`, `last_error` set,
  later row untouched) and drains in order on recovery with headers
  round-tripped; `Run` drains in small batches and stops on cancel.
- `make check` (gofmt, vet, build, golangci-lint, `go test ./... -race`),
  `make arch-test`, and the domain+application coverage gate all pass
  locally; see the PR for the exact output.
- Not verified in this change: a live cluster run (`terraform apply` +
  observing `outbox_events` drain against the shared broker). The chart
  needs no change — `OUTBOX_RELAY_INTERVAL` is optional — but the
  cluster-side probe is a follow-up.
