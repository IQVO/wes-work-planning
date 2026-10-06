---
name: how-to-add-an-integration-event
description: Publish or consume a cross-service Kafka event: CloudEvents 1.0 type naming, AsyncAPI, transactional outbox, consumer-group rules. Use when touching internal/adapters kafka or outbox code, a publisher/consumer, or apis/asyncapi*.yaml.
---

# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident, and that
incident happened IN THIS REPO.

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event this service raises belongs on the wire. Check
`internal/adapters/outbound/kafka/publisher.go`'s doc comment and
`cloudevents.TopicWorkPlanningEvents` (`internal/adapters/kafka/cloudevents/cloudevents.go`)
— this repo publishes to `warehouse.work-planning.events`, and only events
a sibling context genuinely needs (e.g. `WorkReleased`, consumed downstream
by fulfillment-execution, or `PathCapacityChanged`, consumed by
order-management) belong there. Before adding a new event to the
Kafka publisher, confirm who's actually downstream — check
`docs/docs/adr/0004-kafka-integration-events.md` and
`apis/asyncapi.yaml` for the existing contract and its consumers.

### 2. Envelope: CloudEvents 1.0, mandatory (ADR-0027)

Every message is a CloudEvents 1.0 event in structured content mode, built
ONLY through `internal/adapters/kafka/cloudevents` (`cloudevents.New`), which
uses the official `github.com/cloudevents/sdk-go/v2/event` package. No flat
envelope, no dual mode, no envelope env var, no hand-rolled struct:

```json
{
  "specversion": "1.0",
  "id": "uuid-v4",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-10231",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": { }
}
```

- `type` = `com.warehouse.wes.work-planning.<entity>.<EventName>`; add the
  event's entity to `eventTypeEntity` in `publisher.go` (entities:
  `charge`, `plan`, `workunit`, `workpool` — take it from the AsyncAPI tag).
- `id` is a UUID v4 minted ONCE in `Encode` (the outbox persists it, the relay
  republishes it unchanged) and is the integration-topic Kafka key.
- `subject` = aggregate id (add a case to `subjectFor`); never empty.
- `time` = domain occurred-at (`ports.Clock`), never publish wall-clock.
- `dataschema` = `urn:warehouse:wes-work-planning:events:<EventName>:v1`
  (analytics: `...:analytics:<EventName>:v1`).
- Every produced message carries `cloudevents.ContentTypeHeader()`.
- A breaking payload change = new `.v2` type + new dataschema version,
  published as a new event; never mutate an existing type.

### 3. Implementation

Add the event struct to `internal/domain/<aggregate>/` — it should already
exist as a domain event the aggregate raises (e.g. `shared.WorkReleased`);
publishing wires an EXISTING domain event onto Kafka via
`internal/adapters/outbound/kafka/publisher.go`, it doesn't invent a new
payload shape at the adapter layer. See `Publisher.NewPublisher`'s doc
comment for how `WorkReleased` gets enriched with `cpt`/`ref` (by reading
`ports.WorkUnitRepo`) and derived hazmat/fragile hints (by reading
`ports.ProductClassificationLookup`) at publish time — that enrichment
belongs in the adapter, never in the domain event itself.

- Give the message a partition key that keeps ordering where it matters —
  this repo keys the integration topic by the aggregate id (work unit id /
  path id — the CloudEvents `subject`, ADR-0024), so one aggregate's events
  share a partition; there is still no cross-aggregate ordering guarantee
  (see ADR-0004 §"Harder"), tolerable because downstream
  projections apply commutative/last-writer-wins updates)
- Use `cloudevents.TopicWorkPlanningEvents` — this service's own topic
  constant, never a sibling's

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's channel,
  grouped by aggregate (matching the existing convention), not
  chronologically, with its exact `type` const and `dataschema` const (and
  the analytics counterpart if it goes to `warehouse.wes.analytics`).
- `docs/docs/api/events.md` and `docs/docs/ecosystem/integration-events.md`
  are hand-written from the AsyncAPI contract in this repo (no generated
  AsyncAPI static site here) — update both by hand whenever a
  message/channel changes.

### 5. Test

Add the event to `goldenCases` in
`internal/adapters/outbound/kafka/cloudevents_golden_test.go`, run
`go test ./internal/adapters/outbound/kafka -run Golden -update`, and review
the new `testdata/ce_*.golden.json` — they are the exact-JSON wire contract.
Unit test the payload against a fake `Writer` (see
`internal/adapters/outbound/kafka/publisher_test.go` — never a real broker
in a unit test; `Writer` is a small interface over `*kafkago.Writer` for
exactly this reason). If this event now needs an `_integration_test.go`
asserting real delivery, it MUST use testcontainers — the
`TestKafkaIntegrationTestsUseTestcontainers` fitness test in
`internal/architecture/fitness_test.go` scans every `_integration_test.go`
file that constructs a real `kafkago.Writer{}`/`Reader{}`/`DialContext`/etc.
and fails the build if it doesn't also import
`testcontainers-go/modules/kafka`.

## Consuming an integration event from a sibling context

### 0. Dispatch on the full CloudEvents `type`

Add the producer's exact `type` string (from the fleet catalogue in ADR-0027
/ the producer's AsyncAPI) as a constant in
`internal/adapters/kafka/cloudevents`, decode with `cloudevents.Decode`, read
the payload with `evt.DataAs`, `time`/`subject` from the attributes, dedupe on
`evt.ID()`. Never match a short name or a suffix. Invalid CloudEvents go to
the DLQ (main consumer) or are WARN-skipped (cache consumers). Add a
legacy-flat-message-rejected test.

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name and payload shape ONLY — never
its Go types. See `internal/adapters/outbound/kafkacatalog/consumer.go`'s
own doc comment: this package consumes process-path-management's
`warehouse.process-path-management.events` topic and hand-mirrors the
payload struct locally (`pathcatalog.PathDefinition`) rather than importing
that service's module. Note this repo's own catalogue consumer even
diverges deliberately from its sibling copy in fulfillment-execution — its
payload has no `Direct` field because this service never needed it, and the
Kafka payload's `direct` field is read and discarded rather than carried
forward. Mirror only what you actually use.

### 2. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist, and picking the wrong one is THE most
common integration-event mistake in this fleet. **This repo is the origin
of that lesson** — `wes-work-planning#67` is the actual incident, not a
hypothetical borrowed from another service.

**Pattern A — long-lived, single-instance consumer group (a named
constant).** Use when exactly ONE instance of this consumer ever runs at a
time (e.g. this service's own analytics consumer feeding
`cmd/wes-projector`). The group id is a plain named constant
(`AnalyticsConsumerGroup`-style), reused across restarts — Kafka's
committed-offset resume semantics are EXACTLY what you want there: pick up
where the single instance left off. `cmd/wes/main.go`'s
`defaultConsumerGroup = "wes-work-planning"` constant is this pattern, used
for the main inbound consumer that every deployed replica of this service
cooperatively shares (horizontal scaling splits partitions across
replicas — the normal, intended behaviour).

**Pattern B — per-process-unique consumer group (a generated id).** Use
when this consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache, not a work queue).
`internal/adapters/outbound/kafkacatalog/consumer.go`'s
`consumerGroupPrefix` + `uniqueConsumerGroup()` is this repo's own example:
the process-path catalogue cache MUST get a unique group id
(hostname+PID+timestamp) per process instance, never a fixed shared
string, because it replays the full topic on every boot to build its
in-memory cache.

**The actual incident (wes-work-planning#67):** `cmd/wes/main.go`
originally hardcoded its main inbound Kafka consumer group as the fixed
string `"wes-work-planning"` with NO env override. This fleet runs Kafka as
ONE broker platform-wide, so any second process joining that broker under
the same group — the e2e-tests harness's local `bin/wes`, or a developer
running `go run ./cmd/wes` against the shared cluster — joined the EXACT
SAME consumer group as the live in-cluster Deployment. With one partition
per topic, Kafka's rebalance protocol hands the partition to only ONE
group member; the other is marked healthy while silently consuming
nothing. The fix, shipped in #67 and still live today
(`cmd/wes/main.go`'s `consumerGroupID` function): make the group id
env-configurable via `KAFKA_CONSUMER_GROUP`, defaulting to the shared
constant when unset, so an isolated caller (tests, local dev) can pass a
unique value instead of colliding with the deployed instance:

```go
// consumerGroupID resolves the Kafka consumer group id, allowing
// KAFKA_CONSUMER_GROUP to override the default.
func consumerGroupID(override string) string {
    if strings.TrimSpace(override) != "" {
        return override
    }
    return defaultConsumerGroup
}
```

**Never hardcode a `GroupID` as an inline string literal, full stop** —
`TestKafkaConsumerGroupNeverHardcodedInline` in
`internal/architecture/fitness_test.go` statically scans every `.go` file
for a `GroupID:`/`GroupID =` assignment from a bare string literal and
fails CI on one (the `defaultConsumerGroup` named-constant pattern above is
the sanctioned exception the test explicitly allows, because it's traceable
to its own doc comment explaining why a shared name is correct there).

### 3. Readiness gate, if this consumer backs a local cache

If the consumer replays a topic's full history to build a cache other code
depends on (Pattern B), expose a `Ready()`/`WaitReady()` gate the health
check consults, and block readiness — not process startup, a transient
Kafka outage shouldn't be fatal — until the initial replay finishes.
`kafkacatalog/consumer.go`'s own doc comment records two real bugs found
and fixed in the sibling fulfillment-execution copy before this one was
written: (1) a readiness gate that only re-evaluates "am I caught up" on a
NEW message deadlocks forever on an ordinary restart where nothing new has
been published since the last run; (2) the Pattern-B/shared-group bug
above, described from the read-model-cache angle rather than the
work-queue angle. Prefer the per-process-unique group (Pattern B) over
trying to fix a readiness gate around a shared group — it sidesteps the
whole class of bug.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a hardcoded GroupID or a sibling-package import
```

Prove any new fitness-test-adjacent behavior actually matters by running
the specific scenario against a real broker — see
`internal/adapters/outbound/kafkacatalog/consumer_integration_test.go`'s
`TestNewConsumer_TwoInstancesInARow_BothReplayFully`, the real regression
test for the Pattern-B bug: it starts its own testcontainers broker and
asserts a SECOND consumer instance independently replays the full topic
rather than resuming from the first instance's committed offset.
