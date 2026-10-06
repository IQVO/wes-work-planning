---
paths:
  - "internal/adapters/**"
  - "apis/asyncapi*"
  - "features/**"
  - "docs/docs/api/events.md"
  - "docs/docs/ecosystem/**"
---

# Events: CloudEvents 1.0 is MANDATORY (detail)

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference (ADR-0027, `docs/docs/adr/`; it also holds the fleet's
cross-service type catalogue):

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/`; transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/wes-work-planning`, `type`, `subject`
  (aggregate id), `time` (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:wes-work-planning:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.work-planning.<entity>.<EventName>`.
  Breaking payload change => new `.v2` type + new dataschema version, never
  mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

## Contracts of record

- REST: `apis/openapi.yaml` (full schemas, every status code, shared `Problem`
  component). The Docusaurus REST reference pages
  (`docs/docs/api/rest/*.api.mdx`) are generated from it by the `prebuild` npm
  script; never hand-edit them.
- Async: `apis/asyncapi.yaml` (CloudEvents 1.0 over Kafka). Narrative pages
  `docs/docs/api/events.md` and `docs/docs/ecosystem/integration-events.md`
  are written from it and must be updated by hand whenever a message/channel
  changes there (this fleet documents AsyncAPI narratively per-service; there
  is no generated AsyncAPI static site in this repo — that only exists in the
  separate fleet-wide docs aggregator).
