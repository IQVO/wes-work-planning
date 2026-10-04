---
id: 0012-process-path-catalogue-validation
slug: /adr/0012-process-path-catalogue-validation
title: 0012. Process-path catalogue validation, mirroring fulfillment-execution's ADR-0017
sidebar_label: 0012. Path catalogue validation
description: ADR 0012 — validate every caller-supplied path_id (HTTP and Kafka) against the fleet's declared process-path catalogue, loaded from warehouse-infra's published-language YAML file at boot.
---

# 0012. Process-path catalogue validation

## Status

Accepted — amended by [ADR-0030](./0030-kafka-sourced-path-catalogue.md): the
catalogue is consumed through `ports.PathCatalogue` and may also be sourced from
Kafka (`PATH_CATALOGUE_SOURCE=kafka`). The file source described below remains
the default.

## Context

This service accepts a `path_id` from four independent entry points:

- **HTTP**: `POST /paths/{pathId}/charge`, `.../plan`, `.../work-units` —
  a caller-supplied `pathId` URL segment that seeds a brand-new
  `ChargeForecast`, `ShiftPlan`, or `WorkPool` aggregate the first time
  it is used.
- **Kafka `ShiftPlanCommitted`** (from `workforce-management`): a
  `data.path_id` that feeds `ObserveLaborPlan` and is written into the
  labor-plan-view.
- **Kafka `OrderAllocated`/`OrderPartiallyAllocated`** (from
  `order-management`): each line in the payload carries its own
  `path_id`, fed into `EnqueueWorkUnit`, which seeds a `WorkPool` exactly
  like the HTTP path does.

Until now, every one of these simply called `shared.NewPathId(value)` —
which only rejects an *empty* string — and trusted the result. Nothing
anywhere in this service ever validated that a `path_id` corresponds to
a real, currently-declared process path. A typo, a stale deploy
referencing a retired path, or a producer bug on either upstream service
(`workforce-management` or `order-management`) would silently seed a
brand-new `WorkPool`/`ChargeForecast`/`ShiftPlan`/labor-plan-view entry
for a path nothing downstream (fulfillment-execution's own consumer,
station capability planning) will ever recognize or service — the exact
"undeclared paths silently accumulate" failure mode
`fulfillment-execution`'s ADR-0017 documents fixing on its own side of
this same integration.

`fulfillment-execution` closed this gap for its `WorkReleased` consumer
by loading a shared, `warehouse-infra`-published catalogue
(`config/process-paths/sortable-fc.yaml`) at boot and validating every
incoming `path_id` against it. That catalogue is explicitly a
**published language** — the single source of truth for "what process
paths exist in this building's topology" — meant to be read
independently by every consuming service, not owned by any one of them.
This service is one of the three named consumers in that catalogue's own
schema comment (`fulfillment-execution`, `wes-work-planning`,
`workforce-management`), so closing the same gap here means reading the
identical file, not inventing a second one.

### The exact-match pitfall (learned from fulfillment-execution's ADR-0017 addendum)

`fulfillment-execution`'s first version of this idea did an EXACT string
match against the catalogue's bare canonical id (`"PICK"`), which is
wrong: **no real `path_id` in this fleet is ever the bare canonical id.**
This service's own domain, in particular, makes that unmistakable —
`shared.PathId`'s own doc comment describes it as "a named station owning
a queue... e.g. `pick-zone-a` or `pack-station-3`" and this service's
existing WorkPool/labor-plan-view/etc. keys are ALWAYS these granular,
station-qualified forms, never the bare family name. `order-management`'s
own `shared.DefaultPathId` is the plain `"pick"`. Building this feature
from a corrected starting point (mirroring the ALREADY-FIXED
`fulfillment-execution`/`warehouse-infra` schema, which carries
`matchPrefix` per path) avoids repeating that regression here.

## Decision

**Load the same `warehouse-infra`-published process-path catalogue this
fleet already uses, and validate every caller-supplied `path_id` against
it (via `Catalogue.Lookup`, a case-insensitive prefix-family match) at
every one of the four entry points listed above — WITHOUT collapsing
this service's own granular, station-qualified `PathId` values down to
the catalogue's coarser canonical id.**

```go
// internal/domain/pathcatalog/path_definition.go — a byte-for-byte
// mirror of fulfillment-execution's own package, since both read the
// SAME file and must agree on its matching semantics.
type PathDefinition struct {
	Id                   string
	MatchPrefix          string
	RequiredCapabilities []string
}

func (c *Catalogue) Lookup(id string) (PathDefinition, error) // ErrUnknownPath
```

```go
// internal/adapters/outbound/filecatalog/loader.go — identical schema
// and boot-time failure contract to fulfillment-execution's own loader.
func Load(path string) (*pathcatalog.Catalogue, error)
```

Wiring, by entry point:

- **HTTP** (`internal/adapters/inbound/http/handlers.go`): `Handlers`
  gains a `Catalogue ports.PathCatalogue` field (the port; `*pathcatalog.Catalogue`
  satisfies it) and a
  `validatePathId` helper, called from `postChargeForecast`,
  `postShiftPlan`, and `postWorkUnit` — every handler that SEEDS a new
  aggregate keyed by a caller-supplied `path_id`. Read-only handlers
  (`getTelemetry`, `getRebalance`, `getLaborPlanView`) do not need it:
  a nonexistent path already surfaces as `ports.ErrNotFound` on its own,
  which is the correct signal for "ask about a path with no data yet,"
  distinct from "this path_id isn't even declared."
- **Kafka** (`internal/adapters/inbound/kafka/consumer.go`): `Consumer`
  gains a `catalogue ports.PathCatalogue` field, checked in
  `handleWorkforceEvent` (before calling `ObserveLaborPlan`). For
  `OrderAllocated`/`OrderPartiallyAllocated` the check lives in the
  `ApplyOrderAllocated` use case (`enqueueRequests`, via its own
  `ports.PathCatalogue`), validating every line **before** any is enqueued
  ([ADR-0031](./0031-order-allocated-choreography.md)) — an unrecognized
  `path_id` now fails the whole message handling, the same fail-loud contract
  `fulfillment-execution`'s `WorkReleased` consumer already has.

`cmd/wes/main.go` loads the catalogue once, before any adapter stands
up, from `PATH_CATALOGUE_FILE` (default
`/etc/wes-work-planning/process-paths.yaml`); a load failure is fatal —
the process exits before serving any traffic or consuming any message,
mirroring `fulfillment-execution`'s identical boot-time contract.

### What this decision does NOT do

- It does not change `shared.PathId`'s shape or granularity. `WorkPool`,
  `ChargeForecast`, `ShiftPlan`, and the labor-plan-view all stay keyed
  by the ORIGINAL, granular `path_id` (e.g. `"pick-zone-a"`), because
  each station/zone is a genuinely distinct queue this service must
  track independently — the catalogue's coarser `MatchPrefix` family
  (`"pick"`) is a VALIDATION concern only, never a storage key.
- It does not give this service ownership of the catalogue file, and
  does not add a network dependency on another service — the catalogue
  is a local file read once at boot, identical in spirit to
  `fulfillment-execution`'s own loader.
- It does not touch `WorkPool`'s WIP-limit/backlog/release logic, or any
  existing aggregate invariant — only how a `path_id` is validated
  before it is ever allowed to seed one of these aggregates.

## Consequences

### Easier

- **A malformed or stale `path_id` from any of the three producers
  hitting this service (a caller, `workforce-management`, or
  `order-management`) is now loud and traceable** instead of silently
  seeding a WorkPool/labor-plan-view entry nothing downstream will ever
  route real work through.
- **One schema, shared across the fleet, zero forking** — this service
  reads the identical `warehouse-infra` file `fulfillment-execution`
  does; there is no risk of the two developing divergent private
  copies of "what paths exist."

### Harder

- **A new boot-time external dependency**: this service now refuses to
  start without a readable, valid catalogue file — the same trade
  `fulfillment-execution` already accepted for the identical reason
  (fail loud beats silently mis-routing).
- **A breaking behavior change** for any HTTP caller or upstream
  producer (`workforce-management`, `order-management`) currently
  relying on this service accepting an undeclared `path_id` — any such
  request now fails with `pathcatalog.ErrUnknownPath` (mapped to HTTP
  400 via the new `unknown-path-id` RFC 7807 problem type) instead of
  silently succeeding. This is the intended fix for the exact gap this
  ADR exists to close, called out explicitly here rather than buried in
  a diff.

## Verification

Domain layer (`internal/domain/pathcatalog/path_definition_test.go`): 8
tests, including a dedicated real-fleet-variants regression test
(`TestCatalogue_Lookup_RealFleetPathIdVariants`) exercising this
service's own actual `path_id` forms (`"pick"`, `"pick-zone-a"`,
`"pick-soak"`, `"pick-t5-imbalance"`) — the exact regression class
`fulfillment-execution`'s ADR-0017 addendum documents, built correctly
here from day one rather than fixed after the fact.

Adapter layer (`internal/adapters/outbound/filecatalog/loader_test.go`):
8 tests covering every documented failure mode plus a real-integration
test, gated on `WAREHOUSE_INFRA_CATALOGUE_PATH`, that loads the ACTUAL
file `warehouse-infra` publishes.

Consumer layer (`internal/adapters/inbound/kafka/consumer_test.go`): new
`TestHandleOrderManagementEvent_UnknownPathId_ReturnsError`,
`TestHandleOrderManagementEvent_ResolvesRealFleetPathIdVariants`, and
three new `handleWorkforceEvent` tests (previously zero direct coverage
existed for that handler at all) —
`TestHandleWorkforceEvent_ObservesLaborPlanForARecognizedPath`,
`TestHandleWorkforceEvent_UnknownPathId_ReturnsError`,
`TestHandleWorkforceEvent_IgnoresOtherEventTypes`.

`go test ./... -race` (all packages, including `internal/architecture`'s
hexagonal fitness tests), `golangci-lint run ./...` (0 issues),
`make check-all` (fmt/vet/build/lint/test/coverage/arch-test/bdd), and
`gremlins unleash ./internal/domain/pathcatalog` /
`./internal/adapters/outbound/filecatalog` (100% efficacy/100% mutator
coverage on both, the freshly-fixed tie-break test included) all pass.
