---
id: 0001-hexagonal-ports-and-adapters
title: ADR-0001 — Hexagonal (ports & adapters) architecture
sidebar_label: 0001 · Hexagonal architecture
sidebar_position: 1
description: Why the domain layer depends on nothing and every outbound dependency is an interface owned by the application layer.
---

# ADR-0001 — Adopt hexagonal (ports & adapters) architecture

## Status

**Accepted.** In force since the first commit; enforced automatically since
[ADR-0007](./0007-arch-go-fitness-tests.md).

## Context

This bounded context is the **Core subdomain** of the WES tier
([classification](../ddd/subdomain-classification.md)). The rules that make it
core are decisions, not data movement:

- priority is earliest-CPT-first, and nothing else;
- a work unit is handed out **at most once** and cannot complete twice;
- a release-fed pool refuses release past its WIP limit;
- `plannedHeads ≤ installedStations`.

Every one of those needs exhaustive testing including the failing paths, and
several are subtle enough that a regression would be silent in production — a
double-released work unit does not throw, it just gets picked twice.

Meanwhile the infrastructure surface is broad and volatile: Postgres, three
Kafka topics in and one out, REST, and an in-memory mode for tests and local
runs. Two inbound adapters (HTTP and Kafka) drive the *same* use cases.

The obvious alternative — a layered service where handlers call services that
call an ORM — makes every one of those domain rules reachable only through
infrastructure. Testing "release refuses past the WIP limit" then requires a
database, and the rule ends up expressed partly in a `SORT BY` and partly in a
transaction isolation level.

## Decision

**We will structure the service as hexagonal ports & adapters, with a strict,
non-negotiable dependency rule:**

> **domain depends on nothing · application depends on domain · adapters depend
> on application and domain · only `cmd/` wires them together**

Concretely:

1. `internal/domain/` is **pure Go** — standard library only, no framework
   types, no SQL types, no struct tags for serialisation. Aggregates keep
   private fields and expose behaviour; collection getters return copies.
2. All outbound dependencies are **interfaces defined in
   `internal/application/ports`**, so the *application* owns the contract and
   the adapter conforms to it, not the reverse. Every port has at least two
   implementations (in-memory and Postgres/Kafka).
3. `Clock` is a port. The domain never calls `time.Now()`.
4. Inbound adapters map to and from DTOs they own; **domain structs never cross
   the boundary**. Domain errors are typed sentinels, mapped to transport
   concerns by the adapter.
5. `cmd/wes/main.go` is the only place where every layer is visible at once.

## Consequences

### Easier

- **The interesting rules are tested without infrastructure.** The domain and
  application packages carry the bulk of the suite, run in milliseconds, and
  cover the failing paths as thoroughly as the happy ones. Mutation testing is
  practical on the domain packages precisely because they have no I/O to mock.
- **Two inbound adapters, one use case.** `RecordCompletion` is called by
  `POST /work-units/{id}/complete` and by the `TaskCompleted` Kafka consumer.
  Adding the second caller required no change to the use case at all — the
  clearest possible evidence the boundary is real.
- **Infrastructure is swappable per environment.** `DATABASE_URL` unset falls
  back to in-memory repositories; `EVENT_PUBLISHER` selects log or Kafka. The
  use cases cannot tell.
- **The ACL has a home.** Foreign event shapes live as unexported structs inside
  the inbound Kafka adapter and never leak inward.

### Harder

- **More types for the same data.** A charge forecast exists as a request DTO, a
  domain aggregate, a response DTO and a database row. That is four
  representations, and mapping code between them is genuinely tedious.
- **Indirection costs on first read.** Following one HTTP request to the rule it
  exercises means traversing handler → use case → port → aggregate. For a
  reader who wanted a one-line answer, this is slower than a layered service.
- **The discipline is only as good as its enforcement.** A single
  `import "github.com/jackc/pgx/v5"` inside a domain package would quietly
  dissolve the benefit. This is why it is now a test —
  [ADR-0007](./0007-arch-go-fitness-tests.md).

### Costs accepted deliberately

Copying slices out of aggregates (`Buckets()`, `PathPlans()`, `Entries()`)
allocates on every read. That is a real cost, accepted because handing out the
internal slice would let a caller mutate an aggregate behind its own back — the
exact class of bug the boundary exists to prevent.

## Addendum (2026-10) — composition roots and the `web/` remote

Rule 5 above says `cmd/wes/main.go` is where every layer is visible. The
repository now has four composition roots under `cmd/` — `wes` (OLTP API +
Kafka consumers + outbox relay), `wes-projector` and `wes-reports` (the
analytics projector and read-only reports reader,
[ADR-0011](./0011-analytical-data-product.md)) and `mcp`
([ADR-0008](./0008-mcp-inbound-adapter.md)). The rule is unchanged: each root,
and only the roots, wire concrete adapters, and environment variables are read
there. They share `internal/bootretry`, a boot-time dial retry used around each
root's first Postgres/Kafka connection
([ADR-0030](./0030-kafka-sourced-path-catalogue.md)).

`web/` is a separate artifact, not a layer of the Go module: the
`planning-mfe` Module Federation remote mounted by `warehouse-console` at
`/planning/*`, which talks to this service's REST API only. It is built into its
own `frontend` image/workload
([ADR-0025](./0025-horizontal-autoscaling-and-pgxpool-tuning.md)) and is outside
the dependency rule, which governs the Go packages.

