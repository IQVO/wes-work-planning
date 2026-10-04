---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, port, HTTP adapter, apis/openapi.yaml, generated docs, godog scenario). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

Use when asked to add a new REST use case/endpoint to this service. Follow
this order — domain first, adapter last — never the reverse; writing the
HTTP handler before the domain invariant it enforces produces handlers
that validate nothing and use cases that get bypassed.

This walks the exact path `POST /paths/{pathId}/charge` took
(`internal/application/usecases/receive_charge_forecast.go` +
`internal/adapters/inbound/http/handlers.go`'s `postChargeForecast`) as the
concrete worked example — read those two files alongside this guide.

## 1. Domain first: does an invariant already exist, or do you need one?

Check `internal/domain/<aggregate>/` for the rule this endpoint enforces.
A REST endpoint should almost never contain business logic itself — it
decodes a request, calls a use case, encodes the result. `postChargeForecast`
itself contains none: it resolves `pathId`, checks it against the process-path
catalogue (`h.validatePathId`), decodes the DTO into domain-typed
`shared.Quantity`/`shared.CPT` buckets, and hands off. The actual invariant
("a charge forecast's buckets are well-formed") lives in
`internal/domain/charge.NewChargeForecast`. If the operation needs a new
domain rule, add it to the aggregate/value-object in `internal/domain/`, with
its own table-driven unit test, BEFORE touching the application or adapter
layers.

## 2. Application: define the use case

Add a new file in `internal/application/usecases/` (one file per use
case, this repo's convention — not one giant `usecases.go`). Shape, mirroring
`ReceiveChargeForecast`:

```go
package usecases

type <Verb><Noun>Request struct {
    PathId shared.PathId
    // other domain-typed fields the caller needs — never DTOs
}

// <Verb><Noun> — one sentence: what business capability this represents.
type <Verb><Noun> struct {
    repo      ports.<Aggregate>Repo // driven ports only — never a concrete adapter
    publisher ports.EventPublisher  // if this raises an integration event
    clock     ports.Clock           // if it needs "now" — never call time.Now() directly
    uow       ports.UnitOfWork      // optional: brackets Save + Publish atomically (ADR-0014)
}

func New<Verb><Noun>(repo ports.<Aggregate>Repo, publisher ports.EventPublisher, clock ports.Clock) *<Verb><Noun> {
    return &<Verb><Noun>{repo: repo, publisher: publisher, clock: clock}
}

// WithUnitOfWork brackets Save + Publish in one atomic scope (ADR-0014).
// Optional: nil keeps the two calls running back to back.
func (uc *<Verb><Noun>) WithUnitOfWork(u ports.UnitOfWork) *<Verb><Noun> {
    uc.uow = u
    return uc
}

func (uc *<Verb><Noun>) Execute(ctx context.Context, req <Verb><Noun>Request) (*domain.<Aggregate>, error) {
    // 1. build/load the aggregate via the port, applying the rule inside
    //    the aggregate's own constructor/method (never inline the
    //    invariant here — that belongs in internal/domain/)
    // 2. persist + publish atomically via atomically(ctx, uc.uow, ...) if
    //    this raises an event, mirroring ReceiveChargeForecast.Execute
    // 3. return the result
}
```

Add the port to `internal/application/ports/` if it doesn't exist yet —
`TestApplicationPortsContainOnlyInterfaces` in `internal/architecture/`
enforces that every type in `ports` is an interface; a struct or function
there fails CI.

Write the use case's unit test against the in-memory adapter
(`internal/adapters/outbound/memory/`) — never a real Postgres/HTTP call
in a unit test. Cover the success path AND the domain-rule failure path
(e.g. `ReceiveChargeForecast`'s tests cover an invalid `PathId` and an
invalid `Quantity` bucket separately).

## 3. Adapter: wire the HTTP handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — add the request/response DTO structs (JSON tags, this repo's
   naming convention: `<verb><noun>RequestDTO`/`<verb><noun>ResponseDTO`,
   e.g. `receiveChargeForecastRequestDTO`). DTOs live ONLY in the adapter
   layer — domain types never carry JSON tags.
2. `router.go` — add the route inside the existing `r.Route("/paths/{pathId}",
   ...)` block if it's path-scoped (mirroring `/charge`, `/plan`,
   `/work-units`, `/release`, `/telemetry`, `/rebalance`,
   `/labor-plan-view`), or as a top-level route otherwise (see
   `/work-units/{id}/complete`, `/inventory-view/{sku}`).
3. `handlers.go` — add the handler function on `*Handlers`:
   - resolve path params via the existing helpers (`pathIdParam(r)`)
   - call `h.validatePathId(pathId)` FIRST if this handler seeds a NEW
     aggregate keyed by a caller-supplied `path_id` — read-only lookups
     don't need it
   - `decodeJSON(w, r, &body)` to decode + bail out on malformed JSON,
     converting fields to domain value objects immediately
     (`shared.NewQuantity`, `shared.NewCPT`, etc.) — a bad value fails here
     as an RFC 7807 validation error, never reaches the use case
   - call the use case's `Execute`
   - map use-case errors to HTTP status via `writeError` (check `errors.go`
     for the existing error→status mapping before adding a new error type)
   - encode the domain result back to the response DTO and `writeJSON`
4. Add the new use case field to the `Handlers` struct and wire it in the
   composition root (`cmd/wes/main.go`), alongside the existing
   `ReceiveChargeForecast`/`CommitShiftPlan`/etc. fields.

Write at least one httptest per endpoint: one success path, one error
path (validation failure AND/OR the domain-rule failure, whichever this
endpoint can produce). `router_test.go` and `commit_shift_plan_travel_distance_test.go`
are the shape to copy.

## 4. Contract: update OpenAPI, then regenerate docs

Add the path to `apis/openapi.yaml` (request/response schemas, the RFC 7807
problem-detail response for each error case — see the existing
`/paths/{pathId}/charge` entry for the shape).

Regenerate the Docusaurus REST reference:

```bash
cd docs
npm ci
npm run build   # prebuild hook runs clean-api-docs + gen-api-docs from apis/openapi.yaml
```

Never hand-edit the generated `docs/docs/api/rest/*.api.mdx` files.

## 5. Behaviour: add a scenario if this is user-facing

If this endpoint is user-facing behaviour (not purely internal plumbing),
add coverage exercising it end-to-end against the real HTTP server —
`make bdd` runs `TestFeatures`. See the existing acceptance-test layer for
the shape this repo expects (Given/When/Then over real HTTP, not mocked).

## 6. Verify before opening the PR

```bash
make check       # fmt-check vet build lint test -race
make check-all    # + coverage (90% gate) + arch-test + bdd
```

`make coverage` gates `./internal/domain/...,./internal/application/...`
at 90% — a new use case with no test on its failure path is the most
common way to miss this gate.
