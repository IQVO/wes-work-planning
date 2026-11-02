---
id: 0005-rfc-7807-problem-details
title: ADR-0005 — RFC 7807 problem+json for every error response
sidebar_label: 0005 · RFC 7807 errors
sidebar_position: 5
description: Why the bespoke error shape was replaced with Problem Details, and what that costs.
---

# ADR-0005 — Return RFC 7807 Problem Details for every error

## Status

**Accepted.** Migrated from the previous bespoke shape during the REST API
hardening work; the OpenAPI spec and the full httptest suite were updated in the
same change.

## Context

The service originally returned errors as:

```json
{"error": "planned heads exceeds installed stations"}
```

That shape has three concrete problems.

1. **The only machine-readable signal is the HTTP status code, and it is
   ambiguous.** This service maps *thirteen* distinct domain errors to `400` and
   *seven* to `409`. A client that needs to distinguish "the WIP limit is
   reached, back off and retry" from "this unit was already completed, stop
   retrying" — both `409` — has nothing to branch on but the prose in `error`.
2. **Clients end up parsing prose.** The natural workaround is
   `if body.error.contains("WIP limit")`, which turns an error message into an
   API contract that cannot be reworded.
3. **It is a bespoke shape in a platform of five services.** Every client needs
   per-service error handling for no benefit.

[RFC 7807](https://www.rfc-editor.org/rfc/rfc7807) exists for exactly this: a
standard `application/problem+json` body with a stable `type` URI identifying
the *category*, a fixed `title`, the duplicated `status`, a per-occurrence
`detail` and an `instance`.

## Decision

**We will return RFC 7807 `application/problem+json` for every 4xx and 5xx
response, with a stable per-category `type` URI.**

```http
HTTP/1.1 409 Conflict
Content-Type: application/problem+json

{
  "type": "https://errors.wes-work-planning.warehouse-systems.dev/wip-limit-reached",
  "title": "Release-fed pool WIP limit reached",
  "status": 409,
  "detail": "release-fed pool WIP limit reached",
  "instance": "/paths/pick-a/work-releases"
}
```

1. **`type` is the contract.** Namespaced per service so URIs never collide
   across the platform. It **does not resolve** to a real page — RFC 7807
   explicitly permits that, and promising a resolvable URI would create a
   documentation site that must never 404. Clients match on `type`, never on
   `detail`.
2. **`title` is fixed per category; `detail` may vary per occurrence.** The
   split is what makes `type`/`title` safe to depend on while leaving `detail`
   free to be helpful.
3. **The mapping lives in two mirrored pure functions** in the HTTP adapter:
   `statusFor(err) int` and `problemFor(err) (typeURI, title)`. Every sentinel
   one recognises has a case in the other, so no error can get a correct status
   with a generic type. Both use `errors.Is`, so wrapped errors keep their
   mapping.
4. **Malformed JSON is a client error.** A dedicated `errMalformedBody` sentinel
   wraps decode failures so they map to `400`, not to the `default` `500`.
5. **The mapping stays in the adapter.** The domain returns typed sentinels and
   knows nothing about HTTP — which is why the Kafka inbound adapter can handle
   the same errors completely differently.
6. **The spec documents it.** `apis/openapi.yaml` defines one reusable `Problem`
   component referenced by every error response, and is Spectral-linted in CI.

The full catalogue is on the [Error model](../api/errors.md) page.

## Consequences

### Easier

- **Clients can branch on a stable identifier.** `wip-limit-reached` (retry with
  backoff) and `work-unit-already-completed` (stop) are now distinguishable
  without reading prose, despite sharing `409`.
- **Error messages can be reworded freely.** Only `detail` changes; no client
  breaks.
- **The catalogue is exhaustive and reviewable.** `problemFor` is a single
  switch listing every error the service can produce — twenty-one categories
  traceable to a specific sentinel, plus the `internal-error` fallback.
- **`500` means something again.** Every domain error is mapped explicitly, so
  an unmapped error reaching the `default` branch genuinely indicates
  infrastructure failure rather than an unhandled business case.
- **Consistency across the platform.** All five services return the same shape.

### Harder

- **Two places to update per new error.** A new sentinel needs a case in
  `statusFor` *and* in `problemFor`. Forgetting the second yields a correct
  status with a generic `internal-error` type — silently wrong. The mirrored
  structure and a doc comment make it visible, but nothing enforces it.
- **Type URIs are now a public contract.** Renaming `wip-limit-reached` is a
  breaking change for clients, even though it looks like a string literal.
- **`Content-Type: application/problem+json` surprises naive clients.** Some
  HTTP libraries only auto-decode `application/json`. Correct per the RFC, and
  occasionally an integration snag.
- **A non-resolving `type` URI invites bug reports.** Someone will paste it into
  a browser and get nothing. Documented in the spec, the README and the error
  page — and still likely to be asked about.
- **The migration was a breaking change.** Every existing client and every
  error-path test had to change at once. Done deliberately in one pass rather
  than by content-negotiating two shapes, which would have doubled the surface
  permanently to smooth a one-time cost.
