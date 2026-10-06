---
id: errors
title: Error model
sidebar_label: Error model (RFC 7807)
sidebar_position: 3
description: Every 4xx/5xx response is application/problem+json — the full catalogue of type URIs and their status codes.
---

# Error model — RFC 7807

Every error response from this service is
[RFC 7807](https://www.rfc-editor.org/rfc/rfc7807) **Problem Details**, served
with `Content-Type: application/problem+json`. There is no bespoke
`{"error": "..."}` shape anywhere. See
[ADR-0005](../adr/0005-rfc-7807-problem-details.md).

```sh
curl -sD - localhost:8080/paths/does-not-exist/telemetry
```

```http
HTTP/1.1 404 Not Found
Content-Type: application/problem+json

{
  "type": "https://errors.wes-work-planning.warehouse-systems.dev/not-found",
  "title": "Resource not found",
  "status": 404,
  "detail": "resource not found",
  "instance": "/paths/does-not-exist/telemetry"
}
```

| Member | Meaning |
|---|---|
| `type` | Stable per-category identifier. **It does not need to resolve** to a real page — RFC 7807 permits that, and it is namespaced per service so type URIs never collide across the platform. Match on this, not on `detail`. |
| `title` | Fixed human-readable summary of the category. Never varies per occurrence. |
| `status` | Duplicates the HTTP status code, so the body is self-contained when detached from its response. |
| `detail` | The specific message for *this* occurrence. May vary; do not parse it. |
| `instance` | The request path that produced the problem. |

## The full catalogue

All type URIs are prefixed
`https://errors.wes-work-planning.warehouse-systems.dev/`.

### 400 Bad Request — the request is malformed or violates a domain rule

| `type` suffix | `title` | Raised by |
|---|---|---|
| `malformed-request-body` | Malformed request body | JSON that does not decode, a missing required field, or a present-but-empty/invalid `cutoffAt` |
| `reference-required` | Reference query parameter is required | `GET /work-units` without `reference` |
| `invalid-quantity` | Invalid quantity | negative quantity |
| `invalid-rate` | Invalid rate | rate ≤ 0 |
| `invalid-station-count` | Invalid station count | negative station count, or above the int32 range |
| `invalid-path-id` | Invalid path id | empty `pathId` |
| `unknown-path-id` | Unrecognized process-path id | a `pathId` no declared process-path prefix matches ([ADR-0012](../adr/0012-process-path-catalogue-validation.md)) |
| `invalid-hours` | Invalid hours | hours ≤ 0 |
| `charge-forecast-requires-buckets` | Charge forecast requires at least one CPT bucket | empty `buckets` |
| `unknown-cpt` | No bucket exists for the given CPT | querying a CPT with no bucket |
| **`heads-exceed-installed-stations`** | **Planned heads exceed installed stations** | **the `PathPlan` invariant** |
| `shift-plan-requires-path-plans` | Shift plan requires at least one path plan | empty plan |
| `planned-throughput-not-finite` | Planned throughput is not finite | `rate × heads × hours` overflows `float64` |
| `work-unit-id-required` | Work unit id is required | empty id |
| `work-unit-reference-required` | Work unit reference is required | empty reference |
| `work-pool-entry-not-found` | Work unit not found in this pool | releasing an unknown entry |
| `idempotency-key-required` | Idempotency-Key header is required | `POST /paths/{pathId}/work-units` without an `Idempotency-Key` header ([ADR-0022](../adr/0022-idempotency-key-middleware.md)) |

### 422 Unprocessable Entity

| `type` suffix | `title` | Raised by |
|---|---|---|
| `idempotency-key-reused` | Idempotency-Key was already used with a different request | `POST /paths/{pathId}/work-units` re-using an `Idempotency-Key` with a different body — use a new key for a genuinely different request |

### 404 Not Found

| `type` suffix | `title` | Raised by |
|---|---|---|
| `not-found` | Resource not found | no aggregate or projection exists for that key |

Both read-model endpoints return `404` when nothing has been observed yet —
deliberately, rather than a zero-valued body. "No observation" and "observed
zero" are different facts.

### 409 Conflict — the request is well-formed but the state forbids it

| `type` suffix | `title` | Invariant |
|---|---|---|
| **`wip-limit-reached`** | Release-fed pool WIP limit reached | **WIP-limit backpressure** |
| `work-pool-entry-already-released` | Work pool entry already released | at-most-once handout |
| `work-unit-already-enqueued` | Work unit already enqueued in this pool | no duplicate entries |
| `work-pool-empty` | Work pool is empty | nothing pending to release |
| **`work-unit-already-released`** | Work unit already released | **at most one active assignment** |
| **`work-unit-already-completed`** | Work unit already completed | **no double-complete** |
| `work-unit-not-released` | Work unit not released | must be released before completing |
| `concurrent-modification` | The work pool was modified concurrently; retry the request | optimistic-concurrency retry budget (12 attempts) exhausted on a `WorkPool` save ([ADR-0029](../adr/0029-work-pool-optimistic-concurrency.md)) |

`409` is the right code here because these are **state conflicts**, not input
errors: the same request would have succeeded a moment earlier, and retrying it
unchanged will not help — except `concurrent-modification`, which is the one
409 a client should simply retry.

### 500 Internal Server Error

| `type` suffix | `title` |
|---|---|
| `internal-error` | Internal server error |

The fallback for anything unmapped — an adapter failure, not a domain outcome.
Every domain error the service can raise has an explicit mapping above, so a
`500` here genuinely means infrastructure.

## How the mapping is structured

Two pure functions in `internal/adapters/inbound/http/errors.go`:

- `statusFor(err) int` — sentinel error → HTTP status
- `problemFor(err) (typeURI, title)` — sentinel error → RFC 7807 identity

They mirror each other case for case: every sentinel `statusFor` recognises has
a corresponding case in `problemFor`, so no error can get a correct status with
a generic problem type. Both use `errors.Is`, so wrapped errors keep their
mapping. This includes the two `Idempotency-Key` problems raised by the
route-scoped middleware (`errIdempotencyKeyRequired` → 400,
`errIdempotencyKeyReused` → 422): the middleware calls `writeError` rather
than building problem bodies inline.

Keeping this in the **adapter** is the point. The domain returns typed sentinel
errors and knows nothing about HTTP; a second inbound adapter (the Kafka
consumer) handles the same errors completely differently, without either
adapter's policy leaking into the other.
