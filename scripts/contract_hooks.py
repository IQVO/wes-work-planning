"""Schemathesis hooks for scripts/contract-test.sh.

The resource-creation POST of this service (POST /paths/{pathId}/work-units)
sits behind the Idempotency-Key middleware (ADR-0022), which is enforced in
every deployment now that Postgres is the only runtime (ADR-0044). The OpenAPI
document gives the parameter no per-request value, so Schemathesis would send
the SAME key with DIFFERENT bodies and rightly get 422
`idempotency-key-reused`. A real client sends a fresh UUID per logical submit;
this hook does the same for every request that carries the header. A request
that deliberately omits the header (negative testing of the required
parameter) is left alone and must still be answered 400.

Second hook: Schemathesis 4.28 re-runs a fuzzing example that was generated as
a "Positive test case" with its generation mode flipped to NEGATIVE (the
mutation shrank to nothing, so the data is still schema-valid: a path that
matches the PathId pattern, a date-time that jsonschema and jsonschema_rs both
accept). Against an EMPTY Postgres the first such request creates the work
unit (201, correct for valid data) and `negative_data_rejection` then reports
"API accepted schema-violating request / Invalid component: positive test
case"; against a populated database the same request is a 409 and passes, so
the failure depended on the state the run started from. Cases whose own phase
description says they are positive are put back in POSITIVE mode before the
call; real negative cases (any other description) are untouched.
"""

import uuid

import schemathesis
from schemathesis.generation.modes import GenerationMode

_POSITIVE_DESCRIPTION = "Positive test case"


@schemathesis.hook
def before_call(ctx, case, **kwargs):
    if case.headers and "Idempotency-Key" in case.headers:
        case.headers["Idempotency-Key"] = str(uuid.uuid4())
    meta = case.meta
    if (
        meta is not None
        and meta.generation.mode.is_negative
        and getattr(meta.phase.data, "description", None) == _POSITIVE_DESCRIPTION
    ):
        meta.generation.mode = GenerationMode.POSITIVE
    return None
