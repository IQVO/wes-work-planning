---
name: how-to-test
description: Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job: the four test layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Use when writing or reviewing tests in this repo, or diagnosing a failing
`coverage`/`mutation`/`bdd`/`integration`/`arch-test` CI job. This fleet's
quality bar is layered — passing `go test` is necessary but is the WEAKEST
signal of the four; mutation testing exists specifically because green
tests can assert nothing.

## The four layers, in order of what they actually prove

1. **Unit tests** (`make test` / `go test ./... -race`) — prove the code
   runs without panicking and returns SOMETHING. Table-driven, in-memory
   adapters only (`internal/adapters/outbound/memory/`), never a real
   network/DB call.
2. **Coverage** (`make coverage`, 90% gate on
   `./internal/domain/...,./internal/application/...` — see `COVERPKG`/
   `COVERAGE_THRESHOLD` in this repo's `Makefile`) — proves lines executed.
   Proves nothing about whether the test asserted the right thing.
3. **Mutation testing** (`make mutation`, gremlins on
   `./internal/domain/release`; `make mutation-all` for the full
   `./internal/domain` — the exhaustive, scheduled job) — proves the tests
   actually ASSERT, not merely execute. A mutant is a deliberately broken
   version of the code (`<` -> `<=`, `+` -> `-`, etc.); if the test suite
   still passes against the mutant, it "survived" (LIVED) — meaning no
   test would catch that exact bug in production. This is the sensor most
   worth understanding deeply; the pitfalls below are all about it.
4. **Behaviour / architecture fitness** (`make bdd`, `make arch-test`) —
   `arch-test` runs `internal/architecture/...`'s hexagonal + Kafka-safety
   fitness tests (layering, ports-are-interfaces, no-hardcoded-consumer-
   group, testcontainers-required — see below); `bdd` proves use cases
   work end-to-end through the real HTTP surface, not through a mocked
   port.

## Mutation testing: `<=` fails, not `>=`

This repo's `.gremlins.yaml` sets `threshold.efficacy: 99` and
`threshold.mutant-coverage: 99` — read its header comment for the exact
current baseline and when it was last re-measured. gremlins fails when the
MEASURED value is `<=` the threshold (verified empirically there: a run
measuring 100.00% against a threshold of 100 exits with "ERROR: below
efficacy-threshold"), so the threshold is deliberately set one point below
what's currently achieved — `99` locks in today's 100% and goes red the
moment a single mutant survives. When you deliberately lower coverage of a
package (rare, but happens when removing dead code), you may need to lower
the threshold in the SAME PR with a dated comment explaining why — never
silently; a future reader needs to know the drop was intentional, not a
regression that slipped through.

This repo's own `MUTATION.md` records its last full baseline (2026-09-13,
after ADR-0018): `./internal/domain` — 73 mutants, 73 killed, 0 lived,
100.00% efficacy, grown from 51 mutants before ADR-0018 added
`WorkPool.RemainingCapacity`/`PathCapacityChanged`. It also records the
exact fix for two REAL survived mutants, both `CONDITIONALS_BOUNDARY`
(`value < 0` mutated to `value <= 0`) in `shared/quantity.go` and
`shared/station_count.go`'s constructors — both survived because no
existing test constructed the zero-value case and asserted success. Fixed
by adding one boundary test each (`NewQuantity(0)`/`NewStationCount(0)`
succeed with `Value() == 0`), not by touching the source. Use this file as
the record's shape when triaging a new mutation run in this repo.

## Three real pitfalls that have each cost a real CI failure in this fleet

### 1. Zero/origin-value fixtures hide arithmetic mutants

A test built around zero-valued operands makes `a - b` and `a + b`
produce the same result, so a mutant flipping `-` to `+` survives even
though coverage looks complete. Any new value object with real arithmetic
(e.g. `WorkPool.RemainingCapacity`'s `wipLimit - WIP()`) needs fixture
values where EVERY operand and every delta is distinct and non-zero, and
the test must assert the exact expected value, not just "no error".

### 2. Boundary guards need the boundary value itself

A test for `if x < 0 { return err }` that only tries `-1` (clearly
invalid) and `42` (clearly valid) never exercises `0` — so a
`CONDITIONALS_BOUNDARY` mutant rewriting `<` to `<=` survives silently.
This repo's own `MUTATION.md` is the concrete proof: exactly this pattern
in `NewQuantity`/`NewStationCount` is what actually survived here. Every
`< 0`/`> 0`/`<= 0` guard needs an explicit test for the boundary value
itself.

### 3. Tie-break / near-equivalent mutants: know when NOT to chase them

A shortest-path relaxation or a priority-queue `Less` has a `<` -> `<=`
mutant that is undetectable by ANY test whose edge weights are all
distinct — the mutation only diverges on an exact tie. Do NOT force an
artificial tied-weight fixture just to kill this; that pins an arbitrary,
currently-unspecified tie-break order as if it were a real invariant,
which is worse than an accepted near-equivalent survivor. Document it in
this repo's `MUTATION.md` triage section instead, and move on.

## Diagnosing a `mutation`/`mutation-fast` CI failure: diff against develop, don't chase every LIVED line

```bash
gremlins unleash ./internal/domain/release --workers 1 --timeout-coefficient 30   # your branch
git stash && git checkout origin/develop -- . && gremlins unleash ./internal/domain/release --workers 1 --timeout-coefficient 30   # baseline
```

Note this repo's own `MUTATION.md` records that the default concurrent-
worker mode produced spurious `TIMED OUT` verdicts here (build-cache
contention between parallel `go test` invocations) — always run with
`--workers 1` and a generous `--timeout-coefficient`, mirroring the exact
flags `.gremlins.yaml`'s header comment documents CI using.

Only entries NEW on your branch are your regression. Confirm the survivor
SET is unchanged from `origin/develop` (not just that the percentage
cleared the `.gremlins.yaml` gate) — the real proof a fix didn't just get
lucky on the threshold.

## Kafka/Postgres integration tests: testcontainers, never a skip-gate

A `-tags=integration` test touching Kafka MUST start its own container via
`testcontainers-go/modules/kafka`. Never gate on
`os.Getenv("KAFKA_BROKERS")` + `t.Skip(...)`, and never hardcode
`localhost:9092`. This is enforced statically in this repo, not just by
convention: `TestKafkaIntegrationTestsUseTestcontainers` in
`internal/architecture/fitness_test.go` scans every `_integration_test.go`
file that constructs a real `kafkago.Writer{}`/`Reader{}`/`DialContext`/
`TopicConfig{}` (aliased `kafkago`, never the bare `kafka` package name in
this repo) or references `KAFKA_BROKERS`, and fails the build if that file
doesn't also import `testcontainers-go/modules/kafka`. See
`internal/adapters/outbound/kafkacatalog/consumer_integration_test.go`'s
`TestNewConsumer_TwoInstancesInARow_BothReplayFully` for the working
recipe: `tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", ...)`,
`testcontainers.CleanupContainer(t, container)`, a unique
timestamp-suffixed topic per test, and an explicit `createTopic` +
partition-leader poll before the first read/write.

Postgres-only integration tests (`//go:build integration`, e.g.
`internal/adapters/outbound/postgres/outbox_integration_test.go`) are
exempt even if they import `kafkago` — several import it only for the
`kafkago.Message` type on a `noopWriter` stub satisfying the production
`Writer` interface, never dialing a real broker. The fitness test
specifically requires evidence of real reader/writer/connection
construction, not a bare import-path match, to avoid a false positive
there.

## Verify before opening the PR

```bash
make check-all   # check + coverage + arch-test + bdd (the full local gate)
make mutation     # fast blocking mutation subset (internal/domain/release) — CI-enforced
make vuln         # govulncheck — run when touching go.mod
```

CI runs `mutation`/`vuln` even when `check-all` doesn't include them
locally — a PR can pass your local `check-all` and still go red in CI on
these otherwise.
