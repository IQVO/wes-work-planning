---
id: 0007-arch-go-fitness-tests
title: ADR-0007 — Executable architecture fitness tests with arch-go
sidebar_label: 0007 · Architecture fitness tests
sidebar_position: 7
description: Why the hexagonal dependency rule is asserted by a failing test rather than defended by code review.
---

# ADR-0007 — Enforce the dependency rule with executable fitness tests

## Status

**Accepted.** Added as a blocking `arch-test` CI job, and wired into the
`docker-publish` job's `needs` list.

## Context

[ADR-0001](./0001-hexagonal-ports-and-adapters.md) established the dependency
rule, and the repository's `CLAUDE.md` calls it **NON-NEGOTIABLE**. But it was
enforced only by convention and code review, and architecture rules defended
that way decay in a predictable order:

1. someone needs a value that happens to live one layer out;
2. the import is small, local and obviously fine in context;
3. review passes because the diff is three lines;
4. six months later the domain imports `pgx` and no one can say when.

The failure mode is that the *first* violation is always defensible. Nothing in
`go build` or `go vet` objects to a domain package importing a database driver —
it is a perfectly valid Go program. And the cost of the violation is not paid at
the moment it lands; it is paid much later, by whoever discovers that the domain
tests now need a database.

Java has ArchUnit for this. Go's equivalent is
[`arch-go`](https://github.com/arch-go/arch-go), which reads real package
dependency graphs and asserts rules over them.

## Decision

**We will encode the hexagonal dependency rule as executable Go tests in
`internal/architecture/architecture_test.go` using `github.com/arch-go/arch-go`,
and run them as a blocking CI job.**

Six assertions, five for layering and one for a codebase convention:

| Rule | Assertion |
|---|---|
| Domain is at the centre | `internal/domain/**` may **only** depend on `internal/domain/**` |
| Application depends inward only | `internal/application/**` may **only** depend on domain and application |
| Adapters do not know each other | `internal/adapters/inbound/**` must **not** depend on `internal/adapters/outbound/**` |
| …and the reverse | `internal/adapters/outbound/**` must **not** depend on `internal/adapters/inbound/**` |
| Only `cmd/` wires | nothing under `internal/**` may depend on `cmd/**` |
| Ports are pure contracts | `internal/application/ports/**` must contain **no structs, functions or methods** — interfaces only |

The last one is not a layering rule; it guards a subtler decay. A port that
grows a helper function or a struct has stopped being a contract and started
being an implementation living in the application layer, and that is much
easier to miss in review than a bad import.

The rules are written as **subtests** so a failure names precisely which
boundary broke, and the helper reports the offending package and detail rather
than a bare pass/fail.

## Consequences

### Easier

- **The rule is now a build failure, not an opinion.** A domain package
  importing `pgx` fails `arch-test` in CI with the offending package named. No
  reviewer has to spot it.
- **New contributors get the rule enforced, not explained.** The feedback
  arrives from the tool, immediately, in the terms of the rule itself.
- **`docker-publish` is gated on it.** Alongside `lint`, `test`, `integration`,
  `api-lint`, `docs-api-drift` and the other `needs:` jobs, so a violating build
  never reaches Docker Hub. (`helm-lint` is *not* in that list: it runs only
  on pull requests targeting `main`, so it is skipped on the `push` to `main`
  event that triggers `docker-publish`, and a skipped `needs:` job would skip
  the publish too. The chart is validated pre-merge on the PR to `main`
  instead.)
- **The tests document the architecture executably.** Reading
  `architecture_test.go` gives the dependency rule in six lines of intent, and
  that description cannot go stale.
- **It cost nothing to adopt.** All five layering subtests passed on the first
  run — the codebase had zero existing violations, confirming the convention had
  held to that point. The value is in keeping it that way.

### Harder

- **Another CI job and another dependency.** `arch-go` is a test-only dependency
  in `go.mod`, but it is one more thing to keep current.
- **Glob semantics need care.** `arch-go` patterns use `.` as the segment
  separator and translate to permissive regexes, so a pattern can match more
  than it first appears to. Rules must be written and then *verified to actually
  fail* on a deliberate violation, or you get a test that passes vacuously.
- **Test files are not analysed.** `arch-go` loads packages with `Tests: false`,
  so a cross-layer import inside a `_test.go` file is invisible to these rules.
  Acceptable — test wiring across layers is often legitimate — but it is a real
  blind spot, and a violation introduced in test code will not be caught.
- **Only structure is enforced, not meaning.** These tests would happily pass a
  build that wired Workforce's `ShiftPlanCommitted` straight into
  `CommitShiftPlan` — the semantic boundary from
  [ADR-0006](./0006-labor-plan-view-not-shift-plan.md) is still defended by
  convention alone. Fitness tests raise the floor; they do not replace design
  review.
- **False confidence is a risk.** "The architecture tests pass" is easy to hear
  as "the architecture is good". It means the dependency graph is clean, which
  is a much smaller claim.
