# Makefile — local quality gate for wes-work-planning.
#
# These targets mirror the sensors in .github/workflows/ci.yml so the same
# feedback is available BEFORE a commit leaves the machine ("keep quality
# left"). `make check` is the fast self-correction loop; `make check-all` is
# the fuller pre-push gate.

GOLANGCI_LINT_VERSION := v2.13.1
GOLANGCI_LINT_INSTALL := go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
GREMLINS_VERSION      := v0.6.0
GREMLINS_INSTALL      := go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)
GOVULNCHECK_INSTALL   := go install golang.org/x/vuln/cmd/govulncheck@latest

# Coverage gate — same profile, -coverpkg set and threshold as the CI `test` job.
COVERPKG          := ./internal/domain/...,./internal/application/...
COVERAGE_THRESHOLD := 90

# Mutation testing — MUTATION_PKG is the fast, blocking subset run by the CI
# `mutation-fast` job; MUTATION_ALL_PKG is the exhaustive, scheduled `mutation`
# job. Thresholds/workers/timeout-coefficient live in .gremlins.yaml.
MUTATION_PKG     := ./internal/domain/release
MUTATION_ALL_PKG := ./internal/domain

.DEFAULT_GOAL := help

.PHONY: help build vet fmt fmt-check lint test coverage integration bdd contract arch-test mutation mutation-all vuln check check-all

help: ## Print the available targets
	@echo "wes-work-planning — make targets:"
	@echo "  help         Print this help"
	@echo "  build        go build ./..."
	@echo "  vet          go vet ./..."
	@echo "  fmt          gofmt -w . (formats in place)"
	@echo "  fmt-check    Fail if gofmt -l . is non-empty (CI-style check)"
	@echo "  lint         golangci-lint run ./... (pinned $(GOLANGCI_LINT_VERSION))"
	@echo "  test         go test ./... -race (unit + httptest + bdd layers)"
	@echo "  coverage     Coverage profile + $(COVERAGE_THRESHOLD)% gate (same command as CI)"
	@echo "  integration  go test -tags=integration ./... -race -count=1 (NEEDS DATABASE_URL / a running Postgres)"
	@echo "  bdd          go test ./... -run TestFeatures -v (godog acceptance tests)"
	@echo "  contract     scripts/contract-test.sh — Schemathesis vs apis/openapi.yaml"
	@echo "               (boots the service in-memory; needs st: pip install"
	@echo "                'schemathesis==4.28.0')"
	@echo "  arch-test    go test ./internal/architecture/... -v (hexagonal fitness tests)"
	@echo "  mutation     gremlins on $(MUTATION_PKG) — the fast blocking subset CI runs"
	@echo "  mutation-all gremlins on $(MUTATION_ALL_PKG) — the exhaustive scheduled run (slow)"
	@echo "  vuln         govulncheck ./... (supply-chain / CVE sensor)"
	@echo "  check        FAST pre-commit bundle: fmt-check vet build lint test"
	@echo "  check-all    check + coverage arch-test bdd (pre-push gate; no DB needed)"

build: ## go build ./...
	go build ./...

vet: ## go vet ./...
	go vet ./...

fmt: ## gofmt -w .
	gofmt -w .

fmt-check: ## Fail if any file is not gofmt-clean
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt found unformatted files:"; \
		echo "$$unformatted"; \
		echo "run 'make fmt' to fix"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

lint: ## golangci-lint run ./...
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  $(GOLANGCI_LINT_INSTALL)"; \
		exit 1; \
	fi
	golangci-lint run ./...

test: ## go test ./... -race
	go test ./... -race

coverage: ## Coverage profile + gate (mirrors the CI `test` job)
	go test ./... -race -coverprofile=coverage.out -coverpkg=$(COVERPKG)
	@COVERAGE=$$(go tool cover -func=coverage.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "Coverage: $${COVERAGE}% (gate: $(COVERAGE_THRESHOLD)%)"; \
	if awk -v c="$$COVERAGE" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { exit !(c < t) }'; then \
		echo "coverage $${COVERAGE}% is below the $(COVERAGE_THRESHOLD)% gate"; \
		exit 1; \
	fi

integration: ## Integration tests — REQUIRES DATABASE_URL and a running Postgres
	@if [ -z "$$DATABASE_URL" ]; then \
		echo "warning: DATABASE_URL is not set — the integration tests will skip."; \
		echo "start Postgres with 'docker compose up -d' and export DATABASE_URL first."; \
	fi
	go test -tags=integration ./... -race -count=1

bdd: ## godog/Gherkin acceptance tests
	go test ./... -run TestFeatures -v

# Property-based contract tests against apis/openapi.yaml — mirrors the
# `contract` CI job. Not part of check/check-all (needs Python tooling
# installed); CI runs it as its own job on every push and PR.
contract: ## Schemathesis contract tests (boots the service in-memory)
	./scripts/contract-test.sh

arch-test: ## Architecture fitness tests
	go test ./internal/architecture/... -v

mutation: ## Fast blocking mutation subset (same as the CI mutation-fast job)
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed. install the version CI pins with:"; \
		echo "  $(GREMLINS_INSTALL)"; \
		exit 1; \
	fi
	gremlins unleash $(MUTATION_PKG)

mutation-all: ## Exhaustive mutation run over the whole domain (slow; scheduled in CI)
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed. install the version CI pins with:"; \
		echo "  $(GREMLINS_INSTALL)"; \
		exit 1; \
	fi
	gremlins unleash $(MUTATION_ALL_PKG)

vuln: ## govulncheck ./... — known CVEs in deps and the Go stdlib
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "govulncheck is not installed. install it with:"; \
		echo "  $(GOVULNCHECK_INSTALL)"; \
		exit 1; \
	fi
	govulncheck ./...

check: fmt-check vet build lint test ## Fast pre-commit bundle
	@echo "make check: OK"

check-all: check coverage arch-test bdd ## Fuller pre-push gate
	@echo "make check-all: OK"
