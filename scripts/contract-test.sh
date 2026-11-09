#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it against the
# Postgres named by DATABASE_URL and the Kafka broker named by KAFKA_BROKERS
# on a loopback port, waits for /readyz, generates valid AND invalid requests
# for every operation, and asserts the responses conform to the spec (status
# codes, content types, response schemas; positive data accepted, negative
# data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
#
# This service has no in-memory mode (ADR-0044): cmd/wes requires DATABASE_URL
# (it migrates and serves from Postgres), KAFKA_BROKERS (the outbox relay, the
# consumers and /readyz) and one consumer group per Kafka consumer. CI
# provides a postgres:16 and a Kafka service container; for a local run,
# start disposable ones first (the broker must advertise the address you pass
# in KAFKA_BROKERS):
#
#   docker run -d --rm --name wes-contract-pg -p 18098:5432 \
#     -e POSTGRES_USER=wes -e POSTGRES_HOST_AUTH_METHOD=trust \
#     -e POSTGRES_DB=wes_work_planning postgres:16
#   docker run -d --rm --name wes-contract-kafka -p 19098:9092 \
#     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19098 \
#     confluentinc/confluent-local:7.6.1
#   export DATABASE_URL='postgres://wes@localhost:18098/wes_work_planning?sslmode=disable'
#   export KAFKA_BROKERS='localhost:19098'
#
# (user and database names mirror the CI `contract` job's postgres service;
# the disposable container trusts local connections.)
#
# The old in-memory boot started EMPTY (no seed data: the repositories held
# nothing, the process-path catalogue came from a file), and so does this one:
# the migrations create the schema and insert no rows, so Schemathesis still
# starts from an empty store and creates what it needs itself. The service
# still refuses to boot without a process-path catalogue file (cmd/wes loads
# it as a boot-time invariant, ADR-0012), so the script writes the sortable-fc
# topology into its temp dir and points PATH_CATALOGUE_FILE at it. The
# consumed topics do not exist on a fresh broker; the broker auto-creates them
# like the cluster does and the consumers simply idle.
#
# Requires `st` on PATH:
#   python3 -m pip install --user 'schemathesis==4.28.0'
set -euo pipefail

SCHEMATHESIS_VERSION="4.28.0"
PORT="${CONTRACT_PORT:-18086}"
BASE_URL="http://127.0.0.1:${PORT}"
MAX_EXAMPLES="${CONTRACT_MAX_EXAMPLES:-100}"

if ! command -v st >/dev/null 2>&1; then
  echo "schemathesis (st) is not installed (or not on PATH)."
  echo "Install the exact version CI pins:"
  echo "  python3 -m pip install --user 'schemathesis==${SCHEMATHESIS_VERSION}'"
  exit 1
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set — this service has no in-memory mode"
  echo "(cmd/wes requires Postgres and applies migrations on boot)."
  echo "Start a disposable one and export DATABASE_URL, e.g.:"
  echo "  docker run -d --rm --name wes-contract-pg -p 18098:5432 \\"
  echo "    -e POSTGRES_USER=wes -e POSTGRES_HOST_AUTH_METHOD=trust \\"
  echo "    -e POSTGRES_DB=wes_work_planning postgres:16"
  echo "  export DATABASE_URL='postgres://wes@localhost:18098/wes_work_planning?sslmode=disable'"
  exit 1
fi

if [ -z "${KAFKA_BROKERS:-}" ]; then
  echo "KAFKA_BROKERS is not set — this service has no log sink and no optional relay"
  echo "(cmd/wes requires a real Kafka broker; /readyz depends on it)."
  echo "Start a disposable one and export KAFKA_BROKERS, e.g.:"
  echo "  docker run -d --rm --name wes-contract-kafka -p 19098:9092 \\"
  echo "    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19098 \\"
  echo "    confluentinc/confluent-local:7.6.1"
  echo "  export KAFKA_BROKERS='localhost:19098'"
  exit 1
fi

cd "$(dirname "$0")/.."
TMP="$(mktemp -d)"
BIN="${TMP}/wes"
go build -o "$BIN" ./cmd/wes

# cmd/wes refuses to boot without a process-path catalogue (ADR-0012:
# a boot-time invariant, never a silent partial/empty catalogue). CI has
# no warehouse-infra checkout, so bake the sortable-fc topology — the
# one building this fleet runs, and the same families apis/openapi.yaml's
# PathId pattern documents — into a throwaway catalogue file.
cat > "${TMP}/process-paths.yaml" <<'EOF'
building: sortable-fc
paths:
  - id: PICK
    matchPrefix: pick
    direct: true
    requiredCapabilities: [pick]
  - id: PACK
    matchPrefix: pack
    direct: true
    requiredCapabilities: [pack]
  - id: REBIN
    matchPrefix: rebin
    direct: true
    requiredCapabilities: [rebin]
  - id: SLAM
    matchPrefix: slam
    direct: true
    requiredCapabilities: [slam]
EOF

# One stable consumer group per Kafka consumer is mandatory (a boot error
# without it); the contract run uses its own so it never joins a live group.
# TRAVEL_DISTANCE_MODE stays at its permissive default: it is the REST sync
# edge to facility-layout, not a database or a broker.
KAFKA_CONSUMER_GROUP="${KAFKA_CONSUMER_GROUP:-wes-work-planning-contract}" \
  PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${PRODUCT_CLASSIFICATION_CONSUMER_GROUP:-wes-work-planning-contract-product-classification}" \
  REPLENISHMENT_CONSUMER_GROUP="${REPLENISHMENT_CONSUMER_GROUP:-wes-work-planning-contract-replenishment}" \
  PACK_VIEW_CONSUMER_GROUP="${PACK_VIEW_CONSUMER_GROUP:-wes-work-planning-contract-pack-view}" \
  MIGRATIONS_PATH="${MIGRATIONS_PATH:-migrations}" \
  LOG_LEVEL="${LOG_LEVEL:-warn}" HTTP_ADDR="127.0.0.1:${PORT}" \
  PATH_CATALOGUE_FILE="${TMP}/process-paths.yaml" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true; rm -rf "${TMP}"' EXIT

# Wait for /readyz: it is 200 only while Postgres answers a ping AND the
# broker answers a metadata request (up to ~60s: migrations + broker).
for _ in $(seq 1 300); do
  if curl -sf "${BASE_URL}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/readyz" >/dev/null # fail loudly if it never became ready

# commitShiftPlan is excluded: its `plannedHeads <= installedStations`
# aggregate invariant is a cross-field constraint OpenAPI 3.0.3 cannot
# express, so Schemathesis's positive phase generates schema-valid bodies
# the endpoint correctly rejects with 400. The invariant IS tested — by
# the BDD scenario "Committing a shift plan that exceeds installed
# stations is rejected" (features/shift_plan.feature), the domain table
# test TestNewPathPlan, and the httptest
# TestPostShiftPlan_HeadsExceedStationsReturns400. This exclusion only
# stops Schemathesis generating the unexpressible-but-invalid
# combinations.
#
# Idempotency-Key (ADR-0022, enforced in every deployment now): the hooks
# file gives every request that carries the header a fresh UUID, like a real
# client sending one per logical submit (see scripts/contract_hooks.py).
#
# --generation-allow-x00 false: the schemas cannot say "no U+0000", and
# PostgreSQL text cannot hold one. The service answers a NUL in a body with
# 400 and in a path with 404 (internal/adapters/inbound/http/nul.go, pinned by
# TestRejectNUL and TestNUL_IsAClientErrorAndDoesNotConsumeTheKey), which
# Schemathesis would report as "API rejected schema-compliant request".
export SCHEMATHESIS_HOOKS="scripts/contract_hooks.py"
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --generation-allow-x00 false \
  --exclude-operation-id commitShiftPlan \
  "$@"
