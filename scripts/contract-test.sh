#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it with its
# in-memory adapters on a loopback port, waits for /healthz, generates
# valid AND invalid requests for every operation, and asserts the
# responses conform to the spec (status codes, content types, response
# schemas; positive data accepted, negative data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
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

cd "$(dirname "$0")/.."
BIN="$(mktemp -d)/wes"
go build -o "$BIN" ./cmd/wes

# cmd/wes refuses to boot without a process-path catalogue (ADR-0012:
# a boot-time invariant, never a silent partial/empty catalogue). CI has
# no warehouse-infra checkout, so bake the sortable-fc topology — the
# one building this fleet runs, and the same families apis/openapi.yaml's
# PathId pattern documents — into a throwaway catalogue file.
CATALOGUE="$(mktemp -d)/process-paths.yaml"
cat > "$CATALOGUE" <<'EOF'
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

# No DATABASE_URL (in-memory adapters), no KAFKA_BROKERS (no consumer),
# EVENT_PUBLISHER=log, permissive classification/travel lookups: the
# service serves its OWN REST API standalone, with no sibling calls.
HTTP_ADDR="127.0.0.1:${PORT}" \
  PATH_CATALOGUE_FILE="$CATALOGUE" \
  "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for the server to report healthy (up to ~10s).
for _ in $(seq 1 50); do
  if curl -sf "${BASE_URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/healthz" >/dev/null # fail loudly if it never came up

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
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --exclude-operation-id commitShiftPlan
