#!/usr/bin/env bash
# Chart render assertions for charts/wes-work-planning (run in CI by the
# chart-tests job, next to test_service_selectors.py).
#
# Run: bash charts/wes-work-planning/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=wes-work-planning

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

api_deployment="$(helm template "$release" "$chart_dir" --show-only templates/deployment.yaml)"
configmap="$(helm template "$release" "$chart_dir" --show-only templates/configmap.yaml)"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"
[[ -n "$configmap_name" ]] || fail "could not read the ConfigMap name"

# D14 (2026-10-12): every config.* value is rendered into the chart env
# ConfigMap, and the api Deployment -- the only workload whose binary
# (cmd/wes) reads those keys (HTTP_ADDR, EVENT_PUBLISHER, KAFKA_BROKERS,
# OUTBOX_MAX_ATTEMPTS) -- must mount it with envFrom, or a value that is only
# in the ConfigMap silently does nothing. The path-catalogue ConfigMap is a
# separate volume and does not count.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the env ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# A tuned config.* value reaches the ConfigMap the api mounts, and the
# outbox relay cap defaults to the fleet's 20 (D12, ADR-0039).
tuned="$(helm template "$release" "$chart_dir" --set config.outboxMaxAttempts=5 --set config.eventPublisher=kafka --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"
grep -q 'EVENT_PUBLISHER: "kafka"' <<<"$tuned" || fail "config.eventPublisher is not rendered into the ConfigMap"
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 (ADR-0039)"
grep -q 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" || fail "api Deployment must set OUTBOX_MAX_ATTEMPTS explicitly too"

# The other workloads read their own explicit env and must NOT inherit the
# api's HTTP_ADDR (reports would bind :8080) or anything else from it.
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml; do
  rendered="$(helm template "$release" "$chart_dir" --set analytics.enabled=true --set mcp.enabled=true \
    --set analytics.database.projectorUrl=postgres://projector --show-only "templates/$template")"
  if grep -q 'configMapRef:' <<<"$rendered"; then
    fail "$template must not mount the api ConfigMap"
  fi
done

echo "PASS: wes-work-planning chart render assertions"
