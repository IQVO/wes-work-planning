#!/usr/bin/env bash
# Chart render assertions for charts/wes-work-planning (run in CI by the
# chart-tests job, next to test_service_selectors.py).
#
# Postgres and Kafka are the only runtime (ADR-0044): the chart refuses to
# render without a database source, always renders KAFKA_BROKERS and one
# consumer group per Kafka consumer, never renders a permissive/off/log mode,
# and still renders with the values warehouse-infra really passes
# (tests/warehouse-infra-*.yaml, copied from warehouse-infra origin/develop).
#
# Run: bash charts/wes-work-planning/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$chart_dir/../.." && pwd)"
release=wes-work-planning
# Dummy DSN, no password: the chart refuses to render without a database source.
base=(--set database.url=postgres://u@example.invalid:5432/db)
everything=(--set analytics.enabled=true
  --set analytics.database.projectorUrl=postgres://projector@example.invalid:5432/a
  --set mcp.enabled=true)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() { helm template "$release" "$chart_dir" "${base[@]}" "$@"; }

# env_value DOC NAME: the value of the first `- name: NAME` entry in DOC.
env_value() {
  awk -v n="$2" '$0 ~ "- name: " n "$" {found=1; next} found && /value:/ {gsub(/^ *value: *"?|"?$/, ""); print; exit} found {exit}' <<<"$1"
}

api_deployment="$(render --show-only templates/deployment.yaml)"
configmap="$(render --show-only templates/configmap.yaml)"
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
tuned="$(render --set config.outboxMaxAttempts=5 --set config.eventPublisher=kafka --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"
grep -q 'EVENT_PUBLISHER: "kafka"' <<<"$tuned" || fail "config.eventPublisher is not rendered into the ConfigMap"
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 (ADR-0039)"
grep -q 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" || fail "api Deployment must set OUTBOX_MAX_ATTEMPTS explicitly too"

# ADR-0040: the RELOCATE work CPT horizon (approved_at + horizon) reaches
# cmd/wes through the same ConfigMap, defaulting to 24h.
grep -q 'RELOCATION_CPT_HORIZON: "24h"' <<<"$configmap" || fail "RELOCATION_CPT_HORIZON must default to 24h (ADR-0040)"
horizon="$(render --set config.relocationCptHorizon=6h --show-only templates/configmap.yaml)"
grep -q 'RELOCATION_CPT_HORIZON: "6h"' <<<"$horizon" || fail "config.relocationCptHorizon is not rendered into the ConfigMap"

# The other workloads read their own explicit env and must NOT inherit the
# api's HTTP_ADDR (reports would bind :8080) or anything else from it.
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml; do
  rendered="$(render "${everything[@]}" --show-only "templates/$template")"
  if grep -q 'configMapRef:' <<<"$rendered"; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# --- real Postgres and real Kafka only (ADR-0044) -----------------------
all_docs="$(render "${everything[@]}")"
notes="$(render "${everything[@]}" --show-only templates/NOTES.txt 2>/dev/null || true)"

[[ "$(env_value "$api_deployment" EVENT_PUBLISHER)" == "kafka" ]] || fail "EVENT_PUBLISHER must default to kafka"
grep -q '^  EVENT_PUBLISHER: "kafka"' <<<"$configmap" || fail "the ConfigMap must carry EVENT_PUBLISHER=kafka"
grep -q '^  KAFKA_BROKERS: "kafka:9092"' <<<"$configmap" || fail "the ConfigMap must always carry KAFKA_BROKERS"
[[ "$(env_value "$api_deployment" KAFKA_BROKERS)" == "kafka:9092" ]] || fail "KAFKA_BROKERS is not rendered from kafka.brokers"
[[ "$(env_value "$api_deployment" KAFKA_CONSUMER_GROUP)" == "wes-work-planning" ]] \
  || fail "KAFKA_CONSUMER_GROUP must default to the group the binary used before (wes-work-planning)"
[[ "$(env_value "$api_deployment" PRODUCT_CLASSIFICATION_CONSUMER_GROUP)" == "wes-work-planning-product-classification" ]] \
  || fail "PRODUCT_CLASSIFICATION_CONSUMER_GROUP is not rendered by default"
[[ "$(env_value "$api_deployment" REPLENISHMENT_CONSUMER_GROUP)" == "wes-work-planning-replenishment" ]] \
  || fail "REPLENISHMENT_CONSUMER_GROUP is not rendered by default"
[[ "$(env_value "$api_deployment" PACK_VIEW_CONSUMER_GROUP)" == "wes-work-planning-pack-view" ]] \
  || fail "PACK_VIEW_CONSUMER_GROUP is not rendered by default"
grep -A4 'name: DATABASE_URL$' <<<"$api_deployment" | grep -q 'secretKeyRef:' || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
if grep -qE '^  (MIGRATIONS_)?DATABASE_URL:' <<<"$configmap"; then fail "a DSN must never land in the ConfigMap"; fi

tuned_groups="$(render --set config.consumerGroup=g1 --set config.productClassificationConsumerGroup=g2 \
  --set config.replenishmentConsumerGroup=g3 --set config.packViewConsumerGroup=g4 --show-only templates/deployment.yaml)"
[[ "$(env_value "$tuned_groups" KAFKA_CONSUMER_GROUP)" == "g1" ]] || fail "config.consumerGroup is not rendered"
[[ "$(env_value "$tuned_groups" PRODUCT_CLASSIFICATION_CONSUMER_GROUP)" == "g2" ]] || fail "config.productClassificationConsumerGroup is not rendered"
[[ "$(env_value "$tuned_groups" REPLENISHMENT_CONSUMER_GROUP)" == "g3" ]] || fail "config.replenishmentConsumerGroup is not rendered"
[[ "$(env_value "$tuned_groups" PACK_VIEW_CONSUMER_GROUP)" == "g4" ]] || fail "config.packViewConsumerGroup is not rendered"

# No permissive / off / log value and no in-memory mention anywhere.
if grep -qiE 'value: "?(permissive|off|log)"?$' <<<"$all_docs"; then fail "the render carries permissive/off/log, which the binaries reject at boot"; fi
if grep -q 'DATABASE_URL: ""' <<<"$all_docs"; then fail "an empty DATABASE_URL must never be rendered"; fi
if grep -qi 'in-memory' <<<"$all_docs"$'\n'"$notes"; then fail "the render mentions an in-memory mode"; fi

# Every variable a binary reports as "<VAR> is required" is rendered into the
# Deployment that runs that binary, under both the chart defaults and the
# warehouse-infra values.
infra=(--values "$chart_dir/tests/warehouse-infra-platform.yaml"
  --values "$chart_dir/tests/warehouse-infra-values.yaml"
  --values "$chart_dir/tests/warehouse-infra-kind-resources.yaml")
infra_all="$(helm template "$release" "$chart_dir" "${infra[@]}")" || fail "the warehouse-infra values no longer render"
for pair in "wes:deployment.yaml" "mcp:mcp-deployment.yaml" "wes-projector:projector-deployment.yaml" "wes-reports:reports-deployment.yaml"; do
  bin="${pair%%:*}"
  template="${pair##*:}"
  # shellcheck disable=SC2046
  vars="$(grep -ho '"[A-Z][A-Z_]* is required' $(ls "$repo_dir"/cmd/"$bin"/*.go | grep -v '_test\.go$') | sed -E 's/^"//; s/ is required//' | sort -u)"
  [[ -n "$vars" ]] || fail "found no mandatory variables in cmd/$bin; the scan is looking in the wrong place"
  for source in default infra; do
    if [[ "$source" == default ]]; then
      rendered="$(render "${everything[@]}" --show-only "templates/$template")"
    else
      rendered="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only "templates/$template")"
    fi
    for var in $vars; do
      grep -q "name: $var\$" <<<"$rendered" || fail "the $bin workload ($source values) does not render $var, which cmd/$bin requires"
    done
  done
done

# warehouse-infra values: Kafka from the platform file, the DSN from the
# existing secret, every consumer group once (a duplicate env entry breaks
# ArgoCD's diff), the legacy kafka.enabled: true tolerated.
infra_api="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/deployment.yaml)"
[[ "$(env_value "$infra_api" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: KAFKA_BROKERS"
[[ "$(env_value "$infra_api" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: EVENT_PUBLISHER"
[[ "$(env_value "$infra_api" KAFKA_CONSUMER_GROUP)" == "wes-work-planning" ]] || fail "infra values: KAFKA_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" PRODUCT_CLASSIFICATION_CONSUMER_GROUP)" == "wes-work-planning-product-classification" ]] || fail "infra values: PRODUCT_CLASSIFICATION_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" REPLENISHMENT_CONSUMER_GROUP)" == "wes-work-planning-replenishment" ]] || fail "infra values: REPLENISHMENT_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" REPLENISHMENT_MODE)" == "kafka" ]] || fail "infra values: REPLENISHMENT_MODE"
grep -q 'wes-work-planning-db' <<<"$infra_api" || fail "infra values: DATABASE_URL must come from the wes-work-planning-db secret"
dups="$(grep -E '^ {12}- name: ' <<<"$infra_api" | sort | uniq -d)"
[[ -z "$dups" ]] || fail "infra values: duplicate env entries in the api Deployment: $dups"
infra_proj="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/projector-deployment.yaml)"
[[ "$(env_value "$infra_proj" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: projector KAFKA_BROKERS"
infra_mcp="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/mcp-deployment.yaml)"
[[ "$(env_value "$infra_mcp" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: mcp EVENT_PUBLISHER"
[[ "$(env_value "$infra_mcp" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: mcp KAFKA_BROKERS"
for component in api mcp analytics-projector analytics-reports frontend; do
  grep -q "app.kubernetes.io/component: $component" <<<"$infra_all" || fail "infra values: component $component not rendered"
done

# The REST sync-edge *_MODE variables are out of scope and keep working.
[[ "$(env_value "$infra_api" TRAVEL_DISTANCE_MODE)" == "http" ]] || fail "infra values: TRAVEL_DISTANCE_MODE=http must still render"
travel="$(render --set 'extraEnv[0].name=TRAVEL_DISTANCE_MODE' --set 'extraEnv[0].value=permissive' --show-only templates/deployment.yaml)"
[[ "$(env_value "$travel" TRAVEL_DISTANCE_MODE)" == "permissive" ]] || fail "TRAVEL_DISTANCE_MODE=permissive (REST sync edge, out of scope) must keep working"

# The chart's own readinessProbe is /readyz (a real dependency check).
grep -A3 'readinessProbe:' <<<"$api_deployment" | grep -A2 'httpGet:' | grep -q 'path: /readyz' || fail "readinessProbe must hit /readyz"

# --- the chart refuses to render without its mandatory inputs -----------
expect_refusal() {
  local why="$1" needle="$2"
  shift 2
  local err
  if err="$(helm template "$release" "$chart_dir" "$@" 2>&1 >/dev/null)"; then
    fail "the chart must refuse to render: $why"
  fi
  grep -qF -- "$needle" <<<"$err" || fail "the refusal for '$why' does not name '$needle': $err"
}
expect_refusal "no database source" "database.url or database.existingSecret"
expect_refusal "mcp without a database" "database.url or database.existingSecret" --set mcp.enabled=true
expect_refusal "empty kafka.brokers" "kafka.brokers is required" "${base[@]}" --set kafka.brokers=
expect_refusal "kafka.enabled=false" "kafka.enabled=false is not supported" "${base[@]}" --set kafka.enabled=false
expect_refusal "empty consumer group" "config.consumerGroup is required" "${base[@]}" --set config.consumerGroup=
expect_refusal "empty classification group" "config.productClassificationConsumerGroup is required" "${base[@]}" --set config.productClassificationConsumerGroup=
expect_refusal "empty pack view group" "config.packViewConsumerGroup is required" "${base[@]}" --set config.packViewConsumerGroup=
expect_refusal "empty replenishment group" "config.replenishmentConsumerGroup is required" "${base[@]}" --set config.replenishmentConsumerGroup=
expect_refusal "log publisher" "config.eventPublisher" "${base[@]}" --set config.eventPublisher=log
expect_refusal "log publisher on mcp" "mcp.eventPublisher" "${base[@]}" --set mcp.enabled=true --set mcp.eventPublisher=log
expect_refusal "EVENT_PUBLISHER=log in extraEnv" "EVENT_PUBLISHER" "${base[@]}" --set 'extraEnv[0].name=EVENT_PUBLISHER' --set 'extraEnv[0].value=log'
expect_refusal "REPLENISHMENT_MODE=permissive in extraEnv" "REPLENISHMENT_MODE" "${base[@]}" --set 'extraEnv[0].name=REPLENISHMENT_MODE' --set 'extraEnv[0].value=permissive'
expect_refusal "PRODUCT_CLASSIFICATION_MODE=off in extraEnv" "PRODUCT_CLASSIFICATION_MODE" "${base[@]}" --set 'extraEnv[0].name=PRODUCT_CLASSIFICATION_MODE' --set 'extraEnv[0].value=off'
expect_refusal "PRODUCT_CLASSIFICATION_MODE=permissive in mcp.extraEnv" "mcp.extraEnv" "${base[@]}" --set mcp.enabled=true \
  --set 'mcp.extraEnv[0].name=PRODUCT_CLASSIFICATION_MODE' --set 'mcp.extraEnv[0].value=permissive'
expect_refusal "grace period under 20" "terminationGracePeriodSeconds" "${base[@]}" --set terminationGracePeriodSeconds=10

# A legacy kafka.enabled: true is tolerated.
render --set kafka.enabled=true >/dev/null || fail "kafka.enabled=true (legacy) must still render"

echo "PASS: wes-work-planning chart render assertions"
