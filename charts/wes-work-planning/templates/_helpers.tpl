{{/*
Expand the name of the chart.
*/}}
{{- define "wes-work-planning.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "wes-work-planning.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "wes-work-planning.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "wes-work-planning.labels" -}}
helm.sh/chart: {{ include "wes-work-planning.chart" . }}
{{ include "wes-work-planning.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "wes-work-planning.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wes-work-planning.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "wes-work-planning.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "wes-work-planning.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL, when the chart creates its own.
*/}}
{{- define "wes-work-planning.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "wes-work-planning.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Postgres and Kafka are the only runtime (ADR-0044). The binaries exit at boot
without DATABASE_URL, KAFKA_BROKERS and a consumer group per Kafka consumer, so
the chart refuses to render without a source for each, naming the value.
*/}}
{{- define "wes-work-planning.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "database.url or database.existingSecret is required: DATABASE_URL is mandatory, this service runs only on a real Postgres (ADR-0044)" -}}
{{- end -}}
{{- end }}

{{/*
kafka.brokers is required (KAFKA_BROKERS) and kafka.enabled=false is refused.
`kafka.enabled: true` is tolerated: warehouse-infra's values still set it.
*/}}
{{- define "wes-work-planning.requireKafka" -}}
{{- if and (hasKey .Values.kafka "enabled") (not .Values.kafka.enabled) -}}
{{- fail "kafka.enabled=false is not supported: this service runs only on a real Kafka (ADR-0044); remove the value (kafka.enabled: true is still tolerated)" -}}
{{- end -}}
{{- $_ := required "kafka.brokers is required (KAFKA_BROKERS): every binary of this chart runs on a real Kafka (ADR-0044)" .Values.kafka.brokers -}}
{{- end }}

{{/*
EVENT_PUBLISHER is kept because warehouse-infra sets it; the only value is kafka.
*/}}
{{- define "wes-work-planning.requireEventPublisher" -}}
{{- if ne (lower (toString .Values.config.eventPublisher)) "kafka" -}}
{{- fail (printf "config.eventPublisher=%q is not supported: this service publishes through the outbox to a real Kafka, the only value is kafka (ADR-0044)" (toString .Values.config.eventPublisher)) -}}
{{- end -}}
{{- if and .Values.mcp.eventPublisher (ne (lower (toString .Values.mcp.eventPublisher)) "kafka") -}}
{{- fail (printf "mcp.eventPublisher=%q is not supported: the only value is kafka (ADR-0044)" (toString .Values.mcp.eventPublisher)) -}}
{{- end -}}
{{- end }}

{{/*
extraEnv must not turn a Kafka consumer or the publisher off. Only the Kafka
modes are checked: the REST sync-edge *_MODE variables (TRAVEL_DISTANCE_MODE)
are out of scope and keep working. Call with (dict "env" <list> "where" <name>).
*/}}
{{- define "wes-work-planning.requireKafkaEnv" -}}
{{- $where := .where -}}
{{- range .env -}}
{{- if and (hasKey . "value") (has .name (list "PRODUCT_CLASSIFICATION_MODE" "REPLENISHMENT_MODE" "EVENT_PUBLISHER")) -}}
{{- $v := lower (toString .value) -}}
{{- if and (ne $v "") (ne $v "kafka") -}}
{{- fail (printf "%s sets %s=%s: not supported, this service runs only on a real Kafka and the binary rejects it at boot (ADR-0044)" $where .name (toString .value)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
The binary's graceful shutdown (cmd/wes serving.gracefulShutdown) flips
readiness, stops the consumers, drains HTTP and waits for the outbox relay
under a 5s deadline; the readiness probe needs a period to see the flip. Under
20s a SIGKILL can cut that short.
*/}}
{{- define "wes-work-planning.requireGracePeriod" -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) 20 -}}
{{- fail (printf "terminationGracePeriodSeconds is %v, want at least 20: readiness flip + HTTP drain + consumers + outbox relay (ADR-0044)" .Values.terminationGracePeriodSeconds) -}}
{{- end -}}
{{- end }}

{{/*
Fully qualified name of the analytics projector deployment (ADR-0011).
*/}}
{{- define "wes-work-planning.projectorFullname" -}}
{{- include "wes-work-planning.fullname" . }}-projector
{{- end }}

{{/*
Fully qualified name of the analytics reports deployment/service (ADR-0011).
*/}}
{{- define "wes-work-planning.reportsFullname" -}}
{{- include "wes-work-planning.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding the analytics DSNs, when the chart creates its own.
*/}}
{{- define "wes-work-planning.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "wes-work-planning.fullname" . }}-analytics
{{- end }}
{{- end }}

{{/*
Fully qualified name of the MCP server deployment/service (ADR-0008).
*/}}
{{- define "wes-work-planning.mcpFullname" -}}
{{- include "wes-work-planning.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote is served by its own nginx pod and reached through warehouse-infra's
Nginx web gateway at /mfes/wes-work-planning/. It is deliberately a separate
workload from the API: Kong never routes to it, and the OLTP Service must never
select it.
*/}}
{{- define "wes-work-planning.frontendFullname" -}}
{{- include "wes-work-planning.fullname" . }}-frontend
{{- end }}


{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "wes-work-planning.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "wes-work-planning.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end -}}
