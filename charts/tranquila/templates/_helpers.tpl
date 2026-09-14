{{/*
Expand the name of the chart.
*/}}
{{- define "tranquila.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "tranquila.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "tranquila.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "tranquila.labels" -}}
helm.sh/chart: {{ include "tranquila.chart" . }}
{{ include "tranquila.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "tranquila.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tranquila.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "tranquila.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tranquila.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the secret holding redis/valkey connection data
(REDIS_ADDR/REDIS_PASSWORD/REDIS_DB). Uses an existing secret when provided,
otherwise a chart-managed "<fullname>-redis" secret.
*/}}
{{- define "tranquila.redisSecretName" -}}
{{- if .Values.redis.existingSecret -}}
{{- .Values.redis.existingSecret -}}
{{- else -}}
{{- printf "%s-redis" (include "tranquila.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
Whether this chart should render the redis secret itself.
True unless the user pointed at a pre-existing secret.
*/}}
{{- define "tranquila.createRedisSecret" -}}
{{- if .Values.redis.existingSecret -}}false{{- else -}}true{{- end -}}
{{- end }}

{{/*
Redis address "host:port". Explicit redis.addr wins; otherwise derived from the
valkey subchart service when valkey.enabled.
*/}}
{{- define "tranquila.redisAddr" -}}
{{- if .Values.redis.addr -}}
{{- .Values.redis.addr -}}
{{- else if .Values.valkey.enabled -}}
{{- $name := default (printf "%s-valkey" .Release.Name) .Values.valkey.fullnameOverride -}}
{{- $port := 6379 -}}
{{- with .Values.valkey.service }}{{- with .port }}{{- $port = . }}{{- end }}{{- end -}}
{{- printf "%s:%v" $name $port -}}
{{- end -}}
{{- end }}

{{/*
A credentials secret the user manages out of band, or "" when the chart renders
its own. values.yaml documents this as top-level `existingSecret`, but the
templates only ever read `config.existingSecret`, so the documented key did
nothing and the render failed on the `required` calls. Both are honoured, the
documented one wins, so values that relied on the nested form keep working.
*/}}
{{- define "tranquila.existingSecret" -}}
{{- .Values.existingSecret | default (.Values.config | default dict).existingSecret | default "" -}}
{{- end }}

{{/*
Name of the secret holding S3 source/destination credentials (SOURCE_ and DEST_ keys).
*/}}
{{- define "tranquila.configSecretName" -}}
{{- $existing := include "tranquila.existingSecret" . -}}
{{- if $existing -}}
{{- $existing -}}
{{- else -}}
{{- printf "%s-config" (include "tranquila.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
A config ConfigMap the user manages out of band, or "" when the chart renders
its own. values.yaml documents this as top-level `existingConfig`; the templates
read `config.existing`, a key that appears nowhere in values.yaml, so the
feature was unreachable. Both are honoured for the same reason as above.
*/}}
{{- define "tranquila.existingConfig" -}}
{{- .Values.existingConfig | default (.Values.config | default dict).existing | default "" -}}
{{- end }}

{{/*
Name of the configmap containing sync configuration.
*/}}
{{- define "tranquila.configMapName" -}}
{{- $existing := include "tranquila.existingConfig" . -}}
{{- if $existing -}}
{{- $existing -}}
{{- else -}}
{{- printf "%s-config" (include "tranquila.fullname" .) -}}
{{- end -}}
{{- end }}
