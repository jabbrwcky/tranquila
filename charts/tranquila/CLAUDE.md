# CLAUDE.md

Helm chart: **tranquila**. App deployment with valkey subchart.

## Standing directives

- Track all user input and requirements in this file as the session progresses.

## Project layout

- `Chart.yaml` / `Chart.lock` — chart metadata, deps (valkey subchart in `charts/`).
- `values.yaml` — default config.
- `templates/` — k8s manifests.

## Requirements log

- Deploy tranquila as a Deployment.
- Management API (`--mgmt-addr`, container port 8080) is fronted by the Service;
  Ingress + Gateway API HTTPRoute optionally expose it (`ingress.*`, `httpRoute.*`).
- Redis/Valkey access secret (`REDIS_ADDR`/`REDIS_PASSWORD`/`REDIS_DB`):
  - `valkey.enabled=true` → chart creates `<fullname>-redis`; addr derived from
    `{release}-valkey:6379`. `redis.password` mirrors `valkey.auth`.
  - `valkey.enabled=false` → set `redis.existingSecret` (no secret created) OR
    fill `redis.addr/password/db` to have the chart create the secret.
- S3 source/dest creds rendered into `<fullname>-config` secret; bucket mappings
  → `$BUCKET_MAPPINGS`. To manage them out of band set top-level
  `existingSecret` (and `existingConfig` for the ConfigMap). Both were
  documented at the top level but read from under `config`, so the documented
  form rendered a Secret anyway and then failed its `required` calls; the
  helpers now prefer the top-level key and still honour the nested one, so
  values using `config.existingSecret` keep working. `ci/existing-secret-values.yaml`
  plus the `chart` job's render assertions are the regression guard.
- `extraObjects` deploys arbitrary resources. MAP form (keyed) merges across
  multiple `-f` values files; LIST form is simpler but replaced (Helm list
  semantics). Entries may be YAML objects or strings; both run through `tpl`.

## Layout notes

- Helpers: `tranquila.redisSecretName/createRedisSecret/redisAddr/configSecretName/bucketMappings`.
- `templates/secret-redis.yaml`, `secret-config.yaml`, `extra-objects.yaml` added.
- CAUTION: Go-template comments `{{/* */}}` break on literal `*/` (e.g. `SOURCE_*/`).

## Location

This chart moved out of `pflege-de/helm-charts` and now lives in the tranquila
repository itself, at `charts/tranquila`, published to
`ghcr.io/pflege-de-labs/charts` by `.github/workflows/release_helm_chart.yml`.
It is versioned independently of the application: a push to `main` that raises
`version` in `Chart.yaml` publishes it, anything else is a no-op.
