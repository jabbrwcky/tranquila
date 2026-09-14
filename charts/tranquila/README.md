# tranquila

Deploys [tranquila](https://github.com/pflege-de-labs/tranquila), an S3 bucket
synchronization daemon, with an optional bundled [Valkey](https://valkey.io/)
for its state store.

The chart is versioned independently of the application. `appVersion` tracks the
tranquila release the image defaults point at.

## Install

The chart is published as an OCI artifact:

```shell
helm install tranquila oci://ghcr.io/pflege-de-labs/charts/tranquila \
  --version 0.4.1 \
  --values my-values.yaml
```

A minimal `my-values.yaml`:

```yaml
config:
  source:
    endpoint: https://minio.example.com
    accesskey: ...
    secretkey: ...
  destination:
    accesskey: ...
    secretkey: ...
  buckets:
    - source:
        bucket: source-bucket
      destination:
        bucket: destination-bucket
```

## State store

tranquila needs Redis or Valkey. Two supported shapes:

| | Setting |
| --- | --- |
| **Bundled** (default) | `valkey.enabled: true` — the subchart is deployed and the connection details are derived from it. |
| **External** | `valkey.enabled: false`, then either set `redis.addr`/`redis.password`/`redis.db` and let the chart create the secret, or point `redis.existingSecret` at one you manage. |

## Credentials and config out of band

By default the chart renders a Secret holding the S3 credentials and a ConfigMap
holding the sync configuration. To manage either yourself:

```yaml
existingSecret: tranquila-s3-credentials   # SOURCE_/DEST_ access and secret keys
existingConfig: tranquila-sync-config      # the sync configuration
```

Setting either suppresses the corresponding object and mounts yours instead.

> These two keys are documented at the top level, but earlier revisions of the
> chart read them from under `config`, so the documented form rendered a Secret
> anyway and then failed on its `required` calls. Both spellings are honoured
> now, with the top-level one winning, so existing values using
> `config.existingSecret` keep working unchanged.

## Observability

`metrics.enabled` exposes the Prometheus endpoint, and `metrics.serviceMonitor`
/ `metrics.podMonitor` create Prometheus Operator objects for it. A Grafana
dashboard for these metrics ships in
[`deploy/grafana`](https://github.com/pflege-de-labs/tranquila/tree/main/deploy/grafana)
in the same repository.

## Management API

The management API (`/healthz`, `/readyz`, `/api/v1/...`) is served on the
container's management port and fronted by the Service. `ingress.*` and
`httpRoute.*` optionally expose it.

## Values

See [values.yaml](values.yaml) — every key is commented. The combinations under
[`ci/`](ci) are the ones CI lints and renders on every change:

| File | Shape |
| --- | --- |
| `default-values.yaml` | Bundled valkey, no ingress, no autoscaling. |
| `external-redis-values.yaml` | External Redis/Valkey, subchart disabled. |
| `existing-secret-values.yaml` | Credentials and config managed out of band. |
| `monitoring-values.yaml` | ServiceMonitor, PodMonitor and an HPA. |
| `ingress-values.yaml` | Management API behind an Ingress. |
