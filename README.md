# tranquila

A distributed S3 bucket synchronization tool. Tranquila copies objects from source S3 buckets to a destination S3-compatible endpoint, tracking state in Redis so syncs are resumable across runs.

## How It Works

Tranquila pipelines discovery and sync per bucket:

1. **Discovery** — Lists a source bucket in configurable batches (default 100 000 objects). For each object, it checks Redis state and marks new or modified objects as pending.
2. **Sync** — A shared worker pool starts transferring pending objects immediately as each batch is ready. Discovery of the next batch begins only after the current batch has been fully synced, keeping memory usage bounded.
3. **Concurrency** — Multiple buckets are discovered and synced concurrently (bounded by `--workers`).

State is persisted in Redis using object-level keys, so interrupted or failed transfers are automatically retried on the next run.

## Commands

### `tranquila sync`

Runs discovery and sync against all configured buckets.

```shell
tranquila sync [flags]
tranquila -c tranquila.yaml sync
```

### `tranquila status [bucket...]`

Prints per-bucket statistics from the management API (requires a running `tranquila sync` process).

```shell
tranquila status
tranquila status bucket1 bucket2
```

Output columns: `BUCKET | LAST COLLECTED | TOTAL | SYNCED | PENDING | FAILED`

When a sync is actively running, two additional columns are shown: `RATE | ETA`.

## Configuration

Priority order (highest wins):

1. CLI flags
2. Environment variables
3. YAML config file
4. Built-in defaults

### Config File

Load with `--configFile <path>` (short: `-c`) or place a `tranquila.yaml` in the working directory. Tranquila also checks `~/.config/tranquila.yaml`.

All sync settings live under a `sync:` top-level key. Multi-word keys use **hyphens** (e.g. `access-key`, `rate-limit`). Nested keys are equivalent to their hyphen-joined flat form — `source: { access-key: foo }` resolves to the same flag as `source-access-key: foo`.

```yaml
sync:
  source:
    endpoint: ""          # leave empty for AWS; set for MinIO/S3-compatible
    region: "us-east-1"
    access-key: ""
    secret-key: ""

  dest:
    endpoint: ""
    region: "us-east-1"
    access-key: ""
    secret-key: ""
    bucket-prefix: ""     # prepended to auto-discovered destination bucket names

  # Structured bucket mappings (preferred for multiple buckets)
  buckets:
    - source:
        bucket: "my-bucket"
        prefix: "optional/prefix/"   # optional
      destination:
        bucket: "backup-my-bucket"
        prefix: "optional/dest/prefix/"  # optional
    - source:
        bucket: "other-bucket"
      destination:
        bucket: "other-bucket-backup"

  redis:
    addr: "localhost:6379"
    password: ""
    db: 0

  workers: 10
  rate-limit: 0             # max S3 requests/sec; 0 = unlimited
  check-sizes: false        # re-sync if destination size differs from source
  discovery-batch-size: 100000  # objects per batch; sync drains before next batch starts

  # Continuous watch mode
  watch: false
  watch-mode: poll          # poll | minio | sqs
  watch-interval: 60s       # inter-cycle sleep (poll mode only)
  sqs-queue-url: ""         # SQS queue URL (sqs mode only)

  telemetry:
    exporter: "prometheus"  # prometheus | otlp | none
    addr: ":8081"
    otlp-endpoint: ""       # gRPC endpoint, e.g. localhost:4317

  mgmt-addr: ":8080"        # management API listen address
```

**Note:** underscores in YAML keys (e.g. `access_key`) are not equivalent to hyphens — use hyphens to match flag names.

#### Bucket mappings via CLI / file

Legacy string-based mappings are also supported and additive with structured config. CLI flags win on conflict (same source bucket):

```shell
# Comma-separated
tranquila sync --bucket-mappings "src=dst,other"

# From file (one mapping per line; "src=dst" or bare "name"; "#" comments)
tranquila sync --bucket-mapping-file mappings.txt

# Prefix mappings
tranquila sync --prefix-mappings "bucket/src-prefix=dst-prefix"
```

### Environment Variables

| Variable                          | Default          | Description                                          |
| --------------------------------- | ---------------- | ---------------------------------------------------- |
| `SOURCE_ENDPOINT`                 | _(AWS)_          | S3-compatible source endpoint                        |
| `SOURCE_REGION`                   | `us-east-1`      | Source AWS region                                    |
| `SOURCE_ACCESS_KEY`               |                  | Source access key ID                                 |
| `SOURCE_SECRET_KEY`               |                  | Source secret access key                             |
| `DEST_ENDPOINT`                   | _(AWS)_          | S3-compatible destination endpoint                   |
| `DEST_REGION`                     | `us-east-1`      | Destination AWS region                               |
| `DEST_ACCESS_KEY`                 |                  | Destination access key ID                            |
| `DEST_SECRET_KEY`                 |                  | Destination secret access key                        |
| `DEST_BUCKET_PREFIX`              |                  | Prefix prepended to destination bucket names         |
| `BUCKET_MAPPINGS`                 |                  | Comma-separated bucket mappings (`src=dst` or `name`)|
| `BUCKET_MAPPING_FILE`             |                  | Path to bucket mapping file                          |
| `PREFIX_MAPPINGS`                 |                  | Comma-separated prefix mappings                      |
| `REDIS_ADDR`                      | `localhost:6379` | Redis address                                        |
| `REDIS_PASSWORD`                  |                  | Redis password                                       |
| `REDIS_DB`                        | `0`              | Redis database number                                |
| `TRANQUILA_WORKERS`               | `10`             | Number of concurrent sync workers                    |
| `TRANQUILA_RATE_LIMIT`            | `0`              | Max S3 requests per second (0 = unlimited)           |
| `TRANQUILA_CHECK_SIZES`           | `false`          | Re-sync objects whose destination size differs       |
| `TRANQUILA_DISCOVERY_BATCH_SIZE`  | `100000`         | Objects per discovery batch (0 = use default)        |
| `TRANQUILA_WATCH`                 | `false`          | Enable continuous watch mode                         |
| `TRANQUILA_WATCH_MODE`            | `poll`           | Watch backend: `poll`, `minio`, or `sqs`             |
| `TRANQUILA_WATCH_INTERVAL`        | `60s`            | Idle time between poll cycles                        |
| `TRANQUILA_SQS_QUEUE_URL`         |                  | SQS queue URL (sqs watch mode)                       |
| `TELEMETRY_EXPORTER`              | `prometheus`     | Metrics exporter: `prometheus`, `otlp`, or `none`    |
| `TELEMETRY_ADDR`                  | `:8081`          | Prometheus metrics listen address                    |
| `TELEMETRY_OTLP_ENDPOINT`         |                  | OTLP gRPC endpoint                                   |
| `MGMT_ADDR`                       | `:8080`          | Management API listen address                        |
| `TRANQUILA_LOG_LEVEL`             | `info`           | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `TRANQUILA_LOG_JSON`              | `false`          | Emit logs as JSON                                    |

## Continuous Watch Mode

Enable with `--watch`. Three backends are available:

| Mode    | Flag                               | Mechanism                                                                                               |
| ------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `poll`  | `--watch-interval` (default `60s`) | Repeats the full sync cycle with a configurable sleep. Works with any S3-compatible endpoint.           |
| `minio` | —                                  | Subscribes to MinIO bucket notifications via SSE. Reuses source credentials.                            |
| `sqs`   | `--sqs-queue-url`                  | Long-polls an SQS queue for S3 event notifications. Configure the S3->SQS notification externally.      |

Event-driven backends (`minio`, `sqs`) run a full initial sync on startup to catch changes missed while the process was down, then switch to event-driven.

```shell
# Poll every 5 minutes
tranquila sync --watch --watch-interval=5m

# MinIO native events
tranquila sync --watch --watch-mode=minio --source-endpoint=http://minio:9000

# SQS
tranquila sync --watch --watch-mode=sqs \
  --sqs-queue-url=https://sqs.eu-west-1.amazonaws.com/123/my-queue
```

## Required IAM Permissions

**Source account:**

- `s3:ListBucket`
- `s3:GetObject`

**Destination account:**

- `s3:PutObject`
- `s3:HeadBucket`
- `s3:CreateBucket`

## Observability

### Prometheus

Metrics are exposed at `http://localhost:8081/metrics` by default.

```shell
tranquila sync --telemetry-addr=:9090   # change listen address
```

### OTLP

```shell
tranquila sync --telemetry-exporter=otlp --telemetry-otlp-endpoint=localhost:4317
```

### Metrics

| Metric                            | Type           | Description                            |
| --------------------------------- | -------------- | -------------------------------------- |
| `tranquila.objects.synced`        | Counter        | Objects successfully copied            |
| `tranquila.objects.failed`        | Counter        | Objects that failed to copy            |
| `tranquila.bytes.transferred`     | Counter        | Bytes transferred                      |
| `tranquila.transfer.duration`     | Histogram (s)  | Per-object transfer duration           |
| `tranquila.workers.active`        | Gauge.         | Workers currently executing a transfer |
| `tranquila.s3.operation.duration` | Histogram (ms) | Duration of individual S3 API calls    |

### Management API

A lightweight HTTP API is available at `http://localhost:8080` while sync is running.

| Endpoint                     | Description                                   |
| ---------------------------- | --------------------------------------------- |
| `GET /api/v1/buckets`        | List all buckets with Redis state statistics  |
| `GET /api/v1/buckets/{name}` | Per-bucket statistics with live progress      |
| `GET /api/v1/sync`           | Overall sync run progress                     |

## Build

```bash
go build -v
```

Requires Go 1.22 or later.

## Usage Examples

Sync with environment variables:

```shell
export SOURCE_REGION=us-west-2
export DEST_BUCKET_PREFIX=backup-
export REDIS_ADDR=redis.example.com:6379
./tranquila sync
```

Sync with a config file:

```shell
./tranquila -c tranquila.yaml sync
```

Large bucket — reduce batch size to start syncing sooner:

```shell
./tranquila sync --discovery-batch-size=50000
```

Check sync status:

```shell
./tranquila status my-bucket-1 my-bucket-2
```

Resume after interruption — rerun the same command. Pending and failed objects are retried automatically from Redis state.

## Graceful Shutdown

On `SIGTERM` or `SIGINT`, Tranquila stops accepting new jobs and waits for in-flight transfers to complete before exiting.
