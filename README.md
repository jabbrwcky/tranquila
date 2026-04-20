# tranquila

A distributed S3 bucket synchronization tool. Tranquila copies objects from source S3 buckets to a destination S3-compatible endpoint, tracking state in Redis so syncs are resumable across runs.

## How It Works

Tranquila operates in two phases:

1. **Discovery** — Lists the source bucket(s), compares objects against Redis state, and marks new or modified objects as pending.
2. **Sync** — A configurable worker pool processes pending objects concurrently, copying each from source to destination.

State is persisted in Redis using object-level keys, so interrupted or failed transfers are automatically retried on the next run.

## Commands

### `tranquila sync`

Runs discovery and sync against all configured buckets.

```shell
tranquila sync [flags]
```

### `tranquila status [bucket...]`

Prints per-bucket statistics from Redis state.

```shell
tranquila status bucket1 bucket2
```

Output columns: `BUCKET | LAST COLLECTED | TOTAL | SYNCED | PENDING | FAILED`

## Configuration

Configuration is resolved in this priority order (highest wins):

1. CLI flags
2. Environment variables
3. YAML config file (`tranquila.yaml` by default)
4. Built-in defaults

### YAML Config File

```yaml
source:
  endpoint: ""        # leave empty for AWS; set for S3-compatible services
  region: "us-east-1"
  access_key: ""
  secret_key: ""

destination:
  endpoint: ""
  region: "us-east-1"
  access_key: ""
  secret_key: ""

buckets: []           # source buckets to sync; empty = all buckets
bucket_prefix: ""     # prepended to each destination bucket name

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

telemetry:
  exporter: "prometheus"   # prometheus | otlp | none
  addr: ":9090"
  otlp_endpoint: ""        # gRPC endpoint, e.g. localhost:4317

workers: 10
rate_limit: 0.0            # max S3 requests/sec; 0 = unlimited
```

Specify the config file path with `--config <path>` or `TRANQUILA_CONFIG=<path>`.

### Environment Variables

| Variable                  | Default          | Description                                          |
| ------------------------- | ---------------- | ---------------------------------------------------- |
| `SOURCE_ENDPOINT`         | _(AWS)_          | S3-compatible source endpoint                        |
| `SOURCE_REGION`           | `us-east-1`      | Source AWS region                                    |
| `SOURCE_ACCESS_KEY`       |                  | Source access key ID                                 |
| `SOURCE_SECRET_KEY`       |                  | Source secret access key                             |
| `SOURCE_BUCKETS`          | _(all)_          | Comma-separated list of buckets to sync              |
| `DEST_ENDPOINT`           | _(AWS)_          | S3-compatible destination endpoint                   |
| `DEST_REGION`             | `us-east-1`      | Destination AWS region                               |
| `DEST_ACCESS_KEY`         |                  | Destination access key ID                            |
| `DEST_SECRET_KEY`         |                  | Destination secret access key                        |
| `DEST_BUCKET_PREFIX`      |                  | Prefix prepended to destination bucket names         |
| `REDIS_ADDR`              | `localhost:6379` | Redis address                                        |
| `REDIS_PASSWORD`          |                  | Redis password                                       |
| `REDIS_DB`                | `0`              | Redis database number                                |
| `TRANQUILA_WORKERS`       | `10`             | Number of concurrent sync workers                    |
| `TRANQUILA_RATE_LIMIT`    | `0`              | Max S3 requests per second (0 = unlimited)           |
| `TELEMETRY_EXPORTER`      | `prometheus`     | Metrics exporter: `prometheus`, `otlp`, or `none`    |
| `TELEMETRY_ADDR`          | `:9090`          | Prometheus metrics listen address                    |
| `TELEMETRY_OTLP_ENDPOINT` |                  | OTLP gRPC endpoint                                   |
| `TRANQUILA_LOG_LEVEL`     | `info`           | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `TRANQUILA_LOG_JSON`      | `false`          | Emit logs as JSON                                    |
| `TRANQUILA_CONFIG`        | `tranquila.yaml` | Path to YAML config file                             |

All environment variables are also available as CLI flags (e.g. `--source-region`, `--workers`).

## Required IAM Permissions

**Source account:**

- `s3:ListBucket`
- `s3:GetObject`

**Destination account:**

- `s3:PutObject`
- `s3:CreateBucket`

## Observability

### Prometheus

By default, Tranquila exposes a Prometheus metrics endpoint at `http://localhost:9090/metrics`.

### OTLP

```bash
tranquila sync --telemetry-exporter=otlp --telemetry-otlp-endpoint=localhost:4317
```

### Metrics

| Metric                        | Type      | Description                            |
| ----------------------------- | --------- | -------------------------------------- |
| `tranquila.objects.synced`    | Counter   | Objects successfully copied            |
| `tranquila.objects.failed`    | Counter   | Objects that failed to copy            |
| `tranquila.bytes.transferred` | Counter   | Bytes transferred                      |
| `tranquila.transfer.duration` | Histogram | Per-object transfer duration (seconds) |

## Build

```bash
go build -v
```

Requires Go 1.22 or later.

## Usage Examples

Sync with environment variables:

```bash
export SOURCE_REGION=us-west-2
export DEST_BUCKET_PREFIX=backup-
export REDIS_ADDR=redis.example.com:6379
./tranquila sync
```

Sync with a config file:

```bash
./tranquila --config tranquila.yaml sync
```

Check sync status:

```bash
./tranquila status my-bucket-1 my-bucket-2
```

Resume after interruption — rerun the same command. Pending and failed objects are retried automatically from Redis state.

## Graceful Shutdown

On `SIGTERM` or `SIGINT`, Tranquila stops accepting new jobs and waits for in-flight transfers to complete before exiting.
