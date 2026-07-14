# Tranquila — Project Context

@AGENTS.md

## Architecture

| Layer | Package | Notes |
| --- | --- | --- |
| CLI | `main.go`, `cmd_sync.go`, `cmd_status.go` | kong + kong-yaml. Config bound via `kctx.Bind(cfg)` after parse. |
| Config | `config/` | YAML loaded in `main.go`, passed to `SyncCmd.Run(*config.Config)`. |
| Sync engine | `internal/sync/` | `Syncer`: discover → mark pending → transfer via worker pool. Redis state. |
| Watchers | `internal/watcher/` | `Watcher` interface + poll/MinIO/SQS implementations. |
| Storage | `internal/storage/` | `aws-sdk-go-v2` + transfermanager. Works with any S3-compatible endpoint. |
| State | `internal/state/` | Redis. Keys: `tranquila:obj:{bucket}:{key}`, `tranquila:collection:{bucket}`. |
| API | `internal/api/` | Management HTTP API. `/api/v1/buckets`, `/api/v1/sync`. K8s probes: `/healthz` (liveness), `/readyz` (readiness, pings Redis). |

## Implemented Features

### Continuous Watch (`--watch`)

Three backends selected via `--watch-mode`:

| Mode | Flag | Mechanism |
| --- | --- | --- |
| `poll` (default) | `--watch-interval` (default 60s) | Loops `Run()` with sleep. Universal fallback. |
| `minio` | — | `minio-go/v7` `ListenBucketNotification` SSE stream. Reuses source credentials. |
| `sqs` | `--sqs-queue-url` | `aws-sdk-go-v2/service/sqs` long-poll. User pre-configures S3→SQS notification. |

Event-driven modes (`minio`, `sqs`) run an initial full `Run()` on startup to catch missed changes, then switch to event-driven.

Key design: provider-agnostic — polling works everywhere; push modes are opt-in per backend. No SQS/SNS assumption for non-AWS endpoints.

### Structured Bucket Config (YAML)

Replaces awkward `--bucket-mappings "src=dst"` strings for large deployments.

```yaml
buckets:
  - source:
      bucket: sourceBucket
      prefix: foo        # optional
    destination:
      bucket: dstBucket
      prefix: bar        # optional
```

Processed in `cmd_sync.go:resolveBuckets()`. Structured config loaded first; CLI flags (`--bucket-mappings`, `--prefix-mappings`, `--bucket-mapping-file`) are additive and override on conflict (same source bucket).

## Key Design Decisions

- **No S3 event notification config management** — tranquila does not configure SNS/SQS targets on buckets. Users set that up externally. Tranquila only consumes events.
- **MinIO watcher uses `minioNotifier` interface** — `*minio.Client` never referenced directly in `MinIOWatcher`; allows test injection without real server.
- **SQS watcher always deletes messages** — even unparseable ones. Sync is idempotent via Redis; stuck messages in SQS are worse than a missed event.
- **Initial full sync before event loop** — `RunWatcher` calls `Run()` first so objects changed while the program was down are not missed.
- **`runWatch`/`runWatcher` private helpers** — public methods delegate to injectable private versions; enables unit tests without real S3/Redis.

## Configuration Reference (YAML)

```yaml
source:
  endpoint: ""           # empty = AWS; set for MinIO/compatible
  region: us-east-1
  access_key: ""
  secret_key: ""

destination:
  endpoint: ""
  region: us-east-1
  access_key: ""
  secret_key: ""

# Structured bucket mappings (preferred for multiple buckets)
buckets:
  - source:
      bucket: src-bucket
      prefix: optional/prefix
    destination:
      bucket: dst-bucket
      prefix: optional/prefix

# Legacy string mappings (still supported)
bucket_mappings: []        # "name" or "src=dst"
bucket_mapping_file: ""
dest_bucket_prefix: ""

redis:
  addr: localhost:6379
  password: ""
  db: 0

workers: 10
rate_limit: 0              # 0 = unlimited req/s

telemetry:
  exporter: prometheus     # prometheus | otlp | none
  addr: :9090
  otlp_endpoint: ""
```

## CLI Flags Added This Session

```text
--watch                  enable continuous sync mode
--watch-mode             poll|minio|sqs (default: poll)
--watch-interval         inter-cycle sleep for poll mode (default: 60s)
--sqs-queue-url          SQS queue URL (sqs mode only)
```

## Dependencies Added

- `github.com/minio/minio-go/v7` — MinIO native event subscription
- `github.com/aws/aws-sdk-go-v2/service/sqs` — SQS long-poll
