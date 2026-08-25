# tranquila

A distributed S3 bucket synchronization tool. Tranquila copies objects from source S3 buckets to a destination S3-compatible endpoint, tracking state in Redis so syncs are resumable across runs.

## How It Works

Tranquila pipelines discovery and sync per bucket:

1. **Discovery** — Lists a source bucket in configurable batches (default 100 000 objects). For each object, it checks Redis state and marks new or modified objects as pending.
2. **Sync** — A shared worker pool starts transferring pending objects immediately as each batch is ready. Discovery of the next batch begins only after the current batch has been fully synced, keeping memory usage bounded.
3. **Concurrency** — Multiple buckets are discovered and synced concurrently (bounded by `--workers`).

State is persisted in Redis using object-level keys, so interrupted or failed transfers are automatically retried on the next run.

Key properties:

- **Resumable** — Redis tracks every object, so an interrupted run picks up where it left off.
- **Provider-agnostic** — works against AWS S3 or any S3-compatible endpoint (MinIO, Ceph, …) on either side, with Redis or Valkey for state.
- **Survives flaky endpoints** — transient failures are retried with backoff, and a struggling endpoint is automatically paced down and recovered, without the process exiting.
- **Continuous or one-shot** — run once as a `Job`, or `--watch` continuously via polling, MinIO notifications or SQS.
- **Observable** — Prometheus/OTLP metrics, a management API, and Kubernetes probes.

For the internals — data flow, the Redis key design, the resilience machinery and the concurrency model — see **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

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
    rate-limit: 0         # max S3 API calls/sec for source endpoint (0 = unlimited)

  dest:
    endpoint: ""
    region: "us-east-1"
    access-key: ""
    secret-key: ""
    rate-limit: 0         # max S3 API calls/sec for destination endpoint (0 = unlimited)
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
  check-sizes: false        # re-sync if destination size differs from source
  discovery-batch-size: 100000  # objects per batch; sync drains before next batch starts

  # Continuous watch mode
  watch: false
  watch-mode: poll          # poll | minio | sqs
  watch-interval: 60s       # inter-cycle sleep (poll mode only)
  sqs-queue-url: ""         # SQS queue URL (sqs mode only)

  # Retry pacing after a failed cycle in watch mode (exponential, jittered)
  cycle-backoff: 5s
  cycle-backoff-max: 10m
  endpoint-fail-threshold: 5  # transient failures before an endpoint's rate is halved

  telemetry:
    exporter: "prometheus"  # prometheus | otlp | none
    addr: ":8081"
    otlp-endpoint: ""       # gRPC endpoint, e.g. localhost:4317

  mgmt-addr: ":8080"        # management API listen address
```

**Note:** underscores in YAML keys (e.g. `access_key`) are not equivalent to hyphens — use hyphens to match flag names.

#### Burn-After-Reading

Set `burn-after-reading: true` on any bucket mapping to delete source objects after they are successfully synced and verified:

```yaml
sync:
  buckets:
    - source:
        bucket: staging-uploads
      destination:
        bucket: archive-uploads
      burn-after-reading: true
```

**Verification.** Deletion is irreversible, so it only happens behind a check. Which check depends on whether this run uploaded the object:

- **Uploaded in this run** — tranquila compares the CRC32 returned by the upload response with the CRC32 stored by S3 (via `HeadObject`), having already confirmed the destination size matches the source. If either checksum is absent or they differ, the source object is **not** deleted and the job is marked failed for retry.
- **Already synced before the mode was enabled** — there is no upload to checksum, so tranquila confirms the destination still holds the object at the expected size and then deletes the source. No re-upload is performed. Note this path verifies *existence and size only*; if you need checksum-verified deletion for previously synced objects, force a re-sync with `--check-sizes` or clear their Redis state first.

**Dry-run mode:** pass `--dry-run` (or set `TRANQUILA_DRY_RUN=true`) to log what would be deleted without actually removing anything:

```shell
tranquila sync --dry-run -c tranquila.yaml
```

Dry-run logs include the object key, CRC32 comparison result, and the planned deletion for every object that would be removed.

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
| `SOURCE_RATE_LIMIT`               | `0`              | Max S3 API calls/sec for source endpoint             |
| `DEST_ENDPOINT`                   | _(AWS)_          | S3-compatible destination endpoint                   |
| `DEST_REGION`                     | `us-east-1`      | Destination AWS region                               |
| `DEST_ACCESS_KEY`                 |                  | Destination access key ID                            |
| `DEST_SECRET_KEY`                 |                  | Destination secret access key                        |
| `DEST_RATE_LIMIT`                 | `0`              | Max S3 API calls/sec for destination endpoint        |
| `DEST_BUCKET_PREFIX`              |                  | Prefix prepended to destination bucket names         |
| `BUCKET_MAPPINGS`                 |                  | Comma-separated bucket mappings (`src=dst` or `name`)|
| `BUCKET_MAPPING_FILE`             |                  | Path to bucket mapping file                          |
| `PREFIX_MAPPINGS`                 |                  | Comma-separated prefix mappings                      |
| `REDIS_ADDR`                      | `localhost:6379` | Redis address                                        |
| `REDIS_PASSWORD`                  |                  | Redis password                                       |
| `REDIS_DB`                        | `0`              | Redis database number                                |
| `TRANQUILA_WORKERS`               | `10`             | Number of concurrent sync workers                    |
| `TRANQUILA_CHECK_SIZES`           | `false`          | Re-sync objects whose destination size differs       |
| `TRANQUILA_DRY_RUN`               | `false`          | Log planned burn-after-reading deletions, no delete  |
| `TRANQUILA_DISCOVERY_BATCH_SIZE`  | `100000`         | Objects per discovery batch (0 = use default)        |
| `TRANQUILA_WATCH`                 | `false`          | Enable continuous watch mode                         |
| `TRANQUILA_WATCH_MODE`            | `poll`           | Watch backend: `poll`, `minio`, or `sqs`             |
| `TRANQUILA_WATCH_INTERVAL`        | `60s`            | Idle time between poll cycles                        |
| `TRANQUILA_SQS_QUEUE_URL`         |                  | SQS queue URL (sqs watch mode)                       |
| `TRANQUILA_CYCLE_BACKOFF`         | `5s`             | Base retry delay after a failed cycle (watch mode)   |
| `TRANQUILA_CYCLE_BACKOFF_MAX`     | `10m`            | Maximum retry delay after a failed cycle             |
| `TRANQUILA_ENDPOINT_FAIL_THRESHOLD` | `5`            | Transient failures before an endpoint's rate is halved |
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

### Failure handling in watch mode

Watch mode is a long-lived service, so a transient endpoint fault must not become
a pod restart. Failed cycles are retried with exponential backoff plus jitter
(`--cycle-backoff`, `--cycle-backoff-max`) and the process stays alive, keeping
`/healthz` answering.

| Failure | Watch mode | One-shot |
| --- | --- | --- |
| Transient (504, 502, 500, timeouts, dropped connections) | Retried forever with backoff | Exits non-zero |
| Throttle (503, `SlowDown`, 429) | Retried forever with backoff | Exits non-zero |
| Permanent (`AccessDenied`, `NoSuchBucket`, bad credentials) | Exits non-zero | Exits non-zero |

Misconfiguration stays loud: only a cycle whose failures are *all* permanent
terminates. A cycle mixing permanent and transient failures is treated as
transient, so a flaky endpoint can never be misread as misconfiguration.

One-shot runs (no `--watch`) are unchanged and still exit non-zero on any
failure, so a Kubernetes `Job` or CI invocation reports it.

### Adaptive rate limiting

Each endpoint's rate limit is governed by additive-increase/multiplicative-decrease
congestion control, fed by the outcome of every S3 API call:

- After `--endpoint-fail-threshold` consecutive transient failures (default `5`)
  the endpoint's rate limit is **halved**, down to a floor of 1 call/sec.
- An explicit throttle (`503`, `SlowDown`, `429`) is unambiguous back-pressure
  and halves the rate on the **first** signal, without waiting for the threshold.
- After 20 consecutive healthy calls the limit climbs back by 10% of the
  configured base, until it is fully restored. Decrease fast, recover slowly.
- A permanent error counts as *healthy* for pacing purposes: the endpoint
  answered, so it is not congested.

Source and destination are paced independently and symmetrically: a source-side
`504` throttles source reads only, and a destination-side `504` throttles
destination writes only. Every S3 operation feeds the controller, including the
destination's `PutObject`, `HeadObject`, `DeleteObject` and bucket creation.

> **Requires a configured rate limit.** Only endpoints with an explicit
> `--source-rate-limit` / `--dest-rate-limit` are degraded. An endpoint left
> unlimited (the default, `0`) has no ceiling to reduce, and inventing one would
> throttle a healthy endpoint — so it keeps running unlimited and gets only the
> cycle backoff above. To protect the destination, set `--dest-rate-limit`.

Note that the limiter paces *operations*, not HTTP requests: an upload above the
transfer manager's 16 MiB multipart threshold issues several requests but spends
one token, and a failure anywhere in it is one congestion signal. Pacing is
therefore approximate for workloads dominated by large objects.

Current pacing is exposed on `GET /api/v1/sync` as `source` and `destination`
(`rate_limit`, `base_rate_limit`, `degraded`, `degraded_since`) and as the
metrics `tranquila_s3_rate_limit`, `tranquila_s3_rate_limit_degraded`,
`tranquila_s3_rate_limit_changes` and `tranquila_s3_errors`.

`/readyz` stays green while degraded: degraded operation is the designed correct
response to a flaky endpoint, and reporting it as unready would stall rolling
updates without shedding any load. Alert on
`tranquila_s3_rate_limit_degraded == 1` and `tranquila_sync_cycle_failures`
instead.

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

| Metric                             | Type            | Attributes            | Description                                        |
| ---------------------------------- | --------------- | --------------------- | -------------------------------------------------- |
| `tranquila.objects.synced`         | Counter         | `bucket`              | Objects successfully copied                        |
| `tranquila.objects.failed`         | Counter         | `bucket`              | Objects that failed to copy                        |
| `tranquila.bytes.transferred`      | Counter         | `bucket`              | Bytes transferred                                  |
| `tranquila.transfer.duration`      | Histogram (s)   | `bucket`              | Per-object transfer duration                       |
| `tranquila.workers.active`         | UpDownCounter   | —                     | Workers currently executing a transfer             |
| `tranquila.sync.cycle.failures`    | Counter         | —                     | Watch cycles that failed and were retried          |
| `tranquila.s3.operation.duration`  | Histogram (ms)  | `operation`, `bucket`, `status` | Duration of individual S3 API calls      |
| `tranquila.s3.errors`              | Counter         | `endpoint`, `class`   | S3 failures by class (transient/throttle/permanent) |
| `tranquila.s3.rate_limit`          | Gauge ({call}/s)| `endpoint`            | Effective rate limit; 0 when unlimited             |
| `tranquila.s3.rate_limit.degraded` | Gauge           | `endpoint`            | 1 while congestion control has reduced the limit   |
| `tranquila.s3.rate_limit.changes`  | Counter         | `endpoint`, `direction` | Rate-limit adjustments; detects oscillation      |

Useful alerts:

```promql
# An endpoint has been throttled by congestion control for a sustained period
tranquila_s3_rate_limit_degraded == 1

# Watch cycles are failing: the process is alive but not making progress
rate(tranquila_sync_cycle_failures[15m]) > 0
```

### Management API

A lightweight HTTP API is available at `http://localhost:8080` while sync is running.

| Endpoint                     | Description                                   |
| ---------------------------- | --------------------------------------------- |
| `GET /api/v1/buckets`        | List all buckets with Redis state statistics  |
| `GET /api/v1/buckets/{name}` | Per-bucket statistics with live progress      |
| `GET /api/v1/sync`           | Overall sync run progress                     |
| `GET /healthz`               | Liveness probe (always 200 while serving)     |
| `GET /readyz`                | Readiness probe (200 ready / 503 Redis down)  |

`/healthz` is a **liveness** probe: it returns `200 {"status":"ok"}` whenever the process
is serving HTTP and checks no dependencies — so a temporarily unreachable Redis will not
cause a pod restart. `/readyz` is a **readiness** probe: it pings Redis and returns
`200 {"status":"ok"}` when reachable or `503 {"status":"unavailable","error":...}` when
not, so traffic is only routed to pods that can serve requests.

#### Redis and Valkey

Sync state is held in Redis. **Valkey works as a drop-in replacement** — the
state layer maintains its counters with Lua scripts, and the end-to-end suite
runs them against Redis 7, Valkey 8 and Valkey 9 on every CI run, so fork
compatibility is verified rather than assumed. Point `--redis-addr` at either.

Other Redis-compatible engines (KeyDB, Dragonfly, ElastiCache, MemoryDB) are
untested; Dragonfly in particular implements Lua differently. See
[e2e/README.md](e2e/README.md#key-value-engines) for how to verify one.

#### Bucket statistics

The per-bucket counts are maintained incrementally in Redis and read with a single
lookup, so the endpoints respond in milliseconds regardless of how many objects are
tracked. They are updated atomically with each object's status, in the same operation.

The counters are seeded automatically the first time they are read on a keyspace that
predates them: that one request recomputes them from the object records and can take a
few seconds on a large keyspace. Every request afterwards is served from the counters.

If the counters ever drift from reality, force a reconcile by deleting the marker key —
the next request recomputes everything from the object records:

```shell
redis-cli DEL tranquila:statsbuilt
```

#### Kubernetes probes

Point both probes at the management API port (from `--mgmt-addr`, default `8080`):

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Build

```bash
go build -v
```

Requires Go 1.25 or later, as declared in `go.mod`.

## Tests

```bash
go test ./...             # unit tests, no containers
cd e2e && go test ./...    # end-to-end, container-backed (~3-4 min)
```

The end-to-end suite drives the resilience behaviour above against a real MinIO,
injecting HTTP faults (504/503/500) and TCP faults (connection resets) to prove
that transient failures are absorbed, watch mode survives an outage, and rate
limits degrade and recover. It is a separate Go module, so it neither slows the
unit suite nor adds container dependencies to the production module.

It needs a container runtime. Docker works as-is; on macOS, Podman works without
Docker Desktop, but the machine must be **rootful**:

```bash
brew install podman
podman machine init --rootful && podman machine start
cd e2e && go test ./...
```

No environment variables are needed — the suite configures the runtime itself,
and skips with an explanation if none is reachable. Apple's native `container`
CLI is **not** supported (it exposes no Docker-compatible API). Full setup,
configuration reference and troubleshooting: **[e2e/README.md](e2e/README.md)**.

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
