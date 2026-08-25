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
| State | `internal/state/` | Redis. Keys: `tranquila:obj:{bucket}:{key}`, `tranquila:collection:{bucket}`, `tranquila:stats:{bucket}`, `tranquila:buckets`, `tranquila:statsbuilt`. |
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

All of the below nests under a top-level `sync:` key — `Source`/`Destination`/`Buckets`/etc.
are `embed:""` fields flattened onto the `sync` subcommand, and kong-yaml's resolver
builds its lookup path from the command tree, so a top-level `source:`/`redis:`/etc.
(no `sync:` wrapper) is silently ignored. Likewise, flag names use hyphens
(`access-key`, not `access_key`) — the underscore variant is silently ignored too.

```yaml
sync:
  source:
    endpoint: ""           # empty = AWS; set for MinIO/compatible
    region: us-east-1
    access-key: ""
    secret-key: ""

  destination:
    endpoint: ""
    region: us-east-1
    access-key: ""
    secret-key: ""

  # Structured bucket mappings (preferred for multiple buckets)
  buckets:
    - source:
        bucket: src-bucket
        prefix: optional/prefix
      destination:
        bucket: dst-bucket
        prefix: optional/prefix

  # Legacy string mappings (still supported)
  bucket-mappings: []        # "name" or "src=dst"
  bucket-mapping-file: ""
  dest-bucket-prefix: ""

  redis:
    addr: localhost:6379
    password: ""
    db: 0

  workers: 10

  # Resilience (see "Failure Handling" below)
  cycle-backoff: 5s
  cycle-backoff-max: 10m
  endpoint-fail-threshold: 5

  telemetry:
    exporter: prometheus     # prometheus | otlp | none
    addr: :8081
    otlp-endpoint: ""
```

## Testing

| Scope | Command | Notes |
| --- | --- | --- |
| Unit | `go test ./...` | Stdlib `testing`, table-driven. No containers, no sleeps. |
| End-to-end | `cd e2e && go test ./...` | Separate module (`github.com/jabbrwcky/tranquila/e2e`) with a `replace` to `../`. Root `go test ./...` does not descend into it. |

The e2e module is separate on purpose: testcontainers pulls ~89 transitive
dependencies (moby, containerd) that must not enter the production module graph
or `govulncheck` scope. A submodule can still import `internal/...` because the
internal rule is lexical on import paths, not module-scoped.

Two fault injectors, because they cover different layers:

- **Toxiproxy** (container) is L4 only — `latency`, `down`, `bandwidth`,
  `slow_close`, `timeout`, `reset_peer`, `slicer`, `limit_data`. It has no HTTP
  parsing and **cannot emit 504/503/500**.
- **`faultproxy_test.go`** is an in-process L7 `httputil.ReverseProxy` that
  injects HTTP statuses, which is what the production 504 incident needed. It
  emits both XML bodies (SDK decodes an `APIError`) and non-XML gateway pages
  (no `APIError` at all — the case that forces status-first classification).

Gotchas worth not rediscovering: assertions use `HeadObject` because
`listPageWithRetry`'s 8 jittered attempts make a failing list take 2+ minutes;
podman needs `DOCKER_HOST` pointed at a path containing `podman.sock` or Ryuk
dies on the missing `bridge` network; Apple's `container` has no Docker API and
cannot run testcontainers at all. Details in `e2e/README.md`.

## Redis Key Design

`SCAN ... MATCH` filters **server-side after iterating**: `COUNT` bounds keys
examined per call, not keys returned. A pattern scan therefore costs
O(whole keyspace) no matter how few keys match — and this keyspace is dominated
by `tranquila:obj:*` records (~1.1M in production). Anything on a request path
must avoid scanning.

| Key | Purpose |
| --- | --- |
| `tranquila:obj:{bucket}:{key}` | Per-object record. Bucket names cannot contain `:`, object keys can — split on the first `:` after the prefix. |
| `tranquila:collection:{bucket}` | Last discovery timestamp. |
| `tranquila:stats:{bucket}` | Maintained counters (`total`/`synced`/`pending`/`failed`), so `BucketStats` is one HGETALL. |
| `tranquila:buckets` | Set indexing discovered buckets, so `ListBuckets` is one SMEMBERS. |
| `tranquila:statsbuilt` | Marker that counters have been seeded. |

Counters are updated **atomically with the status write** by the Lua scripts in
`state.go` (`setStatusScript`, `deleteObjectScript`), which read the previous
status to decrement the right field. Every write path must go through
`setStatus` / `deleteObjectScript` or counters will drift.

Because those scripts run via `EVALSHA`, engine compatibility is tested rather
than assumed: `e2e/harness_test.go` defines `kvEngines`, and every state test
runs against Redis 7, Valkey 8 and Valkey 9 (all green, including recovery from
`SCRIPT FLUSH` via the `NOSCRIPT` fallback). Add an engine there to check it.

`RebuildStats` recomputes everything from the object records in a **single**
pass (not one per bucket) and reconciles both the counters and the bucket index.
It runs automatically when `tranquila:statsbuilt` is absent, which seeds an
upgraded keyspace on first read. **To force a reconcile, delete that marker** —
that is the operator escape hatch for drift.

`ScanPending` still scans per bucket, but currently has no callers.

## Failure Handling

Watch mode must never exit on a transient endpoint fault — `os.Exit` also kills
the in-process mgmt server, so `/healthz` stops answering and K8s restarts the pod.

| Layer | Mechanism |
| --- | --- |
| Error classification | `storage.Classify` → `ClassOK`/`ClassTransient`/`ClassThrottle`/`ClassPermanent`. HTTP status first (`*awshttp.ResponseError`), then smithy error code. Unknown → transient. |
| Per-call retry | AWS SDK retryer at `s3MaxAttempts` (5); `listPageWithRetry` at 8 attempts, 30s delay cap, jittered. |
| Per-cycle retry | `runWatch`/`initialSync` back off with `cycleBackoff` (jittered exponential) and never return on transient errors. |
| Rate degradation | `internal/storage/aimd.go`, fed one signal per S3 call from `recordOp`. Halve after N transient failures (immediately on throttle), floor 1/s, additive recovery of 10% of base per 20 healthy calls. |

Key decisions:

- **Only permanent errors terminate**, and only when *every* failure in the cycle
  is permanent (`isFatalCycleErr` fans out one level of `errors.Join`). A mixed
  cycle counts as transient so a flaky endpoint cannot look like misconfiguration.
- **One-shot mode is unchanged** — still exits non-zero, so a K8s Job reports failure.
  The asymmetry falls out of the retry loop living inside `runWatch`.
- **`Run` returns `errors.Join`**, not the first error, so a partial-failure cycle
  keeps every bucket's failure visible.
- **Only endpoints with a configured `rate-limit` degrade.** Unlimited has no
  ceiling to halve, and inventing one would throttle a healthy endpoint.
- **The limiter is always constructed** (`rate.Inf` when unlimited) so the pointer
  is never nil and never swapped — only `SetLimit` mutates. Note `rate.Inf` is
  `Limit(math.MaxFloat64)`, *not* IEEE infinity: compare with `== rate.Inf`.
- **AIMD is event-counted, never time-based**, so the control loop is deterministic
  under test — no clock injection, no sleeps.
- **`/readyz` stays green while degraded** — degraded is the designed correct
  response; marking the pod NotReady sheds no load and stalls rollouts.
- **Detached transfers are time-bounded** (`transferGrace`) so a degraded limiter
  cannot pace an uncancellable transfer past `terminationGracePeriodSeconds`.

## CLI Flags Added This Session

```text
--watch                  enable continuous sync mode
--watch-mode             poll|minio|sqs (default: poll)
--watch-interval         inter-cycle sleep for poll mode (default: 60s)
--sqs-queue-url          SQS queue URL (sqs mode only)
--cycle-backoff          base retry delay after a failed cycle (default: 5s)
--cycle-backoff-max      cap for the retry delay (default: 10m)
--endpoint-fail-threshold transient failures before halving the rate (default: 5)
```

## Dependencies Added

- `github.com/minio/minio-go/v7` — MinIO native event subscription
- `github.com/aws/aws-sdk-go-v2/service/sqs` — SQS long-poll
