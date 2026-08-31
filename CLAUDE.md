# Tranquila — Project Context

@AGENTS.md

Human-facing docs: [README.md](./README.md) for usage,
[docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) for internals,
[e2e/README.md](./e2e/README.md) for the container-backed test suite. Keep this
file for agent-specific context and the decisions behind the code; do not
duplicate the reference material there.

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
- **`ListObjectsPage` streams via an `onPage` callback, not a return slice** — the old signature accumulated up to `DiscoveryBatchSize` (default 100k) objects in memory across many S3 pages before returning, so `discoverAndSyncBucket` submitted nothing to the worker pool until the *entire* batch was listed. A bucket that's large or hitting transient S3 errors (e.g. 504s) mid-listing looked completely stalled — zero transfers — for the whole batch. The callback fires per underlying S3 page so objects start transferring as soon as they're discovered; the outer per-batch `batchDone.Wait()` still bounds memory/pending count.
- **Per-bucket transfer concurrency cap (`--max-workers-per-bucket`, default: half of `--workers`)** — the worker pool is shared across all buckets in a cycle. Without a cap, a bucket with many (often small) objects can occupy the whole pool via continuous submission, starving other buckets' transfers even though their discovery goroutines are actively queuing jobs. Enforced with a per-bucket semaphore acquired in `discoverAndSyncBucket`'s `onPage` callback and released in `Job.OnComplete`.
- **Delete propagation (`propagate-deletes`) uses two different mechanisms per watch mode** — `minio`/`sqs` already receive an `ObjectRemoved` notification, so `ObjectEvent.IsDelete` (set from `EventName` in `minio.go`/`sqs.go`) drives an immediate destination delete via `runWatcher`'s `eventDispatch`. `poll` mode has no such notification, so it relies on reconciliation: `discoverAndSyncBucket` calls `state.TouchSeen` for every object seen in a listing (only for `PropagateDeletes` buckets, to avoid the extra write for everyone else), and `Syncer.reconcileDeletes` (called at the end of `Run`) finds synced objects whose `seen_at` predates the cycle via `state.ScanStaleObjects`.
- **Reconciliation never deletes on an inconclusive answer** — before deleting the destination, `reconcileDeletes` re-checks the candidate against the *source* with `HeadObject`, classified through `classifyHeadErr`/`storage.Classify`. Only a confirmed `ClassOK` 404 (`NoSuchKey`) counts as "genuinely deleted"; any other error class (transient, throttled, permanent-but-not-404) is treated as inconclusive and retried next pass — a listing gap or a flaky `HeadObject` must never cause a live destination object to be deleted.
- **`--delete-reconcile-interval` is decoupled from `--watch-interval`** — `ScanStaleObjects` is `SCAN`-based (see Redis Key Design), so it pays the same O(whole-keyspace) cost as `RebuildStats` every time it runs, independent of how small the reconciled bucket is. `Syncer.reconcileDue`/`dueForReconcile` throttle it per-bucket; `0` (default) means "every `Run()` call" for consistency with this codebase's other `0 = no throttling` flags, not "disabled".
- **A failed destination delete does not call `state.MarkFailed`** — unlike a failed upload, `needsSync` would then retry a full sync of a source object that no longer exists. `processResults` leaves the record's status untouched on a delete failure so it is picked up again by the next reconcile pass (or the next watcher event, for event-driven modes) instead of looping on a doomed re-upload.
- **`performBurnAfterReading` falls back to ETag, then content hash, when S3-provided CRC32 metadata is unavailable** — the original implementation trusted only the upload response's/`HeadObject`'s CRC32 and refused to delete (permanently, every cycle) when either was empty. Not every S3-compatible destination echoes flexible checksums, so this made burn-after-reading unusable against such a destination. It now shares the same cheapest-first tier order `performVerifyAndDelete` already used for pre-existing objects: CRC32 metadata → single-part ETag → full content hash, only refusing once every applicable tier has been tried.
- **Verification mismatches are a typed `*verifyMismatchError{Bucket, Key, Method, Source, Destination}`, not values interpolated into the error string** — `processResults` extracts it via `errors.As` and attaches `verify_method`/`source_value`/`dest_value` as their own structured log fields on the same `Error()`-level event, rather than relying on a separate `Info`/`Warn` log line (whose fields would be lost if the configured log level filtered it out) or on parsing them back out of free text.
- **Every `ListObjectsV2` attempt gets a per-attempt deadline (`listAttemptTimeout`, 60s)** — root-caused against a real MinIO deployment: a flat listing over a ~295K-object bucket hung with *zero response*, even bypassing every proxy in front of it. Nothing in `storage.Client` previously imposed any per-call deadline (only the long-lived watch-loop `ctx`, cancelled on SIGTERM), so `listPageWithRetry`'s retry loop never even reached attempt 2 — attempt 1 never returned. `storage.Classify` deliberately treats `context.DeadlineExceeded` as `ClassOK` ("cancellation is our own doing") — correct for the caller's own ctx, wrong for a timeout the retry loop imposes on itself, which must be retried like any transient fault. `listAttemptTimedOut(outerCtx, attemptCtx, err)` distinguishes the two cases explicitly rather than relying on `storage.Classify`/`isTransientErr` for this one.
- **Prefix-sharded discovery (`sharded-discovery`) walks a bucket the way the MinIO/S3 web console does** — folder-by-folder via a `/`-delimited `ListObjectsV2` (`CommonPrefixes`), instead of one flat, bucket-wide scan a struggling backend may never be able to answer. `storage.listObjectsTree` is the S3-independent orchestration core (tested against a fake `listDelimitedFn`, no real client needed): a bounded-concurrency (`shardedDiscoveryConcurrency = 4`) fan-out over prefixes via a `sync.WaitGroup`-tracked task queue, first-error capture + cancellation, and — critically — **`onPage` is invoked from a single consumer goroutine only** (workers push pages onto a channel rather than calling `onPage` directly), so its existing contract in `discoverAndSyncBucket` (mutates closed-over counters and `bucketSem` with no locking) holds unchanged even though the listing calls producing those pages run concurrently.
- **Sharded discovery triggers via automatic fallback, not opt-in alone** — a flat listing that exhausts all retries with a transient/throttle error (checked via `isShardableListErr`, which requires the error to be a `*storage.ListError` specifically — see below) automatically retries as a sharded walk for the rest of that cycle, logging a `Warn` recommending the flag for next time. This protects buckets nobody has flagged yet (the whole point — "there might be buckets with more objects" was the ask); the `sharded-discovery: true` flag exists purely as an efficiency escape hatch to skip the now-known-doomed flat attempt (up to `listMaxRetries × listMaxDelay` ≈ several minutes) on buckets already identified as needing it.
- **`storage.ListError{Bucket, Err}` distinguishes "the listing call itself failed" from "onPage failed"** — `ListObjectsPage` wraps only its own `listPageWithRetry` failures in `ListError`; an error returned by the `onPage` callback (e.g. a Redis mark-pending failure) passes through unwrapped. `isShardableListErr` requires `errors.As` to find a `*ListError` before even checking `storage.Classify` — otherwise a transient Redis error (which also classifies `ClassTransient` under `Classify`'s "unknown error" fallback) would incorrectly trigger a sharded-discovery fallback that can't fix a Redis problem.
- **`discoverObject` is shared between `discoverFlat` and `discoverSharded`** — both extract the identical per-object `needsSync`/`TouchSeen`/`MarkPending`/`bucketSem`/`pool.submit` logic from a single method, parameterized by whichever `*sync.WaitGroup` the caller uses for its own batching semantics (`discoverFlat`: per-`DiscoveryBatchSize`-batch, unchanged; `discoverSharded`: one `WaitGroup` for the whole tree walk, waited on once at the end). Job-submission behavior can never drift between the two discovery strategies as a result.
- **`DiscoveryBatchSize`'s pause-while-a-batch-drains pacing does not apply to sharded discovery** — a tree walk has no single linear continuation token to pause on. Backpressure instead comes from the same per-bucket `bucketSem` (`--max-workers-per-bucket`) that already throttles flat discovery, which bounds it identically in practice; documented as an accepted scope limitation rather than replicated with tree-walk-specific batching.
- **`SetCollectionTime` is called right after `EnsureBucket` succeeds, not at the end of `discoverAndSyncBucket`** — it was originally only called on a fully clean discovery cycle, but `discoverFlat`/`discoverSharded` stream and transfer objects (including burn-after-reading deletes) incrementally as they're listed, so a large bucket can make substantial real progress and still return an error from a *later* listing page. That meant the bucket's `tranquila:buckets` index entry / `tranquila:collection:{bucket}` timestamp — which is *all* `ListBuckets` and `internal/api`'s `getBucket` (its 404-vs-200 check) key off — was never written, so an actively-syncing bucket could be permanently invisible in `tranquila status`. Nothing else in this codebase reads this timestamp for sync decisions (confirmed: it's purely a display/existence check), so moving the write earlier is safe. `RunWatcher`'s `initialSync` still runs a full `Run()` (and therefore this call, once per configured bucket) before switching to event-driven mode, so this also closes the gap for minio/sqs watch modes even though `runWatcher`'s own event dispatch never touches this key.

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

  # Cadence for propagate-deletes reconciliation (poll mode + initial
  # catch-up sync; see "Key Design Decisions"). 0 = every Run() call.
  delete-reconcile-interval: 0s

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

`propagate-deletes` reconciliation adds a `seen_at` field to `tranquila:obj:{bucket}:{key}` (written by `TouchSeen`, a plain `HSET` — no counter mutation, not a status transition) and a second per-bucket scan, `ScanStaleObjects`, that pays the same keyspace-wide `SCAN` cost as `ScanPending`/`RebuildStats`. Both `TouchSeen` and `ScanStaleObjects` only run for buckets with `PropagateDeletes` enabled, so buckets that don't opt in pay neither the extra write nor the scan.

### Redis connection pool recovery

A reported "pool never recovers after a Redis/Valkey outage, only a tranquila
restart fixes it" turned out, on investigation, not to be a bug in tranquila's
own code: only one `*redis.Client`/`*state.Store` is created for the process
lifetime (`cmd_sync.go` `Run()`), it's never closed/recreated mid-run, Redis
errors classify as `ClassTransient` (not fatal — `isFatalCycleErr` doesn't
terminate the watch loop over them), and no Redis call in the sync path can
deadlock or leak a goroutine/semaphore slot on failure.

go-redis's own pool (`internal/pool/pool.go`) trips into a degraded mode once
`dialErrorsNum` reaches `PoolSize` consecutive dial failures: further callers
get a cached error immediately, and a single background `tryDial()` goroutine
probes once/sec, resetting the counter on the first successful dial. This is
confirmed **by design and self-healing** by the go-redis maintainers —
[redis/go-redis#3062](https://github.com/redis/go-redis/issues/3062), closed
as "application related, not client related." `MinIdleConns`'s replenishment
(`checkMinIdleConns`) goes through the *same* `dialConn`/`dialErrorsNum` gate,
so setting it does **not** provide an independent recovery path — deliberately
not configured for that reason.

Root cause of the original report was never conclusively identified (address
was a stable Kubernetes Service ClusterIP, ruling out stale pod-IP caching).
Two changes were made anyway, both independently justified regardless of root
cause: `processResults` previously discarded every `MarkFailed`/`MarkSynced`/
`RemoveObject` error (`_ = s.cfg.State.MarkX(...)`) — during an outage this
left zero tranquila-level log evidence beyond go-redis's own low-level pool
spam; `logStateWriteErr` now logs them at `Warn`. `RedisConfig.PoolSize`
(`--redis-pool-size`, default `0` = go-redis's `10 * GOMAXPROCS`) makes the
dial-error trip threshold explicit and predictable rather than tied to the
container's CPU limit, and gives an operator escape hatch to size it smaller
(reaches the steady single-prober recovery state sooner, trading redial log
noise for a small window with no dial attempts at all) or larger.

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
