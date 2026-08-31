# Architecture

How tranquila is put together, and why. For usage see the
[README](../README.md); for the end-to-end test suite see
[e2e/README.md](../e2e/README.md).

- [Package layout](#package-layout)
- [The sync pipeline](#the-sync-pipeline)
- [Concurrency and memory](#concurrency-and-memory)
- [Redis state](#redis-state)
- [Watch mode](#watch-mode)
- [Resilience](#resilience)
- [Burn-after-reading](#burn-after-reading)
- [Management API and probes](#management-api-and-probes)
- [Telemetry](#telemetry)
- [Testing strategy](#testing-strategy)

## Package layout

| Package | Responsibility |
| --- | --- |
| `main.go`, `cmd_sync.go`, `cmd_status.go` | CLI wiring. kong + kong-yaml; config is bound after parse. |
| `config/` | YAML config types, including structured bucket mappings. |
| `internal/sync/` | The engine: discovery, the worker pool, watch loops, progress. |
| `internal/storage/` | S3 access, error classification, rate limiting. Works against any S3-compatible endpoint. |
| `internal/state/` | Redis: per-object records, per-bucket counters, bucket index. |
| `internal/watcher/` | The `Watcher` interface and its poll / MinIO / SQS implementations. |
| `internal/api/` | Management HTTP API and Kubernetes probes. |
| `internal/telemetry/` | OpenTelemetry meter setup (Prometheus or OTLP). |

Dependencies point inwards: `sync` depends on `storage` and `state`, never the
reverse. Cross-package seams are narrow interfaces declared by the *consumer*
(`objectDeleter`, `destinationVerifier`, `rateAdjuster`, `minioNotifier`), which
is what lets the engine be tested without a real S3 or Redis.

## The sync pipeline

One cycle, per source bucket, runs concurrently with the other buckets:

```
resolveBuckets ──▶ EnsureBucket(destination)
                        │
                        ▼
              ListObjectsPage  ◀──────────────┐   batches of --discovery-batch-size
                        │                     │
                        ▼                     │
              needsSync? ──▶ MarkPending      │
                        │                     │
                        ▼                     │
                  pool.submit(Job)            │
                        │                     │
              ┌─────────┴─────────┐           │
              ▼                   ▼           │
         transfer()          transfer()  …    │   --workers goroutines
              │                   │           │
              └─────────┬─────────┘           │
                        ▼                     │
                 processResults               │
                  MarkSynced /                │
                  MarkFailed                  │
                        │                     │
                  batchDone.Wait() ───────────┘
                        │
                        ▼
              SetCollectionTime
```

`transfer()` is `GetObject` → `PutObject` (with a CRC32 checksum) → `HeadObject`
to verify the destination size, and, when enabled, the burn-after-reading step.

Objects are skipped when Redis already records them as synced, unless
`--check-sizes` is set, which re-queues any object whose destination size
differs from the source.

### Prefix-sharded discovery

`ListObjectsPage` above is the default, flat path: one bucket-wide
`ListObjectsV2` scan. Some backends cannot answer that for a large bucket at
all — observed against a real MinIO deployment (~295K objects), where the call
hung with zero response, even with every proxy removed from the path. Since
the same bucket browses instantly folder-by-folder in the MinIO/S3 console,
`storage.Client.ListObjectsTree` discovers the same way: a recursive,
`/`-delimited listing (`internal/storage/s3.go`'s `listObjectsTree`, bounded
to `shardedDiscoveryConcurrency` concurrent prefixes) instead of one flat
scan. `discoverAndSyncBucket` picks the path: `cfg.ShardedDiscovery` (the
`sharded-discovery` bucket flag) skips flat entirely; otherwise a flat listing
that exhausts its retries with a transient/throttle error
(`isShardableListErr`) falls back to the sharded walk automatically for the
rest of that cycle. Both paths funnel through the same `discoverObject` (per-
object `needsSync`/`MarkPending`/job-submission logic), so nothing about
job semantics differs between them — only how objects are discovered. See the
README's "Prefix-Sharded Discovery" section for the operator-facing view.

## Concurrency and memory

Three bounds keep a multi-million-object bucket from exhausting memory:

- **Discovery is batched.** `discoverAndSyncBucket` lists at most
  `--discovery-batch-size` objects (default 100 000), queues them, and then
  **waits for that batch to drain** (`batchDone.Wait()`) before fetching the next
  page. Listings never accumulate in memory, and sync starts before the listing
  finishes.
- **The worker pool is fixed.** `--workers` goroutines consume a `jobs` channel
  buffered at `2 × workers`.
- **Buckets are bounded too.** A semaphore of `--workers` caps how many buckets
  discover concurrently.

Transfers run on a context deliberately detached from the signal context, so
in-flight work survives `SIGTERM` — but bounded by `transferGrace` (30 min), so a
congestion-degraded rate limiter cannot pace an uncancellable transfer past the
pod's `terminationGracePeriodSeconds`.

## Redis state

**`SCAN ... MATCH` filters server-side *after* iterating.** `COUNT` bounds the
keys examined per call, not the keys returned, so a pattern scan costs
O(whole keyspace) however few keys match. In a keyspace dominated by object
records — ~1.1M in production — anything on a request path must avoid scanning.

| Key | Type | Purpose |
| --- | --- | --- |
| `tranquila:obj:{bucket}:{key}` | hash | Per-object `status`, `modified_at`, `synced_at`. |
| `tranquila:collection:{bucket}` | string | Timestamp of the last completed discovery. |
| `tranquila:stats:{bucket}` | hash | Maintained counters: `total`, `synced`, `pending`, `failed`. |
| `tranquila:buckets` | set | Index of discovered buckets. |
| `tranquila:statsbuilt` | string | Marker that counters have been seeded. |

Bucket names cannot contain `:` but object keys can, so parsing a bucket out of
an object key splits on the **first** `:` after the prefix.

Counters are updated **atomically with the status write** by two Lua scripts
(`setStatusScript`, `deleteObjectScript`), which read the previous status to
decrement the correct field. This makes `BucketStats` a single `HGETALL` and
`ListBuckets` a single `SMEMBERS`, turning a request that once cost ~327 000
Redis round trips into ~86.

The trade is that **every write path must go through `setStatus` /
`deleteObjectScript`**, or counters drift. Two safety nets:

- `RebuildStats` recomputes everything from the object records in a **single**
  pass — not one per bucket — and reconciles both the counters and the bucket
  index.
- It runs automatically when `tranquila:statsbuilt` is absent, which seeds an
  upgraded keyspace on first read. Deleting that marker forces a reconcile; that
  is the operator escape hatch.

Because the scripts run via `EVALSHA`, engine compatibility is tested rather
than assumed — see [Testing strategy](#testing-strategy).

## Watch mode

`--watch` selects one of three backends. They differ only in how changes are
noticed; the transfer path is identical.

| Mode | Mechanism |
| --- | --- |
| `poll` (default) | Repeats the full cycle, sleeping `--watch-interval`. Works against any endpoint. |
| `minio` | `ListenBucketNotification` SSE stream, reusing the source credentials. |
| `sqs` | Long-polls an SQS queue for S3 event notifications. |

Event-driven modes run a **full initial sync first**, so changes made while the
process was down are not missed, then switch to consuming events.

Two deliberate choices: tranquila never configures SNS/SQS notification targets
on buckets — it only consumes what the operator has set up — and the SQS watcher
**always deletes messages**, even unparseable ones, because sync is idempotent
via Redis and a poison message stuck in the queue is worse than a missed event.

## Resilience

Watch mode must never exit on a transient fault: `os.Exit` also kills the
in-process management server, so `/healthz` stops answering and Kubernetes
restarts the pod. A flaky gateway would become a crash-loop.

Four layers, innermost first:

| Layer | Mechanism |
| --- | --- |
| Classification | `storage.Classify` → `ClassOK` / `ClassTransient` / `ClassThrottle` / `ClassPermanent`. HTTP status first, then the smithy error code. |
| Per-call retry | The AWS SDK retryer at `s3MaxAttempts` (5); `listPageWithRetry` at 8 attempts, 30s delay cap, jittered, each attempt bounded by a 60s `listAttemptTimeout` (a hung server otherwise never even reaches a second attempt — see below). |
| Per-cycle retry | `runWatch` / `initialSync` back off with jittered exponential delay and never return on a transient error. |
| Rate degradation | AIMD per endpoint, fed one signal per S3 call. |

**Status is checked before error code** because a gateway's own HTML error page
fails XML decoding, so the SDK produces no `APIError` at all — only the status.
Classifying on the code alone missed exactly that case in production.

**Unknown errors are transient.** A novel fault must never be mistaken for
misconfiguration.

**Only permanent errors terminate**, and only when *every* failure in the cycle
is permanent (`isFatalCycleErr` fans out one level of `errors.Join`). A mixed
cycle counts as transient, so a flaky endpoint cannot masquerade as a bad
config. `Run` returns `errors.Join` rather than the first error so a
partial-failure cycle keeps every bucket's failure visible.

**One-shot mode is unchanged** — it still exits non-zero, so a Kubernetes `Job`
reports failure. The asymmetry falls out of the retry loop living inside
`runWatch`.

### Adaptive rate limiting

`internal/storage/aimd.go` implements additive-increase/multiplicative-decrease
per endpoint, fed from `recordOp` — the single choke point every S3 call passes
through, so it reacts mid-cycle rather than after a whole cycle fails.

- Halve after `--endpoint-fail-threshold` consecutive transient failures, floored
  at 1 call/sec.
- Halve on the **first** throttle (503, `SlowDown`, 429): that is unambiguous
  back-pressure, not ambiguous failure.
- Restore 10% of the configured base per 20 consecutive healthy calls.
- A **permanent** error counts as *healthy* for pacing: the endpoint answered, so
  it is not congested. That failure is the syncer's problem, not the pacer's.

Design notes:

- **Only endpoints with a configured rate limit degrade.** Unlimited has no
  ceiling to halve, and inventing one would throttle a healthy endpoint.
- **The limiter is always constructed** (`rate.Inf` when unlimited) so the
  pointer is never nil and never swapped; only `SetLimit` mutates it. Note
  `rate.Inf` is `Limit(math.MaxFloat64)`, *not* IEEE infinity — compare with
  `== rate.Inf`.
- **AIMD is event-counted, never time-based**, so the control loop is
  deterministic under test: no clock injection, no sleeps.
- Source and destination are paced independently, so a sick destination throttles
  writes without slowing source reads.

## Burn-after-reading

Deleting from the source is irreversible, so it happens only behind verification.

Normal path — the object is uploaded in this run:

1. `PutObject` returns the CRC32 the upload computed.
2. `HeadObject` on the destination returns the stored size and CRC32.
3. Size must match the source (skipped when the source size is unknown).
4. **The two checksums must both be present and equal.** Otherwise the sync
   fails and the source object is kept.

Verify-and-delete path — the object was already synced before the mode was
enabled, so there is no upload to checksum:

1. `HeadObject` on the destination confirms the size matches (skipped when the
   job's recorded size is unknown).
2. **ETag fast path.** Every S3 object carries an ETag; for a single-part
   upload it is the plain MD5 hex digest of the content, so identical content
   produces an identical ETag on any S3-compatible backend — verifiable
   without reading either object. The source's ETag comes from discovery
   listing (`Job.SrcETag`, free); the destination's from the `HeadObject` call
   above. `storage.SinglePartMD5` recognizes and rejects the one case where
   ETag isn't comparable: a multipart upload's ETag is `md5-of-part-md5s` plus
   a `-partCount` suffix, not a hash of the content at all.
   - **Both parse as single-part and match** → verified, no download.
   - **Both parse as single-part and differ** → refuse immediately, no
     download. (An encrypted object's ETag is a hash of ciphertext, which
     differs by encryption context even for identical plaintext — this can
     produce a false *mismatch* under SSE-KMS, never a false match, so it
     fails toward keeping data, not deleting it.)
   - **Either is multipart or unparseable** → fall back to step 3.
3. **Content fallback.** Both objects are downloaded and hashed
   (`crc32Checksum`, streamed through `crc32.NewIEEE`, not buffered). Computing
   both from content, rather than trusting any stored checksum, sidesteps the
   same composite-checksum problem the ETag check has to guard against.
4. The two checksums (from whichever step verified) must be equal, or the sync
   fails and the source is kept — this is what catches a destination silently
   overwritten with same-size content, which the size check alone would miss.

No re-upload is performed either way. For a single-part object this path costs
two `HeadObject` calls (both cheap, the first one already required for the size
check); a multipart object pays the full double-read that always accepted.

`--dry-run` logs every planned deletion and performs none — including logging
that it *would refuse* to delete on a checksum mismatch, so a rehearsal surfaces
the same objections a real run would.

## Management API and probes

`GET /api/v1/buckets`, `/api/v1/buckets/{name}`, `/api/v1/sync`, plus `/healthz`
and `/readyz`, served from a goroutine in the same process as the syncer.

- **`/healthz` is liveness** and checks no dependencies: an unreachable Redis
  must not trigger a pod restart.
- **`/readyz` is readiness** and pings Redis.
- **`/readyz` stays green while an endpoint is rate-degraded.** Degraded
  operation is the designed correct response to a flaky endpoint; reporting it as
  unready sheds no load — nothing fronts this pod — while stalling rolling
  updates against `progressDeadlineSeconds`. Degradation is surfaced as data on
  `/api/v1/sync` and as metrics instead.

The server sets Read/ReadHeader/Write/Idle timeouts so a slow client cannot hold
a connection open indefinitely.

## Telemetry

OpenTelemetry metrics, exported via Prometheus (default) or OTLP. The full metric
list is in the [README](../README.md#metrics).

`Config.Meter` is optional in both `sync` and `storage`; because `metric.Meter`
is an interface whose zero value is nil, both packages fall back to
`noop.Meter{}` rather than panicking.

## Testing strategy

| Scope | Command | Notes |
| --- | --- | --- |
| Unit | `go test ./...` | Table-driven, stdlib `testing`. No containers, no sleeps. |
| End-to-end | `cd e2e && go test ./...` | Separate module; containers via testcontainers. |

The e2e module is separate on purpose: testcontainers pulls ~90 transitive
dependencies (moby, containerd) that must stay out of the production module graph
and out of `govulncheck`'s scope. A submodule can still import `internal/...`
because that restriction is lexical on import paths, not module-scoped.

Determinism is a design constraint, not an accident:

- `runWatch` takes an injectable `cycleFn` **and** an injectable `sleeper`, so
  backoff is asserted without real delays.
- AIMD is event-counted, so its tests need no clock.
- The state layer's Lua scripts run against **Redis 7, Valkey 8 and Valkey 9** on
  every run, because `EVALSHA` is where a Redis-compatible fork is most likely to
  diverge.

Fault injection uses two tools because they work at different layers: Toxiproxy
is L4 and cannot emit an HTTP status at all, so an in-process
`httputil.ReverseProxy` injects 5xx responses — including non-XML gateway pages,
the case that forces status-first classification.
