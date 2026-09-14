# 0002. Sharded discovery persists a per-prefix resume point

* Status: Accepted
* Date: 2026-09-14

## Context

Prefix-sharded discovery walks a bucket folder-by-folder with a `/`-delimited `ListObjectsV2`,
which narrows what each call has to answer. On one production bucket — ~3.4M objects, keyed
`YYYY/M/D/file`, ~190 day-prefixes of ~18 pages each — that was still not enough. Individual
pages time out even after the per-attempt deadline escalates to its cap (ADR-less change, PR #49).

The reason the walk never finished is not the timeouts themselves but what happened after one.
A prefix whose listing failed was abandoned, and `processOne`'s continuation token was a
function-local variable that went out of scope with it. The next cycle re-listed that prefix from
its first page, hit the same slow page, and failed again. **Forward progress per cycle was zero**,
and no amount of retrying, escalating or waiting changed that.

The arithmetic is also unforgiving on its own: ~3,400 pages at a measured ~9s per page is ~2h of
listing at concurrency 4 even with nothing failing. `MaxKeys` is never set, and shrinking it was
measured not to help — a day-prefix page costs ~8–9s even at `MaxKeys=1`, so there is no
page-size lever.

An earlier iteration of this idea was deferred because a "this prefix is complete" memo is hard
to invalidate: the current day's partition keeps growing, and burn-after-reading deletes keys
underneath a prefix already marked done.

## Decision

We persist a **resume point, not a completion memo**: the continuation token for one
`(bucket, prefix)`, in `tranquila:ckpt:{bucket}:{prefix}`. A prefix that fails keeps its token and
resumes there next cycle. A prefix that **completes clears** its token, so the next cycle lists it
in full again.

Clear-on-completion is load-bearing rather than tidiness. It is what gives an object whose
*transfer* failed a chance to be rediscovered — resuming permanently past such an object would
strand it forever. It also dissolves the invalidation problem that deferred this before: there is
no completion state to invalidate, and on a burn-after-reading bucket the re-list is cheap because
the bucket drains itself, so a completed prefix is empty by the time it is walked again.

**Only leaf prefixes are checkpointed.** A delimited listing returns objects and `CommonPrefixes`
interleaved in one paginated lexicographic stream, so resuming past page *k* would also skip the
sub-prefixes page *k* carried — stranding whole subtrees with no error and no metric. A prefix is
therefore checkpointed only while it has produced no sub-prefixes; the moment one appears the
checkpoint is dropped and that prefix is not checkpointed again for the rest of the walk. This
covers exactly the expensive case (a leaf day-folder spanning many pages) and degrades to the
previous behaviour for the shallow index levels that fan out.

**The token is persisted by the single consumer goroutine, strictly after `onPage` has accepted
its page.** `listObjectsTree` already funnelled every page through one consumer so that
`discoverAndSyncBucket`'s unsynchronized counters were safe; that serialization is now required
for correctness too. Because the `pages` channel is unbuffered, a prefix's page *k+1* cannot be
sent until the consumer has finished page *k*'s `onPage` **and** its checkpoint write, which makes
the per-prefix ordering total with no extra synchronization. Persisting from the producer instead
was rejected: the token could reach Redis before — or entirely without — the objects it skips
ever reaching `onPage`.

Checkpoints carry a TTL (`--discovery-checkpoint-ttl`, default 24h), refreshed on every save. An
abandoned prefix expires and is listed from the start again, which is always correct and merely
slower.

Checkpoint failures are never fatal. A store blip degrades that prefix to exactly the
pre-checkpointing behaviour.

## Consequences

* The walk makes monotonic progress. Each cycle banks the pages it managed instead of discarding
  them. This converts *never completes* into *completes over N cycles*.
* **It does not make pages faster.** The timeouts are unchanged; this only stops the work being
  thrown away. On a bucket where nearly every page times out, progress is ~1 page per prefix per
  cycle and the backfill takes a long time. Tuning `--sharded-discovery-concurrency` down remains
  necessary — and checkpointing is what makes lowering it safe, since a slower walk that still
  restarts every cycle is worse, not better.
* A new stall mode: a permanently unanswerable page parks its prefix, because we now resume onto
  it every cycle rather than re-listing the pages before it. Detected within a single cycle
  (resumed, zero pages listed, then failed), logged at `Error` level. It is **not** auto-skipped:
  continuation tokens are opaque, so "skip one page" has no expression in the S3 API, and
  discarding the checkpoint is strictly worse than keeping it. The TTL is the bounded escape, so
  the feature can never be worse than the previous behaviour for longer than one TTL.
* If the process dies between a checkpoint save and the transfer completing, those objects are not
  re-listed until that prefix completes and clears its checkpoint. `onPage` accepting a page means
  `MarkPending` was written and a job was submitted, not that the transfer finished, and nothing
  re-queues pending records today (`state.ScanPending` exists but has no production callers).
  Bounded by one full pass of the prefix; a requeue pass built on `ScanPending` is the follow-up.
* Redis becomes a dependency of discovery efficiency, not just of bookkeeping. Contained by making
  every checkpoint error non-fatal.
* Continuation tokens are assumed stable across processes and days. Both S3 and MinIO encode them
  statelessly, but this is a property of those backends rather than a guarantee in the API
  contract. A backend upgrade that changed the encoding would surface as list errors on resume,
  which is the already-handled abandon-this-prefix path, and the checkpoint expires.
* Disabling the feature later needs no cleanup: nothing reads `tranquila:ckpt:*`, and the keys
  self-delete within one TTL.
