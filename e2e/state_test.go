package e2e

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jabbrwcky/tranquila/internal/state"
)

// The state layer maintains its counters with Lua scripts (EVALSHA), so every
// test here runs against each supported engine — Redis and the Valkey fork —
// rather than assuming the fork is bug-for-bug compatible.

// TestBucketStatsTrackTransitions covers the counter arithmetic in the Lua
// scripts against a real engine: an object must be counted once and move
// between status buckets, never double-counted on a repeated write.
func TestBucketStatsTrackTransitions(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, _ *redis.Client) {
		ctx := context.Background()
		const bucket = "counters"

		assert := func(what string, want state.BucketStats) {
			t.Helper()
			got, err := st.BucketStats(ctx, bucket)
			if err != nil {
				t.Fatalf("BucketStats after %s: %v", what, err)
			}
			if got != want {
				t.Errorf("after %s: got %+v, want %+v", what, got, want)
			}
		}

		assert("no objects", state.BucketStats{})

		if err := st.MarkPending(ctx, bucket, "a", time.Now()); err != nil {
			t.Fatal(err)
		}
		assert("first pending", state.BucketStats{Total: 1, Pending: 1})

		// A repeated identical transition must not double-count.
		if err := st.MarkPending(ctx, bucket, "a", time.Now()); err != nil {
			t.Fatal(err)
		}
		assert("repeated pending", state.BucketStats{Total: 1, Pending: 1})

		if err := st.MarkSynced(ctx, bucket, "a"); err != nil {
			t.Fatal(err)
		}
		assert("pending -> synced", state.BucketStats{Total: 1, Synced: 1})

		if err := st.MarkPending(ctx, bucket, "b", time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkFailed(ctx, bucket, "b"); err != nil {
			t.Fatal(err)
		}
		assert("second object failed", state.BucketStats{Total: 2, Synced: 1, Failed: 1})

		// Burn-after-reading removes the record entirely.
		if err := st.RemoveObject(ctx, bucket, "a"); err != nil {
			t.Fatal(err)
		}
		assert("synced object removed", state.BucketStats{Total: 1, Failed: 1})

		// Removing a key that is already gone must not drive counters negative.
		if err := st.RemoveObject(ctx, bucket, "a"); err != nil {
			t.Fatal(err)
		}
		assert("repeated removal", state.BucketStats{Total: 1, Failed: 1})
	})
}

// TestBucketStatsUpsertCountsOnce pins that UpsertObject participates in the
// same counting, since it is a separate write path from the Mark* helpers.
func TestBucketStatsUpsertCountsOnce(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, _ *redis.Client) {
		ctx := context.Background()
		for range 3 {
			if err := st.UpsertObject(ctx, "upsert", "k", state.ObjectState{
				Status:     state.StatusSynced,
				ModifiedAt: time.Now(),
				SyncedAt:   time.Now(),
			}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := st.BucketStats(ctx, "upsert")
		if err != nil {
			t.Fatal(err)
		}
		if want := (state.BucketStats{Total: 1, Synced: 1}); got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
}

// TestBucketStatsRebuildFromLegacyKeyspace is the migration case: a keyspace
// written before counters existed has object records but no counter hashes.
// The first read must seed them from the objects and then serve from them.
func TestBucketStatsRebuildFromLegacyKeyspace(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, rdb *redis.Client) {
		ctx := context.Background()

		// Write object records exactly as the pre-counter code did: a bare HSET,
		// with no counter maintenance and no marker.
		legacy := map[string]struct{ synced, pending, failed int }{
			"legacy-alpha": {synced: 5, pending: 2, failed: 1},
			"legacy-beta":  {synced: 3},
			// An object key containing colons, to pin the bucket-name parsing.
			"legacy-gamma": {synced: 1},
		}
		for bucket, want := range legacy {
			keyFmt := "obj-%d"
			if bucket == "legacy-gamma" {
				keyFmt = "nested:path:with:colons-%d"
			}
			write := func(status string, n int, format string) {
				for i := range n {
					k := fmt.Sprintf("tranquila:obj:%s:%s", bucket, fmt.Sprintf(format, i))
					if err := rdb.HSet(ctx, k, "status", status).Err(); err != nil {
						t.Fatal(err)
					}
				}
			}
			write(state.StatusSynced, want.synced, keyFmt)
			write(state.StatusPending, want.pending, keyFmt+"-p")
			write(state.StatusFailed, want.failed, keyFmt+"-f")
		}

		for bucket, want := range legacy {
			got, err := st.BucketStats(ctx, bucket)
			if err != nil {
				t.Fatalf("BucketStats(%s): %v", bucket, err)
			}
			expect := state.BucketStats{
				Total:   int64(want.synced + want.pending + want.failed),
				Synced:  int64(want.synced),
				Pending: int64(want.pending),
				Failed:  int64(want.failed),
			}
			if got != expect {
				t.Errorf("%s: got %+v, want %+v", bucket, got, expect)
			}
		}

		// The rebuild must have persisted counters and set the marker, so later
		// reads are served without scanning again.
		built, err := rdb.Exists(ctx, "tranquila:statsbuilt").Result()
		if err != nil {
			t.Fatal(err)
		}
		if built != 1 {
			t.Error("rebuild did not set the built marker; every read would rescan")
		}
		n, err := rdb.HGet(ctx, "tranquila:stats:legacy-alpha", "synced").Int64()
		if err != nil {
			t.Fatalf("counters not persisted: %v", err)
		}
		if n != 5 {
			t.Errorf("persisted synced counter = %d, want 5", n)
		}
	})
}

// TestRebuildStatsReconcilesDrift covers the operator escape hatch: recomputing
// counters from the object records corrects any drift.
func TestRebuildStatsReconcilesDrift(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, rdb *redis.Client) {
		ctx := context.Background()
		const bucket = "drift"

		if err := st.MarkSynced(ctx, bucket, "only-object"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.BucketStats(ctx, bucket); err != nil { // seeds the marker
			t.Fatal(err)
		}

		// Corrupt the counters behind the Store's back.
		if err := rdb.HSet(ctx, "tranquila:stats:"+bucket, "synced", 999).Err(); err != nil {
			t.Fatal(err)
		}
		got, err := st.BucketStats(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		if got.Synced != 999 {
			t.Fatalf("precondition: expected the corrupted counter to be served, got %+v", got)
		}

		if err := st.RebuildStats(ctx); err != nil {
			t.Fatalf("RebuildStats: %v", err)
		}
		got, err = st.BucketStats(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		if want := (state.BucketStats{Total: 1, Synced: 1}); got != want {
			t.Errorf("after reconcile: got %+v, want %+v", got, want)
		}
	})
}

// TestBucketStatsAreCheapToRead is the point of the counters: reading stats must
// not scale with the number of objects. It previously scanned the entire
// keyspace once per bucket.
func TestBucketStatsAreCheapToRead(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, rdb *redis.Client) {
		ctx := context.Background()

		const objects = 20_000
		pipe := rdb.Pipeline()
		for i := range objects {
			pipe.HSet(ctx, fmt.Sprintf("tranquila:obj:perf-bucket:obj-%d", i), "status", state.StatusSynced)
			if i%1000 == 0 {
				if _, err := pipe.Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatal(err)
		}

		// First read pays for the one-off rebuild.
		if _, err := st.BucketStats(ctx, "perf-bucket"); err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		for range 20 {
			got, err := st.BucketStats(ctx, "perf-bucket")
			if err != nil {
				t.Fatal(err)
			}
			if got.Total != objects {
				t.Fatalf("Total = %d, want %d", got.Total, objects)
			}
		}
		elapsed := time.Since(start)

		// The scanning implementation needed ~200 round trips per read just to
		// enumerate keys; the counters need one.
		if elapsed > 2*time.Second {
			t.Errorf("20 stats reads took %v — reads still scale with object count", elapsed)
		}
		t.Logf("20 reads over %d objects in %v", objects, elapsed)
	})
}

// TestListBucketsUsesIndex covers the other half of the slow endpoint: listing
// buckets previously scanned for collection keys, traversing a keyspace
// dominated by object records.
func TestListBucketsUsesIndex(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, rdb *redis.Client) {
		ctx := context.Background()
		want := []string{"idx-a", "idx-b", "idx-c"}
		for _, b := range want {
			if err := st.SetCollectionTime(ctx, b, time.Now()); err != nil {
				t.Fatal(err)
			}
		}

		got, err := st.ListBuckets(ctx)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("ListBuckets = %v, want %v", got, want)
		}

		// The index must be maintained by the write, not rebuilt on every read.
		n, err := rdb.SCard(ctx, "tranquila:buckets").Result()
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(want)) {
			t.Errorf("index holds %d buckets, want %d", n, len(want))
		}
	})
}

// TestListBucketsSeedsIndexFromLegacyKeyspace covers the upgrade path: a
// keyspace with collection keys but no index must still list correctly, and
// seed the index so the scan happens at most once.
func TestListBucketsSeedsIndexFromLegacyKeyspace(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, rdb *redis.Client) {
		ctx := context.Background()
		want := []string{"legacy-idx-a", "legacy-idx-b"}
		for _, b := range want {
			if err := rdb.Set(ctx, "tranquila:collection:"+b,
				time.Now().UTC().Format(time.RFC3339Nano), 0).Err(); err != nil {
				t.Fatal(err)
			}
		}

		got, err := st.ListBuckets(ctx)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("ListBuckets = %v, want %v", got, want)
		}
		if n, err := rdb.SCard(ctx, "tranquila:buckets").Result(); err != nil || n != int64(len(want)) {
			t.Errorf("index not seeded: card=%d err=%v", n, err)
		}
	})
}

// TestScriptsUseEvalsha pins the mechanism the counters depend on: go-redis
// caches each script by SHA and calls EVALSHA, falling back to EVAL only on
// NOSCRIPT. An engine that mishandled the script cache would silently degrade
// or fail here rather than in production.
func TestScriptsUseEvalsha(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, rdb *redis.Client) {
		ctx := context.Background()

		if err := st.MarkPending(ctx, "sha", "k", time.Now()); err != nil {
			t.Fatalf("first write (loads the script): %v", err)
		}
		// A second write must hit the cached SHA rather than re-loading.
		if err := st.MarkSynced(ctx, "sha", "k"); err != nil {
			t.Fatalf("second write (cached EVALSHA): %v", err)
		}

		// Flushing the cache must not break subsequent writes: the client has to
		// recover via the NOSCRIPT fallback.
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			t.Fatalf("SCRIPT FLUSH: %v", err)
		}
		if err := st.MarkFailed(ctx, "sha", "k"); err != nil {
			t.Fatalf("write after SCRIPT FLUSH: %v", err)
		}

		got, err := st.BucketStats(ctx, "sha")
		if err != nil {
			t.Fatal(err)
		}
		if want := (state.BucketStats{Total: 1, Failed: 1}); got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
}

// TestTouchSeenDoesNotAffectCounters pins that TouchSeen (propagate-deletes'
// "still present in source" marker) is a plain field write, not a status
// transition — it must never move an object's bucket counters.
func TestTouchSeenDoesNotAffectCounters(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, _ *redis.Client) {
		ctx := context.Background()
		const bucket = "seen"

		if err := st.MarkSynced(ctx, bucket, "a"); err != nil {
			t.Fatal(err)
		}
		before, err := st.BucketStats(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}

		if err := st.TouchSeen(ctx, bucket, "a", time.Now()); err != nil {
			t.Fatalf("TouchSeen: %v", err)
		}
		// TouchSeen on a key with no prior record must not create a phantom
		// counted object either.
		if err := st.TouchSeen(ctx, bucket, "never-synced", time.Now()); err != nil {
			t.Fatalf("TouchSeen on new key: %v", err)
		}

		after, err := st.BucketStats(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Errorf("TouchSeen changed counters: before=%+v after=%+v", before, after)
		}
	})
}

// TestScanStaleObjects covers the candidate selection propagate-deletes
// reconciliation relies on: only synced objects whose seen_at predates the
// cutoff (or is missing) are reported; pending/failed objects and objects
// seen after the cutoff are not.
func TestScanStaleObjects(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, _ *redis.Client) {
		ctx := context.Background()
		const bucket = "stale"
		cutoff := time.Now()

		if err := st.MarkSynced(ctx, bucket, "stale-no-seen"); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkSynced(ctx, bucket, "stale-old-seen"); err != nil {
			t.Fatal(err)
		}
		if err := st.TouchSeen(ctx, bucket, "stale-old-seen", cutoff.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkSynced(ctx, bucket, "fresh"); err != nil {
			t.Fatal(err)
		}
		if err := st.TouchSeen(ctx, bucket, "fresh", cutoff.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkPending(ctx, bucket, "pending-no-seen", time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkFailed(ctx, bucket, "failed-old-seen"); err != nil {
			t.Fatal(err)
		}
		if err := st.TouchSeen(ctx, bucket, "failed-old-seen", cutoff.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}

		got, err := st.ScanStaleObjects(ctx, bucket, cutoff)
		if err != nil {
			t.Fatalf("ScanStaleObjects: %v", err)
		}
		slices.Sort(got)
		want := []string{"stale-no-seen", "stale-old-seen"}
		if !slices.Equal(got, want) {
			t.Errorf("ScanStaleObjects() = %v, want %v", got, want)
		}
	})
}

// TestScanAgedSyncedObjects covers the candidate selection burn-after-reading
// min-age reconciliation relies on: only synced objects whose modified_at is
// at or before the cutoff are reported; pending/failed objects and objects
// modified after the cutoff are not.
func TestScanAgedSyncedObjects(t *testing.T) {
	forEachEngine(t, func(t *testing.T, st *state.Store, _ *redis.Client) {
		ctx := context.Background()
		const bucket = "aged"
		now := time.Now()
		before := now.Add(-7 * 24 * time.Hour)

		markSyncedAt := func(key string, modifiedAt time.Time) {
			t.Helper()
			if err := st.MarkPending(ctx, bucket, key, modifiedAt); err != nil {
				t.Fatal(err)
			}
			if err := st.MarkSynced(ctx, bucket, key); err != nil {
				t.Fatal(err)
			}
		}

		markSyncedAt("old-synced", now.Add(-30*24*time.Hour)) // well before cutoff -> candidate
		markSyncedAt("recent-synced", now.Add(-time.Hour))    // after cutoff -> not a candidate
		markSyncedAt("at-boundary", before)                   // exactly at cutoff -> candidate (inclusive)

		if err := st.MarkPending(ctx, bucket, "old-pending", now.Add(-30*24*time.Hour)); err != nil {
			t.Fatal(err)
		}

		if err := st.MarkPending(ctx, bucket, "old-failed", now.Add(-30*24*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkFailed(ctx, bucket, "old-failed"); err != nil {
			t.Fatal(err)
		}

		got, err := st.ScanAgedSyncedObjects(ctx, bucket, before)
		if err != nil {
			t.Fatalf("ScanAgedSyncedObjects: %v", err)
		}
		slices.Sort(got)
		want := []string{"at-boundary", "old-synced"}
		if !slices.Equal(got, want) {
			t.Errorf("ScanAgedSyncedObjects() = %v, want %v", got, want)
		}
	})
}
