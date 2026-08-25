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

// rawRedis opens a direct client to the fixture's Redis, for asserting on and
// manipulating keys behind the Store's back.
func rawRedis(t *testing.T, s *stack) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: s.redisAddr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestBucketStatsTrackTransitions covers the counter arithmetic in the Lua
// scripts against a real Redis: an object must be counted once and move between
// status buckets, never double-counted on a repeated or repeated-identical write.
func TestBucketStatsTrackTransitions(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	bucket := "counters-" + t.Name()

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
}

// TestBucketStatsUpsertCountsOnce pins that UpsertObject participates in the
// same counting, since it is a separate write path from the Mark* helpers.
func TestBucketStatsUpsertCountsOnce(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	bucket := "upsert-" + t.Name()

	for range 3 {
		if err := st.UpsertObject(ctx, bucket, "k", state.ObjectState{
			Status:     state.StatusSynced,
			ModifiedAt: time.Now(),
			SyncedAt:   time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.BucketStats(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if want := (state.BucketStats{Total: 1, Synced: 1}); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestBucketStatsRebuildFromLegacyKeyspace is the migration case: a keyspace
// written before counters existed has object records but no counter hashes.
// The first read must seed them from the objects and then serve from them.
func TestBucketStatsRebuildFromLegacyKeyspace(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	rdb := rawRedis(t, s)

	// Write object records exactly as the pre-counter code did: a bare HSET,
	// with no counter maintenance and no marker.
	legacy := map[string]struct{ synced, pending, failed int }{
		"legacy-alpha": {synced: 5, pending: 2, failed: 1},
		"legacy-beta":  {synced: 3},
		// An object key containing colons, to pin the bucket-name parsing.
		"legacy-gamma": {synced: 1},
	}
	for bucket, want := range legacy {
		write := func(status string, n int, keyFmt string) {
			for i := range n {
				k := fmt.Sprintf("tranquila:obj:%s:%s", bucket, fmt.Sprintf(keyFmt, i))
				if err := rdb.HSet(ctx, k, "status", status).Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
		keyFmt := "obj-%d"
		if bucket == "legacy-gamma" {
			keyFmt = "nested:path:with:colons-%d"
		}
		write(state.StatusSynced, want.synced, keyFmt)
		write(state.StatusPending, want.pending, keyFmt+"-p")
		write(state.StatusFailed, want.failed, keyFmt+"-f")
	}

	// Counters and marker are absent, as they would be after an upgrade.
	if err := rdb.Del(ctx, "tranquila:statsbuilt").Err(); err != nil {
		t.Fatal(err)
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
}

// TestRebuildStatsReconcilesDrift covers the operator escape hatch: deleting the
// marker forces counters to be recomputed from the objects, correcting any drift.
func TestRebuildStatsReconcilesDrift(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	rdb := rawRedis(t, s)
	bucket := "drift-" + t.Name()

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
		t.Fatalf("precondition: expected corrupted counter to be served, got %+v", got)
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
}

// TestBucketStatsAreCheapToRead is the point of the whole change: reading stats
// must not scale with the number of objects. It would previously scan the entire
// keyspace once per bucket.
func TestBucketStatsAreCheapToRead(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	rdb := rawRedis(t, s)

	// Seed enough objects that an O(keyspace) implementation is clearly slower
	// than an O(1) one, without making the test itself slow to set up.
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
	if err := rdb.Del(ctx, "tranquila:statsbuilt").Err(); err != nil {
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

	// 20 reads over 20k objects. The scanning implementation needed ~200 round
	// trips per read just to enumerate keys; the counters need one.
	if elapsed > 2*time.Second {
		t.Errorf("20 stats reads took %v — reads still scale with object count", elapsed)
	}
	t.Logf("20 reads over %d objects in %v", objects, elapsed)
}

// TestListBucketsUsesIndex covers the other half of the slow endpoint: listing
// buckets previously scanned for collection keys, which traverses a keyspace
// dominated by object records.
func TestListBucketsUsesIndex(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	rdb := rawRedis(t, s)

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
}

// TestListBucketsSeedsIndexFromLegacyKeyspace covers the upgrade path: a
// keyspace with collection keys but no index must still list correctly, and
// seed the index so the scan happens at most once.
func TestListBucketsSeedsIndexFromLegacyKeyspace(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	st := s.store(t)
	rdb := rawRedis(t, s)

	// Collection keys exactly as the pre-index code wrote them, with no index.
	want := []string{"legacy-idx-a", "legacy-idx-b"}
	for _, b := range want {
		if err := rdb.Set(ctx, "tranquila:collection:"+b,
			time.Now().UTC().Format(time.RFC3339Nano), 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.Del(ctx, "tranquila:buckets").Err(); err != nil {
		t.Fatal(err)
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
}
