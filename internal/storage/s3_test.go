package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestListAttemptTimedOut(t *testing.T) {
	live := context.Background()

	expired, cancelExpired := context.WithTimeout(context.Background(), 0)
	defer cancelExpired()

	cancelled, cancelFn := context.WithCancel(context.Background())
	cancelFn()

	tests := []struct {
		name    string
		outer   context.Context
		attempt context.Context
		err     error
		want    bool
	}{
		{
			name:    "self_imposed_timeout_is_retryable",
			outer:   live,
			attempt: expired,
			err:     context.DeadlineExceeded,
			want:    true,
		},
		{
			name:    "outer_ctx_cancelled_not_retryable",
			outer:   cancelled,
			attempt: cancelled,
			err:     context.DeadlineExceeded,
			want:    false,
		},
		{
			name:    "non_deadline_error_not_retryable",
			outer:   live,
			attempt: expired,
			err:     errors.New("connection refused"),
			want:    false,
		},
		{
			name:    "attempt_ctx_still_live_not_retryable",
			outer:   live,
			attempt: live,
			err:     context.DeadlineExceeded,
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := listAttemptTimedOut(tc.outer, tc.attempt, tc.err); got != tc.want {
				t.Errorf("listAttemptTimedOut() = %v, want %v", got, tc.want)
			}
		})
	}
}

// treeNode is one fake bucket "folder" for TestListObjectsTree and friends.
type treeNode struct {
	objs        []Object
	subPrefixes []string
}

// fakeTree returns a listDelimitedFn serving a fixed, single-page-per-prefix
// tree, tracking concurrent call count via concurrent/maxConcurrent (both
// optional — pass nil to skip tracking).
func fakeTree(t *testing.T, tree map[string]treeNode, delay time.Duration, current, maxConcurrent *int32) listDelimitedFn {
	t.Helper()
	return func(ctx context.Context, prefix string, token *string) ([]Object, []string, *string, error) {
		if current != nil {
			n := atomic.AddInt32(current, 1)
			defer atomic.AddInt32(current, -1)
			for {
				old := atomic.LoadInt32(maxConcurrent)
				if n <= old || atomic.CompareAndSwapInt32(maxConcurrent, old, n) {
					break
				}
			}
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		node, ok := tree[prefix]
		if !ok {
			return nil, nil, nil, fmt.Errorf("fakeTree: unexpected prefix %q", prefix)
		}
		return node.objs, node.subPrefixes, nil, nil
	}
}

func TestListObjectsTree(t *testing.T) {
	tree := map[string]treeNode{
		"":     {objs: []Object{{Key: "root.txt"}}, subPrefixes: []string{"a/", "b/"}},
		"a/":   {objs: []Object{{Key: "a/1"}, {Key: "a/2"}}},
		"b/":   {objs: []Object{{Key: "b/1"}}, subPrefixes: []string{"b/c/"}},
		"b/c/": {objs: []Object{{Key: "b/c/1"}}},
	}
	list := fakeTree(t, tree, 0, nil, nil)

	var inOnPage atomic.Bool
	var mu sync.Mutex
	var got []string
	onPage := func(objs []Object) error {
		if !inOnPage.CompareAndSwap(false, true) {
			t.Fatal("onPage invoked concurrently — must be called from a single goroutine")
		}
		defer inOnPage.Store(false)

		mu.Lock()
		for _, o := range objs {
			got = append(got, o.Key)
		}
		mu.Unlock()
		return nil
	}

	if err := listObjectsTree(context.Background(), "", list, onPage, defaultShardedDiscoveryConcurrency); err != nil {
		t.Fatalf("listObjectsTree: %v", err)
	}

	sort.Strings(got)
	want := []string{"a/1", "a/2", "b/1", "b/c/1", "root.txt"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestListObjectsTreeConcurrencyBounded(t *testing.T) {
	const n = 12
	tree := map[string]treeNode{"": {}}
	for i := range n {
		p := fmt.Sprintf("p%d/", i)
		tree[""] = treeNode{subPrefixes: append(tree[""].subPrefixes, p)}
		tree[p] = treeNode{objs: []Object{{Key: p + "obj"}}}
	}

	const concurrency = 3
	var current, maxConcurrent int32
	list := fakeTree(t, tree, 20*time.Millisecond, &current, &maxConcurrent)

	var count int
	onPage := func(objs []Object) error {
		count += len(objs)
		return nil
	}

	if err := listObjectsTree(context.Background(), "", list, onPage, concurrency); err != nil {
		t.Fatalf("listObjectsTree: %v", err)
	}
	if count != n {
		t.Errorf("delivered %d objects, want %d", count, n)
	}
	if got := atomic.LoadInt32(&maxConcurrent); got > concurrency {
		t.Errorf("observed %d concurrent list calls, want <= %d", got, concurrency)
	}
}

func TestListObjectsTreeListErrorPropagates(t *testing.T) {
	wantErr := errors.New("boom")
	tree := map[string]treeNode{
		"":   {subPrefixes: []string{"a/", "b/"}},
		"a/": {objs: []Object{{Key: "a/1"}}},
	}
	list := func(ctx context.Context, prefix string, token *string) ([]Object, []string, *string, error) {
		if prefix == "b/" {
			return nil, nil, nil, wantErr
		}
		node, ok := tree[prefix]
		if !ok {
			return nil, nil, nil, fmt.Errorf("unexpected prefix %q", prefix)
		}
		return node.objs, node.subPrefixes, nil, nil
	}

	err := listObjectsTree(context.Background(), "", list, func([]Object) error { return nil }, defaultShardedDiscoveryConcurrency)
	if !errors.Is(err, wantErr) {
		t.Errorf("got err %v, want %v", err, wantErr)
	}
}

// TestListObjectsTreeFailedPrefixDoesNotAbortWalk is the regression test for a
// 3.4M-object bucket that never completed discovery: one pathological prefix
// used to cancel the whole walk, discarding every other prefix's progress on
// every cycle.
func TestListObjectsTreeFailedPrefixDoesNotAbortWalk(t *testing.T) {
	wantErr := errors.New("too slow")
	tree := map[string]treeNode{
		"":     {subPrefixes: []string{"a/", "bad/", "c/"}},
		"a/":   {objs: []Object{{Key: "a/1"}, {Key: "a/2"}}},
		"c/":   {objs: []Object{{Key: "c/1"}}, subPrefixes: []string{"c/d/"}},
		"c/d/": {objs: []Object{{Key: "c/d/1"}}},
	}
	list := func(ctx context.Context, prefix string, token *string) ([]Object, []string, *string, error) {
		if prefix == "bad/" {
			return nil, nil, nil, wantErr
		}
		node, ok := tree[prefix]
		if !ok {
			return nil, nil, nil, fmt.Errorf("unexpected prefix %q", prefix)
		}
		return node.objs, node.subPrefixes, nil, nil
	}

	var got []string
	err := listObjectsTree(context.Background(), "", list, func(objs []Object) error {
		for _, o := range objs {
			got = append(got, o.Key)
		}
		return nil
	}, defaultShardedDiscoveryConcurrency)

	if !errors.Is(err, wantErr) {
		t.Errorf("got err %v, want it to wrap %v", err, wantErr)
	}
	sort.Strings(got)
	// Every healthy prefix must still be delivered, including c/d/ which is only
	// reachable by descending past the failure.
	want := []string{"a/1", "a/2", "c/1", "c/d/1"}
	if !slices.Equal(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

// TestListObjectsTreeReportsCappedPrefixErrors pins that a bucket where every
// prefix fails does not produce an unbounded joined error, logged in full on
// every cycle.
func TestListObjectsTreeReportsCappedPrefixErrors(t *testing.T) {
	const prefixes = maxReportedPrefixErrs + 5
	root := treeNode{}
	for i := range prefixes {
		root.subPrefixes = append(root.subPrefixes, fmt.Sprintf("p%d/", i))
	}
	list := func(ctx context.Context, prefix string, token *string) ([]Object, []string, *string, error) {
		if prefix == "" {
			return nil, root.subPrefixes, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("prefix %s failed", prefix)
	}

	err := listObjectsTree(context.Background(), "", list, func([]Object) error { return nil }, 2)
	if err == nil {
		t.Fatal("expected an error when every prefix fails")
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("expected a joined error, got %T", err)
	}
	// maxReportedPrefixErrs retained failures plus one "and N more" summary.
	if n := len(joined.Unwrap()); n != maxReportedPrefixErrs+1 {
		t.Errorf("joined %d errors, want %d", n, maxReportedPrefixErrs+1)
	}
	if !strings.Contains(err.Error(), "and 5 more prefixes failed") {
		t.Errorf("error should summarise the elided failures, got: %v", err)
	}
}

func TestListObjectsTreeOnPageErrorPropagates(t *testing.T) {
	wantErr := errors.New("onpage boom")
	tree := map[string]treeNode{"": {objs: []Object{{Key: "root.txt"}}}}
	list := fakeTree(t, tree, 0, nil, nil)

	err := listObjectsTree(context.Background(), "", list, func([]Object) error { return wantErr }, defaultShardedDiscoveryConcurrency)
	if !errors.Is(err, wantErr) {
		t.Errorf("got err %v, want %v", err, wantErr)
	}
}
