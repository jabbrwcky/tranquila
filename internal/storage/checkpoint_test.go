package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// treePages is a multi-page fake prefix. Page i is addressed by continuation
// token strconv.Itoa(i) — tokens are opaque to listObjectsTree, so any stable
// encoding works. failPages lists page indices whose listing fails, modelling
// the page that is too slow for the backend to answer.
type treePages struct {
	pages     []treeNode
	failPages []int
}

func fakePagedTree(t *testing.T, tree map[string]treePages) listDelimitedFn {
	t.Helper()
	return func(_ context.Context, prefix string, token *string) ([]Object, []string, *string, error) {
		node, ok := tree[prefix]
		if !ok {
			return nil, nil, nil, fmt.Errorf("fakePagedTree: unexpected prefix %q", prefix)
		}
		idx := 0
		if token != nil {
			n, err := strconv.Atoi(*token)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("fakePagedTree: bad token %q for %q", *token, prefix)
			}
			idx = n
		}
		if idx >= len(node.pages) {
			return nil, nil, nil, fmt.Errorf("fakePagedTree: token %d past end of %q", idx, prefix)
		}
		if slices.Contains(node.failPages, idx) {
			return nil, nil, nil, fmt.Errorf("fakePagedTree: page %d of %q is unanswerable", idx, prefix)
		}
		p := node.pages[idx]
		var next *string
		if idx+1 < len(node.pages) {
			s := strconv.Itoa(idx + 1)
			next = &s
		}
		return p.objs, p.subPrefixes, next, nil
	}
}

// fakeCheckpoint is an in-memory prefixCheckpoint. It appends every operation to
// a shared ordered log so a test can assert not just the final state but the
// interleaving with onPage — which is the real invariant: a token is persisted
// only after its page's objects were accepted.
type fakeCheckpoint struct {
	mu       sync.Mutex
	tokens   map[string]string
	ops      *[]string
	loadErr  error
	saveErr  error
	clearErr error
}

func newFakeCheckpoint(ops *[]string) *fakeCheckpoint {
	return &fakeCheckpoint{tokens: map[string]string{}, ops: ops}
}

func (f *fakeCheckpoint) logOp(s string) {
	if f.ops != nil {
		*f.ops = append(*f.ops, s)
	}
}

func (f *fakeCheckpoint) load(_ context.Context, prefix string) (*string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	tok, ok := f.tokens[prefix]
	if !ok {
		return nil, nil
	}
	return &tok, nil
}

func (f *fakeCheckpoint) save(_ context.Context, prefix string, token *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	var tok string
	if token != nil {
		tok = *token
	}
	f.tokens[prefix] = tok
	f.logOp("save:" + prefix + "=" + tok)
	return nil
}

func (f *fakeCheckpoint) clear(_ context.Context, prefix string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.clearErr != nil {
		return f.clearErr
	}
	delete(f.tokens, prefix)
	f.logOp("clear:" + prefix)
	return nil
}

func (f *fakeCheckpoint) snapshot() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k, v := range f.tokens {
		out[k] = v
	}
	return out
}

// obj is a terser Object constructor for the page fixtures below.
func obj(key string) Object { return Object{Bucket: "b", Key: key} }

func pagedLeaf(pages ...[]string) treePages {
	tp := treePages{}
	for _, keys := range pages {
		node := treeNode{}
		for _, k := range keys {
			node.objs = append(node.objs, obj(k))
		}
		tp.pages = append(tp.pages, node)
	}
	return tp
}

// collectKeys runs a walk and returns the keys onPage saw, in order.
func collectKeys(t *testing.T, list listDelimitedFn, ckpt prefixCheckpoint, ops *[]string) ([]string, error) {
	t.Helper()
	var got []string
	_, err := listObjectsTree(context.Background(), "b", "", list, func(objs []Object) error {
		for _, o := range objs {
			got = append(got, o.Key)
			if ops != nil {
				*ops = append(*ops, "page:"+o.Key)
			}
		}
		return nil
	}, 1, 0, ckpt)
	return got, err
}

func TestListObjectsTreeCheckpointResume(t *testing.T) {
	tests := []struct {
		name   string
		stored map[string]string
		load   error
		want   []string
	}{
		{"no stored token lists the whole prefix", nil, nil, []string{"a", "b", "c", "d"}},
		{"stored token resumes mid-prefix", map[string]string{"": "2"}, nil, []string{"c", "d"}},
		{"a load failure falls back to a full list", nil, errors.New("redis down"), []string{"a", "b", "c", "d"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			list := fakePagedTree(t, map[string]treePages{
				"": pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}, []string{"d"}),
			})
			ckpt := newFakeCheckpoint(nil)
			ckpt.loadErr = tc.load
			for k, v := range tc.stored {
				ckpt.tokens[k] = v
			}

			got, err := collectKeys(t, list, ckpt, nil)
			if err != nil {
				t.Fatalf("listObjectsTree: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("delivered %v, want %v", got, tc.want)
			}
		})
	}
}

// Completing a prefix must drop its resume point, so the next cycle lists it in
// full. That full re-list is what rediscovers objects whose transfer failed.
func TestListObjectsTreeCheckpointClearedOnCompletion(t *testing.T) {
	list := fakePagedTree(t, map[string]treePages{
		"": pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}),
	})
	var ops []string
	ckpt := newFakeCheckpoint(&ops)

	if _, err := collectKeys(t, list, ckpt, nil); err != nil {
		t.Fatalf("listObjectsTree: %v", err)
	}
	if got := ckpt.snapshot(); len(got) != 0 {
		t.Errorf("checkpoints left behind after a clean walk: %v", got)
	}
	if len(ops) == 0 || !strings.HasPrefix(ops[len(ops)-1], "clear:") {
		t.Errorf("last checkpoint op was %v, want a clear", ops)
	}
}

// The core regression test: a failed prefix keeps its resume point, and the
// NEXT walk starts from there instead of page 1. Without this the walk makes
// zero forward progress per cycle no matter how many cycles run.
func TestListObjectsTreeFailedPrefixRetainsCheckpoint(t *testing.T) {
	pages := pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}, []string{"d"})
	failing := pages
	failing.failPages = []int{2}

	ckpt := newFakeCheckpoint(nil)

	// Cycle 1: dies on page 2 having delivered pages 0-1.
	got, err := collectKeys(t, fakePagedTree(t, map[string]treePages{"": failing}), ckpt, nil)
	if err == nil {
		t.Fatal("expected the prefix failure to be reported")
	}
	if want := []string{"a", "b"}; !slices.Equal(got, want) {
		t.Errorf("cycle 1 delivered %v, want %v", got, want)
	}
	if tok := ckpt.snapshot()[""]; tok != "2" {
		t.Fatalf("cycle 1 stored token %q, want \"2\" (the page that failed)", tok)
	}

	// Cycle 2: same checkpoint store, backend now healthy.
	got, err = collectKeys(t, fakePagedTree(t, map[string]treePages{"": pages}), ckpt, nil)
	if err != nil {
		t.Fatalf("cycle 2: %v", err)
	}
	if want := []string{"c", "d"}; !slices.Equal(got, want) {
		t.Errorf("cycle 2 delivered %v, want %v — it must resume, not restart", got, want)
	}
	if got := ckpt.snapshot(); len(got) != 0 {
		t.Errorf("checkpoint survived completion: %v", got)
	}
}

// A prefix parks when it RESUMES onto its checkpointed page and that page is
// still unanswerable — cycle 1's first-ever failure does not count (nothing
// to resume onto yet), but cycle 2's repeat failure on the same resumed page
// does. Parked is counted per walk, not carried as state across walks: a
// prefix that recovers simply stops being counted on its next cycle.
func TestListObjectsTreeReportsParkedPrefixes(t *testing.T) {
	pages := pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}, []string{"d"})
	failing := pages
	failing.failPages = []int{2}

	ckpt := newFakeCheckpoint(nil)

	// Cycle 1: first failure ever seen for this prefix. Not resumed (nothing
	// was loaded from the checkpoint store), so not parked.
	parked, err := listObjectsTree(context.Background(), "b", "", fakePagedTree(t, map[string]treePages{"": failing}),
		func([]Object) error { return nil }, 1, 0, ckpt)
	if err == nil {
		t.Fatal("expected the prefix failure to be reported")
	}
	if parked != 0 {
		t.Errorf("cycle 1 parked = %d, want 0 (a first failure is not a resume)", parked)
	}

	// Cycle 2: resumes onto page 2 via the stored checkpoint, which the
	// backend still cannot answer. Delivers zero pages this cycle — parked.
	parked, err = listObjectsTree(context.Background(), "b", "", fakePagedTree(t, map[string]treePages{"": failing}),
		func([]Object) error { return nil }, 1, 0, ckpt)
	if err == nil {
		t.Fatal("expected the resumed prefix to fail again")
	}
	if parked != 1 {
		t.Errorf("cycle 2 parked = %d, want 1", parked)
	}

	// Cycle 3: backend recovers. The prefix completes and is not counted —
	// parked reflects only the walk that just ran, not accumulated history.
	parked, err = listObjectsTree(context.Background(), "b", "", fakePagedTree(t, map[string]treePages{"": pages}),
		func([]Object) error { return nil }, 1, 0, ckpt)
	if err != nil {
		t.Fatalf("cycle 3: %v", err)
	}
	if parked != 0 {
		t.Errorf("cycle 3 parked = %d, want 0 (the prefix recovered)", parked)
	}
}

// A token may only be persisted once its own page's objects have been accepted.
// Saving earlier could record a resume point past objects that never reached
// onPage at all.
func TestListObjectsTreeCheckpointSavedAfterOnPage(t *testing.T) {
	list := fakePagedTree(t, map[string]treePages{
		"": pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}),
	})
	var ops []string
	ckpt := newFakeCheckpoint(&ops)

	var inOnPage atomic.Bool
	_, err := listObjectsTree(context.Background(), "b", "", list, func(objs []Object) error {
		if !inOnPage.CompareAndSwap(false, true) {
			t.Error("onPage invoked concurrently — must be called from a single goroutine")
		}
		defer inOnPage.Store(false)
		for _, o := range objs {
			ops = append(ops, "page:"+o.Key)
		}
		return nil
	}, defaultShardedDiscoveryConcurrency, 0, ckpt)
	if err != nil {
		t.Fatalf("listObjectsTree: %v", err)
	}

	for i, op := range ops {
		if !strings.HasPrefix(op, "save:") {
			continue
		}
		if i == 0 || !strings.HasPrefix(ops[i-1], "page:") {
			t.Errorf("op %d (%q) is not immediately preceded by its page delivery: %v", i, op, ops)
		}
	}
	if want := []string{"page:a", "save:=1", "page:b", "save:=2", "page:c", "clear:"}; !slices.Equal(ops, want) {
		t.Errorf("op order %v, want %v", ops, want)
	}
}

// An aborted walk must not leave a resume point past objects nobody accepted.
func TestListObjectsTreeOnPageErrorDoesNotSaveCheckpoint(t *testing.T) {
	list := fakePagedTree(t, map[string]treePages{
		"": pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}, []string{"d"}),
	})
	ckpt := newFakeCheckpoint(nil)
	wantErr := errors.New("mark pending failed")

	_, err := listObjectsTree(context.Background(), "b", "", list,
		func([]Object) error { return wantErr }, 1, 0, ckpt)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want it to wrap %v", err, wantErr)
	}
	if got := ckpt.snapshot(); len(got) != 0 {
		t.Errorf("checkpoint saved despite onPage rejecting the page: %v", got)
	}
}

// A delimited listing interleaves objects and CommonPrefixes across pages, so
// resuming past a page would also skip the sub-prefixes it carried — stranding
// whole subtrees silently. A prefix that branches must never be checkpointed.
func TestListObjectsTreeBranchingPrefixDropsCheckpoint(t *testing.T) {
	tests := []struct {
		name string
		root treePages
		// wantRootSaves is whether the root prefix should ever have had a resume
		// point written during the walk. Checking the ops log rather than the
		// final map matters because a completed prefix clears its checkpoint
		// either way — the final state cannot distinguish "never saved" from
		// "saved then cleared".
		wantRootSaves bool
	}{
		{
			name: "sub-prefixes on the first page opt the prefix out",
			root: treePages{pages: []treeNode{
				{objs: []Object{obj("a")}, subPrefixes: []string{"sub/"}},
				{objs: []Object{obj("b")}},
			}},
			wantRootSaves: false,
		},
		{
			name: "sub-prefixes on a later page clear an earlier save",
			root: treePages{pages: []treeNode{
				{objs: []Object{obj("a")}},
				{objs: []Object{obj("b")}, subPrefixes: []string{"sub/"}},
				{objs: []Object{obj("c")}},
			}},
			// Page 0 saves before page 1 reveals the branch; the clear is what
			// the final-state assertion below pins.
			wantRootSaves: true,
		},
		{
			name:          "a leaf-only prefix checkpoints normally",
			root:          pagedLeaf([]string{"a"}, []string{"b"}),
			wantRootSaves: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tree := map[string]treePages{"": tc.root, "sub/": pagedLeaf([]string{"s1"})}
			var ops []string
			ckpt := newFakeCheckpoint(&ops)

			if _, err := collectKeys(t, fakePagedTree(t, tree), ckpt, nil); err != nil {
				t.Fatalf("listObjectsTree: %v", err)
			}

			var sawRootSave bool
			for _, op := range ops {
				if strings.HasPrefix(op, "save:=") {
					sawRootSave = true
				}
			}
			if sawRootSave != tc.wantRootSaves {
				t.Errorf("root prefix saved = %v, want %v (ops %v)", sawRootSave, tc.wantRootSaves, ops)
			}
			// Whatever happened mid-walk, nothing may survive: a branching
			// prefix must end un-checkpointed or a later cycle would resume past
			// its CommonPrefixes and strand the subtree.
			if got := ckpt.snapshot(); len(got) != 0 {
				t.Errorf("checkpoints survived the walk: %v", got)
			}
		})
	}
}

// A checkpoint store outage must degrade to the pre-checkpointing behaviour,
// never abort a walk that is otherwise making progress.
func TestListObjectsTreeCheckpointErrorsAreNonFatal(t *testing.T) {
	boom := errors.New("redis down")
	tests := []struct {
		name  string
		apply func(*fakeCheckpoint)
	}{
		{"load fails", func(f *fakeCheckpoint) { f.loadErr = boom }},
		{"save fails", func(f *fakeCheckpoint) { f.saveErr = boom }},
		{"clear fails", func(f *fakeCheckpoint) { f.clearErr = boom }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			list := fakePagedTree(t, map[string]treePages{
				"": pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}),
			})
			ckpt := newFakeCheckpoint(nil)
			tc.apply(ckpt)

			got, err := collectKeys(t, list, ckpt, nil)
			if err != nil {
				t.Fatalf("checkpoint error was fatal to the walk: %v", err)
			}
			if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
				t.Errorf("delivered %v, want %v", got, want)
			}
		})
	}
}

// A prefix must yield its worker slot once its budget is spent, so a handful of
// pathological prefixes cannot monopolise every slot and leave the rest of the
// bucket unlisted — and with checkpointing on, the pages it did manage are
// banked rather than thrown away.
func TestListObjectsTreePrefixBudgetYieldsAndBanksProgress(t *testing.T) {
	const pageDelay = 40 * time.Millisecond
	// Enough budget for a page or two, nowhere near enough for all six.
	const budget = 110 * time.Millisecond

	// An endlessly paginating prefix: without a budget this never returns. The
	// delay honours ctx, as the real SDK call does — a fake that slept through
	// cancellation would make the budget look ineffective.
	slow := func(ctx context.Context, _ string, token *string) ([]Object, []string, *string, error) {
		select {
		case <-time.After(pageDelay):
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		}
		idx := 0
		if token != nil {
			n, err := strconv.Atoi(*token)
			if err != nil {
				return nil, nil, nil, err
			}
			idx = n
		}
		next := strconv.Itoa(idx + 1)
		return []Object{obj("k" + strconv.Itoa(idx))}, nil, &next, nil
	}

	ckpt := newFakeCheckpoint(nil)
	var got []string
	_, err := listObjectsTree(context.Background(), "b", "", slow, func(objs []Object) error {
		for _, o := range objs {
			got = append(got, o.Key)
		}
		return nil
	}, 1, budget, ckpt)

	if err == nil {
		t.Fatal("expected the prefix to report stopping early")
	}
	if len(got) == 0 {
		t.Fatal("budget expired before a single page was delivered; the test budget is too tight")
	}
	// The prefix paginates forever, so finishing at all means the budget did
	// its job; the count just has to be in the ballpark the budget allows.
	if maxPages := 6; len(got) > maxPages {
		t.Errorf("delivered %d pages on a %v budget at %v per page, want the cutoff to bite sooner",
			len(got), budget, pageDelay)
	}
	// The whole point: progress survives the cutoff.
	if tok := ckpt.snapshot()[""]; tok == "" {
		t.Error("no checkpoint written, so the pages listed before the cutoff would be re-listed next cycle")
	}
}

// A budget of zero means unbounded, so an existing slow-but-completing walk is
// not silently truncated by the plumbing.
func TestListObjectsTreeZeroPrefixBudgetIsUnbounded(t *testing.T) {
	list := fakePagedTree(t, map[string]treePages{
		"": pagedLeaf([]string{"a"}, []string{"b"}, []string{"c"}),
	})
	got, err := collectKeys(t, list, nil, nil)
	if err != nil {
		t.Fatalf("listObjectsTree: %v", err)
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}
