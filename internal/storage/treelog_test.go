package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// captureLogs swaps the global logger for the duration of a test. Several
// buckets are walked concurrently in production, so every per-prefix line the
// tree walk emits has to name its bucket; without it an operator reading
// "prefix=20260206/" cannot tell which bucket it belongs to.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev, prevLevel := log.Logger, zerolog.GlobalLevel()
	log.Logger = zerolog.New(buf)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	t.Cleanup(func() {
		log.Logger = prev
		zerolog.SetGlobalLevel(prevLevel)
	})
	return buf
}

func TestListObjectsTreeLogsNameTheirBucket(t *testing.T) {
	tests := []struct {
		name    string
		ckpt    func() *fakeCheckpoint
		list    func(t *testing.T) listDelimitedFn
		wantMsg string
	}{
		{
			name: "resume from a stored checkpoint",
			ckpt: func() *fakeCheckpoint {
				f := newFakeCheckpoint(nil)
				f.tokens[""] = "1"
				return f
			},
			list: func(t *testing.T) listDelimitedFn {
				return fakePagedTree(t, map[string]treePages{
					"": pagedLeaf([]string{"a"}, []string{"b"}),
				})
			},
			wantMsg: "resuming prefix from stored checkpoint",
		},
		{
			name: "checkpoint load failure",
			ckpt: func() *fakeCheckpoint {
				f := newFakeCheckpoint(nil)
				f.loadErr = errors.New("store down")
				return f
			},
			list: func(t *testing.T) listDelimitedFn {
				return fakePagedTree(t, map[string]treePages{"": pagedLeaf([]string{"a"})})
			},
			wantMsg: "checkpoint load failed",
		},
		{
			name: "checkpoint save failure",
			ckpt: func() *fakeCheckpoint {
				f := newFakeCheckpoint(nil)
				f.saveErr = errors.New("store down")
				return f
			},
			list: func(t *testing.T) listDelimitedFn {
				return fakePagedTree(t, map[string]treePages{
					"": pagedLeaf([]string{"a"}, []string{"b"}),
				})
			},
			wantMsg: "checkpoint save failed",
		},
		{
			name: "checkpoint clear failure",
			ckpt: func() *fakeCheckpoint {
				// A stored token makes the prefix dirty, so completing it
				// issues the clear that then fails.
				f := newFakeCheckpoint(nil)
				f.tokens[""] = "0"
				f.clearErr = errors.New("store down")
				return f
			},
			list: func(t *testing.T) listDelimitedFn {
				return fakePagedTree(t, map[string]treePages{"": pagedLeaf([]string{"a"})})
			},
			wantMsg: "checkpoint clear failed",
		},
		{
			name: "prefix listing stopped early",
			ckpt: func() *fakeCheckpoint { return newFakeCheckpoint(nil) },
			list: func(t *testing.T) listDelimitedFn {
				return fakePagedTree(t, map[string]treePages{
					"": {pages: pagedLeaf([]string{"a"}, []string{"b"}).pages, failPages: []int{1}},
				})
			},
			wantMsg: "prefix listing stopped early",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)

			_ = listObjectsTree(context.Background(), "events", "", tc.list(t),
				func([]Object) error { return nil }, 1, 0, tc.ckpt())

			var found bool
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				if !strings.Contains(line, tc.wantMsg) {
					continue
				}
				found = true
				if !strings.Contains(line, `"bucket":"events"`) {
					t.Errorf("log line is not attributable to a bucket: %s", line)
				}
			}
			if !found {
				t.Fatalf("no %q line was logged; got:\n%s", tc.wantMsg, buf.String())
			}
		})
	}
}
