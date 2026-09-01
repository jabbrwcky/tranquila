package sync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
	"github.com/jabbrwcky/tranquila/internal/watcher"
	"go.opentelemetry.io/otel/metric/noop"
)

// testMetrics builds no-op instruments so tests can exercise code paths that
// record metrics without a real meter.
func testMetrics(t *testing.T) metrics {
	t.Helper()
	m, err := newMetrics(noop.Meter{})
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	return m
}

// fakeWatcher implements watcher.Watcher for testing runWatcher.
type fakeWatcher struct {
	events []watcher.ObjectEvent
	err    error
}

func (f *fakeWatcher) Watch(_ context.Context, _ []string) (<-chan watcher.ObjectEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	ch := make(chan watcher.ObjectEvent, len(f.events))
	for _, e := range f.events {
		ch <- e
	}
	close(ch)
	return ch, nil
}

func TestRunWatch(t *testing.T) {
	tests := []struct {
		name        string
		cycleFn     func(ctx context.Context) error
		interval    time.Duration
		cancelAt    time.Duration // 0 = cancel before starting
		wantErr     bool
		minCycles   int
		minAttempts int
	}{
		{
			name:      "cancel_before_first_cycle",
			cycleFn:   func(ctx context.Context) error { return context.Canceled },
			interval:  time.Hour,
			cancelAt:  0,
			minCycles: 0,
		},
		{
			// Was cycle_error_propagates: a transient failure used to abort the
			// loop and exit the process. It must now back off and retry instead.
			name: "transient_cycle_error_does_not_terminate",
			cycleFn: func() func(context.Context) error {
				return func(ctx context.Context) error {
					return errors.New("boom")
				}
			}(),
			interval:    time.Millisecond,
			cancelAt:    time.Second,
			wantErr:     false,
			minAttempts: 2,
		},
		{
			name: "permanent_cycle_error_propagates",
			cycleFn: func() func(context.Context) error {
				return func(ctx context.Context) error {
					return &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
				}
			}(),
			interval: time.Millisecond,
			cancelAt: time.Second,
			wantErr:  true,
		},
		{
			name: "two_cycles_then_cancel",
			cycleFn: func() func(context.Context) error {
				return func(ctx context.Context) error { return nil }
			}(),
			interval:  time.Millisecond,
			cancelAt:  50 * time.Millisecond,
			minCycles: 2,
		},
		{
			name:      "cancel_during_sleep_exits_cleanly",
			cycleFn:   func(ctx context.Context) error { return nil },
			interval:  time.Hour,
			cancelAt:  20 * time.Millisecond,
			minCycles: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Syncer{m: testMetrics(t)}

			var (
				mu        sync.Mutex
				completed int
				attempts  int
			)
			wrapped := func(ctx context.Context) error {
				mu.Lock()
				attempts++
				mu.Unlock()
				err := tc.cycleFn(ctx)
				if err == nil {
					mu.Lock()
					completed++
					mu.Unlock()
				}
				return err
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelAt == 0 {
				cancel()
			} else {
				time.AfterFunc(tc.cancelAt, cancel)
			}

			// Backoff is asserted separately; keep retries fast here.
			fastSleep := func(ctx context.Context, _ time.Duration) bool {
				return waitOrDone(ctx, time.Millisecond)
			}
			err := s.runWatch(ctx, tc.interval, wrapped, fastSleep)

			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			mu.Lock()
			n, a := completed, attempts
			mu.Unlock()
			if n < tc.minCycles {
				t.Errorf("completed %d cycles, want at least %d", n, tc.minCycles)
			}
			if a < tc.minAttempts {
				t.Errorf("made %d attempts, want at least %d", a, tc.minAttempts)
			}
		})
	}
}

func TestRunWatcherStartError(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{err: errors.New("watcher init failed")}
	err := s.runWatcher(context.Background(), w, nil, nil)
	if err == nil {
		t.Error("expected error from Watch(), got nil")
	}
}

func TestRunWatcherEmptyChannel(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{events: nil}
	// Nil bucket map — unknown-bucket events are skipped.
	err := s.runWatcher(context.Background(), w, nil, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunWatcherUnknownBucketSkipped(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{events: []watcher.ObjectEvent{
		{Bucket: "unknown", Key: "k1", Size: 100},
	}}
	// Empty bucket map — event should be skipped, not panic.
	err := s.runWatcher(context.Background(), w, []string{"unknown"}, map[string]BucketConfig{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunWatcherDeleteEventPropagateDeletesDisabled(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{events: []watcher.ObjectEvent{
		{Bucket: "b1", Key: "k1", IsDelete: true},
	}}
	// PropagateDeletes not set on the bucket config — the delete event must be
	// ignored (never reach pool.submit, which would block forever with the
	// zero-worker pool this test's zero-value Syncer creates).
	err := s.runWatcher(context.Background(), w, []string{"b1"}, map[string]BucketConfig{
		"b1": {Destination: "dst1"},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEventDispatch(t *testing.T) {
	tests := []struct {
		name  string
		event watcher.ObjectEvent
		bc    BucketConfig
		want  eventDecision
	}{
		{
			name:  "created_event_is_upload",
			event: watcher.ObjectEvent{IsDelete: false},
			bc:    BucketConfig{PropagateDeletes: true},
			want:  dispatchUpload,
		},
		{
			name:  "delete_event_with_propagate_enabled",
			event: watcher.ObjectEvent{IsDelete: true},
			bc:    BucketConfig{PropagateDeletes: true},
			want:  dispatchDelete,
		},
		{
			name:  "delete_event_with_propagate_disabled",
			event: watcher.ObjectEvent{IsDelete: true},
			bc:    BucketConfig{PropagateDeletes: false},
			want:  dispatchSkipDelete,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := eventDispatch(tc.event, tc.bc); got != tc.want {
				t.Errorf("eventDispatch() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBurnNowForEvent(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		event watcher.ObjectEvent
		bc    BucketConfig
		want  bool
	}{
		{
			name:  "bar_disabled_never_burns",
			event: watcher.ObjectEvent{ModifiedAt: now},
			bc:    BucketConfig{BurnAfterReading: false},
			want:  false,
		},
		{
			name:  "bar_enabled_no_gate_burns_immediately",
			event: watcher.ObjectEvent{ModifiedAt: now},
			bc:    BucketConfig{BurnAfterReading: true},
			want:  true,
		},
		{
			name:  "bar_enabled_gated_and_old_enough",
			event: watcher.ObjectEvent{ModifiedAt: now.Add(-8 * 24 * time.Hour)},
			bc:    BucketConfig{BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			want:  true,
		},
		{
			name:  "bar_enabled_gated_too_young_defers",
			event: watcher.ObjectEvent{ModifiedAt: now},
			bc:    BucketConfig{BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			want:  false,
		},
		{
			name: "bar_enabled_gated_unknown_modified_at_defers",
			// ObjectEvent.ModifiedAt is documented best-effort — may be zero.
			event: watcher.ObjectEvent{},
			bc:    BucketConfig{BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := burnNowForEvent(tc.event, tc.bc, now); got != tc.want {
				t.Errorf("burnNowForEvent() = %v, want %v", got, tc.want)
			}
		})
	}
}
