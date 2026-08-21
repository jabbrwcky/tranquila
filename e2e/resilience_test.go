package e2e

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/jabbrwcky/tranquila/internal/storage"
	tsync "github.com/jabbrwcky/tranquila/internal/sync"
)

// syncerFor builds a Syncer wired to the given clients with test-scale backoff.
func syncerFor(t *testing.T, s *stack, src, dst *storage.Client, srcBucket, dstBucket string) *tsync.Syncer {
	t.Helper()
	syncer, err := tsync.New(tsync.Config{
		Source:      src,
		Destination: dst,
		State:       s.store(t),
		// Meter is left unset: it is optional and falls back to no-op instruments.
		Buckets: map[string]tsync.BucketConfig{srcBucket: {Destination: dstBucket}},
		Workers: 4,
		// Keep retry pacing test-scale; production defaults are 5s/10m.
		CycleBackoff:    250 * time.Millisecond,
		CycleBackoffMax: time.Second,
	})
	if err != nil {
		t.Fatalf("create syncer: %v", err)
	}
	return syncer
}

// TestSyncCompletesDespiteTransient504 is the regression test for the reported
// production incident: a gateway returning 504 during discovery used to abort
// the run. The retry budget must now absorb it and sync every object.
func TestSyncCompletesDespiteTransient504(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "burst-src", 5)

	srcProxy := newFaultProxy(t, s.minioEndpoint)
	src := s.client(t, "source", srcProxy.URL(), 0, 0)
	dst := s.client(t, "destination", s.minioEndpoint, 0, 0)

	// Fail the opening requests of the run, within the retry budget.
	srcProxy.failNext(3, http.StatusGatewayTimeout, true)

	if err := syncerFor(t, s, src, dst, "burst-src", "burst-dst").Run(ctx); err != nil {
		t.Fatalf("Run should have absorbed the 504 burst: %v", err)
	}
	if _, failed := srcProxy.stats(); failed == 0 {
		t.Error("no faults were injected — the test proved nothing")
	}
	if got := s.countObjects(t, "burst-dst", keys); got != len(keys) {
		t.Errorf("synced %d/%d objects", got, len(keys))
	}
}

// TestWatchSurvivesSustainedOutage covers the guarantee that watch mode never
// exits on a transient fault. Previously one failed cycle returned, reached
// log.Fatal and restarted the pod.
//
// The destination is faulted because EnsureBucket runs first in a cycle and has
// no repo-level retry, so cycles fail in seconds rather than minutes.
func TestWatchSurvivesSustainedOutage(t *testing.T) {
	s := newStack(t)
	keys := s.seed(t, "outage-src", 3)

	dstProxy := newFaultProxy(t, s.minioEndpoint)
	src := s.client(t, "source", s.minioEndpoint, 0, 0)
	dst := s.client(t, "destination", dstProxy.URL(), 0, 0)
	syncer := syncerFor(t, s, src, dst, "outage-src", "outage-dst")

	dstProxy.failAll(http.StatusGatewayTimeout, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- syncer.RunWatch(ctx, 200*time.Millisecond) }()

	// The endpoint is hard down: the loop must keep retrying, not return.
	eventually(t, 90*time.Second, "cycles to start failing", func() bool {
		_, failed := dstProxy.stats()
		return failed > 0
	})
	select {
	case err := <-done:
		t.Fatalf("RunWatch returned during a transient outage: %v", err)
	case <-time.After(2 * time.Second):
		// Still running, which is the point.
	}

	// Recovery: once the endpoint is healthy the next cycle must sync.
	dstProxy.clear()
	eventually(t, 120*time.Second, "objects to sync after recovery", func() bool {
		return s.countObjects(t, "outage-dst", keys) == len(keys)
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("RunWatch returned %v, want nil on cancellation", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("RunWatch did not return after cancellation")
	}
}

// TestWatchTerminatesOnPermanentError is the other half of that guarantee:
// misconfiguration must stay loud rather than retry forever.
func TestWatchTerminatesOnPermanentError(t *testing.T) {
	s := newStack(t)
	s.seed(t, "perm-src", 1)

	dstProxy := newFaultProxy(t, s.minioEndpoint)
	src := s.client(t, "source", s.minioEndpoint, 0, 0)
	dst := s.client(t, "destination", dstProxy.URL(), 0, 0)
	syncer := syncerFor(t, s, src, dst, "perm-src", "perm-dst")

	dstProxy.failAll(http.StatusForbidden, true) // AccessDenied

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- syncer.RunWatch(ctx, 200*time.Millisecond) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("RunWatch returned nil on a permanent error, want it to surface")
		}
	case <-ctx.Done():
		t.Fatal("RunWatch kept retrying a permanent error instead of returning")
	}
}

// TestRateLimitDegradesAndRecovers covers congestion control on each endpoint
// independently, including that an unconfigured endpoint is never degraded.
func TestRateLimitDegradesAndRecovers(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "aimd-src", 1)

	tests := []struct {
		name        string
		rateLimit   float64
		wantDegrade bool
		wantLimit   float64
	}{
		{name: "configured_endpoint_degrades", rateLimit: 40, wantDegrade: true, wantLimit: 20},
		// No ceiling was configured, so there is nothing to halve.
		{name: "unlimited_endpoint_never_degrades", rateLimit: 0, wantDegrade: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := newFaultProxy(t, s.minioEndpoint)
			c := s.client(t, "source", fp.URL(), tt.rateLimit, 2)

			fp.failAll(http.StatusGatewayTimeout, true)
			// Two failing calls meet the threshold of 2.
			for range 2 {
				if _, _, err := c.HeadObject(ctx, "aimd-src", keys[0]); err == nil {
					t.Fatal("expected HeadObject to fail while the endpoint returns 504")
				}
			}

			st := c.LimitState()
			if st.Degraded != tt.wantDegrade {
				t.Fatalf("degraded = %v, want %v (state %+v)", st.Degraded, tt.wantDegrade, st)
			}
			if !tt.wantDegrade {
				return
			}
			if st.Current != tt.wantLimit {
				t.Errorf("rate limit = %v, want %v", st.Current, tt.wantLimit)
			}

			// Recovery is additive: 10% of base per 20 healthy calls.
			fp.clear()
			eventually(t, 90*time.Second, "the rate limit to climb back to base", func() bool {
				if _, _, err := c.HeadObject(ctx, "aimd-src", keys[0]); err != nil {
					return false
				}
				return !c.LimitState().Degraded
			})
		})
	}
}

// TestDestinationDegradesIndependently pins that a sick destination throttles
// destination writes without slowing source reads.
func TestDestinationDegradesIndependently(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "indep-src", 1)

	dstProxy := newFaultProxy(t, s.minioEndpoint)
	src := s.client(t, "source", s.minioEndpoint, 100, 2)
	dst := s.client(t, "destination", dstProxy.URL(), 40, 2)

	dstProxy.failAll(http.StatusGatewayTimeout, true)
	for range 2 {
		if _, _, err := dst.HeadObject(ctx, "indep-src", keys[0]); err == nil {
			t.Fatal("expected the destination call to fail")
		}
	}
	// Keep the source busy and healthy throughout.
	for range 3 {
		if _, _, err := src.HeadObject(ctx, "indep-src", keys[0]); err != nil {
			t.Fatalf("source call failed unexpectedly: %v", err)
		}
	}

	if dstSt := dst.LimitState(); !dstSt.Degraded || dstSt.Current != 20 {
		t.Errorf("destination state = %+v, want degraded at 20", dstSt)
	}
	if srcSt := src.LimitState(); srcSt.Degraded || srcSt.Current != 100 {
		t.Errorf("source state = %+v, want untouched at 100", srcSt)
	}
}

// TestL4FaultsAreTransient covers the fault class Toxiproxy can produce that an
// L7 injector cannot: a connection reset mid-stream, at the TCP layer.
func TestL4FaultsAreTransient(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "l4-src", 2)

	c := s.client(t, "source", s.toxicEndpoint, 0, 0)

	if _, err := s.toxics.AddToxic("reset", "reset_peer", "downstream", 1.0, map[string]any{
		"timeout": 0,
	}); err != nil {
		t.Fatalf("add reset_peer toxic: %v", err)
	}

	_, _, err := c.HeadObject(ctx, "l4-src", keys[0])
	if err == nil {
		t.Fatal("expected a connection reset to surface as an error")
	}
	// A reset must be retryable, not mistaken for misconfiguration.
	if got := storage.Classify(err); got != storage.ClassTransient {
		t.Errorf("Classify(reset_peer) = %v, want ClassTransient\nerr: %v", got, err)
	}

	if err := s.toxics.RemoveToxic("reset"); err != nil {
		t.Fatalf("remove toxic: %v", err)
	}
	eventually(t, 30*time.Second, "the endpoint to recover after the reset", func() bool {
		_, _, err := c.HeadObject(ctx, "l4-src", keys[0])
		return err == nil
	})
}
