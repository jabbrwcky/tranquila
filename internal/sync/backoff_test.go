package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
)

func TestCycleBackoff(t *testing.T) {
	const (
		base    = time.Second
		ceiling = 8 * time.Second
	)
	s := &Syncer{cfg: Config{CycleBackoff: base, CycleBackoffMax: ceiling}}

	tests := []struct {
		name    string
		n       int
		wantMax time.Duration // exclusive upper bound: base + window
	}{
		{"zero_clamped_to_first", 0, base + base},
		{"first_failure", 1, base + base},
		{"second_doubles", 2, base + 2*time.Second},
		{"third_doubles", 3, base + 4*time.Second},
		{"fourth_hits_cap", 4, base + ceiling},
		{"far_past_cap_stays_capped", 100, base + ceiling},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Jittered, so assert the band rather than an exact value.
			for range 50 {
				got := s.cycleBackoff(tt.n)
				if got < base {
					t.Fatalf("cycleBackoff(%d) = %v, want >= %v", tt.n, got, base)
				}
				if got >= tt.wantMax {
					t.Fatalf("cycleBackoff(%d) = %v, want < %v", tt.n, got, tt.wantMax)
				}
			}
		})
	}
}

func TestCycleBackoffDefaults(t *testing.T) {
	s := &Syncer{}
	got := s.cycleBackoff(1)
	if got < defaultCycleBackoff || got >= 2*defaultCycleBackoff {
		t.Errorf("cycleBackoff(1) = %v, want [%v, %v)", got, defaultCycleBackoff, 2*defaultCycleBackoff)
	}
}

func TestIsFatalCycleErr(t *testing.T) {
	permanent := &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
	permanent2 := &smithy.GenericAPIError{Code: "InvalidAccessKeyId", Message: "bad key"}
	transient := errors.New("boom")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"transient", transient, false},
		{"permanent", permanent, true},
		{"context_canceled", context.Canceled, false},
		{"empty_join", errors.Join(), false},
		{"joined_all_permanent", errors.Join(permanent, permanent2), true},
		// A flaky endpoint must never masquerade as misconfiguration.
		{"joined_mixed_not_fatal", errors.Join(permanent, transient), false},
		{"joined_single_permanent", errors.Join(permanent), true},
		{"joined_all_transient", errors.Join(transient, transient), false},
		{"wrapped_permanent", fmt.Errorf("bucket x: %w", permanent), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFatalCycleErr(tt.err); got != tt.want {
				t.Errorf("isFatalCycleErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitialSync(t *testing.T) {
	permanent := &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}

	tests := []struct {
		name         string
		failures     int
		failWith     error
		wantErr      bool
		wantAttempts int
	}{
		{"succeeds_first_try", 0, nil, false, 1},
		{"retries_then_succeeds", 2, errors.New("boom"), false, 3},
		{"permanent_returns_immediately", 5, permanent, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Syncer{m: testMetrics(t)}
			var attempts int
			cycleFn := func(context.Context) error {
				attempts++
				if attempts <= tt.failures {
					return tt.failWith
				}
				return nil
			}
			noSleep := func(context.Context, time.Duration) bool { return true }

			err := s.initialSync(context.Background(), cycleFn, noSleep)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if attempts != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
		})
	}
}

func TestInitialSyncCancelDuringBackoff(t *testing.T) {
	s := &Syncer{m: testMetrics(t)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cycleFn := func(context.Context) error { return errors.New("boom") }
	// Cancellation during backoff exits cleanly rather than looping forever.
	cancelledSleep := func(context.Context, time.Duration) bool {
		cancel()
		return false
	}
	if err := s.initialSync(ctx, cycleFn, cancelledSleep); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
