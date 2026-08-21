package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// signals replays a sequence of call outcomes through the controller.
func (a *aimd) signals(classes ...ErrClass) {
	for _, c := range classes {
		switch c {
		case ClassTransient:
			a.onCongestion(false)
		case ClassThrottle:
			a.onCongestion(true)
		default:
			a.onHealthy()
		}
	}
}

func repeat(n int, c ErrClass) []ErrClass {
	out := make([]ErrClass, n)
	for i := range out {
		out[i] = c
	}
	return out
}

func newTestAIMD(base float64, failN int) *aimd {
	limit := rate.Inf
	if base > 0 {
		limit = rate.Limit(base)
	}
	return newAIMD(rate.NewLimiter(limit, 1), limit, failN)
}

func TestAIMD(t *testing.T) {
	tests := []struct {
		name         string
		base         float64 // 0 = unlimited
		failN        int
		signals      []ErrClass
		wantLimit    rate.Limit
		wantDegraded bool
	}{
		{
			name: "below_threshold_no_change", base: 100, failN: 5,
			signals: repeat(4, ClassTransient), wantLimit: 100,
		},
		{
			name: "threshold_halves", base: 100, failN: 5,
			signals: repeat(5, ClassTransient), wantLimit: 50, wantDegraded: true,
		},
		{
			name: "two_windows_quarter", base: 100, failN: 5,
			signals: repeat(10, ClassTransient), wantLimit: 25, wantDegraded: true,
		},
		{
			// Explicit back-pressure is unambiguous; act on the first signal.
			name: "throttle_acts_immediately", base: 100, failN: 5,
			signals: []ErrClass{ClassThrottle}, wantLimit: 50, wantDegraded: true,
		},
		{
			name: "clamps_at_floor", base: 100, failN: 1,
			signals: repeat(40, ClassTransient), wantLimit: aimdFloor, wantDegraded: true,
		},
		{
			name: "success_resets_consecutive_failures", base: 100, failN: 5,
			signals: append(append(repeat(4, ClassTransient), ClassOK), repeat(4, ClassTransient)...),
			// Never reached the threshold, so the base rate is untouched.
			wantLimit: 100,
		},
		{
			// The endpoint answered, so it is not congested.
			name: "permanent_counts_as_healthy", base: 100, failN: 5,
			signals:   append(append(repeat(4, ClassTransient), ClassPermanent), repeat(4, ClassTransient)...),
			wantLimit: 100,
		},
		{
			name: "additive_recovery_one_window", base: 100, failN: 5,
			signals:   append(repeat(5, ClassTransient), repeat(aimdRecoverAfter, ClassOK)...),
			wantLimit: 60, wantDegraded: true,
		},
		{
			name: "recovery_caps_at_base", base: 100, failN: 5,
			signals:   append(repeat(5, ClassTransient), repeat(aimdRecoverAfter*20, ClassOK)...),
			wantLimit: 100,
		},
		{
			// The operator declined to cap this endpoint; do not invent a ceiling.
			name: "unlimited_never_degrades", base: 0, failN: 5,
			signals: repeat(50, ClassTransient), wantLimit: rate.Inf,
		},
		{
			name: "unlimited_throttle_never_degrades", base: 0, failN: 5,
			signals: repeat(10, ClassThrottle), wantLimit: rate.Inf,
		},
		{
			name: "healthy_only_stays_at_base", base: 100, failN: 5,
			signals: repeat(100, ClassOK), wantLimit: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newTestAIMD(tt.base, tt.failN)
			a.signals(tt.signals...)

			if got := a.lim.Limit(); got != tt.wantLimit {
				t.Errorf("limit = %v, want %v", got, tt.wantLimit)
			}
			if got := a.state().Degraded; got != tt.wantDegraded {
				t.Errorf("degraded = %v, want %v", got, tt.wantDegraded)
			}
		})
	}
}

func TestAIMDStateReportsUnlimitedAsZero(t *testing.T) {
	st := newTestAIMD(0, 5).state()
	if st.Current != 0 || st.Base != 0 {
		t.Errorf("unlimited state = %+v, want zero Current and Base", st)
	}
	if st.Degraded {
		t.Error("unlimited endpoint reported as degraded")
	}
}

func TestAIMDDegradedSinceStamped(t *testing.T) {
	a := newTestAIMD(100, 1)
	a.signals(ClassTransient)
	st := a.state()
	if st.Since.IsZero() {
		t.Error("Since not stamped on entering a degraded episode")
	}
	if st.Current != 50 || st.Base != 100 {
		t.Errorf("state = %+v, want Current 50 Base 100", st)
	}
}

// TestAIMDConcurrent exercises the mutex discipline under -race.
func TestAIMDConcurrent(t *testing.T) {
	a := newTestAIMD(100, 5)
	classes := []ErrClass{ClassOK, ClassTransient, ClassThrottle, ClassPermanent}

	var wg sync.WaitGroup
	for g := range 100 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				a.signals(classes[(g+i)%len(classes)])
			}
			a.state()
		}(g)
	}
	wg.Wait()

	got := a.lim.Limit()
	if got < aimdFloor || got > 100 {
		t.Errorf("limit = %v, want within [%v, 100]", got, rate.Limit(aimdFloor))
	}
}

// TestWaitUnlimitedDoesNotBlock pins the rate.Inf short-circuit the whole design
// rests on: an uncapped client must pay nothing, and a degraded one must pace.
func TestWaitUnlimitedDoesNotBlock(t *testing.T) {
	lim := rate.NewLimiter(rate.Inf, 1)
	c := &Client{limiter: lim, aimd: newAIMD(lim, rate.Inf, 5)}

	start := time.Now()
	for range 100 {
		if err := c.wait(context.Background()); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("100 unlimited waits took %v, want near zero", elapsed)
	}

	// Once paced at 1 call/sec, burst 1 is spent immediately and the next call
	// must wait — proved by a deadline too short to satisfy it.
	lim.SetLimit(rate.Limit(1))
	if err := c.wait(context.Background()); err != nil {
		t.Fatalf("first paced wait: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.wait(ctx); err == nil {
		t.Error("expected the degraded limiter to block, got nil")
	}
}
