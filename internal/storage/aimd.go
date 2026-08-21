package storage

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	aimdDecreaseFactor = 0.5 // conventional multiplicative decrease
	aimdIncreaseRatio  = 0.1 // additive increase: 10% of base per recovery window
	aimdRecoverAfter   = 20  // healthy round-trips per additive increase
	aimdFloor          = 1.0 // calls/sec; below this a large bucket never finishes
	defaultFailN       = 5
)

// LimitState is a point-in-time view of one endpoint's pacing.
// Current and Base are 0 when unlimited, matching the config convention.
type LimitState struct {
	Current  float64
	Base     float64
	Degraded bool
	Since    time.Time
}

// aimd paces one S3 endpoint with additive-increase/multiplicative-decrease
// congestion control. State is event-counted rather than time-based, so the
// control loop is deterministic under test.
type aimd struct {
	lim   *rate.Limiter
	base  rate.Limit // configured ceiling; rate.Inf = unlimited, never degraded
	failN int

	mu         sync.Mutex
	current    rate.Limit
	consecFail int
	healthyOps int
	since      time.Time
}

func newAIMD(lim *rate.Limiter, base rate.Limit, failN int) *aimd {
	if failN < 1 {
		failN = defaultFailN
	}
	return &aimd{lim: lim, base: base, failN: failN, current: base}
}

// onCongestion records a transient or throttle failure, halving the rate once
// failN consecutive failures have accrued. A throttle is unambiguous
// back-pressure and acts on the first signal. Reports whether the rate changed.
func (a *aimd) onCongestion(throttle bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.healthyOps = 0
	a.consecFail++
	if !throttle && a.consecFail < a.failN {
		return false
	}
	a.consecFail = 0

	// An endpoint the operator declined to cap has no ceiling to halve, and
	// inventing one would throttle a healthy endpoint.
	if a.base == rate.Inf {
		return false
	}

	next := max(rate.Limit(float64(a.current)*aimdDecreaseFactor), aimdFloor)
	if next == a.current {
		return false // already at the floor
	}
	if a.current == a.base {
		a.since = time.Now()
	}
	a.current = next
	a.lim.SetLimit(next)
	return true
}

// onHealthy records a completed round-trip, additively restoring capacity once
// a recovery window of healthy calls has passed. Reports whether the rate changed.
func (a *aimd) onHealthy() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.consecFail = 0
	if a.current == a.base {
		return false
	}
	a.healthyOps++
	if a.healthyOps < aimdRecoverAfter {
		return false
	}
	a.healthyOps = 0

	next := min(a.current+rate.Limit(float64(a.base)*aimdIncreaseRatio), a.base)
	a.current = next
	a.lim.SetLimit(next)
	return true
}

func (a *aimd) state() LimitState {
	a.mu.Lock()
	defer a.mu.Unlock()

	s := LimitState{Degraded: a.current != a.base}
	if a.current != rate.Inf {
		s.Current = float64(a.current)
	}
	if a.base != rate.Inf {
		s.Base = float64(a.base)
	}
	if s.Degraded {
		s.Since = a.since
	}
	return s
}
