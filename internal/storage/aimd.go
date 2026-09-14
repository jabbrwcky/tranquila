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

	mu      sync.Mutex
	current rate.Limit
	// failScore is a leaky bucket, not a consecutive-failure count: a failure
	// adds one, a healthy call removes one, and the rate halves when it reaches
	// failN. It used to be zeroed by any healthy call, which made the threshold
	// nearly unreachable on a client that multiplexes concurrent work — one
	// endpoint serves discovery listings and the transfer pool at once, so a
	// single successful GetObject erased an arbitrarily long run of ListObjectsV2
	// timeouts and the endpoint stayed pinned at "healthy" while it was visibly
	// failing. "Consecutive" is not a meaningful property of interleaved
	// concurrent calls; outnumbering is.
	failScore  int
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
// the failure score reaches failN — that is, once failures have outnumbered
// healthy calls by failN (see failScore). A throttle is unambiguous
// back-pressure and acts on the first signal. Reports whether the rate changed.
func (a *aimd) onCongestion(throttle bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.healthyOps = 0
	a.failScore++
	if !throttle && a.failScore < a.failN {
		return false
	}
	a.failScore = 0

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

	// Decay by one rather than zeroing: see failScore. The asymmetry with
	// healthyOps below — which a single failure does still reset — is the
	// deliberate congestion-control posture of backing off readily and ramping
	// up only from a genuinely quiet endpoint.
	if a.failScore > 0 {
		a.failScore--
	}
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
