package sync

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jabbrwcky/tranquila/internal/storage"
)

const (
	defaultCycleBackoff    = 5 * time.Second
	defaultCycleBackoffMax = 10 * time.Minute
)

// sleeper waits for d, reporting false when ctx was cancelled first.
// Injectable so backoff tests need no real delays.
type sleeper func(ctx context.Context, d time.Duration) bool

func waitOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// cycleBackoff returns base + uniform[0, min(base*2^(n-1), cap)) for the nth
// consecutive failure. Full jitter de-synchronises replicas hammering the same
// endpoint; the base term keeps the first retry off zero.
func (s *Syncer) cycleBackoff(n int) time.Duration {
	base, ceiling := s.cfg.CycleBackoff, s.cfg.CycleBackoffMax
	if base <= 0 {
		base = defaultCycleBackoff
	}
	if ceiling <= 0 {
		ceiling = defaultCycleBackoffMax
	}
	if ceiling < base {
		ceiling = base
	}
	d := base
	for range max(n-1, 0) {
		if d >= ceiling/2 {
			d = ceiling
			break
		}
		d *= 2
	}
	return base + rand.N(d)
}

// isFatalCycleErr reports whether a failed cycle is misconfiguration that
// retrying cannot fix. A cycle whose failures are a mix of permanent and
// transient counts as transient, so a flaky endpoint can never masquerade as
// misconfiguration and kill the process.
func isFatalCycleErr(err error) bool {
	if err == nil {
		return false
	}
	// One level of errors.Join fan-out, deliberately not errors.As: every child
	// must be permanent for the cycle to be fatal.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, e := range children {
			if !isFatalCycleErr(e) {
				return false
			}
		}
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return storage.Classify(err) == storage.ClassPermanent
}
