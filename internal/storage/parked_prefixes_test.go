package storage

import "testing"

// TestClientParkedPrefixesDefaultsToZero covers the two "nothing to report
// yet" cases: a bucket that has never been sharded-discovered on this Client,
// and a Client whose parkedPrefixes map is nil — NewClient always initializes
// it, but recordParkedPrefixes lazy-inits defensively rather than assuming
// that always held, since writing to a nil map panics.
func TestClientParkedPrefixesDefaultsToZero(t *testing.T) {
	c := &Client{}
	if got := c.ParkedPrefixes("events"); got != 0 {
		t.Errorf("ParkedPrefixes on an untouched Client = %d, want 0", got)
	}

	c.recordParkedPrefixes("orders", 3)
	if got := c.ParkedPrefixes("events"); got != 0 {
		t.Errorf("ParkedPrefixes(\"events\") = %d, want 0 — a different bucket must not leak in", got)
	}
}

// TestClientParkedPrefixesTracksLatestPerBucket proves the map is keyed per
// bucket (buckets don't clobber each other) and that each new record replaces
// the previous one rather than accumulating — the gauge and the status column
// both need "as of the last completed cycle," not a running total.
func TestClientParkedPrefixesTracksLatestPerBucket(t *testing.T) {
	c := &Client{}

	c.recordParkedPrefixes("events", 178)
	c.recordParkedPrefixes("orders", 0)
	if got := c.ParkedPrefixes("events"); got != 178 {
		t.Errorf("events = %d, want 178", got)
	}
	if got := c.ParkedPrefixes("orders"); got != 0 {
		t.Errorf("orders = %d, want 0", got)
	}

	// A later cycle with fewer parked prefixes must overwrite, not add to, the
	// earlier count.
	c.recordParkedPrefixes("events", 12)
	if got := c.ParkedPrefixes("events"); got != 12 {
		t.Errorf("events after a second cycle = %d, want 12 (latest, not cumulative)", got)
	}
	if got := c.ParkedPrefixes("orders"); got != 0 {
		t.Errorf("orders = %d, want 0 — unaffected by events' update", got)
	}
}
