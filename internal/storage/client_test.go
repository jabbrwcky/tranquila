package storage

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestNewClientWiresCongestionControl proves the rate-limit protection is wired
// identically for source and destination: whichever endpoint is configured with
// a rate limit degrades under congestion, and an unconfigured one does not.
func TestNewClientWiresCongestionControl(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		signals      int
		wantLimit    rate.Limit
		wantDegraded bool
	}{
		{
			name:      "source_with_rate_limit_degrades",
			cfg:       Config{Name: "source", Region: "us-east-1", RateLimit: 100, FailThreshold: 2},
			signals:   2,
			wantLimit: 50, wantDegraded: true,
		},
		{
			name:      "destination_with_rate_limit_degrades",
			cfg:       Config{Name: "destination", Region: "us-east-1", RateLimit: 40, FailThreshold: 2},
			signals:   2,
			wantLimit: 20, wantDegraded: true,
		},
		{
			name:      "destination_below_threshold_holds",
			cfg:       Config{Name: "destination", Region: "us-east-1", RateLimit: 40, FailThreshold: 3},
			signals:   2,
			wantLimit: 40,
		},
		{
			name:      "destination_unlimited_stays_unlimited",
			cfg:       Config{Name: "destination", Region: "us-east-1", RateLimit: 0, FailThreshold: 2},
			signals:   10,
			wantLimit: rate.Inf,
		},
		{
			name:      "destination_default_threshold_applies",
			cfg:       Config{Name: "destination", Region: "us-east-1", RateLimit: 40},
			signals:   defaultFailN,
			wantLimit: 20, wantDegraded: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewClient(context.Background(), tt.cfg)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}

			// Drive the real recordOp path an S3 call would take on failure.
			for range tt.signals {
				c.recordOp(context.Background(), "PutObject", "dst-bucket",
					time.Now(), s3Err(504, "GatewayTimeout"))
			}

			if got := c.limiter.Limit(); got != tt.wantLimit {
				t.Errorf("limit = %v, want %v", got, tt.wantLimit)
			}
			st := c.LimitState()
			if st.Degraded != tt.wantDegraded {
				t.Errorf("degraded = %v, want %v", st.Degraded, tt.wantDegraded)
			}
		})
	}
}

// TestClientRecoversAfterCongestion covers the full degrade-then-restore cycle
// through the public surface, for a destination-shaped client.
func TestClientRecoversAfterCongestion(t *testing.T) {
	c, err := NewClient(context.Background(), Config{
		Name: "destination", Region: "us-east-1", RateLimit: 100, FailThreshold: 2,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()

	for range 2 {
		c.recordOp(ctx, "PutObject", "dst", time.Now(), s3Err(504, "GatewayTimeout"))
	}
	if got := c.LimitState(); !got.Degraded || got.Current != 50 {
		t.Fatalf("after congestion: %+v, want degraded at 50", got)
	}

	// Enough healthy calls to walk additively back to the configured base.
	for range aimdRecoverAfter * 20 {
		c.recordOp(ctx, "PutObject", "dst", time.Now(), nil)
	}
	st := c.LimitState()
	if st.Degraded || st.Current != 100 {
		t.Errorf("after recovery: %+v, want restored to 100 and not degraded", st)
	}
}

// TestClientThrottleDegradesImmediately pins that explicit back-pressure does
// not wait for the failure threshold.
func TestClientThrottleDegradesImmediately(t *testing.T) {
	c, err := NewClient(context.Background(), Config{
		Name: "destination", Region: "us-east-1", RateLimit: 100, FailThreshold: 10,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.recordOp(context.Background(), "PutObject", "dst", time.Now(), s3Err(503, "SlowDown"))
	if got := c.limiter.Limit(); got != 50 {
		t.Errorf("limit = %v, want 50 after a single throttle", got)
	}
}
