package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestEscalateListTimeout(t *testing.T) {
	const base = 60 * time.Second
	max := base * listAttemptTimeoutMaxFactor

	tests := []struct {
		name     string
		cur      time.Duration
		timedOut bool
		want     time.Duration
	}{
		// A 504 needs another try, not a longer one.
		{"non-timeout failure holds the deadline", base, false, base},
		{"non-timeout failure holds an already-escalated deadline", 2 * base, false, 2 * base},
		{"first timeout doubles", base, true, 2 * base},
		{"second timeout doubles again", 2 * base, true, 4 * base},
		{"escalation stops at the cap", 4 * base, true, max},
		{"an over-cap deadline is clamped, never grown", 8 * base, true, max},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := escalateListTimeout(tc.cur, max, tc.timedOut); got != tc.want {
				t.Errorf("escalateListTimeout(%v, %v, %v) = %v, want %v",
					tc.cur, max, tc.timedOut, got, tc.want)
			}
		})
	}
}

// The whole point of listErrClass: a self-imposed deadline must reach the AIMD
// controller as congestion, even though Classify calls DeadlineExceeded ClassOK.
func TestListErrClass(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		timedOut bool
		want     ErrClass
	}{
		{"self-inflicted timeout is congestion, not OK", context.DeadlineExceeded, true, ClassTransient},
		{"wrapped self-inflicted timeout is congestion", fmt.Errorf("list: %w", context.DeadlineExceeded), true, ClassTransient},
		{"caller cancellation stays OK", context.Canceled, false, ClassOK},
		{"outer deadline stays OK", context.DeadlineExceeded, false, ClassOK},
		{"success stays OK", nil, false, ClassOK},
		{"other errors classify normally", io.ErrUnexpectedEOF, false, ClassTransient},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := listErrClass(tc.err, tc.timedOut); got != tc.want {
				t.Errorf("listErrClass(%v, %v) = %v, want %v", tc.err, tc.timedOut, got, tc.want)
			}
		})
	}
}

// A timed-out attempt must feed the congestion controller, or concurrent
// discovery workers looping on timeouts keep the endpoint pinned at "healthy":
// onHealthy resets consecFail, so the fail threshold is never reached and the
// rate limit drifts back up against a backend that is already too slow.
func TestListTimeoutDegradesRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	const failThreshold = 2
	c := testClient(t, srv.URL, 5*time.Millisecond, failThreshold)
	before := c.LimitState()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range failThreshold {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 5*time.Millisecond)
		_, err := c.s3.ListObjectsV2(attemptCtx, &s3.ListObjectsV2Input{Bucket: aws.String("b")})
		timedOut := listAttemptTimedOut(ctx, attemptCtx, err)
		attemptCancel()
		if !timedOut {
			t.Fatalf("expected a self-inflicted attempt timeout, got err=%v", err)
		}
		c.recordOpClass(ctx, "ListObjectsV2", "b", time.Now(), err, listErrClass(err, timedOut))
	}

	after := c.LimitState()
	if !(after.Current < before.Current) {
		t.Errorf("rate limit did not degrade after %d list timeouts: before=%v after=%v",
			failThreshold, before.Current, after.Current)
	}
}

// End-to-end proof that the deadline actually grows between attempts: the
// server answers only after the first attempt's deadline has passed, so the
// call can succeed only if attempt 2 is given longer than attempt 1.
func TestListPageWithRetryEscalatesDeadline(t *testing.T) {
	const attemptTimeout = 150 * time.Millisecond
	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slower than attempt 1's deadline, comfortably faster than attempt
		// 2's doubled one, so the test turns on escalation and not on timing.
		delay := attemptTimeout * 3 / 2
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			calls.Add(1)
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+
			`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
			`<Name>b</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated>`+
			`</ListBucketResult>`)
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, attemptTimeout, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := c.listPageWithRetry(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("b")})
	if err != nil {
		t.Fatalf("listPageWithRetry: %v", err)
	}
	if out == nil {
		t.Fatal("listPageWithRetry returned no output")
	}
	if got := calls.Load(); got < 2 {
		t.Errorf("server saw %d attempts, want at least 2 (the retry is the point)", got)
	}
}

// A non-timeout transient failure must not inflate the deadline, so a flaky
// gateway keeps getting the full retry count rather than fewer, longer waits.
func TestListPageWithRetryHoldsDeadlineOnGatewayError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+
			`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
			`<Name>b</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated>`+
			`</ListBucketResult>`)
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, time.Minute, 0)
	if _, err := c.listPageWithRetry(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("b")}); err != nil {
		t.Fatalf("listPageWithRetry: %v", err)
	}
}

// A permanent error must still short-circuit rather than consume the budget.
func TestListPageWithRetryStopsOnPermanentError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, time.Minute, 0)
	_, err := c.listPageWithRetry(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("b")})
	if err == nil {
		t.Fatal("expected a permanent error")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("permanent error was retried into a timeout: %v", err)
	}
}

func testClient(t *testing.T, endpoint string, listAttemptTimeout time.Duration, failThreshold int) *Client {
	t.Helper()
	c, err := NewClient(context.Background(), Config{
		Endpoint:           endpoint,
		Region:             "us-east-1",
		AccessKey:          "test",
		SecretKey:          "test",
		RateLimit:          1000, // finite, so the AIMD controller has a ceiling to halve
		FailThreshold:      failThreshold,
		ListAttemptTimeout: listAttemptTimeout,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}
