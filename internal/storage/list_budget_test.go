package storage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// budgetExhaustedClient returns a client pointed at a server that never answers
// within an attempt's deadline, with a retry budget small enough that the loop
// gives up after the first timed-out attempt. This reproduces the production
// crash: a successful limiter wait used to overwrite the last attempt's error
// with nil immediately before the budget break, so listPageWithRetry returned
// (nil, nil) and every caller dereferenced the nil page.
func budgetExhaustedClient(t *testing.T) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	c := testClient(t, srv.URL, 50*time.Millisecond, 0)
	c.listRetryBudget = 60 * time.Millisecond
	return c
}

func TestListPageWithRetryBudgetExhaustedReturnsError(t *testing.T) {
	c := budgetExhaustedClient(t)

	out, err := c.listPageWithRetry(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("b")})
	if err == nil {
		t.Fatal("listPageWithRetry returned a nil error after exhausting the budget")
	}
	if out != nil {
		t.Fatalf("expected no page alongside the error, got %#v", out)
	}
	// Kept unwrapped so isShardableListErr's DeadlineExceeded special case,
	// and the flat->sharded fallback it drives, still fire.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("attempt timeout lost on the way out: %v", err)
	}
}

// The exact production stack: listObjectsTree worker -> listDelimitedPage ->
// objectsFromContents on a nil page.
func TestListDelimitedPageBudgetExhaustedDoesNotPanic(t *testing.T) {
	c := budgetExhaustedClient(t)

	objs, prefixes, next, err := c.listDelimitedPage("b")(context.Background(), "2026/4/1/", nil)
	if err == nil {
		t.Fatal("expected an error from a prefix whose budget ran out")
	}
	var le *ListError
	if !errors.As(err, &le) {
		t.Errorf("error is not a *ListError, so sharded discovery cannot classify it: %v", err)
	}
	if objs != nil || prefixes != nil || next != nil {
		t.Errorf("expected empty results, got objs=%v prefixes=%v next=%v", objs, prefixes, next)
	}
}

// The flat discovery path dereferences the same result.
func TestListObjectsPageBudgetExhaustedDoesNotPanic(t *testing.T) {
	c := budgetExhaustedClient(t)

	collected, next, err := c.ListObjectsPage(context.Background(), "b", "", nil, 1000,
		func([]Object) error { return errors.New("onPage must not be reached") })
	if err == nil {
		t.Fatal("expected an error from a listing whose budget ran out")
	}
	if collected != 0 || next != nil {
		t.Errorf("expected no progress, got collected=%d next=%v", collected, next)
	}
}

// Config.ListRetryBudget threads through NewClient exactly like
// Config.DiscoveryPrefixBudget: 0 resolves to the default, anything else
// (including negative) survives unchanged.
func TestNewClientListRetryBudgetResolution(t *testing.T) {
	cases := []struct {
		name         string
		configured   time.Duration
		wantResolved time.Duration
	}{
		{"unset resolves to the default", 0, defaultListRetryBudget},
		{"positive override survives", 5 * time.Minute, 5 * time.Minute},
		{"negative (unbounded) survives unchanged", -1, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(context.Background(), Config{
				Endpoint:        "http://127.0.0.1:0",
				Region:          "us-east-1",
				AccessKey:       "test",
				SecretKey:       "test",
				RateLimit:       1000,
				ListRetryBudget: tc.configured,
			})
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			if c.listRetryBudget != tc.wantResolved {
				t.Errorf("listRetryBudget = %v, want %v", c.listRetryBudget, tc.wantResolved)
			}
		})
	}
}

// budgetRemaining is the fix for the actual production bug this file's other
// tests reproduce: naively resolving Config.ListRetryBudget's 0/negative
// convention without gating the loop's break would make a *negative*
// ("unbounded") budget exhaust on attempt 1 instead of never — the opposite
// of what "unbounded" must mean. Table-driven and sleep-free, unlike the
// HTTP-server tests above, because this is pure time.Time arithmetic.
func TestBudgetRemaining(t *testing.T) {
	began := time.Now().Add(-time.Minute) // one minute has already elapsed

	cases := []struct {
		name            string
		listRetryBudget time.Duration
		wantExhausted   bool
	}{
		{"unbounded (negative) is never exhausted despite elapsed time", -1, false},
		{"zero is never exhausted (treated as unbounded, not zero budget)", 0, false},
		{"positive budget already spent is exhausted", 30 * time.Second, true},
		{"positive budget with room left is not exhausted", 2 * time.Minute, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			remaining, exhausted := budgetRemaining(tc.listRetryBudget, began)
			if exhausted != tc.wantExhausted {
				t.Errorf("exhausted = %v, want %v (remaining=%v)", exhausted, tc.wantExhausted, remaining)
			}
			if tc.listRetryBudget <= 0 && remaining != unboundedRemaining {
				t.Errorf("unbounded budget returned remaining=%v, want unboundedRemaining", remaining)
			}
		})
	}
}

// TestListPageWithRetryNegativeBudgetNeverBreaksEarly is the regression test
// for the gating fix: without it, a negative (unbounded) listRetryBudget made
// `remaining <= 0` true from the very first attempt, so the loop broke after
// exactly one attempt — the opposite of "unbounded". It does not run the loop
// to completion: with the budget clamp gone, the real exponential backoff
// (1s, 2s, 4s, ... capped at 30s, per attempt) would take minutes across all
// listMaxRetries attempts, and there is no test seam to fake that clock (only
// listRetryBudget has one). Instead the OUTER ctx bounds the call, which is
// enough to observe the loop proceeding past attempt 1 — the one thing the
// bug prevented — without ever waiting out the full backoff schedule.
func TestListPageWithRetryNegativeBudgetNeverBreaksEarly(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, 5*time.Millisecond, 0)
	c.listRetryBudget = -1 // unbounded

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := c.listPageWithRetry(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("b")})
	if err == nil {
		t.Fatal("expected an error; the server never answers and the outer context times out")
	}
	if got := attempts.Load(); got < 2 {
		t.Errorf("attempts = %d, want >= 2 — a negative budget must not break the loop after only one attempt", got)
	}
}

// TestListPageWithRetryRaisedBudgetKeepsTryingLonger proves the reported bug
// is now avoidable: raising listRetryBudget (the field --list-retry-budget now
// controls) lets the loop keep retrying proportionally longer before giving
// up, where the default 10-minute value would previously have been the only
// option. Elapsed time, not attempt count, is what distinguishes the two
// budgets here: the backoff-delay clamp (`delay = min(delay, remaining)`)
// always shrinks the sleep down to exactly whatever budget remains once the
// real unclamped backoff (seconds-scale, starting at 1s) exceeds it — which
// it does for any budget small enough to keep this test fast — so a second
// real HTTP attempt never has budget left to run regardless of budget size,
// and attempt count alone would show no difference (confirmed empirically:
// both budgets below produced exactly one attempt each).
func TestListPageWithRetryRaisedBudgetKeepsTryingLonger(t *testing.T) {
	elapsed := func(budget time.Duration) time.Duration {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer srv.Close()

		c := testClient(t, srv.URL, 10*time.Millisecond, 0)
		c.listRetryBudget = budget

		start := time.Now()
		_, _ = c.listPageWithRetry(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("b")})
		return time.Since(start)
	}

	small := elapsed(60 * time.Millisecond)
	larger := elapsed(400 * time.Millisecond)
	if larger <= small {
		t.Errorf("raising the budget did not let the retry loop keep trying longer: small=%v larger=%v", small, larger)
	}
}
