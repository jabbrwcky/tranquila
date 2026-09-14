package storage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
