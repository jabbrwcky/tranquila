package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
)

func TestPerformDelete(t *testing.T) {
	tests := []struct {
		name        string
		dryRun      bool
		deleteErr   error
		wantDeleted bool
		wantErr     bool
	}{
		{
			name:        "deletes_destination_object",
			wantDeleted: true,
		},
		{
			name:        "dry_run_no_delete",
			dryRun:      true,
			wantDeleted: false,
		},
		{
			name:        "delete_error_propagates",
			deleteErr:   errors.New("access denied"),
			wantDeleted: true, // call was attempted
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fd := &fakeDeleter{retErr: tc.deleteErr}
			job := Job{
				SrcBucket: "src",
				DstBucket: "dst",
				Key:       "some/object.dat",
				DstKey:    "some/object.dat",
				DryRun:    tc.dryRun,
			}
			err := performDelete(context.Background(), job, fd)

			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			deleted := len(fd.calls) > 0
			if deleted != tc.wantDeleted {
				t.Errorf("deleted=%v, want %v (calls=%v)", deleted, tc.wantDeleted, fd.calls)
			}
			if tc.wantDeleted && len(fd.calls) > 0 && fd.calls[0] != "dst/some/object.dat" {
				t.Errorf("deleted wrong key: got %q, want %q", fd.calls[0], "dst/some/object.dat")
			}
		})
	}
}

func TestDueForReconcile(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		last     time.Time
		interval time.Duration
		want     bool
	}{
		{"zero_interval_always_due", now.Add(-time.Second), 0, true},
		{"never_run_before_is_due", time.Time{}, time.Hour, true},
		{"just_ran_not_due", now.Add(-time.Minute), time.Hour, false},
		{"interval_elapsed_is_due", now.Add(-2 * time.Hour), time.Hour, true},
		{"exactly_at_interval_is_due", now.Add(-time.Hour), time.Hour, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := dueForReconcile(tc.last, tc.interval, now); got != tc.want {
				t.Errorf("dueForReconcile() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcileDue(t *testing.T) {
	s := &Syncer{lastReconcile: make(map[string]time.Time)}
	s.cfg.DeleteReconcileInterval = time.Hour
	now := time.Now()

	if !s.reconcileDue("b1", now) {
		t.Error("first call should be due")
	}
	if s.reconcileDue("b1", now.Add(time.Minute)) {
		t.Error("second call within the interval should not be due")
	}
	if !s.reconcileDue("b1", now.Add(2*time.Hour)) {
		t.Error("call after the interval elapsed should be due")
	}
	if !s.reconcileDue("b2", now) {
		t.Error("a different bucket's due-ness must be tracked independently")
	}
}

func TestClassifyHeadErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want reconcileVerdict
	}{
		{"nil_err_still_present", nil, reconcileStillPresent},
		{"not_found_confirms_deleted", &smithy.GenericAPIError{Code: "NoSuchKey"}, reconcileConfirmedDeleted},
		{"generic_error_inconclusive", errors.New("dial tcp: connection refused"), reconcileInconclusive},
		{"permanent_error_inconclusive", &smithy.GenericAPIError{Code: "AccessDenied"}, reconcileInconclusive},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyHeadErr(tc.err); got != tc.want {
				t.Errorf("classifyHeadErr() = %v, want %v", got, tc.want)
			}
		})
	}
}
