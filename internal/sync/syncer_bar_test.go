package sync

import (
	"context"
	"errors"
	"testing"
)

// fakeDeleter records DeleteObject calls for assertion.
type fakeDeleter struct {
	calls  []string // "bucket/key" per call
	retErr error
}

func (f *fakeDeleter) DeleteObject(_ context.Context, bucket, key string) error {
	f.calls = append(f.calls, bucket+"/"+key)
	return f.retErr
}

// fakeVerifier implements destinationVerifier for testing performVerifyAndDelete.
type fakeVerifier struct {
	size   int64
	crc32  string
	retErr error
}

func (f *fakeVerifier) HeadObject(_ context.Context, _, _ string) (int64, string, error) {
	return f.size, f.crc32, f.retErr
}

func TestPerformBurnAfterReading(t *testing.T) {
	const (
		crc32A = "abcd1234" // any non-empty string
		crc32B = "zzzzzzzz"
	)

	tests := []struct {
		name        string
		uploadCRC32 string
		storedCRC32 string
		dryRun      bool
		deleteErr   error
		wantDeleted bool
		wantErr     bool
	}{
		{
			name:        "matching_checksums_live",
			uploadCRC32: crc32A,
			storedCRC32: crc32A,
			wantDeleted: true,
		},
		{
			name:        "dry_run_no_delete",
			uploadCRC32: crc32A,
			storedCRC32: crc32A,
			dryRun:      true,
			wantDeleted: false,
		},
		{
			name:        "mismatch_refuses_delete",
			uploadCRC32: crc32A,
			storedCRC32: crc32B,
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "empty_upload_crc32_refuses_delete",
			uploadCRC32: "",
			storedCRC32: crc32A,
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "empty_stored_crc32_refuses_delete",
			uploadCRC32: crc32A,
			storedCRC32: "",
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "both_empty_refuses_delete",
			uploadCRC32: "",
			storedCRC32: "",
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "delete_error_propagates",
			uploadCRC32: crc32A,
			storedCRC32: crc32A,
			deleteErr:   errors.New("permission denied"),
			wantDeleted: true, // call was attempted
			wantErr:     true,
		},
		{
			name:        "dry_run_with_mismatched_checksums_still_no_delete",
			uploadCRC32: crc32A,
			storedCRC32: crc32B,
			dryRun:      true,
			wantDeleted: false,
			wantErr:     false, // dry-run exits before the mismatch check
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fd := &fakeDeleter{retErr: tc.deleteErr}
			job := Job{
				SrcBucket: "src",
				DstBucket: "dst",
				Key:       "some/object.dat",
				DryRun:    tc.dryRun,
			}
			err := performBurnAfterReading(context.Background(), job, fd, tc.uploadCRC32, tc.storedCRC32)

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
			if tc.wantDeleted && len(fd.calls) > 0 && fd.calls[0] != "src/some/object.dat" {
				t.Errorf("deleted wrong key: got %q, want %q", fd.calls[0], "src/some/object.dat")
			}
		})
	}
}

func TestPerformVerifyAndDelete(t *testing.T) {
	tests := []struct {
		name        string
		jobSize     int64 // 0 = skip size check
		dstSize     int64
		headErr     error
		deleteErr   error
		dryRun      bool
		wantDeleted bool
		wantErr     bool
	}{
		{
			name:        "succeeds_matching_size",
			jobSize:     1024,
			dstSize:     1024,
			wantDeleted: true,
		},
		{
			name:        "succeeds_zero_job_size_skips_check",
			jobSize:     0,
			dstSize:     500, // different, but skipped because job.Size==0
			wantDeleted: true,
		},
		{
			name:        "dry_run_no_delete",
			jobSize:     1024,
			dstSize:     1024,
			dryRun:      true,
			wantDeleted: false,
		},
		{
			name:        "destination_missing_returns_error",
			jobSize:     1024,
			dstSize:     0,
			headErr:     errors.New("not found"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "size_mismatch_returns_error",
			jobSize:     1024,
			dstSize:     512,
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "delete_error_propagates",
			jobSize:     1024,
			dstSize:     1024,
			deleteErr:   errors.New("access denied"),
			wantDeleted: true, // call was attempted
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fv := &fakeVerifier{size: tc.dstSize, retErr: tc.headErr}
			fd := &fakeDeleter{retErr: tc.deleteErr}
			job := Job{
				SrcBucket: "src",
				DstBucket: "dst",
				Key:       "data/file.bin",
				DstKey:    "data/file.bin",
				Size:      tc.jobSize,
				DryRun:    tc.dryRun,
			}
			err := performVerifyAndDelete(context.Background(), job, fv, fd)

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
			if tc.wantDeleted && len(fd.calls) > 0 && fd.calls[0] != "src/data/file.bin" {
				t.Errorf("deleted wrong key: got %q, want %q", fd.calls[0], "src/data/file.bin")
			}
		})
	}
}
