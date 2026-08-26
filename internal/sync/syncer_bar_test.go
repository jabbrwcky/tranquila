package sync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// fakeDeleter records DeleteObject calls for assertion. It also implements
// GetObject (returning body) so it satisfies sourceReadDeleter for
// TestPerformVerifyAndDelete; TestPerformBurnAfterReading never calls it.
type fakeDeleter struct {
	calls  []string // "bucket/key" per call
	retErr error

	body   []byte
	getErr error
}

func (f *fakeDeleter) DeleteObject(_ context.Context, bucket, key string) error {
	f.calls = append(f.calls, bucket+"/"+key)
	return f.retErr
}

func (f *fakeDeleter) GetObject(_ context.Context, _, _ string) (io.ReadCloser, int64, error) {
	if f.getErr != nil {
		return nil, 0, f.getErr
	}
	return io.NopCloser(bytes.NewReader(f.body)), int64(len(f.body)), nil
}

// fakeVerifier implements destinationVerifier for testing performVerifyAndDelete.
type fakeVerifier struct {
	size   int64
	etag   string
	retErr error

	body   []byte
	getErr error
}

func (f *fakeVerifier) HeadObject(_ context.Context, _, _ string) (int64, string, string, error) {
	return f.size, "", f.etag, f.retErr
}

func (f *fakeVerifier) GetObject(_ context.Context, _, _ string) (io.ReadCloser, int64, error) {
	if f.getErr != nil {
		return nil, 0, f.getErr
	}
	return io.NopCloser(bytes.NewReader(f.body)), int64(len(f.body)), nil
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
		srcETag     string // job.SrcETag; "" = no ETag fast path
		dstETag     string
		srcBody     []byte // content fallback tier; nil on both sides matches (both empty)
		dstBody     []byte
		srcGetErr   error
		dstGetErr   error
		dryRun      bool
		deleteErr   error
		wantDeleted bool
		wantErr     bool
	}{
		{
			name:        "matching_crc32_metadata_deletes",
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
			name:        "crc32_mismatch_refuses_delete_without_reading_content",
			uploadCRC32: crc32A,
			storedCRC32: crc32B,
			srcGetErr:   errors.New("must not be called"),
			dstGetErr:   errors.New("must not be called"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "dry_run_with_mismatched_crc32_still_no_delete",
			uploadCRC32: crc32A,
			storedCRC32: crc32B,
			dryRun:      true,
			wantDeleted: false,
			wantErr:     false, // dry-run never errors; logs a refusal instead of "would delete"
		},
		{
			// The bug this test guards against: a destination that never echoes
			// flexible checksums (common on non-AWS S3-compatible backends) must
			// not permanently refuse to delete — it should fall back to ETag.
			name:        "empty_crc32_falls_back_to_matching_etag",
			uploadCRC32: "",
			storedCRC32: "",
			srcETag:     "d41d8cd98f00b204e9800998ecf8427e",
			dstETag:     `"d41d8cd98f00b204e9800998ecf8427e"`, // S3 quotes ETags; must be stripped
			srcGetErr:   errors.New("must not be called"),
			dstGetErr:   errors.New("must not be called"),
			wantDeleted: true,
		},
		{
			name:        "empty_crc32_with_mismatched_etag_refuses_delete",
			uploadCRC32: "",
			storedCRC32: "",
			srcETag:     "d41d8cd98f00b204e9800998ecf8427e",
			dstETag:     "5d41402abc4b2a76b9719d911017c592",
			srcGetErr:   errors.New("must not be called"),
			dstGetErr:   errors.New("must not be called"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			// One side reported no checksum at all (e.g. destination doesn't
			// support flexible checksums), the other side has no usable ETag
			// either (multipart): falls all the way back to content hashing.
			name:        "empty_crc32_and_multipart_etag_falls_back_to_content_match",
			uploadCRC32: "",
			storedCRC32: "",
			srcETag:     "d41d8cd98f00b204e9800998ecf8427e-3",
			dstETag:     "d41d8cd98f00b204e9800998ecf8427e-3",
			srcBody:     []byte("hello"),
			dstBody:     []byte("hello"),
			wantDeleted: true,
		},
		{
			name:        "empty_crc32_no_etag_falls_back_to_content_match",
			uploadCRC32: "",
			storedCRC32: "",
			srcBody:     []byte("hello"),
			dstBody:     []byte("hello"),
			wantDeleted: true,
		},
		{
			name:        "content_fallback_mismatch_refuses_delete",
			uploadCRC32: "",
			storedCRC32: "",
			srcBody:     []byte("hello"),
			dstBody:     []byte("world"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "content_fallback_dry_run_mismatch_no_delete",
			uploadCRC32: "",
			storedCRC32: "",
			srcBody:     []byte("hello"),
			dstBody:     []byte("world"),
			dryRun:      true,
			wantDeleted: false,
			wantErr:     false,
		},
		{
			name:        "content_fallback_source_read_error_propagates",
			uploadCRC32: "",
			storedCRC32: "",
			srcGetErr:   errors.New("source unreachable"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "content_fallback_destination_read_error_propagates",
			uploadCRC32: "",
			storedCRC32: "",
			dstGetErr:   errors.New("destination unreachable"),
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fd := &fakeDeleter{retErr: tc.deleteErr, body: tc.srcBody, getErr: tc.srcGetErr}
			fv := &fakeVerifier{body: tc.dstBody, getErr: tc.dstGetErr}
			job := Job{
				SrcBucket: "src",
				DstBucket: "dst",
				Key:       "some/object.dat",
				DstKey:    "some/object.dat",
				SrcETag:   tc.srcETag,
				DryRun:    tc.dryRun,
			}
			err := performBurnAfterReading(context.Background(), job, fd, fv, tc.uploadCRC32, tc.storedCRC32, tc.dstETag)

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
		srcETag     string // "" = no ETag fast path; falls back to content comparison
		dstETag     string
		srcBody     []byte // defaults (nil) match on both sides: CRC32 of empty content is equal
		dstBody     []byte
		headErr     error
		srcGetErr   error
		dstGetErr   error
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
		{
			// Same size, different content: the destination has been overwritten
			// since it was synced. Size alone would have let this through.
			name:        "checksum_mismatch_same_size_refuses_delete",
			jobSize:     5,
			dstSize:     5,
			srcBody:     []byte("hello"),
			dstBody:     []byte("world"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "checksum_match_deletes",
			jobSize:     5,
			dstSize:     5,
			srcBody:     []byte("hello"),
			dstBody:     []byte("hello"),
			wantDeleted: true,
		},
		{
			name:        "source_read_error_propagates",
			jobSize:     1024,
			dstSize:     1024,
			srcGetErr:   errors.New("source unreachable"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			name:        "destination_read_error_propagates",
			jobSize:     1024,
			dstSize:     1024,
			dstGetErr:   errors.New("destination unreachable"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			// Matching single-part ETags must skip content comparison entirely:
			// forcing both GetObject calls to fail proves they were never made.
			name:        "matching_etags_skip_content_read",
			jobSize:     1024,
			dstSize:     1024,
			srcETag:     "d41d8cd98f00b204e9800998ecf8427e",
			dstETag:     `"d41d8cd98f00b204e9800998ecf8427e"`, // S3 quotes ETags; must be stripped
			srcGetErr:   errors.New("must not be called"),
			dstGetErr:   errors.New("must not be called"),
			wantDeleted: true,
		},
		{
			// Different single-part ETags conclusively prove different content:
			// refuse immediately, again without reading either object.
			name:        "mismatched_etags_refuse_without_content_read",
			jobSize:     1024,
			dstSize:     1024,
			srcETag:     "d41d8cd98f00b204e9800998ecf8427e",
			dstETag:     "5d41402abc4b2a76b9719d911017c592",
			srcGetErr:   errors.New("must not be called"),
			dstGetErr:   errors.New("must not be called"),
			wantDeleted: false,
			wantErr:     true,
		},
		{
			// A multipart (composite) ETag is never comparable to another
			// object's ETag; this must fall back to hashing content, which
			// here agrees, so the delete proceeds.
			name:        "multipart_etag_falls_back_to_matching_content",
			jobSize:     5,
			dstSize:     5,
			srcETag:     "d41d8cd98f00b204e9800998ecf8427e-3", // composite: 3 parts
			dstETag:     "d41d8cd98f00b204e9800998ecf8427e-3",
			srcBody:     []byte("hello"),
			dstBody:     []byte("hello"),
			wantDeleted: true,
		},
		{
			// A multipart ETag must never be compared even when byte-identical
			// to the other side's composite value; content still disagrees here,
			// which the ETag shape alone could not have caught.
			name:        "multipart_etag_falls_back_and_catches_mismatch",
			jobSize:     5,
			dstSize:     5,
			srcETag:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-2",
			dstETag:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-2",
			srcBody:     []byte("hello"),
			dstBody:     []byte("world"),
			wantDeleted: false,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fv := &fakeVerifier{size: tc.dstSize, etag: tc.dstETag, retErr: tc.headErr, body: tc.dstBody, getErr: tc.dstGetErr}
			fd := &fakeDeleter{retErr: tc.deleteErr, body: tc.srcBody, getErr: tc.srcGetErr}
			job := Job{
				SrcBucket: "src",
				DstBucket: "dst",
				Key:       "data/file.bin",
				DstKey:    "data/file.bin",
				Size:      tc.jobSize,
				SrcETag:   tc.srcETag,
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
