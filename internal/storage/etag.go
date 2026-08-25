package storage

import (
	"encoding/hex"
	"strings"
)

// SinglePartMD5 extracts the MD5 hex digest from an S3 ETag, when the ETag
// represents one. It returns ok=false for a multipart (composite) ETag — which
// carries a "-partCount" suffix and is a hash of the parts' MD5s, not of the
// object's content — and for anything else that doesn't parse as a bare
// 128-bit hex digest. A caller can safely compare two SinglePartMD5 results
// for equality as a stand-in for comparing full object content, without
// downloading either object: identical content produces identical single-part
// ETags on any S3-compatible backend, since that MD5 convention is exactly
// what "single part" means.
//
// Two independently server-side-encrypted (e.g. SSE-KMS) copies of the same
// plaintext will not share an ETag, because S3 computes it over the stored
// ciphertext, which differs by encryption context even for identical
// plaintext. That makes a false "mismatch" possible under encryption, but
// never a false "match": this function is safe to use as a fast path ahead of
// a full content comparison, never as a replacement for one when it reports
// no match.
func SinglePartMD5(etag string) (string, bool) {
	etag = strings.Trim(etag, `"`)
	if etag == "" || strings.Contains(etag, "-") || len(etag) != 32 {
		return "", false
	}
	if _, err := hex.DecodeString(etag); err != nil {
		return "", false
	}
	return etag, true
}
