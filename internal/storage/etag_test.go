package storage

import "testing"

func TestSinglePartMD5(t *testing.T) {
	tests := []struct {
		name     string
		etag     string
		wantHash string
		wantOK   bool
	}{
		{"plain_lowercase_hex", "d41d8cd98f00b204e9800998ecf8427e", "d41d8cd98f00b204e9800998ecf8427e", true},
		{"quoted_as_s3_returns_it", `"d41d8cd98f00b204e9800998ecf8427e"`, "d41d8cd98f00b204e9800998ecf8427e", true},
		{"uppercase_hex_still_decodes", "D41D8CD98F00B204E9800998ECF8427E", "D41D8CD98F00B204E9800998ECF8427E", true},
		{"multipart_composite_rejected", "d41d8cd98f00b204e9800998ecf8427e-3", "", false},
		{"empty", "", "", false},
		{"too_short", "d41d8cd98f00b204e9800998ecf8427", "", false},
		{"too_long", "d41d8cd98f00b204e9800998ecf8427e00", "", false},
		{"non_hex_content", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "", false},
		{"just_a_dash", "-", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := SinglePartMD5(tt.etag)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.wantHash {
				t.Errorf("hash = %q, want %q", got, tt.wantHash)
			}
		})
	}
}
