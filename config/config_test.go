package config

import (
	"testing"
)

func TestDecodeBucketMappings(t *testing.T) {
	tests := []struct {
		name    string
		raw     interface{}
		want    []BucketMapping
		wantErr bool
	}{
		{
			name: "source_and_destination_with_prefixes",
			raw: []interface{}{
				map[string]interface{}{
					"source":      map[string]interface{}{"bucket": "src-bucket", "prefix": "foo/"},
					"destination": map[string]interface{}{"bucket": "dst-bucket", "prefix": "bar/"},
				},
			},
			want: []BucketMapping{{
				Source:      BucketEndpoint{Bucket: "src-bucket", Prefix: "foo/"},
				Destination: BucketEndpoint{Bucket: "dst-bucket", Prefix: "bar/"},
			}},
		},
		{
			name: "optional_prefixes_omitted",
			raw: []interface{}{
				map[string]interface{}{
					"source":      map[string]interface{}{"bucket": "my-bucket"},
					"destination": map[string]interface{}{"bucket": "my-bucket-copy"},
				},
			},
			want: []BucketMapping{{
				Source:      BucketEndpoint{Bucket: "my-bucket"},
				Destination: BucketEndpoint{Bucket: "my-bucket-copy"},
			}},
		},
		{
			name: "multiple_mappings",
			raw: []interface{}{
				map[string]interface{}{
					"source":      map[string]interface{}{"bucket": "bucket-a"},
					"destination": map[string]interface{}{"bucket": "bucket-a-backup"},
				},
				map[string]interface{}{
					"source":      map[string]interface{}{"bucket": "bucket-b", "prefix": "logs/"},
					"destination": map[string]interface{}{"bucket": "archive", "prefix": "bucket-b-logs/"},
				},
			},
			want: []BucketMapping{
				{
					Source:      BucketEndpoint{Bucket: "bucket-a"},
					Destination: BucketEndpoint{Bucket: "bucket-a-backup"},
				},
				{
					Source:      BucketEndpoint{Bucket: "bucket-b", Prefix: "logs/"},
					Destination: BucketEndpoint{Bucket: "archive", Prefix: "bucket-b-logs/"},
				},
			},
		},
		{
			name:    "nil_raw_returns_nil_slice",
			raw:     nil,
			want:    nil,
			wantErr: false,
		},
		{
			name:    "empty_slice",
			raw:     []interface{}{},
			want:    []BucketMapping{},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeBucketMappings(tc.raw)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.want))
			}
			for i, bm := range got {
				w := tc.want[i]
				if bm.Source.Bucket != w.Source.Bucket {
					t.Errorf("[%d] Source.Bucket = %q, want %q", i, bm.Source.Bucket, w.Source.Bucket)
				}
				if bm.Source.Prefix != w.Source.Prefix {
					t.Errorf("[%d] Source.Prefix = %q, want %q", i, bm.Source.Prefix, w.Source.Prefix)
				}
				if bm.Destination.Bucket != w.Destination.Bucket {
					t.Errorf("[%d] Destination.Bucket = %q, want %q", i, bm.Destination.Bucket, w.Destination.Bucket)
				}
				if bm.Destination.Prefix != w.Destination.Prefix {
					t.Errorf("[%d] Destination.Prefix = %q, want %q", i, bm.Destination.Prefix, w.Destination.Prefix)
				}
			}
		})
	}
}
