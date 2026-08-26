package main

import (
	"testing"

	"github.com/jabbrwcky/tranquila/config"
	internalsync "github.com/jabbrwcky/tranquila/internal/sync"
)

func TestResolveBucketsPropagateDeletes(t *testing.T) {
	tests := []struct {
		name string
		cmd  SyncCmd
		want map[string]internalsync.BucketConfig
	}{
		{
			name: "structured_yaml_threads_propagate_deletes",
			cmd: SyncCmd{
				Buckets: config.BucketMappings{
					{
						Source:           config.BucketEndpoint{Bucket: "src"},
						Destination:      config.BucketEndpoint{Bucket: "dst"},
						PropagateDeletes: true,
					},
				},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", PropagateDeletes: true},
			},
		},
		{
			name: "structured_yaml_defaults_to_false",
			cmd: SyncCmd{
				Buckets: config.BucketMappings{
					{Source: config.BucketEndpoint{Bucket: "src"}, Destination: config.BucketEndpoint{Bucket: "dst"}},
				},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", PropagateDeletes: false},
			},
		},
		{
			name: "legacy_bucket_mappings_never_set_propagate_deletes",
			cmd: SyncCmd{
				BucketMappings: []string{"src=dst"},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", PropagateDeletes: false},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.cmd.resolveBuckets()
			if err != nil {
				t.Fatalf("resolveBuckets: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d buckets, want %d: %+v", len(got), len(tc.want), got)
			}
			for src, wantBC := range tc.want {
				gotBC, ok := got[src]
				if !ok {
					t.Fatalf("missing bucket %q in result: %+v", src, got)
				}
				if gotBC != wantBC {
					t.Errorf("bucket %q: got %+v, want %+v", src, gotBC, wantBC)
				}
			}
		})
	}
}
