package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/pflege-de-labs/tranquila/config"
	internalsync "github.com/pflege-de-labs/tranquila/internal/sync"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func cfgDuration(d time.Duration) *config.Duration {
	cd := config.Duration(d)
	return &cd
}

// captureLogs swaps the global logger for the duration of a test, same
// pattern as internal/storage's own captureLogs (package-local since loggers
// aren't shared across packages).
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev, prevLevel := log.Logger, zerolog.GlobalLevel()
	log.Logger = zerolog.New(buf)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	t.Cleanup(func() {
		log.Logger = prev
		zerolog.SetGlobalLevel(prevLevel)
	})
	return buf
}

func TestWarnIfListBudgetsBindFirst(t *testing.T) {
	const wantMsg = "will bind first"

	tests := []struct {
		name       string
		cmd        SyncCmd
		wantWarn   bool
		wantFields []string // substrings expected in the log line when wantWarn
	}{
		{
			name:     "both budgets at their default (10m) are above any realistic ceiling",
			cmd:      SyncCmd{},
			wantWarn: false,
		},
		{
			name: "bounded list-retry-budget below the escalated ceiling warns",
			// ceiling = 120s * 4 = 480s; 60s budget is below it.
			cmd:      SyncCmd{ListAttemptTimeout: 120 * time.Second, ListRetryBudget: 60 * time.Second},
			wantWarn: true,
			wantFields: []string{
				`"flag":"list-retry-budget"`,
			},
		},
		{
			name:     "bounded discovery-prefix-budget below the escalated ceiling warns",
			cmd:      SyncCmd{ListAttemptTimeout: 120 * time.Second, DiscoveryPrefixBudget: 60 * time.Second},
			wantWarn: true,
			wantFields: []string{
				`"flag":"discovery-prefix-budget"`,
			},
		},
		{
			name:     "negative (unbounded) budget never warns, however low the ceiling",
			cmd:      SyncCmd{ListAttemptTimeout: 120 * time.Second, ListRetryBudget: -1, DiscoveryPrefixBudget: -1},
			wantWarn: false,
		},
		{
			name:     "budget above the ceiling does not warn",
			cmd:      SyncCmd{ListAttemptTimeout: 60 * time.Second, ListRetryBudget: 10 * time.Minute},
			wantWarn: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			warnIfListBudgetsBindFirst(&tc.cmd)
			out := buf.String()

			gotWarn := strings.Contains(out, wantMsg)
			if gotWarn != tc.wantWarn {
				t.Fatalf("warned = %v, want %v; log output: %s", gotWarn, tc.wantWarn, out)
			}
			for _, f := range tc.wantFields {
				if !strings.Contains(out, f) {
					t.Errorf("log output missing %q: %s", f, out)
				}
			}
		})
	}
}

func TestResolveBucketsPerBucketFlags(t *testing.T) {
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
		{
			name: "structured_yaml_threads_sharded_discovery",
			cmd: SyncCmd{
				Buckets: config.BucketMappings{
					{
						Source:           config.BucketEndpoint{Bucket: "src"},
						Destination:      config.BucketEndpoint{Bucket: "dst"},
						ShardedDiscovery: true,
					},
				},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", ShardedDiscovery: true},
			},
		},
		{
			name: "legacy_bucket_mappings_never_set_sharded_discovery",
			cmd: SyncCmd{
				BucketMappings: []string{"src=dst"},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", ShardedDiscovery: false},
			},
		},
		{
			name: "bucket_without_override_inherits_global_min_age",
			cmd: SyncCmd{
				BurnAfterReadingMinAge: config.Duration(7 * 24 * time.Hour),
				Buckets: config.BucketMappings{
					{Source: config.BucketEndpoint{Bucket: "src"}, Destination: config.BucketEndpoint{Bucket: "dst"}, BurnAfterReading: true},
				},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			},
		},
		{
			name: "bucket_override_wins_over_global_min_age",
			cmd: SyncCmd{
				BurnAfterReadingMinAge: config.Duration(7 * 24 * time.Hour),
				Buckets: config.BucketMappings{
					{
						Source:                 config.BucketEndpoint{Bucket: "src"},
						Destination:            config.BucketEndpoint{Bucket: "dst"},
						BurnAfterReading:       true,
						BurnAfterReadingMinAge: cfgDuration(30 * 24 * time.Hour),
					},
				},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", BurnAfterReading: true, BurnAfterReadingMinAge: 30 * 24 * time.Hour},
			},
		},
		{
			name: "bucket_explicit_zero_overrides_nonzero_global_min_age",
			cmd: SyncCmd{
				BurnAfterReadingMinAge: config.Duration(7 * 24 * time.Hour),
				Buckets: config.BucketMappings{
					{
						Source:                 config.BucketEndpoint{Bucket: "src"},
						Destination:            config.BucketEndpoint{Bucket: "dst"},
						BurnAfterReading:       true,
						BurnAfterReadingMinAge: cfgDuration(0),
					},
				},
			},
			want: map[string]internalsync.BucketConfig{
				"src": {Destination: "dst", BurnAfterReading: true, BurnAfterReadingMinAge: 0},
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
