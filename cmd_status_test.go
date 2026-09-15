package main

import (
	"strings"
	"testing"
	"time"

	"github.com/pflege-de-labs/tranquila/internal/api"
)

func i64(n int64) *int64 { return &n }

func TestRenderStatusParkedColumn(t *testing.T) {
	tests := []struct {
		name       string
		statuses   []api.BucketStatus
		wantHeader string
		wantRow    string // substring of the data row
	}{
		{
			name: "known parked count renders as a number",
			statuses: []api.BucketStatus{
				{Name: "events", Stats: api.BucketStats{Total: 525}, ParkedPrefixes: i64(178)},
			},
			wantHeader: "PARKED",
			wantRow:    "178",
		},
		{
			name: "nil parked count renders as - (unknown), not 0",
			statuses: []api.BucketStatus{
				{Name: "orders", Stats: api.BucketStats{Total: 10}, ParkedPrefixes: nil},
			},
			wantHeader: "PARKED",
			wantRow:    "-",
		},
		{
			name: "genuinely zero parked renders as 0, not -",
			statuses: []api.BucketStatus{
				{Name: "keycloak-audit-events", Stats: api.BucketStats{Total: 2}, ParkedPrefixes: i64(0)},
			},
			wantHeader: "PARKED",
			wantRow:    "0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf strings.Builder
			if err := renderStatus(&buf, tc.statuses); err != nil {
				t.Fatalf("renderStatus: %v", err)
			}
			out := buf.String()
			lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
			if len(lines) != 2 {
				t.Fatalf("expected a header + one data row, got %d lines: %q", len(lines), out)
			}
			if !strings.Contains(lines[0], tc.wantHeader) {
				t.Errorf("header %q missing %q", lines[0], tc.wantHeader)
			}
			fields := strings.Fields(lines[1])
			var got string
			for _, f := range fields {
				if f == tc.wantRow {
					got = f
					break
				}
			}
			if got == "" {
				t.Errorf("data row %q does not contain field %q", lines[1], tc.wantRow)
			}
		})
	}
}

// The nil-vs-zero distinction (- vs 0) must survive the RATE/ETA column set
// too — hasProgress selects a different header/row format entirely, and the
// PARKED column's rendering must not regress under it.
func TestRenderStatusParkedColumnWithProgress(t *testing.T) {
	eta := 42.0
	statuses := []api.BucketStatus{
		{
			Name:           "events",
			Stats:          api.BucketStats{Total: 525},
			ParkedPrefixes: i64(178),
			SyncProgress: &api.BucketSyncProgress{
				StartedAt:  time.Now(),
				RatePerSec: 1.5,
				ETASeconds: &eta,
			},
		},
	}
	var buf strings.Builder
	if err := renderStatus(&buf, statuses); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	out := buf.String()
	// tabwriter renders with space-padded columns, not literal tabs, so
	// column order is checked positionally rather than by a tab-joined
	// substring.
	header := strings.Split(out, "\n")[0]
	parkedIdx, rateIdx := strings.Index(header, "PARKED"), strings.Index(header, "RATE")
	if parkedIdx == -1 || rateIdx == -1 || parkedIdx > rateIdx {
		t.Errorf("expected PARKED before RATE in the progress header, got: %q", header)
	}
	if !strings.Contains(out, "178") {
		t.Errorf("expected the parked count 178 in the row, got: %q", out)
	}
}
