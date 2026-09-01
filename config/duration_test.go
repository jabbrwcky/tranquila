package config

import (
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"gopkg.in/yaml.v3"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"native_hours", "12h", 12 * time.Hour, false},
		{"native_combo", "1h30m", 90 * time.Minute, false},
		{"native_seconds", "90s", 90 * time.Second, false},
		{"days", "7d", 7 * 24 * time.Hour, false},
		{"fractional_days", "1.5d", 36 * time.Hour, false},
		{"weeks", "2w", 14 * 24 * time.Hour, false},
		{"zero_seconds", "0s", 0, false},
		{"bare_number_invalid", "7", 0, true},
		{"garbage_invalid", "banana", 0, true},
		{"combined_day_hour_unsupported", "1d12h", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDuration(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestDurationUnmarshalText(t *testing.T) {
	var d Duration
	if err := d.UnmarshalText([]byte("7d")); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if time.Duration(d) != 7*24*time.Hour {
		t.Errorf("got %v, want 7d", time.Duration(d))
	}
}

func TestDurationYAML(t *testing.T) {
	type doc struct {
		MinAge  *Duration `yaml:"min-age"`
		Present Duration  `yaml:"present"`
	}

	tests := []struct {
		name        string
		yaml        string
		wantMinAge  *time.Duration
		wantPresent time.Duration
	}{
		{
			name:        "pointer_field_absent_stays_nil",
			yaml:        "present: 1h\n",
			wantMinAge:  nil,
			wantPresent: time.Hour,
		},
		{
			name:        "pointer_field_present_with_day_unit",
			yaml:        "min-age: 7d\npresent: 1h\n",
			wantMinAge:  durationPtr(7 * 24 * time.Hour),
			wantPresent: time.Hour,
		},
		{
			name:        "pointer_field_explicit_zero_overrides",
			yaml:        "min-age: 0s\npresent: 1h\n",
			wantMinAge:  durationPtr(0),
			wantPresent: time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got doc
			if err := yaml.Unmarshal([]byte(tc.yaml), &got); err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			if tc.wantMinAge == nil {
				if got.MinAge != nil {
					t.Errorf("MinAge = %v, want nil", got.MinAge)
				}
			} else {
				if got.MinAge == nil {
					t.Fatalf("MinAge = nil, want %v", *tc.wantMinAge)
				}
				if time.Duration(*got.MinAge) != *tc.wantMinAge {
					t.Errorf("MinAge = %v, want %v", time.Duration(*got.MinAge), *tc.wantMinAge)
				}
			}
			if time.Duration(got.Present) != tc.wantPresent {
				t.Errorf("Present = %v, want %v", time.Duration(got.Present), tc.wantPresent)
			}
		})
	}
}

func durationPtr(d time.Duration) *time.Duration { return &d }

// TestKongParsesDuration confirms kong's flag/env parsing picks up Duration's
// UnmarshalText automatically (no custom kong.MapperValue needed) — both for
// an explicit flag value and for the kong-tag default.
func TestKongParsesDuration(t *testing.T) {
	type cli struct {
		MinAge Duration `name:"min-age" default:"0s"`
	}

	var withFlag cli
	parser, err := kong.New(&withFlag)
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{"--min-age=7d"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if time.Duration(withFlag.MinAge) != 7*24*time.Hour {
		t.Errorf("got %v, want 7d", time.Duration(withFlag.MinAge))
	}

	var withDefault cli
	parser2, err := kong.New(&withDefault)
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser2.Parse(nil); err != nil {
		t.Fatalf("Parse (default): %v", err)
	}
	if time.Duration(withDefault.MinAge) != 0 {
		t.Errorf("default: got %v, want 0", time.Duration(withDefault.MinAge))
	}
}
