package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration wraps time.Duration with parsing that also understands "d" (day)
// and "w" (week) suffixes, e.g. "7d", "2w" — time.ParseDuration only
// understands ns/us/ms/s/m/h. Implements encoding.TextUnmarshaler, which both
// kong (CLI flags and env vars) and yaml.v3 (config file) use automatically
// for a custom scalar type, so this one implementation covers both.
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// ParseDuration parses s as a time.Duration, extending time.ParseDuration
// with "d" (24h) and "w" (7d) suffixes. Only a single unit is supported for
// those two suffixes — "1d12h" is not — callers needing that precision can
// already express it in native Go duration syntax ("36h").
func ParseDuration(s string) (time.Duration, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	var unit time.Duration
	switch {
	case strings.HasSuffix(s, "d"):
		unit = 24 * time.Hour
	case strings.HasSuffix(s, "w"):
		unit = 7 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("invalid duration %q: use Go duration syntax (e.g. 90m, 12h) or a single d/w unit (e.g. 7d, 2w)", s)
	}

	n, err := strconv.ParseFloat(strings.TrimSuffix(s, s[len(s)-1:]), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: use Go duration syntax (e.g. 90m, 12h) or a single d/w unit (e.g. 7d, 2w)", s)
	}
	return time.Duration(n * float64(unit)), nil
}
