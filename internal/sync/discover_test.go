package sync

import (
	"errors"
	"fmt"
	"testing"

	smithy "github.com/aws/smithy-go"
	"github.com/jabbrwcky/tranquila/internal/storage"
)

func TestIsShardableListErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "gateway_timeout_list_error_is_shardable",
			err:  &storage.ListError{Bucket: "b", Err: &smithy.GenericAPIError{Code: "GatewayTimeout"}},
			want: true,
		},
		{
			name: "throttle_list_error_is_shardable",
			err:  &storage.ListError{Bucket: "b", Err: &smithy.GenericAPIError{Code: "SlowDown"}},
			want: true,
		},
		{
			name: "permanent_list_error_not_shardable",
			err:  &storage.ListError{Bucket: "b", Err: &smithy.GenericAPIError{Code: "AccessDenied"}},
			want: false,
		},
		{
			name: "wrapped_list_error_still_detected",
			err:  fmt.Errorf("list objects: %w", &storage.ListError{Bucket: "b", Err: &smithy.GenericAPIError{Code: "GatewayTimeout"}}),
			want: true,
		},
		{
			name: "non_list_error_never_shardable",
			// Same transient classification a bare error would get from
			// storage.Classify's fallback, but it's not a *storage.ListError
			// (e.g. a Redis mark-pending failure) — sharding can't help this.
			err:  fmt.Errorf("mark pending %s: %w", "key", errors.New("redis down")),
			want: false,
		},
		{
			name: "nil_err_not_shardable",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isShardableListErr(tc.err); got != tc.want {
				t.Errorf("isShardableListErr() = %v, want %v", got, tc.want)
			}
		})
	}
}
