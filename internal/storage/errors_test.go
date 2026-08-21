package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// s3Err builds the error chain the SDK actually produces for an HTTP error
// response: OperationError → MaxAttemptsError → ResponseError → APIError.
// An empty code models a non-XML gateway body, which yields no APIError at all.
func s3Err(status int, code string) error {
	var inner error = errors.New("failed to deserialize xml error response")
	if code != "" {
		inner = &smithy.GenericAPIError{Code: code, Message: http.StatusText(status)}
	}
	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: "ListObjectsV2",
		Err: &retry.MaxAttemptsError{Attempt: 3, Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
				Err:      inner,
			},
			RequestID: "req-1",
		}},
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrClass
	}{
		{"nil", nil, ClassOK},
		// The exact shape from the reported production crash.
		{"reported_crash_504_gateway_timeout", s3Err(504, "GatewayTimeout"), ClassTransient},
		{"504_non_xml_body_no_apierror", s3Err(504, ""), ClassTransient},
		{"502_bad_gateway", s3Err(502, "BadGateway"), ClassTransient},
		{"500_derived_code", s3Err(500, "InternalServerError"), ClassTransient},
		{"500_s3_xml_code", s3Err(500, "InternalError"), ClassTransient},
		{"503_service_unavailable", s3Err(503, "ServiceUnavailable"), ClassThrottle},
		{"503_slow_down", s3Err(503, "SlowDown"), ClassThrottle},
		{"429_too_many_requests", s3Err(429, ""), ClassThrottle},
		{"403_access_denied", s3Err(403, "AccessDenied"), ClassPermanent},
		{"403_invalid_access_key", s3Err(403, "InvalidAccessKeyId"), ClassPermanent},
		{"401_unauthorized", s3Err(401, ""), ClassPermanent},
		{"no_such_bucket_typed", &s3types.NoSuchBucket{}, ClassPermanent},
		{"404_no_such_key", s3Err(404, "NoSuchKey"), ClassOK},
		{"unexpected_eof", io.ErrUnexpectedEOF, ClassTransient},
		{"wrapped_eof", fmt.Errorf("list: %w", io.EOF), ClassTransient},
		{"context_canceled", context.Canceled, ClassOK},
		{"context_deadline", context.DeadlineExceeded, ClassOK},
		{"unknown_error", errors.New("boom"), ClassTransient},
		{"dial_timeout", errors.New("dial tcp: i/o timeout"), ClassTransient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.err); got != tt.want {
				t.Errorf("Classify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTransientErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"gateway_timeout", s3Err(504, "GatewayTimeout"), true},
		{"throttle_counts_as_retryable", s3Err(503, "SlowDown"), true},
		{"access_denied", s3Err(403, "AccessDenied"), false},
		{"no_such_key", s3Err(404, "NoSuchKey"), false},
		{"context_canceled", context.Canceled, false},
		// Legacy transport-level string matches, pinned so listPageWithRetry
		// keeps its existing behaviour.
		{"connection_reset", errors.New("read: connection reset by peer"), true},
		{"broken_pipe", errors.New("write: broken pipe"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientErr(tt.err); got != tt.want {
				t.Errorf("isTransientErr() = %v, want %v", got, tt.want)
			}
		})
	}
}
