package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithy "github.com/aws/smithy-go"
)

// ErrClass groups S3 failures by the response they warrant.
type ErrClass int

const (
	ClassOK        ErrClass = iota // success, or a healthy negative answer (404)
	ClassTransient                 // server/transport fault: retry, back off if it persists
	ClassThrottle                  // explicit back-pressure: reduce the send rate at once
	ClassPermanent                 // misconfiguration: retrying cannot help
)

func (c ErrClass) String() string {
	switch c {
	case ClassTransient:
		return "transient"
	case ClassThrottle:
		return "throttle"
	case ClassPermanent:
		return "permanent"
	}
	return "ok"
}

// Classify maps an S3 error to the response it warrants. Unknown errors are
// transient so a novel fault is never mistaken for misconfiguration.
func Classify(err error) ErrClass {
	if err == nil {
		return ClassOK
	}
	// Cancellation is our own doing, never a congestion signal.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassOK
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ClassTransient
	}

	// Status first: a non-XML gateway body (an nginx HTML 504) fails XML decode,
	// so the deserializer yields no APIError at all — only this wrapper.
	// errors.As matches by assignability before ResponseError's own As method
	// (which delegates past itself) is ever consulted.
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			return ClassThrottle
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusGatewayTimeout:
			return ClassTransient
		case http.StatusUnauthorized, http.StatusForbidden:
			return ClassPermanent
		case http.StatusNotFound:
			// A 404 is a valid answer; callers that care already handle it.
			return classifyAPICode(err, ClassOK)
		}
	}
	return classifyAPICode(err, ClassTransient)
}

// classifyAPICode classifies by S3/smithy error code, falling back to def.
func classifyAPICode(err error, def ErrClass) ErrClass {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return def
	}
	switch apiErr.ErrorCode() {
	case "SlowDown", "ServiceUnavailable", "RequestLimitExceeded", "TooManyRequests":
		return ClassThrottle
	case "GatewayTimeout", "BadGateway",
		"InternalServerError", // derived from HTTP 500 by s3shared
		"InternalError",       // S3's own XML code for 500
		"RequestTimeout", "RequestTimeoutException",
		"ClientDisconnected", "OperationAborted", "PriorRequestNotComplete":
		return ClassTransient
	case "AccessDenied", "AllAccessDisabled", "InvalidAccessKeyId",
		"SignatureDoesNotMatch", "AuthorizationHeaderMalformed",
		"InvalidBucketName", "NoSuchBucket", "TokenRefreshRequired",
		"ExpiredToken", "InvalidToken", "AccountProblem":
		return ClassPermanent
	case "NoSuchKey", "NotFound":
		return ClassOK
	}
	return def
}

// isTransientErr reports whether a failed call is worth retrying in place.
func isTransientErr(err error) bool {
	if err == nil {
		return false
	}
	switch Classify(err) {
	case ClassTransient, ClassThrottle:
		return true
	}
	// Transport-level failures the SDK does not model as typed errors. Kept out
	// of Classify: string matching must not drive a rate-limit decision.
	msg := err.Error()
	return strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe")
}
