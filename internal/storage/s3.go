package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/time/rate"
)

type Config struct {
	Endpoint      string
	Region        string
	AccessKey     string
	SecretKey     string
	RateLimit     float64      // max S3 API calls/sec for this client; 0 = unlimited
	FailThreshold int          // consecutive transient failures before the rate is halved (0 = default 5)
	Name          string       // "source"/"destination"; labels metrics
	Meter         metric.Meter // optional; zero value produces no-op instruments
}

type Object struct {
	Bucket     string
	Key        string
	ModifiedAt time.Time
	Size       int64
	ETag       string
}

type clientMetrics struct {
	opDuration   metric.Float64Histogram
	errors       metric.Int64Counter
	limitChanges metric.Int64Counter
	attrs        []attribute.KeyValue // cached endpoint label
}

type Client struct {
	s3      *s3.Client
	tm      *transfermanager.Client
	region  string
	limiter *rate.Limiter // never nil; rate.Inf = unlimited, mutated only via aimd
	aimd    *aimd
	m       clientMetrics
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	// The SDK default of 3 attempts is not enough to ride out a gateway that
	// returns 504 under load. Applies to every operation, including the ones
	// with no retry wrapper of their own (Get/Put/Head/Delete, EnsureBucket).
	awsCfg.Retryer = func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), s3MaxAttempts)
	}

	clientOpts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		endpoint := cfg.Endpoint
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	s3c := s3.NewFromConfig(awsCfg, clientOpts...)

	// Always construct the limiter so the pointer is never nil and never
	// swapped: rate.Inf short-circuits Wait, and only SetLimit ever mutates it.
	base := rate.Inf
	if cfg.RateLimit > 0 {
		base = rate.Limit(cfg.RateLimit)
	}
	// Burst of 1 enforces strict per-call pacing with no token accumulation.
	lim := rate.NewLimiter(base, 1)

	c := &Client{
		s3:      s3c,
		tm:      transfermanager.New(s3c),
		region:  cfg.Region,
		limiter: lim,
		aimd:    newAIMD(lim, base, cfg.FailThreshold),
	}
	if err := c.initMetrics(cfg); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) initMetrics(cfg Config) error {
	name := cfg.Name
	if name == "" {
		name = "s3"
	}
	m := clientMetrics{attrs: []attribute.KeyValue{attribute.String("endpoint", name)}}

	// metric.Meter is an interface, so its zero value is nil rather than a no-op.
	meter := cfg.Meter
	if meter == nil {
		meter = noop.Meter{}
	}

	var err error
	if m.opDuration, err = meter.Float64Histogram("tranquila.s3.operation.duration",
		metric.WithDescription("Duration of individual S3 API calls"),
		metric.WithUnit("ms")); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if m.errors, err = meter.Int64Counter("tranquila.s3.errors",
		metric.WithDescription("S3 API call failures by class")); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if m.limitChanges, err = meter.Int64Counter("tranquila.s3.rate_limit.changes",
		metric.WithDescription("Rate limit adjustments made by congestion control")); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if _, err = meter.Float64ObservableGauge("tranquila.s3.rate_limit",
		metric.WithDescription("Effective S3 API call rate limit; 0 = unlimited"),
		metric.WithUnit("{call}/s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(c.aimd.state().Current, metric.WithAttributes(m.attrs...))
			return nil
		})); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if _, err = meter.Int64ObservableGauge("tranquila.s3.rate_limit.degraded",
		metric.WithDescription("1 while the endpoint's rate limit is reduced by congestion control"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			var v int64
			if c.aimd.state().Degraded {
				v = 1
			}
			o.Observe(v, metric.WithAttributes(m.attrs...))
			return nil
		})); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}

	c.m = m
	return nil
}

// wait blocks until the rate limiter allows the next S3 API call.
// Returns immediately while the limit is rate.Inf (unlimited).
func (c *Client) wait(ctx context.Context) error {
	return c.limiter.Wait(ctx)
}

// recordOp records a completed S3 API call with operation name, bucket, success flag, and duration.
func (c *Client) recordOp(ctx context.Context, op, bucket string, start time.Time, err error) {
	class := Classify(err)
	status := "ok"
	if err != nil {
		status = "error"
	}
	c.m.opDuration.Record(ctx, float64(time.Since(start).Milliseconds()),
		metric.WithAttributes(
			attribute.String("operation", op),
			attribute.String("bucket", bucket),
			attribute.String("status", status),
		))
	c.observe(ctx, class)
}

// observe feeds one call outcome to the endpoint's congestion controller.
func (c *Client) observe(ctx context.Context, class ErrClass) {
	if class != ClassOK {
		c.m.errors.Add(ctx, 1, metric.WithAttributes(
			append(c.m.attrs, attribute.String("class", class.String()))...))
	}

	switch class {
	case ClassTransient, ClassThrottle:
		if c.aimd.onCongestion(class == ClassThrottle) {
			c.logLimitChange(ctx, "decrease", "endpoint congested, reducing S3 rate limit")
		}
	default:
		// A permanent error still means the endpoint answered, so it is not a
		// congestion signal; that failure is the syncer's problem, not the pacer's.
		if c.aimd.onHealthy() {
			c.logLimitChange(ctx, "increase", "endpoint recovering, raising S3 rate limit")
		}
	}
}

func (c *Client) logLimitChange(ctx context.Context, direction, msg string) {
	st := c.aimd.state()
	c.m.limitChanges.Add(ctx, 1, metric.WithAttributes(
		append(c.m.attrs, attribute.String("direction", direction))...))
	log.Warn().
		Float64("rate_limit", st.Current).
		Float64("base_rate_limit", st.Base).
		Bool("degraded", st.Degraded).
		Msg(msg)
}

// LimitState reports the endpoint's current pacing state.
func (c *Client) LimitState() LimitState { return c.aimd.state() }

func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	start := time.Now()
	out, err := c.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
	c.recordOp(ctx, "ListBuckets", "", start, err)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	names := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		if b.Name != nil {
			names = append(names, *b.Name)
		}
	}
	return names, nil
}

// ListObjects streams objects from a bucket. prefix limits results to keys with
// that prefix (empty = all objects). Each S3 page is fetched individually with
// exponential-backoff retries so transient EOF/connection-reset errors mid-scan
// do not abort a large bucket. The caller must drain the returned channel or
// cancel ctx to avoid a goroutine leak.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix string) (<-chan Object, <-chan error) {
	objects := make(chan Object, 100)
	errc := make(chan error, 1)

	go func() {
		defer close(objects)
		defer close(errc)

		var token *string
		var pageNum, total int

		for {
			input := &s3.ListObjectsV2Input{
				Bucket:            aws.String(bucket),
				ContinuationToken: token,
			}
			if prefix != "" {
				input.Prefix = aws.String(prefix)
			}

			page, err := c.listPageWithRetry(ctx, input)
			if err != nil {
				errc <- fmt.Errorf("list objects in %s: %w", bucket, err)
				return
			}

			pageNum++
			total += len(page.Contents)
			log.Debug().
				Str("bucket", bucket).
				Str("prefix", prefix).
				Int("page", pageNum).
				Int("page_objects", len(page.Contents)).
				Int("total", total).
				Msg("discovery page complete")

			for _, item := range page.Contents {
				if item.Key == nil {
					continue
				}
				obj := Object{
					Bucket: bucket,
					Key:    *item.Key,
					Size:   aws.ToInt64(item.Size),
				}
				if item.LastModified != nil {
					obj.ModifiedAt = *item.LastModified
				}
				if item.ETag != nil {
					obj.ETag = *item.ETag
				}
				select {
				case objects <- obj:
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				}
			}

			if !aws.ToBool(page.IsTruncated) {
				break
			}
			token = page.NextContinuationToken
		}
	}()

	return objects, errc
}

// ListObjectsPage fetches up to maxObjects from bucket starting after token,
// accumulating complete S3 API pages. Returns the objects, the continuation
// token for the next call (nil when the listing is exhausted), and any error.
// Callers can use repeated calls with the returned token to page through a
// large bucket in bounded-memory batches.
func (c *Client) ListObjectsPage(ctx context.Context, bucket, prefix string, token *string, maxObjects int) ([]Object, *string, error) {
	var collected []Object
	current := token
	var pageNum int

	for len(collected) < maxObjects {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: current,
		}
		if prefix != "" {
			input.Prefix = aws.String(prefix)
		}

		page, err := c.listPageWithRetry(ctx, input)
		if err != nil {
			return nil, nil, fmt.Errorf("list objects in %s: %w", bucket, err)
		}

		pageNum++
		log.Debug().
			Str("bucket", bucket).
			Str("prefix", prefix).
			Int("page", pageNum).
			Int("page_objects", len(page.Contents)).
			Int("total", len(collected)+len(page.Contents)).
			Msg("discovery page complete")

		for _, item := range page.Contents {
			if item.Key == nil {
				continue
			}
			obj := Object{
				Bucket: bucket,
				Key:    *item.Key,
				Size:   aws.ToInt64(item.Size),
			}
			if item.LastModified != nil {
				obj.ModifiedAt = *item.LastModified
			}
			if item.ETag != nil {
				obj.ETag = *item.ETag
			}
			collected = append(collected, obj)
		}

		if !aws.ToBool(page.IsTruncated) {
			return collected, nil, nil
		}
		current = page.NextContinuationToken

		if len(collected) >= maxObjects {
			return collected, current, nil
		}
	}

	return collected, current, nil
}

const (
	// s3MaxAttempts overrides the SDK default of 3.
	s3MaxAttempts = 5

	listMaxRetries = 8
	// listMaxDelay caps the doubling so late attempts do not stall for minutes.
	listMaxDelay = 30 * time.Second
)

// listPageWithRetry fetches a single ListObjectsV2 page, retrying transient
// errors (5xx, EOF, connection reset, broken pipe) with exponential backoff.
func (c *Client) listPageWithRetry(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	bucket := aws.ToString(input.Bucket)
	var err error
	for attempt := range listMaxRetries {
		if err = c.wait(ctx); err != nil {
			return nil, err
		}
		start := time.Now()
		var out *s3.ListObjectsV2Output
		out, err = c.s3.ListObjectsV2(ctx, input)
		c.recordOp(ctx, "ListObjectsV2", bucket, start, err)
		if err == nil {
			return out, nil
		}
		if !isTransientErr(err) {
			return nil, err
		}
		delay := min(time.Duration(1<<uint(attempt))*time.Second, listMaxDelay)
		// Jitter keeps replicas sharing an endpoint from retrying in lockstep.
		delay += rand.N(delay / 2)
		log.Warn().Err(err).Int("attempt", attempt+1).Dur("retry_in", delay).Msg("transient list error, retrying")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, err
}

func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if err := c.wait(ctx); err != nil {
		return err
	}
	start := time.Now()
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	c.recordOp(ctx, "HeadBucket", bucket, start, err)
	if err == nil {
		return nil
	}

	if err := c.wait(ctx); err != nil {
		return err
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if c.region != "" && c.region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(c.region),
		}
	}

	start = time.Now()
	_, err = c.s3.CreateBucket(ctx, input)
	c.recordOp(ctx, "CreateBucket", bucket, start, err)
	if err != nil {
		var alreadyExists *s3types.BucketAlreadyExists
		var alreadyOwned *s3types.BucketAlreadyOwnedByYou
		if errors.As(err, &alreadyExists) || errors.As(err, &alreadyOwned) {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", bucket, err)
	}
	return nil
}

// HeadObject returns the content length, CRC32 checksum, and ETag of an object.
// The CRC32 is populated only when the object was stored with a checksum algorithm;
// it is empty string otherwise. The ETag is always present: for a single-part
// upload it is the object's MD5 hex digest, comparable across independent
// uploads of identical content; for a multipart upload it is a composite of the
// parts' MD5s plus a "-partCount" suffix, comparable only to another upload
// that used the exact same part boundaries — see storage.SinglePartMD5.
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (size int64, checksumCRC32, etag string, err error) {
	if err := c.wait(ctx); err != nil {
		return 0, "", "", err
	}
	start := time.Now()
	out, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: s3types.ChecksumModeEnabled,
	})
	c.recordOp(ctx, "HeadObject", bucket, start, err)
	if err != nil {
		return 0, "", "", fmt.Errorf("head object %s/%s: %w", bucket, key, err)
	}
	return aws.ToInt64(out.ContentLength), aws.ToString(out.ChecksumCRC32), aws.ToString(out.ETag), nil
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	if err := c.wait(ctx); err != nil {
		return nil, 0, err
	}
	start := time.Now()
	out, err := c.tm.GetObject(ctx, &transfermanager.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	c.recordOp(ctx, "GetObject", bucket, start, err)
	if err != nil {
		return nil, 0, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	return io.NopCloser(out.Body), aws.ToInt64(out.ContentLength), nil
}

// PutObject uploads body to bucket/key using CRC32 checksum validation.
// Returns the base64-encoded CRC32 checksum from the upload response (empty if unavailable).
func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64) (checksumCRC32 string, err error) {
	if err := c.wait(ctx); err != nil {
		return "", err
	}
	start := time.Now()
	out, err := c.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              body,
		ChecksumAlgorithm: tmtypes.ChecksumAlgorithmCrc32,
	})
	c.recordOp(ctx, "PutObject", bucket, start, err)
	if err != nil {
		return "", fmt.Errorf("put object %s/%s: %w", bucket, key, err)
	}
	return aws.ToString(out.ChecksumCRC32), nil
}

// DeleteObject removes an object from bucket. Returns nil if the object does not exist.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := c.wait(ctx); err != nil {
		return err
	}
	start := time.Now()
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	c.recordOp(ctx, "DeleteObject", bucket, start, err)
	if err != nil {
		return fmt.Errorf("delete object %s/%s: %w", bucket, key, err)
	}
	return nil
}
