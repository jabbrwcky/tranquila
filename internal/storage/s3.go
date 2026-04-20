package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
)

type Config struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
}

type Object struct {
	Bucket     string
	Key        string
	ModifiedAt time.Time
	Size       int64
	ETag       string
}

type Client struct {
	s3       *s3.Client
	uploader *manager.Uploader
	region   string
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

	clientOpts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		endpoint := cfg.Endpoint
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	s3c := s3.NewFromConfig(awsCfg, clientOpts...)
	return &Client{
		s3:       s3c,
		uploader: manager.NewUploader(s3c),
		region:   cfg.Region,
	}, nil
}

func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	out, err := c.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
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

const listMaxRetries = 5

// listPageWithRetry fetches a single ListObjectsV2 page, retrying on transient
// network errors (EOF, connection reset, broken pipe) with exponential backoff.
func (c *Client) listPageWithRetry(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	var err error
	for attempt := 0; attempt < listMaxRetries; attempt++ {
		var out *s3.ListObjectsV2Output
		out, err = c.s3.ListObjectsV2(ctx, input)
		if err == nil {
			return out, nil
		}
		if !isTransientErr(err) {
			return nil, err
		}
		delay := time.Duration(1<<uint(attempt)) * time.Second
		log.Warn().Err(err).Int("attempt", attempt+1).Msg("transient list error, retrying")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, err
}

func isTransientErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe")
}

func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}

	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if c.region != "" && c.region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(c.region),
		}
	}

	_, err = c.s3.CreateBucket(ctx, input)
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

func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	return out.Body, aws.ToInt64(out.ContentLength), nil
}

func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if size > 0 {
		input.ContentLength = aws.Int64(size)
	}
	_, err := c.uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("put object %s/%s: %w", bucket, key, err)
	}
	return nil
}
