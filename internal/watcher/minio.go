package watcher

import (
	"context"
	"net/url"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/rs/zerolog/log"
)

// MinIOConfig holds connection details for the MinIO native event stream.
type MinIOConfig struct {
	Endpoint  string // host:port or full URL
	AccessKey string
	SecretKey string
	Secure    bool // true for HTTPS
}

// minioNotifier is the subset of minio.Client used by MinIOWatcher, allowing
// injection of fakes in tests.
type minioNotifier interface {
	ListenBucketNotification(ctx context.Context, bucketName, prefix, suffix string, events []string) <-chan notification.Info
}

// MinIOWatcher subscribes to MinIO's ListenBucketNotification SSE stream.
type MinIOWatcher struct {
	client minioNotifier
}

// NewMinIO creates a MinIOWatcher using ListenBucketNotification for real-time events.
func NewMinIO(cfg MinIOConfig) (*MinIOWatcher, error) {
	endpoint := cfg.Endpoint
	// Strip scheme if present — minio-go expects host:port only.
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		endpoint = u.Host
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, err
	}
	return &MinIOWatcher{client: client}, nil
}

// Watch subscribes to object-created and object-removed events for each bucket
// and fans all notifications into a single channel. The channel is closed when
// ctx is cancelled or all per-bucket streams end.
func (w *MinIOWatcher) Watch(ctx context.Context, buckets []string) (<-chan ObjectEvent, error) {
	out := make(chan ObjectEvent, 100)

	var wg sync.WaitGroup
	for _, b := range buckets {
		wg.Add(1)
		go func(bucket string) {
			defer wg.Done()
			w.watchBucket(ctx, bucket, out)
		}(b)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out, nil
}

func (w *MinIOWatcher) watchBucket(ctx context.Context, bucket string, out chan<- ObjectEvent) {
	events := []string{
		string(notification.ObjectCreatedAll),
		string(notification.ObjectRemovedAll),
	}
	ch := w.client.ListenBucketNotification(ctx, bucket, "", "", events)
	for info := range ch {
		if info.Err != nil {
			log.Warn().Err(info.Err).Str("bucket", bucket).Msg("minio notification error")
			continue
		}
		for _, rec := range info.Records {
			out <- ObjectEvent{
				Bucket: rec.S3.Bucket.Name,
				Key:    rec.S3.Object.Key,
				Size:   rec.S3.Object.Size,
			}
		}
	}
}
