package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jabbrwcky/tranquila/internal/state"
	"github.com/jabbrwcky/tranquila/internal/storage"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Config struct {
	Source           *storage.Client
	Destination      *storage.Client
	State            *state.Store
	Meter            metric.Meter
	SourceBuckets    []string
	DestBucketPrefix string
	Workers          int
	RateLimit        float64
}

type metrics struct {
	synced           metric.Int64Counter
	failed           metric.Int64Counter
	bytesTransferred metric.Int64Counter
	duration         metric.Float64Histogram
}

type Syncer struct {
	cfg Config
	m   metrics
}

func New(cfg Config) (*Syncer, error) {
	m, err := newMetrics(cfg.Meter)
	if err != nil {
		return nil, fmt.Errorf("init metrics: %w", err)
	}
	return &Syncer{cfg: cfg, m: m}, nil
}

func newMetrics(meter metric.Meter) (metrics, error) {
	synced, err := meter.Int64Counter("tranquila.objects.synced",
		metric.WithDescription("Total objects successfully synced"))
	if err != nil {
		return metrics{}, err
	}
	failed, err := meter.Int64Counter("tranquila.objects.failed",
		metric.WithDescription("Total object sync failures"))
	if err != nil {
		return metrics{}, err
	}
	bytes, err := meter.Int64Counter("tranquila.bytes.transferred",
		metric.WithDescription("Total bytes transferred"),
		metric.WithUnit("By"))
	if err != nil {
		return metrics{}, err
	}
	dur, err := meter.Float64Histogram("tranquila.transfer.duration",
		metric.WithDescription("Transfer duration per object"),
		metric.WithUnit("s"))
	if err != nil {
		return metrics{}, err
	}
	return metrics{synced: synced, failed: failed, bytesTransferred: bytes, duration: dur}, nil
}

// Run performs discovery then syncs all pending objects. It blocks until done
// or ctx is cancelled (graceful shutdown: in-flight transfers finish).
func (s *Syncer) Run(ctx context.Context) error {
	buckets, err := s.resolveBuckets(ctx)
	if err != nil {
		return err
	}

	log.Info().Strs("buckets", buckets).Msg("starting discovery")
	if err := s.discover(ctx, buckets); err != nil {
		return fmt.Errorf("discovery: %w", err)
	}

	log.Info().Msg("starting sync")
	return s.sync(ctx, buckets)
}

func (s *Syncer) resolveBuckets(ctx context.Context) ([]string, error) {
	if len(s.cfg.SourceBuckets) > 0 {
		return s.cfg.SourceBuckets, nil
	}
	buckets, err := s.cfg.Source.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list source buckets: %w", err)
	}
	return buckets, nil
}

// discover scans each source bucket, compares objects against stored state,
// and marks objects as pending when they are new or modified since last sync.
func (s *Syncer) discover(ctx context.Context, buckets []string) error {
	collectionTime := time.Now().UTC()

	for _, bucket := range buckets {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.discoverBucket(ctx, bucket, collectionTime); err != nil {
			return fmt.Errorf("discover bucket %s: %w", bucket, err)
		}
	}
	return nil
}

func (s *Syncer) discoverBucket(ctx context.Context, bucket string, collectionTime time.Time) error {
	logger := log.With().Str("bucket", bucket).Logger()

	objects, errc := s.cfg.Source.ListObjects(ctx, bucket)
	var count, pending int

	for obj := range objects {
		count++
		needsSync, err := s.needsSync(ctx, bucket, obj)
		if err != nil {
			logger.Warn().Err(err).Str("key", obj.Key).Msg("state check failed, marking pending")
			needsSync = true
		}
		if needsSync {
			pending++
			if err := s.cfg.State.MarkPending(ctx, bucket, obj.Key, obj.ModifiedAt); err != nil {
				return fmt.Errorf("mark pending %s: %w", obj.Key, err)
			}
		}
	}
	if err := <-errc; err != nil {
		return err
	}

	if err := s.cfg.State.SetCollectionTime(ctx, bucket, collectionTime); err != nil {
		return fmt.Errorf("set collection time: %w", err)
	}

	logger.Info().
		Int("total", count).
		Int("pending", pending).
		Msg("discovery complete")
	return nil
}

func (s *Syncer) needsSync(ctx context.Context, bucket string, obj storage.Object) (bool, error) {
	stored, err := s.cfg.State.GetObject(ctx, bucket, obj.Key)
	if err != nil {
		return false, err
	}
	if stored == nil {
		return true, nil
	}
	if stored.Status == state.StatusPending || stored.Status == state.StatusFailed {
		return true, nil
	}
	// Re-sync if source object was modified after the stored modification time.
	return obj.ModifiedAt.After(stored.ModifiedAt), nil
}

func (s *Syncer) sync(ctx context.Context, buckets []string) error {
	pool := newWorkerPool(ctx, s.cfg.Workers, s.cfg.RateLimit, s.transfer)

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		s.processResults(ctx, pool.resultsCh())
	}()

	for _, bucket := range buckets {
		if ctx.Err() != nil {
			break
		}
		pending, err := s.cfg.State.ScanPending(ctx, bucket)
		if err != nil {
			log.Error().Err(err).Str("bucket", bucket).Msg("scan pending failed")
			continue
		}

		dstBucket := s.cfg.DestBucketPrefix + bucket
		if err := s.cfg.Destination.EnsureBucket(ctx, dstBucket); err != nil {
			log.Error().Err(err).Str("bucket", dstBucket).Msg("ensure destination bucket failed")
			continue
		}

		logger := log.With().Str("bucket", bucket).Logger()
		logger.Info().Int("count", len(pending)).Msg("queuing objects")

		for _, key := range pending {
			if ctx.Err() != nil {
				break
			}
			pool.submit(Job{
				SrcBucket: bucket,
				DstBucket: dstBucket,
				Key:       key,
			})
		}
	}

	pool.close()
	resultWg.Wait()
	return nil
}

func (s *Syncer) processResults(ctx context.Context, results <-chan Result) {
	for r := range results {
		attrs := []attribute.KeyValue{attribute.String("bucket", r.Job.SrcBucket)}
		if r.Err != nil {
			log.Error().
				Err(r.Err).
				Str("bucket", r.Job.SrcBucket).
				Str("key", r.Job.Key).
				Msg("transfer failed")
			_ = s.cfg.State.MarkFailed(ctx, r.Job.SrcBucket, r.Job.Key)
			s.m.failed.Add(ctx, 1, metric.WithAttributes(attrs...))
		} else {
			log.Debug().
				Str("bucket", r.Job.SrcBucket).
				Str("key", r.Job.Key).
				Dur("duration", r.Duration).
				Msg("transfer complete")
			_ = s.cfg.State.MarkSynced(ctx, r.Job.SrcBucket, r.Job.Key)
			s.m.synced.Add(ctx, 1, metric.WithAttributes(attrs...))
			s.m.bytesTransferred.Add(ctx, r.Job.Size, metric.WithAttributes(attrs...))
			s.m.duration.Record(ctx, r.Duration.Seconds(), metric.WithAttributes(attrs...))
		}
	}
}

func (s *Syncer) transfer(ctx context.Context, job Job) error {
	body, size, err := s.cfg.Source.GetObject(ctx, job.SrcBucket, job.Key)
	if err != nil {
		return err
	}
	defer body.Close()

	job.Size = size
	return s.cfg.Destination.PutObject(ctx, job.DstBucket, job.Key, body, size)
}
