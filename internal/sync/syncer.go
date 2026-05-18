package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jabbrwcky/tranquila/internal/state"
	"github.com/jabbrwcky/tranquila/internal/storage"
	"github.com/jabbrwcky/tranquila/internal/watcher"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

// BucketConfig holds destination routing and path-prefix configuration for a source bucket.
type BucketConfig struct {
	Destination string // destination bucket name
	SrcPrefix   string // list/filter prefix applied when scanning the source; empty = all objects
	DstPrefix   string // replaces SrcPrefix in the destination key; empty = keep original key
}

// destKey returns the destination object key for srcKey, applying prefix replacement when configured.
func (bc BucketConfig) destKey(srcKey string) string {
	if bc.SrcPrefix == "" || bc.DstPrefix == "" {
		return srcKey
	}
	return bc.DstPrefix + strings.TrimPrefix(srcKey, bc.SrcPrefix)
}

type Config struct {
	Source           *storage.Client
	Destination      *storage.Client
	State            *state.Store
	Meter            metric.Meter
	Buckets          map[string]BucketConfig // src → config; nil = auto-discover all
	DestBucketPrefix string                  // prefix for auto-discovered destination bucket names
	Workers          int
	RateLimit        float64
	CheckSizes       bool      // re-queue synced objects whose destination size differs from source
	Progress         *Progress // optional; enables live progress tracking for the management API
}

type metrics struct {
	synced           metric.Int64Counter
	failed           metric.Int64Counter
	bytesTransferred metric.Int64Counter
	duration         metric.Float64Histogram
	activeWorkers    metric.Int64UpDownCounter
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
	activeWorkers, err := meter.Int64UpDownCounter("tranquila.workers.active",
		metric.WithDescription("Number of workers currently executing a transfer"))
	if err != nil {
		return metrics{}, err
	}
	return metrics{
		synced:           synced,
		failed:           failed,
		bytesTransferred: bytes,
		duration:         dur,
		activeWorkers:    activeWorkers,
	}, nil
}

// Run performs discovery then syncs all pending objects. It blocks until done
// or ctx is cancelled (graceful shutdown: in-flight transfers finish).
func (s *Syncer) Run(ctx context.Context) error {
	if s.cfg.Progress != nil {
		s.cfg.Progress.start(time.Now().UTC())
		defer s.cfg.Progress.stop()
	}

	bucketMap, err := s.resolveBuckets(ctx)
	if err != nil {
		return err
	}

	srcs := make([]string, 0, len(bucketMap))
	for src := range bucketMap {
		srcs = append(srcs, src)
	}
	log.Info().Strs("buckets", srcs).Msg("starting discovery")

	if err := s.discover(ctx, bucketMap); err != nil {
		return fmt.Errorf("discovery: %w", err)
	}

	log.Info().Msg("starting sync")
	return s.sync(ctx, bucketMap)
}

func (s *Syncer) resolveBuckets(ctx context.Context) (map[string]BucketConfig, error) {
	if len(s.cfg.Buckets) > 0 {
		return s.cfg.Buckets, nil
	}
	discovered, err := s.cfg.Source.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list source buckets: %w", err)
	}
	m := make(map[string]BucketConfig, len(discovered))
	for _, b := range discovered {
		m[b] = BucketConfig{Destination: s.cfg.DestBucketPrefix + b}
	}
	return m, nil
}

// discover scans each source bucket in parallel (up to cfg.Workers goroutines),
// compares objects against stored state, and marks objects as pending when they
// are new or modified since last sync.
func (s *Syncer) discover(ctx context.Context, buckets map[string]BucketConfig) error {
	collectionTime := time.Now().UTC()

	lim := rate.NewLimiter(rate.Inf, 0)
	if s.cfg.RateLimit > 0 {
		lim = rate.NewLimiter(rate.Limit(s.cfg.RateLimit), int(s.cfg.RateLimit)+1)
	}

	sem := make(chan struct{}, s.cfg.Workers)
	errc := make(chan error, len(buckets))
	var wg sync.WaitGroup

	for bucket, bc := range buckets {
		select {
		case <-ctx.Done():
			goto wait
		case sem <- struct{}{}:
			wg.Add(1)
			go func(b string, cfg BucketConfig) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := lim.Wait(ctx); err != nil {
					errc <- err
					return
				}
				if err := s.discoverBucket(ctx, b, cfg, collectionTime); err != nil {
					errc <- fmt.Errorf("discover bucket %s: %w", b, err)
				}
			}(bucket, bc)
		}
	}

wait:
	wg.Wait()
	close(errc)
	return <-errc
}

func (s *Syncer) discoverBucket(ctx context.Context, bucket string, cfg BucketConfig, collectionTime time.Time) error {
	logger := log.With().Str("bucket", bucket).Str("prefix", cfg.SrcPrefix).Logger()

	objects, errc := s.cfg.Source.ListObjects(ctx, bucket, cfg.SrcPrefix)
	var count, pending int

	for obj := range objects {
		count++
		needsSync, err := s.needsSync(ctx, bucket, obj, cfg)
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

func (s *Syncer) needsSync(ctx context.Context, bucket string, obj storage.Object, cfg BucketConfig) (bool, error) {
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
	if obj.ModifiedAt.After(stored.ModifiedAt) {
		return true, nil
	}
	// Optionally verify destination size matches source to catch incomplete uploads.
	if s.cfg.CheckSizes && obj.Size > 0 {
		dstSize, err := s.cfg.Destination.HeadObject(ctx, cfg.Destination, cfg.destKey(obj.Key))
		if err != nil {
			// Object missing or inaccessible on destination — re-sync.
			return true, nil
		}
		if dstSize != obj.Size {
			return true, nil
		}
	}
	return false, nil
}

func (s *Syncer) sync(ctx context.Context, buckets map[string]BucketConfig) error {
	pool := newWorkerPool(ctx, s.cfg.Workers, s.cfg.RateLimit, s.transfer, s.m.activeWorkers)

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		s.processResults(ctx, pool.resultsCh())
	}()

	for srcBucket, bc := range buckets {
		if ctx.Err() != nil {
			break
		}
		pending, err := s.cfg.State.ScanPending(ctx, srcBucket)
		if err != nil {
			log.Error().Err(err).Str("bucket", srcBucket).Msg("scan pending failed")
			continue
		}

		if err := s.cfg.Destination.EnsureBucket(ctx, bc.Destination); err != nil {
			log.Error().Err(err).Str("bucket", bc.Destination).Msg("ensure destination bucket failed")
			continue
		}

		if s.cfg.Progress != nil {
			s.cfg.Progress.startBucket(srcBucket, int64(len(pending)))
		}

		logger := log.With().Str("src", srcBucket).Str("dst", bc.Destination).Logger()
		logger.Info().Int("count", len(pending)).Msg("queuing objects")

		for _, key := range pending {
			if ctx.Err() != nil {
				break
			}
			pool.submit(Job{
				SrcBucket: srcBucket,
				DstBucket: bc.Destination,
				Key:       key,
				DstKey:    bc.destKey(key),
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
			if s.cfg.Progress != nil {
				s.cfg.Progress.recordFailed(r.Job.SrcBucket)
			}
		} else {
			log.Debug().
				Str("bucket", r.Job.SrcBucket).
				Str("key", r.Job.Key).
				Str("size", humanize.Bytes(uint64(r.Job.Size))).
				Dur("duration", r.Duration).
				Msg("transfer complete")
			_ = s.cfg.State.MarkSynced(ctx, r.Job.SrcBucket, r.Job.Key)
			s.m.synced.Add(ctx, 1, metric.WithAttributes(attrs...))
			s.m.bytesTransferred.Add(ctx, r.Job.Size, metric.WithAttributes(attrs...))
			s.m.duration.Record(ctx, r.Duration.Seconds(), metric.WithAttributes(attrs...))
			if s.cfg.Progress != nil {
				s.cfg.Progress.recordSynced(r.Job.SrcBucket)
			}
		}
	}
}

// RunWatch repeatedly calls Run until ctx is cancelled, sleeping interval between
// each completed cycle. The sleep is context-aware: cancellation during the sleep
// exits immediately and cleanly.
func (s *Syncer) RunWatch(ctx context.Context, interval time.Duration) error {
	return s.runWatch(ctx, interval, s.Run)
}

// runWatch is the testable core of RunWatch; cycleFn replaces s.Run so tests can
// inject controlled behaviour without requiring real S3 or Redis connections.
func (s *Syncer) runWatch(ctx context.Context, interval time.Duration, cycleFn func(context.Context) error) error {
	for {
		if err := cycleFn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		log.Info().Dur("interval", interval).Msg("watch: cycle complete, waiting before next discovery")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

// RunWatcher performs an initial full sync cycle to catch any changes missed while
// the program was down, then switches to event-driven mode consuming events from w.
// In-flight transfers complete before returning on context cancellation.
func (s *Syncer) RunWatcher(ctx context.Context, w watcher.Watcher) error {
	if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}

	bucketMap, err := s.resolveBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bc := range bucketMap {
		if err := s.cfg.Destination.EnsureBucket(ctx, bc.Destination); err != nil {
			log.Error().Err(err).Str("bucket", bc.Destination).Msg("ensure destination bucket failed")
		}
	}

	srcBuckets := make([]string, 0, len(bucketMap))
	for b := range bucketMap {
		srcBuckets = append(srcBuckets, b)
	}

	return s.runWatcher(ctx, w, srcBuckets, bucketMap)
}

// runWatcher is the testable event-loop core of RunWatcher; it accepts a pre-resolved
// bucket map and an already-started Watcher so tests can inject controlled behaviour.
func (s *Syncer) runWatcher(ctx context.Context, w watcher.Watcher, srcBuckets []string, bucketMap map[string]BucketConfig) error {
	events, err := w.Watch(ctx, srcBuckets)
	if err != nil {
		return fmt.Errorf("start watcher: %w", err)
	}

	pool := newWorkerPool(ctx, s.cfg.Workers, s.cfg.RateLimit, s.transfer, s.m.activeWorkers)

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		s.processResults(ctx, pool.resultsCh())
	}()

	log.Info().Strs("buckets", srcBuckets).Msg("watch: listening for object events")

	for event := range events {
		bc, ok := bucketMap[event.Bucket]
		if !ok {
			log.Warn().Str("bucket", event.Bucket).Msg("watch: received event for unknown bucket, skipping")
			continue
		}
		if s.cfg.State != nil {
			if err := s.cfg.State.MarkPending(ctx, event.Bucket, event.Key, event.ModifiedAt); err != nil {
				log.Error().Err(err).Str("bucket", event.Bucket).Str("key", event.Key).Msg("watch: mark pending failed")
				continue
			}
		}
		pool.submit(Job{
			SrcBucket:  event.Bucket,
			DstBucket:  bc.Destination,
			Key:        event.Key,
			DstKey:     bc.destKey(event.Key),
			Size:       event.Size,
			ModifiedAt: event.ModifiedAt,
		})
	}

	pool.close()
	resultWg.Wait()
	return nil
}

func (s *Syncer) transfer(ctx context.Context, job Job) error {
	body, srcSize, err := s.cfg.Source.GetObject(ctx, job.SrcBucket, job.Key)
	if err != nil {
		return err
	}
	defer body.Close()

	job.Size = srcSize
	if err := s.cfg.Destination.PutObject(ctx, job.DstBucket, job.DstKey, body, srcSize); err != nil {
		return err
	}

	// Verify destination size matches source to catch silent data-loss during upload.
	// Skip when srcSize is unknown (server did not provide Content-Length).
	if srcSize > 0 {
		dstSize, err := s.cfg.Destination.HeadObject(ctx, job.DstBucket, job.DstKey)
		if err != nil {
			return fmt.Errorf("verify %s/%s: %w", job.DstBucket, job.DstKey, err)
		}
		if dstSize != srcSize {
			return fmt.Errorf("size mismatch for %s/%s: source=%d destination=%d",
				job.DstBucket, job.DstKey, srcSize, dstSize)
		}
	}

	return nil
}
