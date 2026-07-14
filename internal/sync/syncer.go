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
)

const defaultDiscoveryBatchSize = 100_000

// BucketConfig holds destination routing and path-prefix configuration for a source bucket.
type BucketConfig struct {
	Destination      string // destination bucket name
	SrcPrefix        string // list/filter prefix applied when scanning the source; empty = all objects
	DstPrefix        string // replaces SrcPrefix in the destination key; empty = keep original key
	BurnAfterReading bool   // delete source object after verified sync
}

// destKey returns the destination object key for srcKey, applying prefix replacement when configured.
func (bc BucketConfig) destKey(srcKey string) string {
	if bc.SrcPrefix == "" || bc.DstPrefix == "" {
		return srcKey
	}
	return bc.DstPrefix + strings.TrimPrefix(srcKey, bc.SrcPrefix)
}

type Config struct {
	Source             *storage.Client
	Destination        *storage.Client
	State              *state.Store
	Meter              metric.Meter
	Buckets            map[string]BucketConfig // src → config; nil = auto-discover all
	DestBucketPrefix   string                  // prefix for auto-discovered destination bucket names
	Workers            int
	CheckSizes         bool      // re-queue synced objects whose destination size differs from source
	DryRun             bool      // log planned burn-after-reading deletions without executing them
	Progress           *Progress // optional; enables live progress tracking for the management API
	DiscoveryBatchSize int       // max objects to discover per bucket before syncing (0 = default 100 000)
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

// discoveryBatchSize returns the effective batch size: cfg value if positive, default otherwise.
func (s *Syncer) discoveryBatchSize() int {
	if s.cfg.DiscoveryBatchSize > 0 {
		return s.cfg.DiscoveryBatchSize
	}
	return defaultDiscoveryBatchSize
}

// Run performs discovery and sync for all configured buckets. Each bucket's
// discovery runs concurrently with other buckets; sync starts as soon as a
// bucket's discovery batch is ready. Large buckets are processed in batches of
// DiscoveryBatchSize objects so that sync begins without waiting for the full
// listing and memory usage stays bounded.
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

	pool := newWorkerPool(ctx, s.cfg.Workers, s.transfer, s.m.activeWorkers)

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		s.processResults(ctx, pool.resultsCh())
	}()

	collectionTime := time.Now().UTC()
	sem := make(chan struct{}, s.cfg.Workers)
	errc := make(chan error, len(bucketMap))
	var discoverWg sync.WaitGroup

	for bucket, bc := range bucketMap {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		discoverWg.Add(1)
		go func(b string, cfg BucketConfig) {
			defer discoverWg.Done()
			defer func() { <-sem }()
			if err := s.discoverAndSyncBucket(ctx, b, cfg, collectionTime, pool); err != nil {
				if !errors.Is(err, context.Canceled) {
					errc <- fmt.Errorf("bucket %s: %w", b, err)
				}
			}
		}(bucket, bc)
	}

	discoverWg.Wait()
	pool.close()
	resultWg.Wait()
	close(errc)
	return <-errc
}

// discoverAndSyncBucket lists the source bucket in batches of DiscoveryBatchSize
// objects, marks each pending in Redis, submits them to the worker pool, and
// waits for the batch to finish syncing before fetching the next page. This
// keeps memory bounded and starts transferring objects without waiting for the
// full listing to complete. Called concurrently by Run for each bucket.
func (s *Syncer) discoverAndSyncBucket(ctx context.Context, bucket string, cfg BucketConfig, collectionTime time.Time, pool *workerPool) error {
	logger := log.With().Str("bucket", bucket).Str("prefix", cfg.SrcPrefix).Logger()

	if err := s.cfg.Destination.EnsureBucket(ctx, cfg.Destination); err != nil {
		return fmt.Errorf("ensure destination bucket %s: %w", cfg.Destination, err)
	}

	if s.cfg.Progress != nil {
		s.cfg.Progress.startBucket(bucket)
	}

	batchSize := s.discoveryBatchSize()
	var token *string
	var batchNum, totalCount, totalPending int

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		page, nextToken, err := s.cfg.Source.ListObjectsPage(ctx, bucket, cfg.SrcPrefix, token, batchSize)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		batchNum++
		var batchPending int
		var batchDone sync.WaitGroup

		for _, obj := range page {
			totalCount++
			needsFullSync, err := s.needsSync(ctx, bucket, obj, cfg)
			if err != nil {
				logger.Warn().Err(err).Str("key", obj.Key).Msg("state check failed, marking pending")
				needsFullSync = true
			}

			// For non-BAR buckets: skip objects that don't need sync.
			// For BAR buckets: even already-synced objects need verify-and-delete
			// (source was not deleted when the bucket was previously in normal mode).
			if !needsFullSync && !cfg.BurnAfterReading {
				continue
			}

			verifyAndDelete := !needsFullSync // already-synced; skip re-upload, just verify+delete

			if needsFullSync && s.cfg.State != nil {
				if err := s.cfg.State.MarkPending(ctx, bucket, obj.Key, obj.ModifiedAt); err != nil {
					return fmt.Errorf("mark pending %s: %w", obj.Key, err)
				}
			}
			totalPending++
			batchPending++
			batchDone.Add(1)
			pool.submit(Job{
				SrcBucket:        bucket,
				DstBucket:        cfg.Destination,
				Key:              obj.Key,
				DstKey:           cfg.destKey(obj.Key),
				Size:             obj.Size,
				ModifiedAt:       obj.ModifiedAt,
				OnComplete:       batchDone.Done,
				BurnAfterReading: cfg.BurnAfterReading,
				DryRun:           s.cfg.DryRun,
				VerifyAndDelete:  verifyAndDelete,
			})
		}

		if s.cfg.Progress != nil {
			s.cfg.Progress.addPending(bucket, int64(batchPending))
		}

		if nextToken != nil {
			logger.Info().
				Int("batch", batchNum).
				Int("discovered", len(page)).
				Int("queued", batchPending).
				Msg("batch queued, waiting for sync before continuing discovery")
			batchDone.Wait()
			logger.Info().Int("batch", batchNum).Msg("batch synced, resuming discovery")
		} else {
			logger.Info().
				Int("total", totalCount).
				Int("pending", totalPending).
				Int("batches", batchNum).
				Msg("discovery complete")
			batchDone.Wait()
		}

		token = nextToken
		if token == nil {
			break
		}
	}

	if s.cfg.State != nil {
		if err := s.cfg.State.SetCollectionTime(ctx, bucket, collectionTime); err != nil {
			return fmt.Errorf("set collection time: %w", err)
		}
	}

	return nil
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
		dstSize, _, err := s.cfg.Destination.HeadObject(ctx, cfg.Destination, cfg.destKey(obj.Key))
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
			// BAR (non-dry-run): source was deleted — remove the tracking record so
			// BucketStats stays accurate and future runs don't encounter stale entries.
			if r.Job.BurnAfterReading && !r.Job.DryRun {
				_ = s.cfg.State.RemoveObject(ctx, r.Job.SrcBucket, r.Job.Key)
			} else {
				_ = s.cfg.State.MarkSynced(ctx, r.Job.SrcBucket, r.Job.Key)
			}
			s.m.synced.Add(ctx, 1, metric.WithAttributes(attrs...))
			s.m.bytesTransferred.Add(ctx, r.Job.Size, metric.WithAttributes(attrs...))
			s.m.duration.Record(ctx, r.Duration.Seconds(), metric.WithAttributes(attrs...))
			if s.cfg.Progress != nil {
				s.cfg.Progress.recordSynced(r.Job.SrcBucket)
			}
		}
		if r.Job.OnComplete != nil {
			r.Job.OnComplete()
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

	pool := newWorkerPool(ctx, s.cfg.Workers, s.transfer, s.m.activeWorkers)

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
			SrcBucket:        event.Bucket,
			DstBucket:        bc.Destination,
			Key:              event.Key,
			DstKey:           bc.destKey(event.Key),
			Size:             event.Size,
			ModifiedAt:       event.ModifiedAt,
			BurnAfterReading: bc.BurnAfterReading,
			DryRun:           s.cfg.DryRun,
		})
	}

	pool.close()
	resultWg.Wait()
	return nil
}

func (s *Syncer) transfer(ctx context.Context, job Job) error {
	// VerifyAndDelete: object was already synced before BAR mode was enabled.
	// Skip re-upload; confirm destination still has it (size check), then delete source.
	if job.VerifyAndDelete {
		return performVerifyAndDelete(ctx, job, s.cfg.Destination, s.cfg.Source)
	}

	body, srcSize, err := s.cfg.Source.GetObject(ctx, job.SrcBucket, job.Key)
	if err != nil {
		return err
	}
	defer body.Close()

	job.Size = srcSize
	uploadCRC32, err := s.cfg.Destination.PutObject(ctx, job.DstBucket, job.DstKey, body, srcSize)
	if err != nil {
		return err
	}

	// Verify destination size and capture stored CRC32 for burn-after-reading.
	// Skip size check when srcSize is unknown (server did not provide Content-Length).
	dstSize, storedCRC32, err := s.cfg.Destination.HeadObject(ctx, job.DstBucket, job.DstKey)
	if err != nil {
		return fmt.Errorf("verify %s/%s: %w", job.DstBucket, job.DstKey, err)
	}
	if srcSize > 0 && dstSize != srcSize {
		return fmt.Errorf("size mismatch for %s/%s: source=%d destination=%d",
			job.DstBucket, job.DstKey, srcSize, dstSize)
	}

	if !job.BurnAfterReading {
		return nil
	}
	return performBurnAfterReading(ctx, job, s.cfg.Source, uploadCRC32, storedCRC32)
}

// objectDeleter is the narrow interface that performBurnAfterReading needs from the source client.
type objectDeleter interface {
	DeleteObject(ctx context.Context, bucket, key string) error
}

// destinationVerifier is the narrow interface that performVerifyAndDelete needs from the destination client.
type destinationVerifier interface {
	HeadObject(ctx context.Context, bucket, key string) (size int64, checksumCRC32 string, err error)
}

// performVerifyAndDelete handles the verify-and-delete path for objects that were already synced
// before burn-after-reading mode was enabled. It confirms the destination still has the object
// (existence + size check), then deletes from source. No re-upload is performed.
func performVerifyAndDelete(ctx context.Context, job Job, dst destinationVerifier, src objectDeleter) error {
	dstSize, _, err := dst.HeadObject(ctx, job.DstBucket, job.DstKey)
	if err != nil {
		return fmt.Errorf("burn-after-reading verify: destination check %s/%s: %w", job.DstBucket, job.DstKey, err)
	}
	if job.Size > 0 && dstSize != job.Size {
		return fmt.Errorf("burn-after-reading verify: size mismatch %s/%s: expected=%d got=%d",
			job.DstBucket, job.DstKey, job.Size, dstSize)
	}
	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Int64("size", dstSize).
		Msg("burn-after-reading: destination verified")
	if job.DryRun {
		log.Info().
			Str("bucket", job.SrcBucket).
			Str("key", job.Key).
			Msg("burn-after-reading: DRY-RUN would delete source object")
		return nil
	}
	if err := src.DeleteObject(ctx, job.SrcBucket, job.Key); err != nil {
		return fmt.Errorf("burn-after-reading: delete source %s/%s: %w", job.SrcBucket, job.Key, err)
	}
	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Msg("burn-after-reading: source object deleted")
	return nil
}

// performBurnAfterReading verifies destination integrity and deletes the source object.
// Both CRC32 values come from S3's own computation (upload response and stored value).
// Safe-by-default: if either is empty (object stored without a checksum algorithm) the
// delete is refused so data is never silently lost.
func performBurnAfterReading(ctx context.Context, job Job, src objectDeleter, uploadCRC32, storedCRC32 string) error {
	checksumMatch := uploadCRC32 != "" && storedCRC32 != "" && uploadCRC32 == storedCRC32
	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Str("upload_crc32", uploadCRC32).
		Str("stored_crc32", storedCRC32).
		Bool("checksum_match", checksumMatch).
		Msg("burn-after-reading: checksum verification")

	if job.DryRun {
		log.Info().
			Str("bucket", job.SrcBucket).
			Str("key", job.Key).
			Msg("burn-after-reading: DRY-RUN would delete source object")
		return nil
	}
	if !checksumMatch {
		return fmt.Errorf("burn-after-reading: checksum mismatch for %s/%s (upload=%q stored=%q), refusing to delete source",
			job.SrcBucket, job.Key, uploadCRC32, storedCRC32)
	}
	if err := src.DeleteObject(ctx, job.SrcBucket, job.Key); err != nil {
		return fmt.Errorf("burn-after-reading: delete source %s/%s: %w", job.SrcBucket, job.Key, err)
	}
	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Msg("burn-after-reading: source object deleted")
	return nil
}
