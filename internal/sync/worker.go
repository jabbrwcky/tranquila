package sync

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type Job struct {
	SrcBucket  string
	DstBucket  string
	Key        string // source object key
	DstKey     string // destination object key (may differ via prefix rewrite)
	Size       int64
	ModifiedAt time.Time
	// SrcETag is the source object's ETag from discovery listing, used by
	// verify-and-delete to compare against the destination's ETag without a
	// download when both sides are single-part uploads. Empty for jobs not
	// built from a listing (e.g. watch-mode events).
	SrcETag string
	// OnComplete is called exactly once after the job is either transferred,
	// failed, or skipped due to context cancellation. Used by discoverAndSyncBucket
	// to wait for a batch to drain before continuing discovery.
	OnComplete       func()
	BurnAfterReading bool // delete source object after verified sync
	DryRun           bool // log planned deletions without executing them
	VerifyAndDelete  bool // skip upload; only verify destination presence + delete source (BAR for already-synced objects)
}

type Result struct {
	Job      Job
	Duration time.Duration
	Err      error
}

type transferFn func(ctx context.Context, job Job) error

// transferGrace bounds a single detached transfer so a degraded rate limiter
// cannot stall shutdown indefinitely.
const transferGrace = 30 * time.Minute

type workerPool struct {
	jobs          chan Job
	results       chan Result
	wg            sync.WaitGroup
	activeWorkers metric.Int64UpDownCounter
}

func newWorkerPool(ctx context.Context, n int, fn transferFn, activeWorkers metric.Int64UpDownCounter) *workerPool {
	p := &workerPool{
		jobs:          make(chan Job, n*2),
		results:       make(chan Result, n*2),
		activeWorkers: activeWorkers,
	}

	for range n {
		p.wg.Add(1)
		go p.runWorker(ctx, fn)
	}

	// Close results after all workers finish.
	go func() {
		p.wg.Wait()
		close(p.results)
	}()

	return p
}

func (p *workerPool) runWorker(ctx context.Context, fn transferFn) {
	defer p.wg.Done()
	for job := range p.jobs {
		// Skip job (without transferring) when ctx is cancelled. OnComplete is
		// still called so that any batchDone.Wait() in discoverAndSyncBucket can
		// unblock — the job remains pending in Redis and is retried on the next run.
		if ctx.Err() != nil {
			if job.OnComplete != nil {
				job.OnComplete()
			}
			continue
		}
		p.activeWorkers.Add(context.Background(), 1)
		// Detach from the signal context so in-flight transfers complete after
		// SIGTERM, but bound it: rate limiting happens inside the storage client,
		// and a congestion-degraded limiter would otherwise pace an uncancellable
		// transfer past terminationGracePeriodSeconds.
		start := time.Now()
		transferCtx, cancelTransfer := context.WithTimeout(context.WithoutCancel(ctx), transferGrace)
		err := fn(transferCtx, job)
		cancelTransfer()
		p.activeWorkers.Add(context.Background(), -1)
		p.results <- Result{Job: job, Duration: time.Since(start), Err: err}
	}
}

func (p *workerPool) submit(job Job) {
	p.jobs <- job
}

func (p *workerPool) close() {
	close(p.jobs)
}

func (p *workerPool) resultsCh() <-chan Result {
	return p.results
}
