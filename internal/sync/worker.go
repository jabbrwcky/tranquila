package sync

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

type Job struct {
	SrcBucket  string
	DstBucket  string
	Key        string // source object key
	DstKey     string // destination object key (may differ via prefix rewrite)
	Size       int64
	ModifiedAt time.Time
	// OnComplete is called exactly once after the job is either transferred,
	// failed, or skipped due to context cancellation. Used by discoverAndSyncBucket
	// to wait for a batch to drain before continuing discovery.
	OnComplete func()
}

type Result struct {
	Job      Job
	Duration time.Duration
	Err      error
}

type transferFn func(ctx context.Context, job Job) error

type workerPool struct {
	jobs          chan Job
	results       chan Result
	wg            sync.WaitGroup
	limiter       *rate.Limiter
	activeWorkers metric.Int64UpDownCounter
}

func newWorkerPool(ctx context.Context, n int, rateLimit float64, fn transferFn, activeWorkers metric.Int64UpDownCounter) *workerPool {
	lim := rate.NewLimiter(rate.Inf, 0)
	if rateLimit > 0 {
		lim = rate.NewLimiter(rate.Limit(rateLimit), int(rateLimit)+1)
	}

	p := &workerPool{
		jobs:          make(chan Job, n*2),
		results:       make(chan Result, n*2),
		limiter:       lim,
		activeWorkers: activeWorkers,
	}

	for i := 0; i < n; i++ {
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
		if err := p.limiter.Wait(ctx); err != nil {
			if job.OnComplete != nil {
				job.OnComplete()
			}
			continue
		}
		p.activeWorkers.Add(context.Background(), 1)
		// Use background context for the transfer itself so in-flight
		// transfers complete even after the signal context is cancelled.
		start := time.Now()
		err := fn(context.Background(), job)
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
