package sync

import (
	"sync"
	"time"
)

// Progress tracks in-flight sync state for consumption by the management API.
// All methods are safe for concurrent use.
type Progress struct {
	mu        sync.RWMutex
	running   bool
	startedAt time.Time
	buckets   map[string]*bucketProgress
}

type bucketProgress struct {
	pendingAtStart int64
	synced         int64
	failed         int64
	startedAt      time.Time
}

// BucketProgressSnapshot is an immutable point-in-time view of one bucket.
type BucketProgressSnapshot struct {
	PendingAtStart int64
	Synced         int64
	Failed         int64
	StartedAt      time.Time
}

// ProgressSnapshot is an immutable point-in-time view of the full sync run.
type ProgressSnapshot struct {
	Running   bool
	StartedAt time.Time
	Buckets   map[string]BucketProgressSnapshot
}

func NewProgress() *Progress {
	return &Progress{buckets: make(map[string]*bucketProgress)}
}

func (p *Progress) start(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = true
	p.startedAt = t
	p.buckets = make(map[string]*bucketProgress)
}

func (p *Progress) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
}

func (p *Progress) startBucket(bucket string, pendingCount int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buckets[bucket] = &bucketProgress{
		pendingAtStart: pendingCount,
		startedAt:      time.Now().UTC(),
	}
}

func (p *Progress) recordSynced(bucket string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if bp, ok := p.buckets[bucket]; ok {
		bp.synced++
	}
}

func (p *Progress) recordFailed(bucket string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if bp, ok := p.buckets[bucket]; ok {
		bp.failed++
	}
}

// Snapshot returns a deep copy of all progress state suitable for reads
// without holding the lock.
func (p *Progress) Snapshot() ProgressSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snap := ProgressSnapshot{
		Running:   p.running,
		StartedAt: p.startedAt,
		Buckets:   make(map[string]BucketProgressSnapshot, len(p.buckets)),
	}
	for name, bp := range p.buckets {
		snap.Buckets[name] = BucketProgressSnapshot{
			PendingAtStart: bp.pendingAtStart,
			Synced:         bp.synced,
			Failed:         bp.failed,
			StartedAt:      bp.startedAt,
		}
	}
	return snap
}
