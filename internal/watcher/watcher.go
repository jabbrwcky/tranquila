package watcher

import (
	"context"
	"time"
)

// ObjectEvent represents a change to an object in a source bucket.
// Size and ModifiedAt are best-effort — they may be zero if the backend
// does not include them in the notification payload.
type ObjectEvent struct {
	Bucket     string
	Key        string
	Size       int64
	ModifiedAt time.Time
}

// Watcher streams object-change events for a set of source buckets.
// Watch returns a channel that is closed when ctx is cancelled.
type Watcher interface {
	Watch(ctx context.Context, buckets []string) (<-chan ObjectEvent, error)
}
