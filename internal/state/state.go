package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StatusPending = "pending"
	StatusSynced  = "synced"
	StatusFailed  = "failed"

	scanBatchSize = 100
)

type ObjectState struct {
	Status     string
	ModifiedAt time.Time
	SyncedAt   time.Time
}

type BucketStats struct {
	Total   int64
	Synced  int64
	Pending int64
	Failed  int64
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type Store struct {
	client *redis.Client
}

func NewStore(cfg RedisConfig) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %s: %w", cfg.Addr, err)
	}
	return &Store{client: client}, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func objKey(bucket, key string) string {
	return "tranquila:obj:" + bucket + ":" + key
}

func collKey(bucket string) string {
	return "tranquila:collection:" + bucket
}

func (s *Store) UpsertObject(ctx context.Context, bucket, key string, obj ObjectState) error {
	fields := map[string]interface{}{
		"status":      obj.Status,
		"modified_at": obj.ModifiedAt.UTC().Format(time.RFC3339Nano),
	}
	if !obj.SyncedAt.IsZero() {
		fields["synced_at"] = obj.SyncedAt.UTC().Format(time.RFC3339Nano)
	}
	return s.client.HSet(ctx, objKey(bucket, key), fields).Err()
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (*ObjectState, error) {
	vals, err := s.client.HGetAll(ctx, objKey(bucket, key)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall %s/%s: %w", bucket, key, err)
	}
	if len(vals) == 0 {
		return nil, nil
	}
	obj := &ObjectState{Status: vals["status"]}
	if v, ok := vals["modified_at"]; ok {
		obj.ModifiedAt, _ = time.Parse(time.RFC3339Nano, v)
	}
	if v, ok := vals["synced_at"]; ok {
		obj.SyncedAt, _ = time.Parse(time.RFC3339Nano, v)
	}
	return obj, nil
}

func (s *Store) MarkSynced(ctx context.Context, bucket, key string) error {
	return s.client.HSet(ctx, objKey(bucket, key), map[string]interface{}{
		"status":    StatusSynced,
		"synced_at": time.Now().UTC().Format(time.RFC3339Nano),
	}).Err()
}

func (s *Store) MarkFailed(ctx context.Context, bucket, key string) error {
	return s.client.HSet(ctx, objKey(bucket, key), "status", StatusFailed).Err()
}

func (s *Store) MarkPending(ctx context.Context, bucket, key string, modifiedAt time.Time) error {
	return s.client.HSet(ctx, objKey(bucket, key), map[string]interface{}{
		"status":      StatusPending,
		"modified_at": modifiedAt.UTC().Format(time.RFC3339Nano),
	}).Err()
}

// ScanPending returns all object keys with status=pending for a bucket.
func (s *Store) ScanPending(ctx context.Context, bucket string) ([]string, error) {
	pattern := "tranquila:obj:" + bucket + ":*"
	prefix := "tranquila:obj:" + bucket + ":"

	var keys []string
	iter := s.client.Scan(ctx, 0, pattern, scanBatchSize).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan keys for bucket %s: %w", bucket, err)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	var pending []string
	for i := 0; i < len(keys); i += scanBatchSize {
		end := i + scanBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for j, k := range batch {
			cmds[j] = pipe.HGet(ctx, k, "status")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("pipeline hget: %w", err)
		}
		for j, cmd := range cmds {
			if cmd.Val() == StatusPending {
				pending = append(pending, strings.TrimPrefix(batch[j], prefix))
			}
		}
	}
	return pending, nil
}

func (s *Store) SetCollectionTime(ctx context.Context, bucket string, t time.Time) error {
	return s.client.Set(ctx, collKey(bucket), t.UTC().Format(time.RFC3339Nano), 0).Err()
}

func (s *Store) GetCollectionTime(ctx context.Context, bucket string) (time.Time, error) {
	v, err := s.client.Get(ctx, collKey(bucket)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("get collection time for %s: %w", bucket, err)
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse collection time: %w", err)
	}
	return t, nil
}

// ListBuckets returns the names of all source buckets that have completed at
// least one discovery run (i.e. have a stored collection timestamp).
func (s *Store) ListBuckets(ctx context.Context) ([]string, error) {
	const prefix = "tranquila:collection:"
	var buckets []string
	iter := s.client.Scan(ctx, 0, prefix+"*", scanBatchSize).Iterator()
	for iter.Next(ctx) {
		buckets = append(buckets, strings.TrimPrefix(iter.Val(), prefix))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan collection keys: %w", err)
	}
	return buckets, nil
}

func (s *Store) BucketStats(ctx context.Context, bucket string) (BucketStats, error) {
	pattern := "tranquila:obj:" + bucket + ":*"
	var keys []string
	iter := s.client.Scan(ctx, 0, pattern, scanBatchSize).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return BucketStats{}, fmt.Errorf("scan: %w", err)
	}

	var stats BucketStats
	for i := 0; i < len(keys); i += scanBatchSize {
		end := i + scanBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for j, k := range batch {
			cmds[j] = pipe.HGet(ctx, k, "status")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return BucketStats{}, fmt.Errorf("pipeline: %w", err)
		}
		for _, cmd := range cmds {
			stats.Total++
			switch cmd.Val() {
			case StatusSynced:
				stats.Synced++
			case StatusPending:
				stats.Pending++
			case StatusFailed:
				stats.Failed++
			}
		}
	}
	return stats, nil
}
