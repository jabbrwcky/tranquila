package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jabbrwcky/tranquila/internal/state"
)

type StatusCmd struct {
	RedisAddr     string   `kong:"name='redis-addr',env='REDIS_ADDR',default='localhost:6379',help='Redis server address'"`
	RedisPassword string   `kong:"name='redis-password',env='REDIS_PASSWORD',help='Redis password'"`
	RedisDB       int      `kong:"name='redis-db',env='REDIS_DB',default='0',help='Redis database number'"`
	Buckets       []string `kong:"arg,optional,help='Buckets to show (empty = all from args)'"`
}

func (cmd *StatusCmd) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := state.NewStore(state.RedisConfig{
		Addr:     cmd.RedisAddr,
		Password: cmd.RedisPassword,
		DB:       cmd.RedisDB,
	})
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer store.Close()

	buckets := cmd.Buckets
	if len(buckets) == 0 {
		return fmt.Errorf("specify bucket names as arguments: tranquila status <bucket> [bucket...]")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "BUCKET\tLAST COLLECTED\tTOTAL\tSYNCED\tPENDING\tFAILED")

	for _, bucket := range buckets {
		ct, err := store.GetCollectionTime(ctx, bucket)
		if err != nil {
			fmt.Fprintf(w, "%s\terror: %v\n", bucket, err)
			continue
		}
		stats, err := store.BucketStats(ctx, bucket)
		if err != nil {
			fmt.Fprintf(w, "%s\terror: %v\n", bucket, err)
			continue
		}

		collected := "never"
		if !ct.IsZero() {
			collected = ct.Local().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\n",
			bucket, collected, stats.Total, stats.Synced, stats.Pending, stats.Failed)
	}

	return w.Flush()
}
