package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jabbrwcky/tranquila/internal/state"
	"github.com/jabbrwcky/tranquila/internal/storage"
	internalsync "github.com/jabbrwcky/tranquila/internal/sync"
	"github.com/jabbrwcky/tranquila/internal/telemetry"
	"github.com/rs/zerolog/log"
)

type SyncCmd struct {
	SourceEndpoint  string   `kong:"name='source-endpoint',env='SOURCE_ENDPOINT',help='Source S3-compatible endpoint URL (empty = AWS)'"`
	SourceRegion    string   `kong:"name='source-region',env='SOURCE_REGION',default='us-east-1',help='Source AWS region'"`
	SourceAccessKey string   `kong:"name='source-access-key',env='SOURCE_ACCESS_KEY',help='Source AWS access key ID'"`
	SourceSecretKey string   `kong:"name='source-secret-key',env='SOURCE_SECRET_KEY',help='Source AWS secret access key'"`
	SourceBuckets   []string `kong:"name='source-buckets',env='SOURCE_BUCKETS',help='Buckets to sync (empty = all)',sep=','"`

	DestEndpoint     string `kong:"name='dest-endpoint',env='DEST_ENDPOINT',help='Destination S3-compatible endpoint URL (empty = AWS)'"`
	DestRegion       string `kong:"name='dest-region',env='DEST_REGION',default='us-east-1',help='Destination AWS region'"`
	DestAccessKey    string `kong:"name='dest-access-key',env='DEST_ACCESS_KEY',help='Destination AWS access key ID'"`
	DestSecretKey    string `kong:"name='dest-secret-key',env='DEST_SECRET_KEY',help='Destination AWS secret access key'"`
	DestBucketPrefix string `kong:"name='dest-bucket-prefix',env='DEST_BUCKET_PREFIX',help='Prefix prepended to destination bucket names'"`

	RedisAddr     string `kong:"name='redis-addr',env='REDIS_ADDR',default='localhost:6379',help='Redis server address'"`
	RedisPassword string `kong:"name='redis-password',env='REDIS_PASSWORD',help='Redis password'"`
	RedisDB       int    `kong:"name='redis-db',env='REDIS_DB',default='0',help='Redis database number'"`

	Workers   int     `kong:"name='workers',env='TRANQUILA_WORKERS',default='10',help='Number of concurrent sync workers'"`
	RateLimit float64 `kong:"name='rate-limit',env='TRANQUILA_RATE_LIMIT',default='0',help='Max S3 requests per second (0 = unlimited)'"`

	TelemetryExporter     string `kong:"name='telemetry-exporter',env='TELEMETRY_EXPORTER',default='prometheus',enum='prometheus,otlp,none',help='Metrics exporter'"`
	TelemetryAddr         string `kong:"name='telemetry-addr',env='TELEMETRY_ADDR',default=':9090',help='Prometheus metrics listen address'"`
	TelemetryOTLPEndpoint string `kong:"name='telemetry-otlp-endpoint',env='TELEMETRY_OTLP_ENDPOINT',help='OTLP gRPC endpoint'"`
}

func (cmd *SyncCmd) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Setup(ctx, telemetry.Config{
		Exporter:     cmd.TelemetryExporter,
		Addr:         cmd.TelemetryAddr,
		OTLPEndpoint: cmd.TelemetryOTLPEndpoint,
	})
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer tel.Shutdown(context.Background())

	store, err := state.NewStore(state.RedisConfig{
		Addr:     cmd.RedisAddr,
		Password: cmd.RedisPassword,
		DB:       cmd.RedisDB,
	})
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer store.Close()

	src, err := storage.NewClient(ctx, storage.Config{
		Endpoint:  cmd.SourceEndpoint,
		Region:    cmd.SourceRegion,
		AccessKey: cmd.SourceAccessKey,
		SecretKey: cmd.SourceSecretKey,
	})
	if err != nil {
		return fmt.Errorf("create source S3 client: %w", err)
	}

	dst, err := storage.NewClient(ctx, storage.Config{
		Endpoint:  cmd.DestEndpoint,
		Region:    cmd.DestRegion,
		AccessKey: cmd.DestAccessKey,
		SecretKey: cmd.DestSecretKey,
	})
	if err != nil {
		return fmt.Errorf("create destination S3 client: %w", err)
	}

	syncer, err := internalsync.New(internalsync.Config{
		Source:           src,
		Destination:      dst,
		State:            store,
		Meter:            tel.Meter,
		SourceBuckets:    cmd.SourceBuckets,
		DestBucketPrefix: cmd.DestBucketPrefix,
		Workers:          cmd.Workers,
		RateLimit:        cmd.RateLimit,
	})
	if err != nil {
		return fmt.Errorf("create syncer: %w", err)
	}

	log.Info().
		Int("workers", cmd.Workers).
		Float64("rate_limit", cmd.RateLimit).
		Str("telemetry", cmd.TelemetryExporter).
		Msg("tranquila starting")

	if err := syncer.Run(ctx); err != nil && err != context.Canceled {
		return err
	}

	log.Info().Msg("tranquila done")
	return nil
}
