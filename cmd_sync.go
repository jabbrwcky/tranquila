package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jabbrwcky/tranquila/config"
	"github.com/jabbrwcky/tranquila/internal/api"
	"github.com/jabbrwcky/tranquila/internal/state"
	"github.com/jabbrwcky/tranquila/internal/storage"
	internalsync "github.com/jabbrwcky/tranquila/internal/sync"
	"github.com/jabbrwcky/tranquila/internal/telemetry"
	"github.com/jabbrwcky/tranquila/internal/watcher"
	"github.com/rs/zerolog/log"
)

type S3Server struct {
	Endpoint  string `name:"endpoint" env:"ENDPOINT" help:"Source S3-compatible endpoint URL (empty = AWS)"`
	Region    string
	AccessKey string
	SecretKey string
}

type SyncCmd struct {
	SourceEndpoint  string `name:"source-endpoint" env:"SOURCE_ENDPOINT" help:"Source S3-compatible endpoint URL (empty = AWS)"`
	SourceRegion    string `name:"source-region" env:"SOURCE_REGION" default:"us-east-1" help:"Source AWS region"`
	SourceAccessKey string `name:"source-access-key" env:"SOURCE_ACCESS_KEY" help:"Source AWS access key ID"`
	SourceSecretKey string `name:"source-secret-key" env:"SOURCE_SECRET_KEY" help:"Source AWS secret access key"`

	DestEndpoint     string `name:"dest-endpoint" env:"DEST_ENDPOINT" help:"Destination S3-compatible endpoint URL (empty = AWS)"`
	DestRegion       string `name:"dest-region" env:"DEST_REGION" default:"us-east-1" help:"Destination AWS region"`
	DestAccessKey    string `name:"dest-access-key" env:"DEST_ACCESS_KEY" help:"Destination AWS access key ID"`
	DestSecretKey    string `name:"dest-secret-key" env:"DEST_SECRET_KEY" help:"Destination AWS secret access key"`
	DestBucketPrefix string `name:"dest-bucket-prefix" env:"DEST_BUCKET_PREFIX" help:"Prefix prepended to auto-discovered destination bucket names"`

	BucketMappings    []string `name:"bucket-mappings" env:"BUCKET_MAPPINGS" help:"Bucket mappings: \"name\" or \"src=dst\". Comma-separated." sep:","`
	BucketMappingFile string   `name:"bucket-mapping-file" env:"BUCKET_MAPPING_FILE" help:"Path to file with bucket mappings (one per line)"`
	PrefixMappings    []string `name:"prefix-mappings" env:"PREFIX_MAPPINGS" help:"Path prefix mappings: \"bucket/src-prefix\" or \"bucket/src-prefix=dst-prefix\". Comma-separated." sep:","`

	RedisAddr     string `name:"redis-addr" env:"REDIS_ADDR" default:"localhost:6379" help:"Redis server address"`
	RedisPassword string `name:"redis-password" env:"REDIS_PASSWORD" help:"Redis password"`
	RedisDB       int    `name:"redis-db" env:"REDIS_DB" default:"0" help:"Redis database number"`

	Workers            int     `name:"workers" env:"TRANQUILA_WORKERS" default:"10" help:"Number of concurrent sync workers"`
	RateLimit          float64 `name:"rate-limit" env:"TRANQUILA_RATE_LIMIT" default:"0" help:"Max S3 requests per second (0 = unlimited)"`
	CheckSizes         bool    `name:"check-sizes" env:"TRANQUILA_CHECK_SIZES" default:"false" help:"Re-sync objects whose destination size differs from source"`
	DiscoveryBatchSize int     `name:"discovery-batch-size" env:"TRANQUILA_DISCOVERY_BATCH_SIZE" default:"100000" help:"Objects to discover per bucket before syncing; next batch starts after sync drains (0 = default 100000)"`

	Watch         bool          `name:"watch" env:"TRANQUILA_WATCH" default:"false" help:"Continuously re-run sync until interrupted"`
	WatchMode     string        `name:"watch-mode" env:"TRANQUILA_WATCH_MODE" default:"poll" enum:"poll,minio,sqs" help:"Watch backend: poll|minio|sqs"`
	WatchInterval time.Duration `name:"watch-interval" env:"TRANQUILA_WATCH_INTERVAL" default:"60s" help:"Idle time between poll cycles (poll mode only)"`
	SQSQueueURL   string        `name:"sqs-queue-url" env:"TRANQUILA_SQS_QUEUE_URL" help:"SQS queue URL for S3 event notifications (sqs mode)"`

	TelemetryExporter     string `name:"telemetry-exporter" env:"TELEMETRY_EXPORTER" default:"prometheus" enum:"prometheus,otlp,none" help:"Metrics exporter"`
	TelemetryAddr         string `name:"telemetry-addr" env:"TELEMETRY_ADDR" default:":8081" help:"Prometheus metrics listen address"`
	TelemetryOTLPEndpoint string `name:"telemetry-otlp-endpoint" env:"TELEMETRY_OTLP_ENDPOINT" help:"OTLP gRPC endpoint"`

	MgmtAddr string `name:"mgmt-addr" env:"MGMT_ADDR" default:":8080" help:"Management API listen address"`

	Buckets config.BucketMappings `name:"buckets" help:"Structured bucket mappings (from config file)"`
}

// resolveBuckets merges --bucket-mappings, --prefix-mappings, and --bucket-mapping-file
// into a per-source-bucket config. The mapping file may contain both bucket-mapping lines
// ("name" or "src=dst") and prefix-mapping lines ("bucket/src-prefix[=dst-prefix]") —
// lines with a "/" in the source part are treated as prefix mappings.
// Returns nil when nothing is specified (signals the syncer to auto-discover).
func (cmd *SyncCmd) resolveBuckets() (map[string]internalsync.BucketConfig, error) {
	result := make(map[string]internalsync.BucketConfig)

	// Structured buckets from config file — processed first so CLI flags can override.
	for _, bm := range cmd.Buckets {
		src := bm.Source.Bucket
		if src == "" {
			continue
		}
		dst := bm.Destination.Bucket
		if dst == "" {
			dst = src
		}
		result[src] = internalsync.BucketConfig{
			Destination: dst,
			SrcPrefix:   bm.Source.Prefix,
			DstPrefix:   bm.Destination.Prefix,
		}
	}

	// Legacy string-based mappings from CLI flags and mapping file.
	var entries []string
	entries = append(entries, cmd.BucketMappings...)
	entries = append(entries, cmd.PrefixMappings...)
	if cmd.BucketMappingFile != "" {
		fileEntries, err := loadMappingFile(cmd.BucketMappingFile)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fileEntries...)
	}

	var bucketEntries, prefixEntries []string
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		src, _, _ := strings.Cut(e, "=")
		if strings.ContainsRune(src, '/') {
			prefixEntries = append(prefixEntries, e)
		} else {
			bucketEntries = append(bucketEntries, e)
		}
	}

	for src, dst := range parseBucketMappings(bucketEntries) {
		result[src] = internalsync.BucketConfig{Destination: dst}
	}
	for _, e := range prefixEntries {
		src, dstPrefix, _ := strings.Cut(e, "=")
		bucket, srcPrefix, _ := strings.Cut(src, "/")
		bc := result[bucket]
		if bc.Destination == "" {
			bc.Destination = bucket
		}
		bc.SrcPrefix = srcPrefix
		bc.DstPrefix = dstPrefix
		result[bucket] = bc
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// parseBucketMappings converts "name" or "src=dst" strings into a map.
// For duplicate source keys, the last entry wins.
func parseBucketMappings(entries []string) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		src, dst, found := strings.Cut(e, "=")
		if !found || dst == "" {
			dst = src
		}
		m[src] = dst
	}
	return m
}

// loadMappingFile reads one mapping per line from path (supports "name" and "src=dst", # comments).
func loadMappingFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open bucket mapping file: %w", err)
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read bucket mapping file: %w", err)
	}
	return lines, nil
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

	log.Debug().Str("endpoint", cmd.SourceEndpoint).Str("region", cmd.SourceRegion).Msg("creating source client")
	src, err := storage.NewClient(ctx, storage.Config{
		Endpoint:  cmd.SourceEndpoint,
		Region:    cmd.SourceRegion,
		AccessKey: cmd.SourceAccessKey,
		SecretKey: cmd.SourceSecretKey,
		Meter:     tel.Meter,
	})
	if err != nil {
		return fmt.Errorf("create source S3 client: %w", err)
	}

	dst, err := storage.NewClient(ctx, storage.Config{
		Endpoint:  cmd.DestEndpoint,
		Region:    cmd.DestRegion,
		AccessKey: cmd.DestAccessKey,
		SecretKey: cmd.DestSecretKey,
		Meter:     tel.Meter,
	})
	if err != nil {
		return fmt.Errorf("create destination S3 client: %w", err)
	}

	bucketMap, err := cmd.resolveBuckets()
	if err != nil {
		return fmt.Errorf("resolve bucket mappings: %w", err)
	}

	progress := internalsync.NewProgress()

	mgmt := api.NewServer(api.Config{
		Addr:     cmd.MgmtAddr,
		State:    store,
		Progress: progress,
	})
	go func() {
		if err := mgmt.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("management API error")
		}
	}()
	defer mgmt.Shutdown(context.Background())

	syncer, err := internalsync.New(internalsync.Config{
		Source:             src,
		Destination:        dst,
		State:              store,
		Meter:              tel.Meter,
		Buckets:            bucketMap,
		DestBucketPrefix:   cmd.DestBucketPrefix,
		Workers:            cmd.Workers,
		RateLimit:          cmd.RateLimit,
		CheckSizes:         cmd.CheckSizes,
		Progress:           progress,
		DiscoveryBatchSize: cmd.DiscoveryBatchSize,
	})
	if err != nil {
		return fmt.Errorf("create syncer: %w", err)
	}

	log.Info().
		Int("workers", cmd.Workers).
		Float64("rate_limit", cmd.RateLimit).
		Bool("watch", cmd.Watch).
		Str("watch_mode", cmd.WatchMode).
		Str("telemetry", cmd.TelemetryExporter).
		Str("mgmt_addr", cmd.MgmtAddr).
		Msg("tranquila starting")

	var runErr error
	if !cmd.Watch {
		runErr = syncer.Run(ctx)
	} else {
		switch cmd.WatchMode {
		case "poll":
			runErr = syncer.RunWatch(ctx, cmd.WatchInterval)
		case "minio":
			w, err := watcher.NewMinIO(watcher.MinIOConfig{
				Endpoint:  cmd.SourceEndpoint,
				AccessKey: cmd.SourceAccessKey,
				SecretKey: cmd.SourceSecretKey,
				Secure:    strings.HasPrefix(cmd.SourceEndpoint, "https://"),
			})
			if err != nil {
				return fmt.Errorf("create minio watcher: %w", err)
			}
			runErr = syncer.RunWatcher(ctx, w)
		case "sqs":
			w, err := watcher.NewSQS(watcher.SQSConfig{
				QueueURL:  cmd.SQSQueueURL,
				Region:    cmd.SourceRegion,
				AccessKey: cmd.SourceAccessKey,
				SecretKey: cmd.SourceSecretKey,
			})
			if err != nil {
				return fmt.Errorf("create sqs watcher: %w", err)
			}
			runErr = syncer.RunWatcher(ctx, w)
		}
	}

	if runErr != nil && runErr != context.Canceled {
		return runErr
	}

	log.Info().Msg("tranquila done")
	return nil
}
