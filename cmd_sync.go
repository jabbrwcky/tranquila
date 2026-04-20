package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jabbrwcky/tranquila/internal/state"
	"github.com/jabbrwcky/tranquila/internal/storage"
	internalsync "github.com/jabbrwcky/tranquila/internal/sync"
	"github.com/jabbrwcky/tranquila/internal/telemetry"
	"github.com/rs/zerolog/log"
)

type SyncCmd struct {
	SourceEndpoint  string `kong:"name='source-endpoint',env='SOURCE_ENDPOINT',help='Source S3-compatible endpoint URL (empty = AWS)'"`
	SourceRegion    string `kong:"name='source-region',env='SOURCE_REGION',default='us-east-1',help='Source AWS region'"`
	SourceAccessKey string `kong:"name='source-access-key',env='SOURCE_ACCESS_KEY',help='Source AWS access key ID'"`
	SourceSecretKey string `kong:"name='source-secret-key',env='SOURCE_SECRET_KEY',help='Source AWS secret access key'"`

	DestEndpoint     string `kong:"name='dest-endpoint',env='DEST_ENDPOINT',help='Destination S3-compatible endpoint URL (empty = AWS)'"`
	DestRegion       string `kong:"name='dest-region',env='DEST_REGION',default='us-east-1',help='Destination AWS region'"`
	DestAccessKey    string `kong:"name='dest-access-key',env='DEST_ACCESS_KEY',help='Destination AWS access key ID'"`
	DestSecretKey    string `kong:"name='dest-secret-key',env='DEST_SECRET_KEY',help='Destination AWS secret access key'"`
	DestBucketPrefix string `kong:"name='dest-bucket-prefix',env='DEST_BUCKET_PREFIX',help='Prefix prepended to auto-discovered destination bucket names'"`

	BucketMappings    []string `kong:"name='bucket-mappings',env='BUCKET_MAPPINGS',help='Bucket mappings: \"name\" or \"src=dst\". Comma-separated.',sep=','"`
	BucketMappingFile string   `kong:"name='bucket-mapping-file',env='BUCKET_MAPPING_FILE',help='Path to file with bucket mappings (one per line)'"`
	PrefixMappings    []string `kong:"name='prefix-mappings',env='PREFIX_MAPPINGS',help='Path prefix mappings: \"bucket/src-prefix\" or \"bucket/src-prefix=dst-prefix\". Comma-separated.',sep=','"`

	RedisAddr     string `kong:"name='redis-addr',env='REDIS_ADDR',default='localhost:6379',help='Redis server address'"`
	RedisPassword string `kong:"name='redis-password',env='REDIS_PASSWORD',help='Redis password'"`
	RedisDB       int    `kong:"name='redis-db',env='REDIS_DB',default='0',help='Redis database number'"`

	Workers   int     `kong:"name='workers',env='TRANQUILA_WORKERS',default='10',help='Number of concurrent sync workers'"`
	RateLimit float64 `kong:"name='rate-limit',env='TRANQUILA_RATE_LIMIT',default='0',help='Max S3 requests per second (0 = unlimited)'"`

	TelemetryExporter     string `kong:"name='telemetry-exporter',env='TELEMETRY_EXPORTER',default='prometheus',enum='prometheus,otlp,none',help='Metrics exporter'"`
	TelemetryAddr         string `kong:"name='telemetry-addr',env='TELEMETRY_ADDR',default=':9090',help='Prometheus metrics listen address'"`
	TelemetryOTLPEndpoint string `kong:"name='telemetry-otlp-endpoint',env='TELEMETRY_OTLP_ENDPOINT',help='OTLP gRPC endpoint'"`
}

// resolveBuckets merges --bucket-mappings, --prefix-mappings, and --bucket-mapping-file
// into a per-source-bucket config. The mapping file may contain both bucket-mapping lines
// ("name" or "src=dst") and prefix-mapping lines ("bucket/src-prefix[=dst-prefix]") —
// lines with a "/" in the source part are treated as prefix mappings.
// Returns nil when nothing is specified (signals the syncer to auto-discover).
func (cmd *SyncCmd) resolveBuckets() (map[string]internalsync.BucketConfig, error) {
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
	if len(entries) == 0 {
		return nil, nil
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

	result := make(map[string]internalsync.BucketConfig)
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

	bucketMap, err := cmd.resolveBuckets()
	if err != nil {
		return fmt.Errorf("resolve bucket mappings: %w", err)
	}

	syncer, err := internalsync.New(internalsync.Config{
		Source:           src,
		Destination:      dst,
		State:            store,
		Meter:            tel.Meter,
		Buckets:          bucketMap,
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
