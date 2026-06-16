package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// configFromYAML parses the given YAML content as a config file and returns
// the populated SyncCmd after parsing "sync" as the subcommand.
func configFromYAML(t *testing.T, yaml string) SyncCmd {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(yaml), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cli := &CLI{}
	parser := buildParser(cli, cfgPath)
	if _, err := parser.Parse([]string{"sync"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cli.Sync
}

func TestConfigFileSource(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  source:
    endpoint: "http://minio:9000"
    region: "eu-west-1"
    access-key: "srckey"
    secret-key: "srcsecret"
`)
	if sync.Source.Endpoint != "http://minio:9000" {
		t.Errorf("Source.Endpoint = %q, want %q", sync.Source.Endpoint, "http://minio:9000")
	}
	if sync.Source.Region != "eu-west-1" {
		t.Errorf("Source.Region = %q, want %q", sync.Source.Region, "eu-west-1")
	}
	if sync.Source.AccessKey != "srckey" {
		t.Errorf("Source.AccessKey = %q, want %q", sync.Source.AccessKey, "srckey")
	}
	if sync.Source.SecretKey != "srcsecret" {
		t.Errorf("Source.SecretKey = %q, want %q", sync.Source.SecretKey, "srcsecret")
	}
}

func TestConfigFileDest(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  dest:
    endpoint: "http://minio-dst:9000"
    region: "us-west-2"
    access-key: "dstkey"
    secret-key: "dstsecret"
`)
	if sync.Destination.Endpoint != "http://minio-dst:9000" {
		t.Errorf("Destination.Endpoint = %q, want %q", sync.Destination.Endpoint, "http://minio-dst:9000")
	}
	if sync.Destination.Region != "us-west-2" {
		t.Errorf("Destination.Region = %q, want %q", sync.Destination.Region, "us-west-2")
	}
	if sync.Destination.AccessKey != "dstkey" {
		t.Errorf("Destination.AccessKey = %q, want %q", sync.Destination.AccessKey, "dstkey")
	}
	if sync.Destination.SecretKey != "dstsecret" {
		t.Errorf("Destination.SecretKey = %q, want %q", sync.Destination.SecretKey, "dstsecret")
	}
}

func TestConfigFileSourceRateLimit(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  source:
    rate-limit: 10.0
`)
	if sync.Source.RateLimit != 10.0 {
		t.Errorf("Source.RateLimit = %f, want 10.0", sync.Source.RateLimit)
	}
}

func TestConfigFileDestRateLimit(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  dest:
    rate-limit: 5.0
`)
	if sync.Destination.RateLimit != 5.0 {
		t.Errorf("Destination.RateLimit = %f, want 5.0", sync.Destination.RateLimit)
	}
}

func TestConfigFileRedis(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  redis:
    addr: "redis-host:6380"
    password: "redispass"
    db: 3
`)
	if sync.RedisAddr != "redis-host:6380" {
		t.Errorf("RedisAddr = %q, want %q", sync.RedisAddr, "redis-host:6380")
	}
	if sync.RedisPassword != "redispass" {
		t.Errorf("RedisPassword = %q, want %q", sync.RedisPassword, "redispass")
	}
	if sync.RedisDB != 3 {
		t.Errorf("RedisDB = %d, want 3", sync.RedisDB)
	}
}

func TestConfigFileSyncTuning(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  workers: 5
  check-sizes: true
`)
	if sync.Workers != 5 {
		t.Errorf("Workers = %d, want 5", sync.Workers)
	}
	if !sync.CheckSizes {
		t.Error("CheckSizes = false, want true")
	}
}

func TestConfigFileSyncTuningHyphenKeys(t *testing.T) {
	// check-sizes uses hyphens in its flag name; the underscore variant
	// (check_sizes) is silently ignored by kong-yaml.
	sync := configFromYAML(t, `
sync:
  workers: 3
  check-sizes: true
`)
	if sync.Workers != 3 {
		t.Errorf("Workers = %d, want 3", sync.Workers)
	}
	if !sync.CheckSizes {
		t.Error("CheckSizes = false, want true")
	}
}

func TestConfigFileWatch(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  watch: true
  watch-mode: sqs
  watch-interval: 30s
  sqs-queue-url: "https://sqs.eu-west-1.amazonaws.com/123456789/my-queue"
`)
	if !sync.Watch {
		t.Error("Watch = false, want true")
	}
	if sync.WatchMode != "sqs" {
		t.Errorf("WatchMode = %q, want %q", sync.WatchMode, "sqs")
	}
	if sync.WatchInterval != 30*time.Second {
		t.Errorf("WatchInterval = %v, want 30s", sync.WatchInterval)
	}
	if sync.SQSQueueURL != "https://sqs.eu-west-1.amazonaws.com/123456789/my-queue" {
		t.Errorf("SQSQueueURL = %q", sync.SQSQueueURL)
	}
}

func TestConfigFileTelemetry(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  telemetry:
    exporter: "otlp"
    addr: ":9091"
    otlp-endpoint: "localhost:4317"
`)
	if sync.TelemetryExporter != "otlp" {
		t.Errorf("TelemetryExporter = %q, want %q", sync.TelemetryExporter, "otlp")
	}
	if sync.TelemetryAddr != ":9091" {
		t.Errorf("TelemetryAddr = %q, want %q", sync.TelemetryAddr, ":9091")
	}
	if sync.TelemetryOTLPEndpoint != "localhost:4317" {
		t.Errorf("TelemetryOTLPEndpoint = %q, want %q", sync.TelemetryOTLPEndpoint, "localhost:4317")
	}
}

func TestConfigFileMgmtAddr(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  mgmt-addr: ":9090"
`)
	if sync.MgmtAddr != ":9090" {
		t.Errorf("MgmtAddr = %q, want %q", sync.MgmtAddr, ":9090")
	}
}

func TestConfigFileBuckets(t *testing.T) {
	sync := configFromYAML(t, `
sync:
  buckets:
    - source:
        bucket: src1
        prefix: in/
      destination:
        bucket: dst1
        prefix: out/
    - source:
        bucket: src2
      destination:
        bucket: dst2
`)
	if len(sync.Buckets) != 2 {
		t.Fatalf("len(Buckets) = %d, want 2", len(sync.Buckets))
	}
	b0 := sync.Buckets[0]
	if b0.Source.Bucket != "src1" || b0.Source.Prefix != "in/" {
		t.Errorf("Buckets[0].Source = {%q, %q}, want {src1, in/}", b0.Source.Bucket, b0.Source.Prefix)
	}
	if b0.Destination.Bucket != "dst1" || b0.Destination.Prefix != "out/" {
		t.Errorf("Buckets[0].Destination = {%q, %q}, want {dst1, out/}", b0.Destination.Bucket, b0.Destination.Prefix)
	}
	b1 := sync.Buckets[1]
	if b1.Source.Bucket != "src2" || b1.Destination.Bucket != "dst2" {
		t.Errorf("Buckets[1] = {%q → %q}, want {src2 → dst2}", b1.Source.Bucket, b1.Destination.Bucket)
	}
}

// clearSyncEnvVars removes env vars that SyncCmd fields bind to so that flag
// defaults are not shadowed by the caller's shell environment.
func clearSyncEnvVars(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"SOURCE_ENDPOINT", "SOURCE_REGION", "SOURCE_ACCESS_KEY", "SOURCE_SECRET_KEY", "SOURCE_RATE_LIMIT",
		"DEST_ENDPOINT", "DEST_REGION", "DEST_ACCESS_KEY", "DEST_SECRET_KEY", "DEST_RATE_LIMIT",
		"DEST_BUCKET_PREFIX", "BUCKET_MAPPINGS", "BUCKET_MAPPING_FILE", "PREFIX_MAPPINGS",
		"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB",
		"TRANQUILA_WORKERS", "TRANQUILA_CHECK_SIZES",
		"TRANQUILA_WATCH", "TRANQUILA_WATCH_MODE", "TRANQUILA_WATCH_INTERVAL", "TRANQUILA_SQS_QUEUE_URL",
		"TELEMETRY_EXPORTER", "TELEMETRY_ADDR", "TELEMETRY_OTLP_ENDPOINT",
		"MGMT_ADDR",
	} {
		orig, set := os.LookupEnv(v)
		if set {
			os.Unsetenv(v)
			t.Cleanup(func() { os.Setenv(v, orig) })
		}
	}
}

func TestConfigFileDefaults(t *testing.T) {
	clearSyncEnvVars(t)
	// Empty config — verify flag defaults apply.
	sync := configFromYAML(t, "sync: {}\n")
	if sync.Source.Region != "us-east-1" {
		t.Errorf("Source.Region default = %q, want us-east-1", sync.Source.Region)
	}
	if sync.Destination.Region != "us-east-1" {
		t.Errorf("Destination.Region default = %q, want us-east-1", sync.Destination.Region)
	}
	if sync.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr default = %q, want localhost:6379", sync.RedisAddr)
	}
	if sync.Workers != 10 {
		t.Errorf("Workers default = %d, want 10", sync.Workers)
	}
	if sync.WatchMode != "poll" {
		t.Errorf("WatchMode default = %q, want poll", sync.WatchMode)
	}
	if sync.WatchInterval != 60*time.Second {
		t.Errorf("WatchInterval default = %v, want 60s", sync.WatchInterval)
	}
	if sync.TelemetryExporter != "prometheus" {
		t.Errorf("TelemetryExporter default = %q, want prometheus", sync.TelemetryExporter)
	}
	if sync.TelemetryAddr != ":8081" {
		t.Errorf("TelemetryAddr default = %q, want :8081", sync.TelemetryAddr)
	}
	if sync.MgmtAddr != ":8080" {
		t.Errorf("MgmtAddr default = %q, want :8080", sync.MgmtAddr)
	}
}
