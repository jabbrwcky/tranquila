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
	if sync.SourceEndpoint != "http://minio:9000" {
		t.Errorf("SourceEndpoint = %q, want %q", sync.SourceEndpoint, "http://minio:9000")
	}
	if sync.SourceRegion != "eu-west-1" {
		t.Errorf("SourceRegion = %q, want %q", sync.SourceRegion, "eu-west-1")
	}
	if sync.SourceAccessKey != "srckey" {
		t.Errorf("SourceAccessKey = %q, want %q", sync.SourceAccessKey, "srckey")
	}
	if sync.SourceSecretKey != "srcsecret" {
		t.Errorf("SourceSecretKey = %q, want %q", sync.SourceSecretKey, "srcsecret")
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
	if sync.DestEndpoint != "http://minio-dst:9000" {
		t.Errorf("DestEndpoint = %q, want %q", sync.DestEndpoint, "http://minio-dst:9000")
	}
	if sync.DestRegion != "us-west-2" {
		t.Errorf("DestRegion = %q, want %q", sync.DestRegion, "us-west-2")
	}
	if sync.DestAccessKey != "dstkey" {
		t.Errorf("DestAccessKey = %q, want %q", sync.DestAccessKey, "dstkey")
	}
	if sync.DestSecretKey != "dstsecret" {
		t.Errorf("DestSecretKey = %q, want %q", sync.DestSecretKey, "dstsecret")
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
  rate-limit: 2.5
  check-sizes: true
`)
	if sync.Workers != 5 {
		t.Errorf("Workers = %d, want 5", sync.Workers)
	}
	if sync.RateLimit != 2.5 {
		t.Errorf("RateLimit = %f, want 2.5", sync.RateLimit)
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
		"SOURCE_ENDPOINT", "SOURCE_REGION", "SOURCE_ACCESS_KEY", "SOURCE_SECRET_KEY",
		"DEST_ENDPOINT", "DEST_REGION", "DEST_ACCESS_KEY", "DEST_SECRET_KEY",
		"DEST_BUCKET_PREFIX", "BUCKET_MAPPINGS", "BUCKET_MAPPING_FILE", "PREFIX_MAPPINGS",
		"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB",
		"TRANQUILA_WORKERS", "TRANQUILA_RATE_LIMIT", "TRANQUILA_CHECK_SIZES",
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
	if sync.SourceRegion != "us-east-1" {
		t.Errorf("SourceRegion default = %q, want us-east-1", sync.SourceRegion)
	}
	if sync.DestRegion != "us-east-1" {
		t.Errorf("DestRegion default = %q, want us-east-1", sync.DestRegion)
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
