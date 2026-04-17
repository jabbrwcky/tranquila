package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	"gopkg.in/yaml.v3"
)

type S3Config struct {
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type TelemetryConfig struct {
	Exporter     string `yaml:"exporter"`
	Addr         string `yaml:"addr"`
	OTLPEndpoint string `yaml:"otlp_endpoint"`
}

type Config struct {
	Source       S3Config        `yaml:"source"`
	Destination  S3Config        `yaml:"destination"`
	Buckets      []string        `yaml:"buckets"`
	BucketPrefix string          `yaml:"bucket_prefix"`
	Redis        RedisConfig     `yaml:"redis"`
	Telemetry    TelemetryConfig `yaml:"telemetry"`
	Workers      int             `yaml:"workers"`
	RateLimit    float64         `yaml:"rate_limit"`
}

func defaults() Config {
	return Config{
		Source:      S3Config{Region: "us-east-1"},
		Destination: S3Config{Region: "us-east-1"},
		Redis:       RedisConfig{Addr: "localhost:6379"},
		Telemetry:   TelemetryConfig{Exporter: "prometheus", Addr: ":9090"},
		Workers:     10,
	}
}

func Load(path string) (*Config, error) {
	cfg := defaults()
	if path == "" {
		return &cfg, nil
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &cfg, nil
		}
		return nil, fmt.Errorf("open config file: %w", err)
	}
	defer f.Close()
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config file: %w", err)
	}
	return &cfg, nil
}

// NewResolver returns a kong.Resolver that maps YAML config values to CLI flag names.
// Priority (highest to lowest): CLI flags > env vars > this resolver > kong defaults.
func NewResolver(cfg *Config) kong.Resolver {
	flat := map[string]interface{}{
		"source-endpoint":         cfg.Source.Endpoint,
		"source-region":           cfg.Source.Region,
		"source-access-key":       cfg.Source.AccessKey,
		"source-secret-key":       cfg.Source.SecretKey,
		"source-buckets":          cfg.Buckets,
		"dest-endpoint":           cfg.Destination.Endpoint,
		"dest-region":             cfg.Destination.Region,
		"dest-access-key":         cfg.Destination.AccessKey,
		"dest-secret-key":         cfg.Destination.SecretKey,
		"dest-bucket-prefix":      cfg.BucketPrefix,
		"redis-addr":              cfg.Redis.Addr,
		"redis-password":          cfg.Redis.Password,
		"redis-db":                cfg.Redis.DB,
		"workers":                 cfg.Workers,
		"rate-limit":              cfg.RateLimit,
		"telemetry-exporter":      cfg.Telemetry.Exporter,
		"telemetry-addr":          cfg.Telemetry.Addr,
		"telemetry-otlp-endpoint": cfg.Telemetry.OTLPEndpoint,
	}
	return kong.ResolverFunc(func(_ *kong.Context, _ *kong.Path, flag *kong.Flag) (interface{}, error) {
		name := flag.Name
		if v, ok := flat[name]; ok {
			// Don't override with zero values — let kong defaults win.
			switch vv := v.(type) {
			case string:
				if vv == "" {
					return nil, nil
				}
			case int:
				if vv == 0 {
					return nil, nil
				}
			case []string:
				if len(vv) == 0 {
					return nil, nil
				}
			}
			return v, nil
		}
		// Try dash-to-underscore variant
		if v, ok := flat[strings.ReplaceAll(name, "_", "-")]; ok {
			return v, nil
		}
		return nil, nil
	})
}
