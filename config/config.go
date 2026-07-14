package config

import (
	"fmt"

	"github.com/alecthomas/kong"
	"gopkg.in/yaml.v3"
)

// BucketEndpoint identifies a bucket and optional key prefix.
type BucketEndpoint struct {
	Bucket string `yaml:"bucket"`
	Prefix string `yaml:"prefix"`
}

// BucketMapping pairs a source bucket/prefix with a destination bucket/prefix.
type BucketMapping struct {
	Source           BucketEndpoint `yaml:"source"`
	Destination      BucketEndpoint `yaml:"destination"`
	BurnAfterReading bool           `yaml:"burn-after-reading"`
}

// BucketMappings is a slice of BucketMapping that implements kong.MapperValue
// so kong can decode the raw interface{} produced by the YAML config loader.
type BucketMappings []BucketMapping

// Decode implements kong.MapperValue.
func (b *BucketMappings) Decode(ctx *kong.DecodeContext) error {
	tok, err := ctx.Scan.PopValue("buckets")
	if err != nil {
		return err
	}
	mappings, err := decodeBucketMappings(tok.Value)
	if err != nil {
		return err
	}
	*b = mappings
	return nil
}

// decodeBucketMappings converts the raw interface{} produced by kong-yaml into
// a typed slice via a YAML round-trip.
func decodeBucketMappings(raw interface{}) ([]BucketMapping, error) {
	b, err := yaml.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode bucket mappings: %w", err)
	}
	var mappings []BucketMapping
	if err := yaml.Unmarshal(b, &mappings); err != nil {
		return nil, fmt.Errorf("decode bucket mappings: %w", err)
	}
	return mappings, nil
}
