package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/jabbrwcky/tranquila/internal/state"
	"github.com/jabbrwcky/tranquila/internal/storage"
)

const (
	// Pinned deliberately. Note the community MinIO image publishes arm64 only
	// on plain RELEASE tags — the enterprise *.hotfix.* tags are amd64-only and
	// fail to start on Apple Silicon.
	minioImage     = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
	toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"
	redisImage     = "redis:7-alpine"

	minioUser = "minioadmin"
	minioPass = "minioadmin"

	// Port inside the toxiproxy container that fronts MinIO.
	toxicMinioPort = "8666"
)

// stack is the container fixture shared by a test: MinIO for object storage,
// Redis for sync state, and Toxiproxy in front of MinIO for L4 fault injection.
type stack struct {
	minioEndpoint string // direct, no faults
	toxicEndpoint string // via toxiproxy
	redisAddr     string
	toxics        *toxiproxy.Proxy
}

// newStack brings up the container fixture and registers cleanup. Cleanup is
// explicit rather than relying on Ryuk, so the suite behaves the same whether
// or not the reaper is available on the local runtime.
func newStack(t *testing.T) *stack {
	requireContainerRuntime(t)
	t.Helper()
	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	minioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          minioImage,
			Cmd:            []string{"server", "/data"},
			Env:            map[string]string{"MINIO_ROOT_USER": minioUser, "MINIO_ROOT_PASSWORD": minioPass},
			ExposedPorts:   []string{"9000/tcp"},
			Networks:       []string{net.Name},
			NetworkAliases: map[string][]string{net.Name: {"minio"}},
			WaitingFor: wait.ForHTTP("/minio/health/live").
				WithPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(minioC) })

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        redisImage,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(redisC) })

	toxiC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        toxiproxyImage,
			ExposedPorts: []string{"8474/tcp", toxicMinioPort + "/tcp"},
			Networks:     []string{net.Name},
			WaitingFor:   wait.ForListeningPort("8474/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start toxiproxy: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(toxiC) })

	s := &stack{}
	if s.minioEndpoint, err = minioC.PortEndpoint(ctx, "9000/tcp", "http"); err != nil {
		t.Fatalf("minio endpoint: %v", err)
	}
	redisEP, err := redisC.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatalf("redis endpoint: %v", err)
	}
	s.redisAddr = redisEP

	ctrlEP, err := toxiC.PortEndpoint(ctx, "8474/tcp", "http")
	if err != nil {
		t.Fatalf("toxiproxy control endpoint: %v", err)
	}
	if s.toxicEndpoint, err = toxiC.PortEndpoint(ctx, toxicMinioPort+"/tcp", "http"); err != nil {
		t.Fatalf("toxiproxy proxied endpoint: %v", err)
	}

	// Listen on all interfaces inside the container so the mapped port reaches it.
	tc := toxiproxy.NewClient(ctrlEP)
	s.toxics, err = tc.CreateProxy("minio", "0.0.0.0:"+toxicMinioPort, "minio:9000")
	if err != nil {
		t.Fatalf("create toxiproxy proxy: %v", err)
	}
	t.Cleanup(func() { _ = s.toxics.Delete() })

	return s
}

// client builds a tranquila S3 client against endpoint. rateLimit of 0 leaves
// the endpoint unlimited, which disables congestion-driven degradation.
func (s *stack) client(t *testing.T, name, endpoint string, rateLimit float64, failThreshold int) *storage.Client {
	t.Helper()
	c, err := storage.NewClient(context.Background(), storage.Config{
		Endpoint:      endpoint,
		Region:        "us-east-1",
		AccessKey:     minioUser,
		SecretKey:     minioPass,
		RateLimit:     rateLimit,
		FailThreshold: failThreshold,
		Name:          name,
	})
	if err != nil {
		t.Fatalf("create %s client: %v", name, err)
	}
	return c
}

// store connects to the fixture's Redis.
func (s *stack) store(t *testing.T) *state.Store {
	t.Helper()
	st, err := state.NewStore(state.RedisConfig{Addr: s.redisAddr})
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// seed creates bucket and writes n objects into it, using a fault-free client.
func (s *stack) seed(t *testing.T, bucket string, n int) []string {
	t.Helper()
	ctx := context.Background()
	c := s.client(t, "seed", s.minioEndpoint, 0, 0)
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		t.Fatalf("ensure bucket %s: %v", bucket, err)
	}
	keys := make([]string, 0, n)
	for i := range n {
		key := fmt.Sprintf("obj-%03d.txt", i)
		body := strings.Repeat("x", 256)
		if _, err := c.PutObject(ctx, bucket, key, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("seed %s/%s: %v", bucket, key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

// countObjects reports how many of keys exist in bucket, via a fault-free client.
func (s *stack) countObjects(t *testing.T, bucket string, keys []string) int {
	t.Helper()
	ctx := context.Background()
	c := s.client(t, "verify", s.minioEndpoint, 0, 0)
	var n int
	for _, k := range keys {
		if _, _, err := c.HeadObject(ctx, bucket, k); err == nil {
			n++
		}
	}
	return n
}

// eventually polls cond until it holds or timeout elapses, so tests assert on
// convergence rather than on sleeps.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}
