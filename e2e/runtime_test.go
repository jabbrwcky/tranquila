package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain adapts the environment to the local container runtime before any
// container starts, so `go test ./...` in this module works unchanged on both a
// podman machine and a Docker host such as a GitHub Actions runner.
//
// This module is intentionally separate from the production module: `go test
// ./...` at the repository root does not descend into it, so these tests never
// run as part of the ordinary unit-test suite.
func TestMain(m *testing.M) {
	if err := configureContainerRuntime(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// requireContainerRuntime skips rather than fails when no container runtime is
// reachable, so the suite is harmless on a machine that cannot run it.
func requireContainerRuntime(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed e2e test in -short mode")
	}
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		if path, ok := strings.CutPrefix(host, "unix://"); ok {
			if _, err := os.Stat(path); err != nil {
				t.Skipf("no container runtime: DOCKER_HOST=%s is not reachable (%v)", host, err)
			}
		}
		return
	}
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		t.Skip("no container runtime: /var/run/docker.sock not found and DOCKER_HOST unset. " +
			"Start Docker, or a podman machine (`podman machine start`)")
	}
}

// configureContainerRuntime points testcontainers at a podman machine when one
// is backing /var/run/docker.sock.
//
// testcontainers-go decides between its Docker and podman providers by looking
// for the literal substring "podman.sock" in DOCKER_HOST. A podman machine
// symlinks /var/run/docker.sock to its own socket, which makes podman look like
// Docker: the Docker provider is selected and Ryuk is then created on a network
// named "bridge", which podman does not have and refuses to create ("conflicts
// with a valid network mode"). Pointing DOCKER_HOST at podman's stable
// podman.sock symlink restores provider detection.
//
// See e2e/README.md for the full reasoning.
func configureContainerRuntime() error {
	if os.Getenv("DOCKER_HOST") != "" {
		return nil // explicit configuration wins; CI needs nothing.
	}
	sock, ok := podmanSocket()
	if !ok {
		return nil // plain Docker: defaults are correct.
	}

	// Must contain "podman.sock" or provider detection silently stays on Docker.
	if !strings.Contains(sock, "podman.sock") {
		return fmt.Errorf("podman socket %q lacks the \"podman.sock\" substring testcontainers matches on; "+
			"set DOCKER_HOST manually", sock)
	}
	setIfUnset("DOCKER_HOST", "unix://"+sock)
	// Ryuk is bind-mounted the socket by its in-VM path, not the host path.
	setIfUnset("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")
	setIfUnset("TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED", "true")
	return nil
}

// podmanSocket locates a podman socket, preferring paths that are stable and
// contain the "podman.sock" substring testcontainers matches on. The machine's
// own API socket is deliberately a last resort: it lives in a temp directory and
// is named podman-machine-default-api.sock, which does not contain that
// substring and so would not trigger provider detection.
func podmanSocket() (string, bool) {
	var candidates []string
	if home, err := os.UserHomeDir(); err == nil {
		// podman machine on macOS and Linux.
		candidates = append(candidates,
			filepath.Join(home, ".local", "share", "containers", "podman", "machine", "podman.sock"))
	}
	if run := os.Getenv("XDG_RUNTIME_DIR"); run != "" {
		// Rootless podman service on Linux.
		candidates = append(candidates, filepath.Join(run, "podman", "podman.sock"))
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, true
		}
	}
	// Fall back to whatever the Docker socket points at, for unusual layouts.
	target, err := filepath.EvalSymlinks("/var/run/docker.sock")
	if err != nil || !strings.Contains(target, "podman") {
		return "", false
	}
	return target, true
}

func setIfUnset(key, value string) {
	if os.Getenv(key) == "" {
		_ = os.Setenv(key, value)
	}
}
