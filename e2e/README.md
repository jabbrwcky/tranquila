# End-to-end resilience tests

Container-backed tests that exercise tranquila's failure handling against a real
MinIO, with real fault injection — the behaviour the unit tests can only
approximate with synthetic errors.

## Running

```shell
cd e2e
go test ./...              # ~5-10 min: pulls images, starts containers
go test -short ./...       # skips everything (no containers)
go test -run TestHarnessSmoke -v ./...
```

This is a **separate Go module**. `go test ./...` at the repository root does not
descend into it, so these tests never slow down the ordinary unit suite — and
testcontainers' ~89 transitive dependencies (moby, containerd, gopsutil) stay
out of the production module's graph and out of `govulncheck`'s scope.

Tests skip with an explanatory message when no container runtime is reachable.

## What is covered

| Test | Guarantee |
| --- | --- |
| `TestHarnessSmoke` | The fixture itself: SigV4 survives the proxy, and real SDK errors for 504/502/500/503/403 land in the intended `storage.ErrClass`. |
| `TestSyncCompletesDespiteTransient504` | Regression for the production incident: a 504 burst during discovery is absorbed and every object still syncs. |
| `TestWatchSurvivesSustainedOutage` | Watch mode does not exit while an endpoint is hard down, and syncs once it recovers. |
| `TestWatchTerminatesOnPermanentError` | Misconfiguration (403) still surfaces instead of retrying forever. |
| `TestRateLimitDegradesAndRecovers` | Congestion control halves a configured limit, recovers additively, and never degrades an unlimited endpoint. |
| `TestDestinationDegradesIndependently` | A sick destination throttles writes without slowing source reads. |
| `TestL4FaultsAreTransient` | A TCP-level connection reset classifies as transient. |

## Why two fault injectors

**Toxiproxy is L4 only.** Its toxics — `latency`, `down`, `bandwidth`,
`slow_close`, `timeout`, `reset_peer`, `slicer`, `limit_data` — operate on the
TCP byte stream. It has no HTTP parsing, so **it cannot emit a 504, 503 or 500**.

The incident these tests exist to reproduce was an HTTP `504 GatewayTimeout` from
a gateway in front of MinIO. Reproducing that needs an L7 injector, so
`faultproxy_test.go` provides an in-process `httputil.ReverseProxy` that returns
a chosen status on demand. It needs no container and is precisely controllable
from the test.

The injector deliberately supports two body shapes, because they exercise
different classification paths:

- **XML body** — the SDK decodes it into a `smithy.APIError`, so classification
  can key on the error code.
- **Non-XML body** (an nginx-style HTML page) — XML decoding fails and the SDK
  surfaces *no* `APIError` at all, leaving only the HTTP status. This is why
  `storage.Classify` must check status before error code.

Toxiproxy still earns its place for the orthogonal L4 faults an L7 proxy cannot
produce, such as a mid-stream connection reset.

## Why assertions use HeadObject

`listPageWithRetry` retries 8 times with jittered backoff capped at 30s, so a
*fully* failing list call takes over two minutes. `HeadObject` has no repo-level
retry wrapper, so a failing call costs only the SDK's own attempts — seconds.
Tests that need a failure to surface therefore use `HeadObject`.

For the same reason, `TestWatchSurvivesSustainedOutage` faults the
**destination**: `discoverAndSyncBucket` calls `EnsureBucket` before it lists, so
a faulted destination fails a cycle in seconds instead of minutes.

## Container runtime support

| Runtime | Status | Notes |
| --- | --- | --- |
| Docker / Docker Desktop | Works | Nothing to configure. |
| **Podman** (macOS/Linux, rootful) | **Works** | Configured automatically — see below. Verified on macOS 26 / Apple Silicon with podman 5.8.3. |
| GitHub Actions `ubuntu-latest` | Works | Docker is preinstalled; no setup needed. |
| Podman rootless | Untested | Ryuk needs a privileged container; prefer a rootful machine. |
| **Apple `container`** (macOS 26) | **Does not work** | No Docker-compatible API — see below. |

### Podman

`TestMain` configures podman automatically, because the obvious setup fails in a
non-obvious way.

testcontainers-go chooses between its Docker and podman providers by looking for
the literal substring `podman.sock` in `DOCKER_HOST`. A podman machine symlinks
`/var/run/docker.sock` to its own socket, so podman looks exactly like Docker:
the Docker provider is selected, and Ryuk is then created on a network named
`bridge`. Podman has no such network and **refuses to create one** — `podman
network create bridge` fails with *"conflicts with a valid network mode"* — so
every test dies with:

```
reaper: new reaper: run container: container create: Error response from daemon:
container create: unable to find network with name or ID bridge: network not found
```

`configureContainerRuntime` fixes this by pointing `DOCKER_HOST` at podman's
stable `podman.sock` symlink, restoring provider detection. Note that
`podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'` is
*not* usable here: it returns a temp path named
`podman-machine-default-api.sock`, which does not contain the `podman.sock`
substring and so does not trigger detection.

It also sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` (Ryuk
is bind-mounted the socket by its in-VM path) and
`TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true`. Note that name — the frequently
cited `TESTCONTAINERS_RYUK_PRIVILEGED` does not exist.

An explicitly set `DOCKER_HOST` is always respected, so CI and unusual local
setups are never overridden. Container cleanup is also registered explicitly via
`t.Cleanup`, so the suite does not depend on Ryuk being available.

### Apple `container` — not usable

Apple's Containerization framework and `container` CLI (macOS 26) expose no
Docker-compatible HTTP API; they use their own CLI/XPC interface with one VM per
container. testcontainers cannot talk to it, and
[apple/container#131](https://github.com/apple/container/issues/131) was closed
without a compatibility layer. Third-party shims such as
[socktainer](https://socktainer.github.io/) exist but describe themselves as
under heavy development and do not support privileged containers. **Use podman.**

## Images

Pinned in `harness_test.go`, all multi-arch (`linux/amd64` + `linux/arm64`) so
the same tags work on an Apple Silicon laptop and an x86 CI runner:

- `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` — the community image
  publishes arm64 only on **plain** `RELEASE.*` tags. The enterprise
  `RELEASE.*.hotfix.*` tags are amd64-only and fail to start on Apple Silicon
  with *"no image found in image index for architecture arm64"*.
- `ghcr.io/shopify/toxiproxy:2.12.0` — use ghcr, not Docker Hub:
  `docker.io/shopify/toxiproxy` is stale at 2.1.4 and amd64-only.
- `redis:7-alpine`
