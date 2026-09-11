# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

# Building on the native platform and cross-compiling avoids QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.1@sha256:f44f6e88636cfb311f9ebace870ded69d943f227bb3cb27d32ffd84ea18c43ea AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# VERSION reaches main.version, which kong reports through --version.
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
# The build cache is keyed per target so the two platform legs do not evict
# each other; the module cache is shared because it is platform independent.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build,id=go-build-$TARGETOS-$TARGETARCH \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /tranquila .

FROM gcr.io/distroless/static-debian13@sha256:f2ea2709ac8db56323cbd7d014277f32cb572d9ea124b0076f7aafe5980678fe
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
