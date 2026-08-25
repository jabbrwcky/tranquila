FROM golang:1.27.0@sha256:f42f8545265b7fe4124ecdd50a7778c15d5e3fc4d0af648e508e4f4c6a4c572b AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tranquila .

FROM gcr.io/distroless/static-debian13@sha256:f2ea2709ac8db56323cbd7d014277f32cb572d9ea124b0076f7aafe5980678fe
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
