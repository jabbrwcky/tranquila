FROM golang:1.27.0@sha256:65b6f280bf050ec5af12716857e8ea8439d694dbba8f31ceeb7630670071f2bb AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tranquila .

FROM gcr.io/distroless/static-debian13@sha256:9197324ba51d9cd071af8505989365c006adf9d6d2067eada25aef00abbb5278
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
