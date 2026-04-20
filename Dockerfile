FROM golang:1-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tranquila .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
