FROM golang:1.26.5@sha256:0f70d7d828acd8456022127f31975364e58d792999a7e92af6fc972e124bb6b0 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tranquila .

FROM gcr.io/distroless/static-debian13@sha256:9197324ba51d9cd071af8505989365c006adf9d6d2067eada25aef00abbb5278
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
