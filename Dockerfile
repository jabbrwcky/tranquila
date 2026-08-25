FROM golang:1.27.0@sha256:713ecb45d77c39af241e08371ada5bb5643e1e9806994e955ac90cbf17fa5ffd AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tranquila .

FROM gcr.io/distroless/static-debian13@sha256:9197324ba51d9cd071af8505989365c006adf9d6d2067eada25aef00abbb5278
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
