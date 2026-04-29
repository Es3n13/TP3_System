FROM golang:1.25.7-alpine AS builder

# Outils nécessaires pour CGO
RUN apk add --no-cache build-base

WORKDIR /app

# Dépendances Go
COPY go.mod go.sum ./
RUN go mod download

# Code source
COPY . .

# Build pour Alpine/musl
ENV CGO_ENABLED=1
RUN go build -tags musl -o user-processor-final main.go

# --- Final Stage ---
FROM alpine:3.20

# Runtime librdkafka
RUN apk add --no-cache librdkafka
WORKDIR /root/

COPY --from=builder /app/user-processor-final .

ENTRYPOINT ["./user-processor-final"]