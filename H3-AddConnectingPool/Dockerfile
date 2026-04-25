FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git gcc g++ musl-dev pkgconf

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -o user-processor-h3 .

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/
COPY --from=builder /app/user-processor-h3 .

USER 1000
ENTRYPOINT ["./user-processor-h3"]