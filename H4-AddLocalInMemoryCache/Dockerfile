FROM golang:1.25.7-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o user-processor-h2 main.go

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/
COPY --from=builder /app/user-processor-h2 .

CMD ["./user-processor-h2"]