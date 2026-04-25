# Stage 1: Build
FROM golang:1.25.7-alpine AS builder

# Install git for fetching dependencies
RUN apk add --no-cache git

WORKDIR /app
# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Compile the binary for Linux
RUN CGO_ENABLED=0 GOOS=linux go build -o user-processor main.go

# Stage 2: Final Image
FROM alpine:latest

# Install ca-certificates for secure connections
RUN apk --no-cache add ca-certificates
WORKDIR /root/

# Copy the compiled binary from the builder stage
COPY --from=builder /app/user-processor .

# Run the application
CMD ["./user-processor"]