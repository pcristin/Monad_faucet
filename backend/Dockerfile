FROM golang:1.22-alpine AS builder

# Set working directory
WORKDIR /app

# Check if go.mod and go.sum exist and copy them
COPY . .

# Download dependencies
RUN go mod download

# Build the application with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o main ./cmd/faucet

# Use a minimal alpine image for the final stage
FROM alpine:latest

# Add ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Set working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy any config files if needed
COPY --from=builder /app/config ./config

# The PORT environment variable will be provided by Heroku
ENV PORT=8080

# Expose the port (not strictly needed for Heroku but good for documentation)
EXPOSE $PORT

# Run the binary
CMD ["./main"] 