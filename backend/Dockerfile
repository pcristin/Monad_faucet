FROM golang:1.22-alpine AS builder

# Install build dependencies for PostgreSQL
RUN apk add --no-cache gcc musl-dev

# Set working directory
WORKDIR /app

# Copy everything from backend
COPY backend/ ./

# Download dependencies
RUN go mod download

# Build the application with CGO enabled
RUN CGO_ENABLED=1 GOOS=linux go build -a -ldflags="-w -s" -o main ./cmd/faucet

# Use a minimal alpine image for the final stage
FROM alpine:latest

# Add ca-certificates and PostgreSQL client for connections
RUN apk --no-cache add ca-certificates

# Set working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy any config files if needed
COPY --from=builder /app/config ./config

# The PORT environment variable will be provided by Render
ENV PORT=8080

# Mark Render environment to automatically detect we're running on Render
ENV RENDER=true

# Expose the port
EXPOSE $PORT

# Run the binary
CMD ["./main"] 