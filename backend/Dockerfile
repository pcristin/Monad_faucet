FROM golang:1.22-alpine AS builder

# Install build dependencies for SQLite
RUN apk add --no-cache gcc musl-dev sqlite-dev

# Set working directory
WORKDIR /app

# Copy everything from backend
COPY backend/ ./

# Download dependencies
RUN go mod download

# Build the application with CGO enabled for SQLite support
RUN CGO_ENABLED=1 GOOS=linux go build -a -ldflags="-w -s" -o main ./cmd/faucet

# Use a minimal alpine image for the final stage
FROM alpine:latest

# Add ca-certificates for HTTPS requests and SQLite dependencies
RUN apk --no-cache add ca-certificates sqlite

# Set working directory
WORKDIR /app

# Create data directory for SQLite database
RUN mkdir -p /app/data /var/data /tmp

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy any config files if needed
COPY --from=builder /app/config ./config

# The PORT environment variable will be provided by Heroku/Render
ENV PORT=8080
# Set the database directory
ENV DATA_DIR=/app/data

# Mark Render environment to automatically detect we're running on Render
ENV RENDER=true

# Expose the port
EXPOSE $PORT

# Run the binary
CMD ["./main"] 