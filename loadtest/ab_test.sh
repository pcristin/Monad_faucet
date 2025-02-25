#!/bin/bash
# Simple Apache Bench load testing script for Monad Faucet

# Configuration
URL="https://monad-faucet-f5cx.onrender.com/api/info"
CONCURRENCY=20
REQUESTS=50

# Check if ab is installed
if ! command -v ab &> /dev/null; then
    echo "Apache Bench (ab) is not installed. Please install it:"
    echo "  - macOS: brew install httpd (comes with ab)"
    echo "  - Linux: apt-get install apache2-utils"
    exit 1
fi

echo "Starting load test for $URL"
echo "Concurrency: $CONCURRENCY, Total Requests: $REQUESTS"
echo "----------------------------------------------------------------"

# Run the test
ab -n $REQUESTS -c $CONCURRENCY -k $URL

echo "----------------------------------------------------------------"
echo "Note: To test a POST endpoint, create a JSON file with the payload and use:"
echo "ab -n 100 -c 10 -p payload.json -T 'application/json' https://YOUR-URL.onrender.com/api/endpoint" 