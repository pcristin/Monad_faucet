# Monad Faucet Load Testing

This directory contains load testing scripts for determining the maximum RPS (Requests Per Second) your service can handle.

## Prerequisites

1. Install k6:
   ```bash
   # macOS
   brew install k6

   # Linux
   sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
   echo "deb https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
   sudo apt-get update
   sudo apt-get install k6

   # Windows
   choco install k6
   ```

## Available Tests

1. **Basic API Test** (`test.js`):
   - Tests a lightweight endpoint to determine raw RPS capability
   - Gradually ramps up from 1 to 200 virtual users

2. **Swap Endpoint Test** (`swap_test.js`):
   - Simulates real-world swap requests
   - Tests actual business logic performance
   - More representative of production load

## Configuration

Before running the tests, update the following in each script:

1. Set `BASE_URL` to your actual Render deployment URL
2. Adjust the test parameters if needed:
   - Duration
   - Number of virtual users
   - Thresholds for success

## Running Tests

### Basic Endpoint Test (Maximum RPS)

```bash
k6 run loadtest/test.js
```

### Swap Endpoint Test (Business Logic Performance)

```bash
k6 run loadtest/swap_test.js
```

## Interpreting Results

After running the tests, k6 will provide detailed metrics:

- **http_reqs**: Total number of requests made
- **http_req_duration**: Response time statistics
- **iterations**: Number of complete test iterations
- **vus**: Number of virtual users
- **vus_max**: Maximum number of virtual users reached

The **RPS** can be calculated as: `http_reqs / total_test_duration_in_seconds`

## Render Free Tier Limitations

Note that on Render's free tier:
- Your service has limited CPU (0.1) and RAM (512MB)
- Services spin down after inactivity
- Cold starts can affect initial test results
- There may be bandwidth limitations

Run tests multiple times for consistent results, and consider the cold start penalty of the first test run.

## Expected Performance

Based on the architecture of your application:
- Simple endpoints (like `/api/info`): 50-100 RPS
- Complex endpoints with DB and blockchain ops (like `/api/swap`): 5-20 RPS

These are estimates - actual performance will vary based on implementation details and Render's specific limitations. 