import http from 'k6/http';
import { sleep, check, group } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';

// Custom metrics
const infoLatency = new Trend('info_endpoint_latency');
const healthLatency = new Trend('health_endpoint_latency');
const swapFailRate = new Rate('swap_failure_rate');
const cacheHits = new Counter('cache_hits');

// Configuration
const BASE_URL = 'https://monad-faucet-f5cx.onrender.com';
const ENDPOINTS = {
  HEALTH: '/health',
  INFO: '/api/info',
  TX_STATUS: '/api/tx-status'
};

// Test scenarios - more controlled and targeted
export const options = {
  scenarios: {
    // First scenario - test the info endpoint (likely has blockchain queries)
    info_endpoint: {
      executor: 'constant-arrival-rate',
      rate: 5,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 5,
      maxVUs: 20,
      exec: 'testInfoEndpoint',
    },
    // Second scenario - test the health endpoint (should be lightweight)
    health_endpoint: {
      executor: 'constant-arrival-rate',
      rate: 10,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 5,
      maxVUs: 20,
      exec: 'testHealthEndpoint',
      startTime: '35s',
    },
    // Third scenario - test the swap endpoint (most complex)
    swap_endpoint: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      stages: [
        { duration: '20s', target: 1 },
        { duration: '20s', target: 2 },
        { duration: '20s', target: 3 },
      ],
      preAllocatedVUs: 5,
      maxVUs: 10,
      exec: 'testSwapEndpoint',
      startTime: '70s',
    }
  },
  thresholds: {
    'info_endpoint_latency': ['p(95)<1000'], // Info endpoint should respond in under 1s
    'health_endpoint_latency': ['p(95)<200'], // Health endpoint should be very fast
    'swap_failure_rate': ['rate<0.1'], // Accept up to 10% swap failures
    'http_req_duration': ['p(95)<3000'], // All requests should complete in under 3s
  },
};

// Generate random Ethereum addresses for testing
function randomEthAddress() {
  let addr = '0x';
  for (let i = 0; i < 40; i++) {
    addr += randomItem('0123456789abcdef');
  }
  return addr;
}

// Generate a pool of test wallets
const TEST_WALLETS = Array(20).fill().map(() => randomEthAddress());

// Test the info endpoint
export function testInfoEndpoint() {
  group('Info Endpoint', () => {
    const response = http.get(`${BASE_URL}${ENDPOINTS.INFO}`);
    
    // Record custom metrics
    infoLatency.add(response.timings.duration);
    
    // Check if response indicates a cache hit (this is a heuristic)
    if (response.timings.duration < 100) {
      cacheHits.add(1);
    }
    
    check(response, {
      'info status is 200': (r) => r.status === 200,
      'info contains exchange rates': (r) => JSON.parse(r.body).exchangeRate !== undefined,
    });
  });
  
  sleep(1);
}

// Test the health endpoint (should be lightweight)
export function testHealthEndpoint() {
  group('Health Endpoint', () => {
    const response = http.get(`${BASE_URL}${ENDPOINTS.HEALTH}`);
    
    // Record custom metrics
    healthLatency.add(response.timings.duration);
    
    check(response, {
      'health status is 200': (r) => r.status === 200,
      'health response is OK': (r) => r.body.includes('OK'),
    });
  });
  
  sleep(0.5);
}

// Test the swap endpoint (complex business logic)
export function testSwapEndpoint() {
  group('Swap Endpoint', () => {
    // Randomly select a wallet from our pool
    const testWallet = randomItem(TEST_WALLETS);
    
    // Prepare swap request payload
    const payload = JSON.stringify({
      wallet_address: testWallet,
      amount: "1000000000000000", // 0.001 ETH in wei
      request_id: uuidv4(),
      target_currency: "MON"
    });
    
    const params = {
      headers: {
        'Content-Type': 'application/json',
      },
    };
    
    // Make the swap request
    const response = http.post(
      `${BASE_URL}${ENDPOINTS.SWAP}`,
      payload,
      params
    );
    
    // Log error responses for debugging
    if (response.status !== 200) {
      console.log(`Swap error: ${response.status} - ${response.body}`);
      swapFailRate.add(1);
    } else {
      swapFailRate.add(0);
      
      // If successful, also test the transaction status endpoint
      const txResponse = JSON.parse(response.body);
      if (txResponse.request_id) {
        const statusPayload = JSON.stringify({
          request_id: txResponse.request_id
        });
        
        // Wait a short time before checking status
        sleep(1);
        
        http.post(
          `${BASE_URL}${ENDPOINTS.TX_STATUS}`,
          statusPayload,
          params
        );
      }
    }
    
    check(response, {
      'swap response is 200 or 429': (r) => r.status === 200 || r.status === 429,
      'no server errors': (r) => r.status < 500,
    });
  });
  
  // Add reasonable delay between swap requests
  sleep(3);
}