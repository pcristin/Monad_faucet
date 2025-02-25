import http from 'k6/http';
import { sleep, check } from 'k6';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';

// Configuration
const BASE_URL = 'https://YOUR-RENDER-URL.onrender.com'; // Replace with your actual URL
const SWAP_ENDPOINT = '/api/swap';

// Generate random Ethereum addresses for testing
function randomEthAddress() {
  let addr = '0x';
  for (let i = 0; i < 40; i++) {
    addr += randomItem('0123456789abcdef');
  }
  return addr;
}

// Test scenarios - more conservative than basic test
export const options = {
  scenarios: {
    constant_load: {
      executor: 'constant-arrival-rate',
      rate: 5,               // 5 requests per second
      timeUnit: '1s',        // 1 second
      duration: '1m',        // Run for 1 minute
      preAllocatedVUs: 10,   // Initial pool of VUs
      maxVUs: 50,            // Maximum VUs if needed
    }
  },
  thresholds: {
    http_req_failed: ['rate<0.1'],      // Allow 10% failure rate for this test
    http_req_duration: ['p(95)<3000'],  // 95% under 3s (blockchain ops can be slow)
  },
};

// Generate a pool of test wallets to avoid rate limiting per-wallet
const TEST_WALLETS = Array(20).fill().map(() => randomEthAddress());

export default function () {
  // Randomly select a wallet from our pool
  const testWallet = randomItem(TEST_WALLETS);
  
  // Prepare swap request payload
  const payload = JSON.stringify({
    wallet_address: testWallet,
    amount: "10000000000000000", // 0.01 ETH in wei
    request_id: uuidv4(),        // Unique request ID
    target_currency: "MON"       // Swap to MON
  });
  
  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };
  
  // Make the swap request
  const response = http.post(
    `${BASE_URL}${SWAP_ENDPOINT}`,
    payload,
    params
  );
  
  check(response, {
    'swap response is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'no server errors': (r) => r.status < 500,
  });
  
  // Add reasonable delay between requests
  sleep(1);
} 