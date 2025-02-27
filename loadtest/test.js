import http from 'k6/http';
import { sleep, check } from 'k6';

// Configuration
const BASE_URL = 'http://localhost:8080'; // Replace with your actual URL
const API_ENDPOINT = '/api/info'; // Use a lightweight endpoint for maximum RPS testing

// Test scenarios
export const options = {
  // Test 1: Ramp-up test to find breaking point
  scenarios: {
    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 10 },  // Start with 10 concurrent users
        { duration: '30s', target: 50 },  // Increase to 50
        { duration: '30s', target: 100 }, // Push to 100
        { duration: '30s', target: 200 }, // Max test at 200
        { duration: '30s', target: 0 },   // Scale back down
      ],
    },
  },
  thresholds: {
    // We expect 95% of requests to complete successfully
    'http_req_failed': ['rate<0.05'],
    // 95% of requests should be under 500ms
    'http_req_duration': ['p(95)<500'],
  },
};

// Main test function
export default function () {
  const response = http.get(`${BASE_URL}${API_ENDPOINT}`);
  
  // Verify the response was successful
  check(response, {
    'status is 200': (r) => r.status === 200,
    'response is valid': (r) => r.body.length > 0,
  });
  
  // Brief pause between requests (remove for absolute max RPS)
  sleep(0.1);
}

// For testing specific endpoints that require authentication:
/*
export function testProtectedEndpoint() {
  const payload = JSON.stringify({
    // Add request payload if needed
  });
  
  const params = {
    headers: {
      'Content-Type': 'application/json',
      // Add auth headers if needed
    },
  };
  
  const response = http.post(
    `${BASE_URL}/api/protected-endpoint`,
    payload,
    params
  );
  
  check(response, {
    'protected endpoint returns 200': (r) => r.status === 200,
  });
}
*/
