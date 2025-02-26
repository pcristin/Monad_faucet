# Monad Faucet Backend

This is the backend service for the Monad Faucet, which facilitates token swaps between Arbitrum and Monad networks.

## Features

- Listens to deposit events on Arbitrum network
- Validates deposits and checks contract states
- Dynamic swap ratio calculation:
  - ETH/MON: Based on Chainlink ETH/USD price feed and current MON/USD ratio
  - USDC/MON and USDT/MON: Based on current MON/USD ratio
- Mints MON tokens on the Monad network
- Provides REST API for frontend integration
- Handles automated refunds for failed transactions
- Admin API for dynamic MON/USD ratio updates
- Wallet-based distribution limits to prevent abuse (configurable percentage of total MON balance per transaction)

## Prerequisites

- Go 1.22 or newer
- Access to Arbitrum and Monad networks
- API keys for Arbitrum RPC (if using Alchemy or Infura)

## Project Structure

```
backend/
├── cmd/
│   └── faucet/          # Main application entry point
├── config/              # Configuration management
├── internal/            # Private application code
│   ├── api/             # HTTP API handlers
│   └── blockchain/      # Blockchain interaction code
├── pkg/                 # Public packages
│   └── logger/          # Structured logging
├── .env.example         # Example environment variables
├── go.mod               # Go module definition
└── README.md            # Project documentation
```

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in the required values:
   ```env
   PORT=8080
   ARB_RPC_URL=your-arbitrum-rpc-url
   ARB_DEPOSITOR_ADDRESS=your-arbitrum-contract-address
   MONAD_RPC_URL=your-monad-rpc-url
   MONAD_DISTRIBUTOR_ADDRESS=your-monad-contract-address
   WALLET_PRIVATE_KEY=your-private-key-here
   ADMIN_API_KEY_1=your-first-admin-key-here
   ADMIN_API_KEY_2=your-second-admin-key-here
   ```
3. Install dependencies:
   ```bash
   go mod download
   ```
4. Build and run the service:
   ```bash
   go build -o faucet ./cmd/faucet
   ./faucet
   ```

   Or for development:
   ```bash
   go run cmd/faucet/main.go
   ```

## API Endpoints

### GET /api/state
Returns the current state of the bridge.

Response:
```json
{
  "is_paused": false,
  "min_amount": "1000000000000000",
  "mon_balance": "1000000000000000000000",
  "swap_ratios": {
    "ETH": "300000000000000000000000",
    "USDC": "10000000000000000000",
    "USDT": "10000000000000000000"
  }
}
```

### GET /api/info
Returns simplified faucet information.

Response:
```json
{
  "faucetWorking": true,
  "faucetReserve": "3.429538",
  "exchangeRate": {
    "ETH": "0.000418",
    "USDC": "1.000000",
    "USDT": "1.000000"
  },
  "walletLimit": "1.371815",
  "limitType": "per transaction"
}
```

### POST /api/estimate
Estimates the amount of MON tokens to be received.

Request:
```json
{
  "amount": "1000000000000000000",
  "currency": "ETH"
}
```

Response:
```json
{
  "input_amount": "1000000000000000000",
  "input_currency": "ETH",
  "mon_amount": "300000000000000000000000",
  "swap_ratio": "300000000000000000000000"
}
```

### POST /api/admin/ratio
Updates the MON/USD ratio (requires admin authentication).

Request:
```json
{
  "mon_usd_ratio": "0.1"
}
```
Headers:
```
X-Admin-Key: your-admin-key-here
```

Response:
```json
{
  "message": "MON/USD ratio updated successfully",
  "new_ratio": "0.1"
}
```

### POST /api/admin/pause
Pauses deposit functionality on the Arbitrum contract (requires admin authentication).

Headers:
```
X-Admin-Key: your-admin-key-here
```

Response:
```json
{
  "message": "Deposits paused successfully"
}
```

### POST /api/admin/resume
Resumes deposit functionality on the Arbitrum contract (requires admin authentication).

Headers:
```
X-Admin-Key: your-admin-key-here
```

Response:
```json
{
  "message": "Deposits resumed successfully"
}
```

### POST /api/admin/wallet-limit
Updates the wallet limit percentage (requires admin authentication).

Request:
```json
{
  "limit_percentage": 40
}
```
Headers:
```
X-Admin-Key: your-admin-key-here
```

Response:
```json
{
  "message": "Wallet limit percentage updated successfully",
  "limit_percentage": 40
}
```

> Note: Setting `limit_percentage` to 0 disables the wallet limit entirely. The limit is applied per transaction rather than over a time period.

### POST /api/tx-status
### POST /api/v1/transaction/status
Retrieves the status of a transaction by its Arbitrum transaction hash. Only transactions processed by our system will be found.

Request:
```json
{
  "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
}
```

Response:
```json
{
  "status": "success",
  "message": "MON tokens have been distributed to your wallet",
  "txs": {
    "Arbitrum": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
  }
}
```

Possible status values:
- `success`: Transaction was successful, MON tokens were distributed
- `pending`: Transaction is still being processed
- `error`: Transaction execution reverted
- `refunded`: Deposit was successful, but MON couldn't be distributed and was refunded
- `not_found`: Transaction was not found in our system

## Transaction Processing

The faucet processes transactions in several steps:

1. User initiates a deposit on Arbitrum network
2. Backend detects the deposit event
3. System validates the deposit and waits for confirmations
4. MON tokens are minted on Monad network
5. Transaction status is updated in the database

The transaction status endpoint allows users to check the current status of their transaction. Only transactions initiated through our system will be found in the transaction status endpoint.

## Price Calculation

The service calculates token swap ratios based on:

1. MON/USD ratio (configurable via admin API)
   - Example: If 1 MON = $0.1, then 1 USDC/USDT = 10 MON

2. ETH price from Chainlink oracle
   - Example: If ETH = $3000 and 1 MON = $0.1, then 1 ETH = 30000 MON

## Architecture

The backend consists of several key components:

1. **Event Listener**: Monitors the Arbitrum network for deposit events
2. **Bridge Service**: Handles the business logic for processing deposits and refunds
3. **Contract Interfaces**: Interacts with smart contracts on both networks
4. **API Layer**: Provides HTTP endpoints for frontend integration
5. **Price Oracle**: Integrates with Chainlink for ETH/USD price feeds
6. **Admin System**: Manages MON/USD ratio updates securely
7. **Configuration**: Centralized configuration management
8. **Logging**: Structured logging with different levels (INFO, WARN, ERROR)

## Error Handling

The service implements robust error handling:

- Failed deposits are automatically queued for refund
- Network issues trigger automatic reconnection
- Invalid requests receive appropriate error responses
- All errors are logged for monitoring
- Price feed failures fallback to last known good price

## Logging

The service uses structured logging with different levels:

- INFO: Normal operation events
- WARN: Potential issues that don't affect operation
- ERROR: Critical issues that require attention
- FATAL: Issues that cause the service to terminate

Important events that are logged include:
- Deposit events
- Processing status
- Refund operations
- Contract state changes
- Network connectivity issues
- Price ratio updates
- Admin operations

## Security

1. **Admin Access**:
   - Two separate admin keys for redundancy and security
   - API key authentication required for ratio updates
   - Keys stored in environment variables
   - Recommended to use long, random strings (32+ characters)

2. **Transaction Safety**:
   - Gas price estimation with safety buffer
   - Nonce management for transaction ordering
   - Receipt verification for all transactions
   - Automatic refunds for failed operations

## Development

To run the service in development mode:

```bash
go run cmd/faucet/main.go
```

For testing:

```bash
go test ./...
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Wallet Limits

The service implements wallet-based distribution limits to prevent abuse:

1. **Limit Calculation**:
   - Each wallet is limited to a configurable percentage of the total MON balance per transaction
   - Default: 30% of total MON balance
   - Configurable via admin API (0-100%, where 0 means no limit)

2. **Per-Transaction Basis**:
   - Limits are applied on a per-transaction basis
   - No time-based tracking of wallet usage
   - Each transaction is evaluated independently

3. **Validation**:
   - Deposits are validated against wallet limits before processing
   - If a wallet exceeds its limit, the deposit is rejected and refunded
   - Detailed error messages indicate the maximum allowed amount

4. **Admin Control**:
   - Administrators can adjust the limit percentage via API
   - Setting the limit to 0 disables the limit entirely
   - Useful for promotional periods or adjusting to demand

5. **Transparency**:
   - Current wallet limit is included in the `/api/info` endpoint
   - Frontend can display this information to users
   - When limits are disabled, the API returns "No limit" for the wallet limit 