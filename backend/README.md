# Monad Faucet Backend

This is the backend service for the Monad Faucet, which facilitates token swaps between Arbitrum and Monad networks.

## Features

- Listens to deposit events on Arbitrum network
- Validates deposits and checks contract states
- Dynamic swap ratio calculation:
  - ETH/MON: Based on Chainlink ETH/USD price feed and current MON/USD ratio
  - USDC/MON and USDT/MON: Based on current MON/USD ratio (accounts for 6 decimal places in stablecoins)
- Mints MON tokens on the Monad network
- Provides REST API for frontend integration
- Handles automated refunds for failed transactions
- Admin API for dynamic MON/USD ratio updates
- Wallet-based distribution limits to prevent abuse (configurable percentage of total MON balance per transaction)
- Robust transaction status tracking with database storage
- Production-ready logging with configurable verbosity

## Prerequisites

- Go 1.22 or newer
- PostgreSQL database (for transaction tracking)
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
│   ├── blockchain/      # Blockchain interaction code
│   ├── bridge/          # Bridge service and logic
│   ├── database/        # Database operations
│   └── core/            # Core interfaces and types
├── pkg/                 # Public packages
│   └── logger/          # Structured logging
├── scripts/             # Utility scripts
├── utils/               # Helper utilities
├── .env.example         # Example environment variables
├── go.mod               # Go module definition
└── README.md            # Project documentation
```

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in the required values:
   ```env
   # HTTP server port
   PORT=8080
   
   # Arbitrum network configuration
   ARB_RPC_URL=wss://arb-goerli.g.alchemy.com/v2/your-api-key
   ARB_DEPOSITOR_ADDRESS=0x487177c3278faa36dd317dbb4ca97425a4f4ee31
   CHAINLINK_ETH_USD_FEED=0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612
   
   # Monad network configuration
   MONAD_RPC_URL=https://rpc-testnet.monad.xyz/
   MONAD_DISTRIBUTOR_ADDRESS=0xc11350Fd29aC48181b0117bd1935dBE781cdd03d
   
   # Wallet configuration
   WALLET_PRIVATE_KEY=your-private-key-here
   
   # Admin configuration
   ADMIN_API_KEY_1=your-first-admin-key-here
   ADMIN_API_KEY_2=your-second-admin-key-here
   
   # Database configuration
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=postgres
   DB_NAME=bridgedb
   DB_SSLMODE=disable
   
   # Worker pool configuration
   DEPOSIT_WORKERS=5
   CALCULATION_WORKERS=3
   DISTRIBUTION_WORKERS=5
   DB_WORKERS=2
   
   # Optional: Set to "true" for production mode logging
   PRODUCTION=false
   ```

3. Set up the database:
   ```bash
   # Using Docker
   docker run --name postgres-bridge -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=bridgedb -p 5432:5432 -d postgres
   
   # Or use existing PostgreSQL instance and create the database
   createdb bridgedb
   ```

4. Install dependencies:
   ```bash
   go mod download
   ```

5. Build and run the service:
   ```bash
   go build -o faucet ./cmd/faucet
   ./faucet
   ```

   Or for development:
   ```bash
   go run cmd/faucet/main.go
   ```

## API Endpoints

### GET /bridge/health
Health check endpoint to verify the service is running.

Response:
```json
{
  "status": "ok",
  "timestamp": "2025-03-04T01:02:43Z"
}
```

### GET /bridge/api/info
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

### GET /bridge/api/v1/info
Updated version of the faucet info endpoint with the same response format.

### POST /bridge/api/tx-status
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
    "Arbitrum": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    "Monad": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
  }
}
```

### POST /bridge/api/v1/transaction/status
Updated version of the transaction status endpoint with the same request/response format.

Possible status values:
- `success`: Transaction was successful, MON tokens were distributed
- `pending`: Transaction is still being processed
- `error`: Transaction execution reverted
- `refunded`: Deposit was successful, but MON couldn't be distributed and was refunded
- `not_found`: Transaction was not found in our system

### POST /bridge/api/admin/ratio
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

### POST /bridge/api/admin/pause
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

### POST /bridge/api/admin/resume
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

### POST /bridge/api/admin/wallet-limit
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

### GET /bridge/metrics
Returns system metrics for monitoring (requires admin authentication).

Headers:
```
X-Admin-Key: your-admin-key-here
```

Response:
```json
{
  "uptime": "3h27m15s",
  "requests": 1527,
  "depositEvents": 142,
  "distributionEvents": 138,
  "pendingDistributions": 4,
  "failedDistributions": 0,
  "monBalance": "3.429538"
}
```

## Database Schema

The service uses a PostgreSQL database with the following tables:

1. **deposits** - Tracks deposit events from Arbitrum
   - `deposit_id`: Unique identifier for the deposit
   - `wallet_address`: Address that made the deposit
   - `amount`: Amount deposited
   - `currency`: Currency type (0=ETH, 1=USDC, 2=USDT)
   - `tx_hash`: Transaction hash from Arbitrum
   - `block_number`: Block number of the transaction
   - `status`: Current status (pending, processed, failed, refunded)

2. **distributions** - Tracks MON token distributions on Monad
   - `deposit_id`: Linked to deposit ID
   - `wallet_address`: Recipient address
   - `mon_amount`: Amount of MON tokens distributed
   - `status`: Current status (pending, completed, failed)
   - `monad_tx_hash`: Transaction hash on Monad

3. **transaction_history** - Legacy table for backward compatibility
   - Combines deposit and distribution information

4. **processing_locks** - Prevents duplicate processing of deposits
   - `deposit_id`: Deposit being processed
   - `instance_id`: Instance processing the deposit
   - `expires_at`: When the lock expires

## Transaction Processing Flow

The bridge service processes transactions through a multi-stage pipeline:

1. **Event Listener**:
   - Monitors Arbitrum blockchain for deposit events
   - Validates and queues events for processing

2. **Deposit Processing**:
   - Verifies deposit details and status
   - Creates database records for tracking
   - Calculates MON amount based on current exchange rates

3. **Distribution**:
   - Sends MON tokens to the recipient wallet on Monad
   - Handles transaction gas estimation and management
   - Manages nonce for transaction ordering

4. **Status Tracking**:
   - Updates transaction status in the database
   - Makes status available via API for frontend integration
   - Handles error cases and recovery

5. **Error Recovery**:
   - Automatically refunds failed distributions
   - Retries transactions with appropriate backoff

## Production Deployment

### Docker

The service can be deployed using Docker:

```bash
docker build -t monad-faucet-backend -f backend/Dockerfile .
docker run -p 8080:8080 --env-file .env monad-faucet-backend
```

### Render

The service is configured for deployment on Render:

1. Connect your GitHub repository to Render
2. Create a new Web Service with:
   - Environment: Docker
   - Dockerfile Path: backend/Dockerfile
   - Add your environment variables in the Render dashboard

## Logging

The service uses structured logging with different verbosity levels:

- **Production Mode**: Enable by setting `PRODUCTION=true` or when deployed on Render
  - INFO: Important business events only
  - ERROR/WARN: All error conditions
  - DEBUG: Suppressed in production

- **Development Mode**: Default when `PRODUCTION` is not set
  - Verbose logging of all operations
  - Detailed calculation information
  - Transaction processing steps

## Monitoring

The service exposes metrics for monitoring via the `/bridge/metrics` endpoint.

Key metrics include:
- System uptime
- Request count
- Distribution success rate
- Pending transactions
- Current MON balance

## Security Considerations

1. **Admin Access**:
   - Two separate admin keys for redundancy and security
   - API key authentication required for admin operations
   - Keys should be long, random strings (32+ characters)

2. **Transaction Safety**:
   - Gas price estimation with safety buffer
   - Nonce management for transaction ordering
   - Receipt verification for all transactions
   - Automatic refunds for failed operations

3. **Database Security**:
   - Connection pooling with configurable limits
   - Parameterized queries to prevent SQL injection
   - Transaction-level consistency for critical operations

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

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 