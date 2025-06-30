# Monad Faucet

A high-performance cross-chain bridge service that facilitates seamless token swaps between Layer 2 networks (Arbitrum, Base, Optimism) and the Monad network. Features dynamic pricing via Chainlink oracles, robust transaction tracking, and enterprise-grade reliability.

**Frontend UI**: [https://bridge.mwdao.xyz](https://bridge.mwdao.xyz)

Smart contracts and frontend developed by [@smeshny](https://github.com/smeshny)

## Badges

[![CI](https://github.com/pcristin/monad-faucet/actions/workflows/ci.yml/badge.svg)](https://github.com/pcristin/monad-faucet/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)](https://github.com/pcristin/monad-faucet)
[![Docker](https://img.shields.io/badge/docker-ready-blue?style=flat&logo=docker)](https://hub.docker.com/)
[![Uptime](https://img.shields.io/badge/uptime-99.9%25-brightgreen)](https://api.mwdao.xyz/bridge/health)

## Table of Contents

- [Demo](#demo)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Installation](#installation)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Analytics](#analytics)
- [Smart Contracts](#smart-contracts)
- [Contributing](#contributing)
- [License](#license)

## Demo

**Live Service**: [https://api.mwdao.xyz](https://api.mwdao.xyz)  
**Bridge Interface**: [https://bridge.mwdao.xyz](https://bridge.mwdao.xyz)

```bash
# Check service health
curl https://api.mwdao.xyz/bridge/health

# Get current faucet information
curl https://api.mwdao.xyz/bridge/api/v1/info
```

## Features

- **Multi-Chain Support**: Bridge tokens from Arbitrum, Base, and Optimism to Monad
- **Dynamic Pricing**: Real-time exchange rates using Chainlink ETH/USD price feeds
- **Automated Processing**: Event-driven architecture with robust worker pools
- **Transaction Tracking**: Complete audit trail with PostgreSQL storage
- **Admin Controls**: Secure API endpoints for ratio and limit management
- **High Performance**: Handles 50-100 RPS with sub-second response times
- **Fault Tolerance**: Automatic retries, refunds, and circuit breakers
- **Real-time Monitoring**: Comprehensive logging and health checks
- **Auto-refunds**: Intelligent refund system for failed transactions
- **Load Balancing**: Configurable worker pools for optimal performance
- **REST API**: Clean, documented endpoints for frontend integration
- **Docker Ready**: Full containerization with docker-compose support

## Tech Stack

### Backend
- **Go 1.23+** with Gin framework
- **PostgreSQL** for transaction storage
- **Redis** for caching and rate limiting
- **Docker** for containerization

### Blockchain
- **Ethereum/EVM** compatible chains (Arbitrum, Base, Optimism, Monad)
- **Solidity 0.8.20+** smart contracts
- **OpenZeppelin** security standards
- **Chainlink** price oracles

### Analytics & Monitoring
- **Python 3.9+** with pandas, matplotlib
- **SQLAlchemy** for database analysis
- **Statsmodels** for forecasting
- **Seaborn** for visualization

### DevOps & Testing
- **GitHub Actions** for CI/CD
- **k6** for load testing
- **Render** for cloud deployment
- **Docker Compose** for local development

## Installation

### Prerequisites

- Go 1.23 or newer
- PostgreSQL 15+
- Docker & Docker Compose
- Node.js (for load testing)

### Quick Start with Docker

```bash
# Clone the repository
git clone https://github.com/pcristin/monad-faucet.git
cd monad-faucet

# Copy environment template
cp backend/.env.example .env

# Edit environment variables
nano .env

# Start all services
docker-compose up -d

# Check health
curl http://localhost:8080/bridge/health
```

### Manual Installation

```bash
# 1. Setup PostgreSQL
createdb bridgedb

# 2. Clone and setup backend
cd backend
go mod download

# 3. Configure environment
cp .env.example .env
# Edit .env with your configuration

# 4. Run database migrations
go run cmd/faucet/main.go --migrate

# 5. Start the service
go run cmd/faucet/main.go
```

### Environment Configuration

```bash
# Network Configuration
ARB_RPC_URL=wss://arb-goerli.g.alchemy.com/v2/your-api-key
MONAD_RPC_URL=https://rpc-testnet.monad.xyz/
BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/your-api-key

# Contract Addresses
ARB_DEPOSITOR_ADDRESS=0x487177C3278FAA36dd317DBB4CA97425a4F4Ee31
MONAD_DISTRIBUTOR_ADDRESS=0xc11350Fd29aC48181b0117bd1935dBE781cdd03d

# Security
WALLET_PRIVATE_KEY=your-private-key-here
ADMIN_API_KEY_1=your-admin-key-here

# Database
DATABASE_URL=postgres://user:pass@localhost:5432/bridgedb

# Performance Tuning
DEPOSIT_WORKERS=5
DISTRIBUTION_WORKERS=5
CALCULATION_WORKERS=3
DB_WORKERS=2
```

## Usage

### Starting the Service

```bash
# Development mode
go run cmd/faucet/main.go

# Production mode
./faucet

# With custom config
./faucet --config=/path/to/config.yaml

# Enable debug logging
PRODUCTION=false ./faucet
```

### Basic Operations

```bash
# Check service status
curl http://localhost:8080/bridge/health

# Get faucet information
curl http://localhost:8080/bridge/api/v1/info

# Check transaction status
curl -X POST http://localhost:8080/bridge/api/v1/transaction/status \
  -H "Content-Type: application/json" \
  -d '{"tx_hash": "0x123..."}'

# Admin: Update MON/USD ratio
curl -X POST http://localhost:8080/bridge/api/admin/ratio \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: your-admin-key" \
  -d '{"mon_usd_ratio": "0.15"}'
```

## API Documentation

### Public Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/bridge/health` | Service health check |
| `GET` | `/bridge/api/v1/info` | Faucet info and exchange rates |
| `POST` | `/bridge/api/v1/transaction/status` | Transaction status lookup |

### Admin Endpoints

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| `POST` | `/bridge/api/admin/ratio` | Update MON/USD ratio | ✅ |
| `POST` | `/bridge/api/admin/limits` | Update wallet limits | ✅ |
| `POST` | `/bridge/api/admin/pause` | Pause deposits | ✅ |
| `POST` | `/bridge/api/admin/resume` | Resume deposits | ✅ |

### Response Examples

```json
// GET /bridge/api/v1/info
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

// POST /bridge/api/v1/transaction/status
{
  "status": "success",
  "message": "MON tokens have been distributed to your wallet",
  "txs": {
    "Arbitrum": "0x1234...",
    "Monad": "0xabcd..."
  }
}
```

## Testing

### Unit Tests

```bash
# Run all tests
cd backend && go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test ./internal/bridge
```

### Load Testing

```bash
# Install k6
brew install k6  # macOS
# or sudo apt-get install k6  # Linux

# Basic API performance test
k6 run loadtest/test.js

# Business logic performance test  
k6 run loadtest/swap_test.js

# Targeted endpoint testing
k6 run loadtest/targeted_test.js
```

### Integration Tests

```bash
# Start test environment
docker-compose -f docker-compose.test.yml up -d

# Run integration tests
go test -tags=integration ./tests/...
```

### Performance Benchmarks

```bash
# Run performance regression tests
cd backend/tests/profile
go run main.go

# Expected performance:
# - Simple endpoints: 50-100 RPS
# - Complex operations: 5-20 RPS
# - Memory usage: <512MB
# - Response time: <1s (p95)
```

## CI/CD

### GitHub Actions Workflow

The project uses GitHub Actions for automated testing and deployment:

```yaml
# .github/workflows/main.yml
- Performance regression testing
- Multi-commit comparison
- Automated deployment to Render
- Load testing validation
```

### Deployment Pipeline

1. **Code Push** → Triggers GitHub Actions
2. **Performance Tests** → Validates no regression
3. **Build & Test** → Runs full test suite  
4. **Deploy to Staging** → Render staging environment
5. **Load Testing** → Validates performance under load
6. **Deploy to Production** → Render production environment

### Manual Deployment

```bash
# Deploy to Render
git push origin main

# Deploy with specific tag
git tag v1.2.3
git push origin v1.2.3

# Local production build
docker build -t monad-faucet:latest .
docker run -p 8080:8080 monad-faucet:latest
```

## Analytics

### Data Analysis Tools

```bash
# Setup analytics environment
cd analytics
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt

# Generate deposit analysis
python plot_deposits.py

# Process failed transactions
python process_failed_deposits_in_db.py

# Extract failed deposits
python extract_failed_deposits.py
```

### Available Analytics

- **Deposit Trends**: Volume and frequency analysis
- **Failed Transaction Analysis**: Root cause investigation  
- **Performance Metrics**: Response time and throughput trends
- **Revenue Analytics**: Fee collection and distribution patterns

## Smart Contracts

### Deployed Contracts

| Network | Contract | Address |
|---------|----------|---------|
| Arbitrum | Bridge Deposit | `0x487177C3278FAA36dd317DBB4CA97425a4F4Ee31` |
| Monad | Fund Distributor | `0xc11350Fd29aC48181b0117bd1935dBE781cdd03d` |
| Arbitrum | Chainlink ETH/USD | `0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612` |

### Contract Features

- **MIT Licensed** OpenZeppelin-based contracts
- **Reentrancy Protection** via ReentrancyGuard
- **Access Control** with Ownable pattern
- **Atomic Operations** for multi-recipient distributions
- **Event Logging** for comprehensive audit trails

## Contributing

### Development Setup

```bash
# 1. Fork the repository on GitHub
# 2. Clone your fork
git clone https://github.com/YOUR_USERNAME/monad-faucet.git
cd monad-faucet

# 3. Set up development environment
make setup-dev  # or follow manual installation

# 4. Create feature branch
git checkout -b feature/amazing-feature

# 5. Make changes and test
go test ./...
k6 run loadtest/test.js

# 6. Commit and push
git commit -m "Add amazing feature"
git push origin feature/amazing-feature

# 7. Create Pull Request
```

### Code Style

- Follow Go conventions and `gofmt` formatting
- Use meaningful variable and function names
- Add comments for complex business logic
- Write tests for new features
- Update documentation as needed

### Pull Request Process

1. Ensure all tests pass locally
2. Add tests for new functionality
3. Update documentation if needed
4. Ensure performance regression tests pass
5. Request review from maintainers

### Reporting Issues

Please use the GitHub Issues template and include:
- Environment details (OS, Go version, etc.)
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### Third-Party Licenses

- **OpenZeppelin Contracts**: MIT License
- **Gin Framework**: MIT License  
- **PostgreSQL Driver**: MIT License
- **Ethereum Go Client**: LGPL-3.0 License

---

**Maintained by**: [pcristin](https://github.com/pcristin)  
**Live Service**: [https://api.mwdao.xyz](https://api.mwdao.xyz)  
**Documentation**: [GitHub Wiki](https://github.com/pcristin/monad-faucet/wiki)  

*Built for the Monad ecosystem* 