# Monad Faucet Backend

Backend service for the Monad testnet faucet.

## Prerequisites

- Go 1.21 or later
- Make (optional, for using Makefile commands)

## Setup

1. Clone the repository:
```bash
git clone https://github.com/monad-labs/monad-faucet.git
cd monad-faucet/backend
```

2. Install dependencies:
```bash
go mod download
```

3. Create a `.env` file:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Development

To run the server in development mode:

```bash
go run main.go
```

The server will start on port 8080 by default. You can change this by setting the `PORT` environment variable.

## API Endpoints

- `GET /health` - Health check endpoint
- More endpoints coming soon...

## Environment Variables

- `PORT` - Server port (default: 8080)
- More variables will be added as needed

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request 