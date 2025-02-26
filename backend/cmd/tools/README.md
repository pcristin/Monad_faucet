# Transaction Hash Fix Tool

This tool allows you to manually update the transaction hash for a specific deposit in the database.

## Usage

```bash
# Using the Makefile
cd backend
make fix-tx-hash ARGS="-deposit-id=123456789 -tx-hash=0x8fd6f97283173355c670a4e20d3c0206d8a704ff233edc1ef95af4a16fc6585e"

# Or directly
cd backend
go run cmd/tools/fix_tx_hash.go -deposit-id=123456789 -tx-hash=0x8fd6f97283173355c670a4e20d3c0206d8a704ff233edc1ef95af4a16fc6585e
```

## Parameters

- `-deposit-id`: The deposit ID to update (required)
- `-tx-hash`: The new transaction hash to set (required)

## Environment Variables

The tool uses the `DATABASE_URL` environment variable for database connection. Make sure this is set correctly before running the tool.

## Example

```bash
# Set the database URL
export DATABASE_URL="postgres://username:password@localhost:5432/monad_faucet?sslmode=disable"

# Run the tool
make fix-tx-hash ARGS="-deposit-id=123456789 -tx-hash=0x8fd6f97283173355c670a4e20d3c0206d8a704ff233edc1ef95af4a16fc6585e"
```

This will update the transaction hash for deposit ID 123456789 to the specified hash. 