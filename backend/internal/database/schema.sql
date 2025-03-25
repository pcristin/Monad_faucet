-- Table for managing distributed locks to prevent race conditions
CREATE TABLE IF NOT EXISTS processing_locks (
    deposit_id VARCHAR(100) PRIMARY KEY,
    instance_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    CONSTRAINT deposit_id_unique UNIQUE (deposit_id)
);

-- Index for quick lookups and expirations
CREATE INDEX IF NOT EXISTS idx_processing_locks_expires_at ON processing_locks(expires_at);

-- Table for storing deposit transactions from Arbitrum
CREATE TABLE IF NOT EXISTS deposits (
    id SERIAL PRIMARY KEY,
    deposit_id VARCHAR(100) NOT NULL,
    wallet_address VARCHAR(42) NOT NULL,
    amount VARCHAR(78) NOT NULL,
    currency INTEGER NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    metadata TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT deposit_id_unique_deposits UNIQUE (deposit_id)
);

-- Table for storing distribution transactions on Monad
CREATE TABLE IF NOT EXISTS distributions (
    id SERIAL PRIMARY KEY,
    deposit_id VARCHAR(100) NOT NULL,
    wallet_address VARCHAR(42) NOT NULL,
    mon_amount VARCHAR(78) NOT NULL,
    status VARCHAR(20) NOT NULL,
    monad_tx_hash VARCHAR(66),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT deposit_id_unique_distributions UNIQUE (deposit_id),
    CONSTRAINT fk_deposit FOREIGN KEY (deposit_id) REFERENCES deposits(deposit_id)
);

-- Transaction history table for backward compatibility
CREATE TABLE IF NOT EXISTS transaction_history (
    id SERIAL PRIMARY KEY,
    deposit_id VARCHAR(100) NOT NULL,
    wallet_address VARCHAR(42) NOT NULL,
    amount VARCHAR(78) NOT NULL,
    currency INTEGER NOT NULL,
    mon_amount VARCHAR(78),
    status VARCHAR(20) NOT NULL,
    tx_hash VARCHAR(66),
    monad_tx_hash VARCHAR(66),
    metadata TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_deposits_wallet_address ON deposits(wallet_address);
CREATE INDEX IF NOT EXISTS idx_deposits_tx_hash ON deposits(tx_hash);
CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status);
CREATE INDEX IF NOT EXISTS idx_distributions_wallet_address ON distributions(wallet_address);
CREATE INDEX IF NOT EXISTS idx_distributions_status ON distributions(status);
CREATE INDEX IF NOT EXISTS idx_distributions_monad_tx_hash ON distributions(monad_tx_hash);

-- Table for storing Arbitrum transaction timestamps
CREATE TABLE IF NOT EXISTS arbitrum_tx_timestamps (
    tx_hash VARCHAR(66) PRIMARY KEY,
    deposit_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index on deposit_id for tx timestamp lookups
CREATE INDEX IF NOT EXISTS idx_arbitrum_tx_timestamps_deposit_id ON arbitrum_tx_timestamps(deposit_id); 