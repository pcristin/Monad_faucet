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
    metadata TEXT NOT NULL DEFAULT '',
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
    refund_tx_hash VARCHAR(66),
    metadata TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT deposit_id_unique_tx UNIQUE (deposit_id)
);

-- Create function for sanitizing mon_amount
CREATE OR REPLACE FUNCTION sanitize_mon_amount() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.mon_amount IS NULL THEN
        NEW.mon_amount := '0';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create function to ensure mon_amount is not null
CREATE OR REPLACE FUNCTION ensure_mon_amount_not_null() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.mon_amount IS NULL THEN
        NEW.mon_amount := '0';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for transaction_history
CREATE TRIGGER transaction_history_sanitize_trigger
BEFORE INSERT OR UPDATE ON transaction_history
FOR EACH ROW EXECUTE FUNCTION sanitize_mon_amount();

CREATE TRIGGER transaction_history_mon_amount_trigger
BEFORE INSERT OR UPDATE ON transaction_history
FOR EACH ROW EXECUTE FUNCTION ensure_mon_amount_not_null();

-- Create a backup table for transaction history
CREATE TABLE IF NOT EXISTS transaction_history_backup (LIKE transaction_history);

-- Table for storing Arbitrum transaction timestamps
CREATE TABLE IF NOT EXISTS arbitrum_tx_timestamps (
    tx_hash VARCHAR(66) PRIMARY KEY,
    deposit_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index on deposit_id for tx timestamp lookups
CREATE INDEX IF NOT EXISTS idx_arbitrum_tx_timestamps_deposit_id ON arbitrum_tx_timestamps(deposit_id);

-- Table for tracking schema version
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

-- Table for admin actions
CREATE TABLE IF NOT EXISTS admin_actions (
    id SERIAL PRIMARY KEY,
    action TEXT NOT NULL,
    params TEXT NOT NULL,
    admin_key TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table for application settings
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert default settings (same as production)
INSERT INTO settings (key, value) VALUES
('schema_version', '2'),
('mon_usd_ratio', '120000000000000000'),
('wallet_limit_percentage', '30')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_deposits_wallet_address ON deposits(wallet_address);
CREATE INDEX IF NOT EXISTS idx_deposits_tx_hash ON deposits(tx_hash);
CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status);
CREATE INDEX IF NOT EXISTS idx_distributions_wallet_address ON distributions(wallet_address);
CREATE INDEX IF NOT EXISTS idx_distributions_status ON distributions(status);
CREATE INDEX IF NOT EXISTS idx_distributions_monad_tx_hash ON distributions(monad_tx_hash);
CREATE INDEX IF NOT EXISTS idx_transaction_history_wallet_address ON transaction_history(wallet_address);
CREATE INDEX IF NOT EXISTS idx_transaction_history_status ON transaction_history(status);
CREATE INDEX IF NOT EXISTS idx_transaction_history_created_at ON transaction_history(created_at DESC);

-- Create index for refund_tx_hash
CREATE INDEX IF NOT EXISTS idx_transaction_history_refund_tx_hash ON transaction_history(refund_tx_hash); 