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