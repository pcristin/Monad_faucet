-- SQL script to add source_chain columns to existing tables
-- This can be run manually if automatic migrations fail
-- Run with: psql -d your_database -f add_source_chain_columns.sql

-- Start a transaction so we can roll back if anything fails
BEGIN;

-- 1. Add source_chain column to transaction_history if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.columns 
        WHERE table_name = 'transaction_history' 
        AND column_name = 'source_chain'
    ) THEN
        ALTER TABLE transaction_history ADD COLUMN source_chain VARCHAR(50) DEFAULT 'Arbitrum';
        RAISE NOTICE 'Added source_chain column to transaction_history';
    ELSE
        RAISE NOTICE 'source_chain column already exists in transaction_history';
    END IF;
END $$;

-- 2. Add source_chain column to deposits if it doesn't exist
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'deposits'
    ) THEN
        IF NOT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_name = 'deposits' 
            AND column_name = 'source_chain'
        ) THEN
            ALTER TABLE deposits ADD COLUMN source_chain VARCHAR(50) DEFAULT 'Arbitrum';
            RAISE NOTICE 'Added source_chain column to deposits';
        ELSE
            RAISE NOTICE 'source_chain column already exists in deposits';
        END IF;
    ELSE
        RAISE NOTICE 'deposits table does not exist yet';
    END IF;
END $$;

-- 3. Create indexes for faster lookups
DO $$
BEGIN
    -- Index for transaction_history
    IF NOT EXISTS (
        SELECT FROM pg_indexes 
        WHERE tablename = 'transaction_history' 
        AND indexname = 'idx_transaction_history_deposit_id_source_chain'
    ) THEN
        CREATE INDEX idx_transaction_history_deposit_id_source_chain 
        ON transaction_history(deposit_id, source_chain);
        RAISE NOTICE 'Created index on transaction_history(deposit_id, source_chain)';
    ELSE
        RAISE NOTICE 'Index on transaction_history(deposit_id, source_chain) already exists';
    END IF;

    -- Only create deposits index if deposits table exists
    IF EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'deposits'
    ) THEN
        IF NOT EXISTS (
            SELECT FROM pg_indexes 
            WHERE tablename = 'deposits' 
            AND indexname = 'idx_deposits_deposit_id_source_chain'
        ) THEN
            CREATE INDEX idx_deposits_deposit_id_source_chain 
            ON deposits(deposit_id, source_chain);
            RAISE NOTICE 'Created index on deposits(deposit_id, source_chain)';
        ELSE
            RAISE NOTICE 'Index on deposits(deposit_id, source_chain) already exists';
        END IF;
    END IF;
END $$;

-- Commit the transaction if everything succeeded
COMMIT; 