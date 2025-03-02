package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Status value specific to the new database schema
const processedStatus = "processed"

// SchemaMigration performs the migration from the old schema to the new schema.
func (db *DB) SchemaMigration() error {
	logger.Info("Starting database schema migration...")

	// Check if transaction_history table exists
	tableExists, err := db.checkTableExists("transaction_history")
	if err != nil {
		return fmt.Errorf("failed to check if transaction_history table exists: %w", err)
	}

	if !tableExists {
		logger.Info("No transaction_history table found, skipping migration")
		return nil
	}

	// Check if we've already migrated
	depositsExists, err := db.checkTableExists("deposits")
	if err != nil {
		return fmt.Errorf("failed to check if deposits table exists: %w", err)
	}

	distributionsExists, err := db.checkTableExists("distributions")
	if err != nil {
		return fmt.Errorf("failed to check if distributions table exists: %w", err)
	}

	if depositsExists && distributionsExists {
		// Check if we need to migrate data
		depositsCount, err := db.getTableRowCount("deposits")
		if err != nil {
			return fmt.Errorf("failed to get deposits count: %w", err)
		}

		if depositsCount > 0 {
			logger.Info("Schema migration already completed, skipping")
			return nil
		}
	}

	// Create the new tables if they don't exist
	if err := db.execSchemaSQL(); err != nil {
		return fmt.Errorf("failed to create new tables: %w", err)
	}

	// Begin transaction for migration
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Get all records from transaction_history
	rows, err := tx.QueryContext(ctx, `
		SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, 
		       tx_hash, monad_tx_hash, created_at, updated_at 
		FROM transaction_history
	`)
	if err != nil {
		return fmt.Errorf("failed to query transaction_history: %w", err)
	}
	defer rows.Close()

	migratedCount := 0

	for rows.Next() {
		var (
			id, currency                                        int64
			depositID, walletAddress, amount, monAmount, status string
			txHash, monadTxHash                                 sql.NullString
			createdAt, updatedAt                                time.Time
			blockNumber                                         int64 = 0 // Default for older records
		)

		if err := rows.Scan(
			&id, &depositID, &walletAddress, &amount, &currency, &monAmount,
			&status, &txHash, &monadTxHash, &createdAt, &updatedAt,
		); err != nil {
			return fmt.Errorf("failed to scan transaction_history row: %w", err)
		}

		// For deposits table
		var arbiTxHash string
		if txHash.Valid {
			arbiTxHash = txHash.String
		} else {
			arbiTxHash = ""
		}

		depositStatus := status
		if status == StatusCompleted {
			depositStatus = processedStatus
		}

		// Insert into deposits
		_, err = tx.ExecContext(ctx, `
			INSERT INTO deposits (deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (deposit_id) DO NOTHING
		`, depositID, walletAddress, amount, currency, arbiTxHash, blockNumber, depositStatus, createdAt, updatedAt)
		if err != nil {
			return fmt.Errorf("failed to insert into deposits: %w", err)
		}

		// Insert into distributions
		distributionStatus := DistStatusPending
		if status == StatusCompleted {
			distributionStatus = DistStatusCompleted
		} else if status == StatusFailed {
			distributionStatus = DistStatusFailed
		}

		var monadHash string
		if monadTxHash.Valid {
			monadHash = monadTxHash.String
		} else {
			monadHash = ""
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO distributions (deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (deposit_id) DO NOTHING
		`, depositID, walletAddress, monAmount, distributionStatus, monadHash, createdAt, updatedAt)
		if err != nil {
			return fmt.Errorf("failed to insert into distributions: %w", err)
		}

		migratedCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating transaction_history rows: %w", err)
	}

	// Create a backup of the transaction_history table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS transaction_history_backup AS 
		SELECT * FROM transaction_history
	`)
	if err != nil {
		return fmt.Errorf("failed to create transaction_history backup: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Info("Schema migration completed successfully: migrated %d records", migratedCount)
	return nil
}

// checkTableExists checks if a table exists in the database.
func (db *DB) checkTableExists(tableName string) (bool, error) {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
	`
	err := db.QueryRow(query, tableName).Scan(&exists)
	return exists, err
}

// getTableRowCount gets the number of rows in a table.
func (db *DB) getTableRowCount(tableName string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

// execSchemaSQL executes the schema SQL to create the new tables.
func (db *DB) execSchemaSQL() error {
	// Read schema SQL from file or embed it here
	schema := `
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

		-- Create indexes for faster lookups
		CREATE INDEX IF NOT EXISTS idx_deposits_wallet_address ON deposits(wallet_address);
		CREATE INDEX IF NOT EXISTS idx_deposits_tx_hash ON deposits(tx_hash);
		CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status);
		CREATE INDEX IF NOT EXISTS idx_distributions_wallet_address ON distributions(wallet_address);
		CREATE INDEX IF NOT EXISTS idx_distributions_status ON distributions(status);
		CREATE INDEX IF NOT EXISTS idx_distributions_monad_tx_hash ON distributions(monad_tx_hash);
	`

	_, err := db.Exec(schema)
	return err
}
