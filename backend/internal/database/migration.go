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

	// Get all records from transaction_history without a transaction first
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
		SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, 
		       tx_hash, monad_tx_hash, created_at, updated_at 
		FROM transaction_history
	`)
	if err != nil {
		return fmt.Errorf("failed to query transaction_history: %w", err)
	}
	defer rows.Close()

	// Collect all migrations to process
	type MigrationRecord struct {
		ID          int64
		DepositID   string
		WalletAddr  string
		Amount      string
		Currency    int64
		MonAmount   string
		Status      string
		TxHash      sql.NullString
		MonadTxHash sql.NullString
		CreatedAt   time.Time
		UpdatedAt   time.Time
		BlockNumber int64
	}

	var records []MigrationRecord
	for rows.Next() {
		var rec MigrationRecord
		rec.BlockNumber = 0 // Default for older records

		if err := rows.Scan(
			&rec.ID, &rec.DepositID, &rec.WalletAddr, &rec.Amount, &rec.Currency, &rec.MonAmount,
			&rec.Status, &rec.TxHash, &rec.MonadTxHash, &rec.CreatedAt, &rec.UpdatedAt,
		); err != nil {
			return fmt.Errorf("failed to scan transaction_history row: %w", err)
		}

		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating transaction_history rows: %w", err)
	}

	// Process each record in separate transactions
	migratedCount := 0

	for _, rec := range records {
		// Process deposit record
		err := db.processMigrationRecord(ctx, rec)
		if err != nil {
			logger.Error("Failed to process migration record %d: %v", rec.ID, err)
			// Continue with other records instead of failing completely
			continue
		}
		migratedCount++

		// Add a delay between records to avoid overwhelming the connection
		time.Sleep(50 * time.Millisecond)
	}

	// Create a backup of the transaction_history table in a separate transaction
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS transaction_history_backup AS 
		SELECT * FROM transaction_history
	`)
	if err != nil {
		return fmt.Errorf("failed to create transaction_history backup: %w", err)
	}

	logger.Info("Schema migration completed successfully: migrated %d records", migratedCount)
	return nil
}

// processMigrationRecord processes a single record in its own transaction
func (db *DB) processMigrationRecord(ctx context.Context, rec MigrationRecord) error {
	// Use a less strict isolation level to avoid locking issues
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// For deposits table
	var arbiTxHash string
	if rec.TxHash.Valid {
		arbiTxHash = rec.TxHash.String
	} else {
		arbiTxHash = ""
	}

	depositStatus := rec.Status
	if rec.Status == StatusCompleted {
		depositStatus = processedStatus
	}

	// Insert into deposits with basic statement
	_, err = tx.ExecContext(ctx,
		"INSERT INTO deposits (deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (deposit_id) DO NOTHING",
		rec.DepositID, rec.WalletAddr, rec.Amount, rec.Currency, arbiTxHash, rec.BlockNumber, depositStatus, rec.CreatedAt, rec.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to insert into deposits: %w", err)
	}

	// Insert into distributions
	distributionStatus := DistStatusPending
	if rec.Status == StatusCompleted {
		distributionStatus = DistStatusCompleted
	} else if rec.Status == StatusFailed {
		distributionStatus = DistStatusFailed
	}

	var monadHash string
	if rec.MonadTxHash.Valid {
		monadHash = rec.MonadTxHash.String
	} else {
		monadHash = ""
	}

	_, err = tx.ExecContext(ctx,
		"INSERT INTO distributions (deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (deposit_id) DO NOTHING",
		rec.DepositID, rec.WalletAddr, rec.MonAmount, distributionStatus, monadHash, rec.CreatedAt, rec.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to insert into distributions: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

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
	// Execute SQL statements individually to avoid protocol errors

	// Create processing_locks table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS processing_locks (
			deposit_id VARCHAR(100) PRIMARY KEY,
			instance_id VARCHAR(50) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NOT NULL,
			CONSTRAINT deposit_id_unique UNIQUE (deposit_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create processing_locks table: %w", err)
	}

	// Create index on processing_locks
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_processing_locks_expires_at ON processing_locks(expires_at)
	`)
	if err != nil {
		return fmt.Errorf("failed to create index on processing_locks: %w", err)
	}

	// Create deposits table
	_, err = db.Exec(`
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create deposits table: %w", err)
	}

	// Create indexes on deposits
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_deposits_wallet_address ON deposits(wallet_address)
	`)
	if err != nil {
		return fmt.Errorf("failed to create wallet_address index: %w", err)
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_deposits_tx_hash ON deposits(tx_hash)
	`)
	if err != nil {
		return fmt.Errorf("failed to create tx_hash index: %w", err)
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status)
	`)
	if err != nil {
		return fmt.Errorf("failed to create status index: %w", err)
	}

	// Create distributions table
	_, err = db.Exec(`
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
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create distributions table: %w", err)
	}

	// Create indexes on distributions
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_distributions_wallet_address ON distributions(wallet_address)
	`)
	if err != nil {
		return fmt.Errorf("failed to create distributions wallet_address index: %w", err)
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_distributions_status ON distributions(status)
	`)
	if err != nil {
		return fmt.Errorf("failed to create distributions status index: %w", err)
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_distributions_monad_tx_hash ON distributions(monad_tx_hash)
	`)
	if err != nil {
		return fmt.Errorf("failed to create distributions monad_tx_hash index: %w", err)
	}

	return nil
}
