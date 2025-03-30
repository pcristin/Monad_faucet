package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Status value specific to the new database schema
const processedStatus = "processed"

// MigrationRecord represents a record from the old transaction_history table
type MigrationRecord struct {
	ID            int64
	DepositID     string
	WalletAddress string
	Amount        string
	Currency      int64
	MonAmount     string
	Status        string
	TxHash        sql.NullString
	MonadTxHash   sql.NullString
	Metadata      sql.NullString
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SchemaMigration performs the migration from the old schema to the new schema.
func (db *DB) SchemaMigration() error {
	logger.Info("Starting database schema migration...")

	// Add refund_tx_hash column if it doesn't exist
	if err := db.addRefundTxHashColumn(); err != nil {
		return fmt.Errorf("failed to add refund_tx_hash column: %w", err)
	}

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

	// Get data without using a transaction for reading
	logger.Info("Querying transaction_history table...")
	rows, err := db.Query(`
		SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, 
		       tx_hash, monad_tx_hash, metadata, created_at, updated_at 
		FROM transaction_history
	`)
	if err != nil {
		return fmt.Errorf("failed to query transaction_history: %w", err)
	}
	defer rows.Close()

	// Collect all records to process
	logger.Info("Reading transaction records...")
	var records []MigrationRecord
	for rows.Next() {
		var record MigrationRecord

		if err := rows.Scan(
			&record.ID, &record.DepositID, &record.WalletAddress, &record.Amount, &record.Currency, &record.MonAmount,
			&record.Status, &record.TxHash, &record.MonadTxHash, &record.Metadata, &record.CreatedAt, &record.UpdatedAt,
		); err != nil {
			return fmt.Errorf("failed to scan transaction_history row: %w", err)
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating transaction_history rows: %w", err)
	}

	logger.Info("Found %d records to migrate", len(records))
	if len(records) == 0 {
		return nil
	}

	// Process records in smaller batches to avoid protocol issues
	batchSize := 10
	totalBatches := (len(records) + batchSize - 1) / batchSize // Ceiling division
	migratedCount := 0

	for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
		startIdx := batchIdx * batchSize
		endIdx := (batchIdx + 1) * batchSize
		if endIdx > len(records) {
			endIdx = len(records)
		}

		batchRecords := records[startIdx:endIdx]
		logger.Info("Processing batch %d/%d (%d records)", batchIdx+1, totalBatches, len(batchRecords))

		// Process each record individually to avoid large transactions
		for _, record := range batchRecords {
			if err := db.migrateRecord(record); err != nil {
				logger.Error("Failed to migrate record: %v", err)
				continue // Continue with next record on error
			}
			migratedCount++
		}

		// Add a delay between batches
		time.Sleep(100 * time.Millisecond)
	}

	// Create a backup of the transaction_history table outside of the main transaction
	logger.Info("Creating backup of transaction_history...")
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

// migrateRecord migrates a single record from transaction_history to the new schema
func (db *DB) migrateRecord(record MigrationRecord) error {
	// Create a transaction for just this record
	tx, err := db.Begin()
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
	if record.TxHash.Valid {
		arbiTxHash = record.TxHash.String
	} else {
		arbiTxHash = ""
	}

	depositStatus := record.Status
	if record.Status == StatusCompleted {
		depositStatus = processedStatus
	}

	// Get metadata if available
	var metadata string
	if record.Metadata.Valid {
		metadata = record.Metadata.String
	} else {
		metadata = ""
	}

	// Insert into deposits with retry
	const maxRetries = 3
	var retryDelay = 50 * time.Millisecond

	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			logger.Info("Retrying deposit insert (attempt %d)...", retry+1)
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
		}

		// Insert into deposits
		depositInsert := `
			INSERT INTO deposits (deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (deposit_id) DO NOTHING
		`
		_, err = tx.Exec(depositInsert, record.DepositID, record.WalletAddress, record.Amount, record.Currency, arbiTxHash, 0, depositStatus, metadata, record.CreatedAt, record.UpdatedAt)
		if err == nil {
			break // Success, exit retry loop
		}

		logger.Error("Failed to insert into deposits (attempt %d): %v", retry+1, err)

		if retry == maxRetries-1 {
			// Last attempt failed
			return fmt.Errorf("failed to insert into deposits after %d attempts: %w", maxRetries, err)
		}
	}

	// Insert into distributions with retry
	distributionStatus := DistStatusPending
	if record.Status == StatusCompleted {
		distributionStatus = DistStatusCompleted
	} else if record.Status == StatusFailed {
		distributionStatus = DistStatusFailed
	}

	var monadHash string
	if record.MonadTxHash.Valid {
		monadHash = record.MonadTxHash.String
	} else {
		monadHash = ""
	}

	retryDelay = 50 * time.Millisecond
	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			logger.Info("Retrying distribution insert (attempt %d)...", retry+1)
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
		}

		// Insert into distributions
		distributionInsert := `
			INSERT INTO distributions (deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (deposit_id) DO NOTHING
		`
		_, err = tx.Exec(distributionInsert, record.DepositID, record.WalletAddress, record.MonAmount, distributionStatus, monadHash, record.CreatedAt, record.UpdatedAt)
		if err == nil {
			break // Success, exit retry loop
		}

		logger.Error("Failed to insert into distributions (attempt %d): %v", retry+1, err)

		if retry == maxRetries-1 {
			// Last attempt failed
			return fmt.Errorf("failed to insert into distributions after %d attempts: %w", maxRetries, err)
		}
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

// addRefundTxHashColumn adds the refund_tx_hash column to the transaction_history table if it doesn't exist
func (db *DB) addRefundTxHashColumn() error {
	// Check if column exists
	var columnExists bool
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.columns 
			WHERE table_name = 'transaction_history' 
			AND column_name = 'refund_tx_hash'
		)
	`
	err := db.QueryRow(query).Scan(&columnExists)
	if err != nil {
		return fmt.Errorf("failed to check if refund_tx_hash column exists: %w", err)
	}

	if columnExists {
		logger.Info("refund_tx_hash column already exists, skipping addition")
	} else {
		// Add the column
		logger.Info("Adding refund_tx_hash column to transaction_history table...")
		_, err = db.Exec(`
			ALTER TABLE transaction_history ADD COLUMN refund_tx_hash VARCHAR(66);
			CREATE INDEX IF NOT EXISTS idx_transaction_history_refund_tx_hash ON transaction_history(refund_tx_hash);
		`)
		if err != nil {
			return fmt.Errorf("failed to add refund_tx_hash column: %w", err)
		}

		logger.Info("Successfully added refund_tx_hash column to transaction_history table")
	}

	// Check if the unique constraint exists
	var constraintExists bool
	constraintQuery := `
		SELECT EXISTS (
			SELECT FROM information_schema.table_constraints 
			WHERE constraint_name = 'deposit_id_unique_tx' 
			AND table_name = 'transaction_history'
		)
	`
	err = db.QueryRow(constraintQuery).Scan(&constraintExists)
	if err != nil {
		return fmt.Errorf("failed to check if unique constraint exists: %w", err)
	}

	if constraintExists {
		logger.Info("Unique constraint on deposit_id already exists, skipping addition")
		return nil
	}

	// Run deduplication and add constraint
	logger.Info("Deduplicating transaction records and adding unique constraint...")
	_, err = db.Exec(`
		-- First, remove duplicate records keeping only the most recent one
		DELETE FROM transaction_history a 
		USING (
			SELECT MAX(id) as max_id, deposit_id
			FROM transaction_history
			GROUP BY deposit_id
			HAVING COUNT(*) > 1
		) b
		WHERE a.deposit_id = b.deposit_id AND a.id < b.max_id;

		-- Add unique constraint on deposit_id
		ALTER TABLE transaction_history 
		ADD CONSTRAINT IF NOT EXISTS deposit_id_unique_tx UNIQUE (deposit_id);
	`)
	if err != nil {
		return fmt.Errorf("failed to deduplicate records and add constraint: %w", err)
	}

	logger.Info("Successfully deduplicated transaction records and added unique constraint")
	return nil
}
