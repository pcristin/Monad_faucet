package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/pcristin/monad-faucet/pkg/logger"
)

const (
	// Schema version for migrations
	schemaVersion = 2
)

// DB represents the database connection
type DB struct {
	*sql.DB
}

// NewDB creates a new database connection.
func NewDB(ctx context.Context, dsn string) (*DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Check connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	logger.Info("Connected to database")
	wrappedDB := &DB{DB: db}

	// Create processing_locks table if it doesn't exist to avoid issues with locks
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS processing_locks (
			deposit_id VARCHAR(100) PRIMARY KEY,
			instance_id VARCHAR(50) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NOT NULL,
			CONSTRAINT deposit_id_unique UNIQUE (deposit_id)
		);
		
		CREATE INDEX IF NOT EXISTS idx_processing_locks_expires_at ON processing_locks(expires_at);
	`)
	if err != nil {
		return nil, fmt.Errorf("error creating processing_locks table: %w", err)
	}

	// Run schema migration if needed
	if err := wrappedDB.SchemaMigration(); err != nil {
		return nil, fmt.Errorf("error migrating schema: %w", err)
	}

	return wrappedDB, nil
}

// getEnv gets an environment variable or returns the default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return defaultValue
}

// initSchema creates the database tables if they don't exist
func (db *DB) initSchema() error {
	// Check if the version table exists
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'schema_version'
		)
	`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if schema_version table exists: %w", err)
	}

	// Create the version table if it doesn't exist
	if !exists {
		_, err = db.Exec(`
			CREATE TABLE schema_version (
				version INTEGER NOT NULL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create schema_version table: %w", err)
		}

		// Insert initial version
		_, err = db.Exec(`INSERT INTO schema_version (version) VALUES (0)`)
		if err != nil {
			return fmt.Errorf("failed to initialize schema version: %w", err)
		}
	}

	// Get current schema version
	var version int
	err = db.QueryRow(`SELECT version FROM schema_version`).Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to get schema version: %w", err)
	}

	// If the schema is already at the current version, we're done
	if version == schemaVersion {
		logger.Info("Database schema is up to date (version %d)", version)
		return nil
	}

	// Start a transaction for schema creation/migration
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Create tables
	if version == 0 {
		// Create settings table
		_, err = tx.Exec(`
			CREATE TABLE IF NOT EXISTS settings (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create settings table: %w", err)
		}

		// Create wallet_usage table
		_, err = tx.Exec(`
			CREATE TABLE IF NOT EXISTS wallet_usage (
				wallet_address TEXT PRIMARY KEY,
				total_amount TEXT NOT NULL,
				last_updated TIMESTAMP NOT NULL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create wallet_usage table: %w", err)
		}

		// Create transaction_history table
		_, err = tx.Exec(`
			CREATE TABLE IF NOT EXISTS transaction_history (
				id SERIAL PRIMARY KEY,
				deposit_id TEXT NOT NULL,
				wallet_address TEXT NOT NULL,
				amount TEXT NOT NULL,
				currency INTEGER NOT NULL,
				mon_amount TEXT NOT NULL,
				status TEXT NOT NULL,
				tx_hash TEXT,
				monad_tx_hash TEXT,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT unique_deposit_id UNIQUE(deposit_id)
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create transaction_history table: %w", err)
		}

		// Create admin_actions table
		_, err = tx.Exec(`
			CREATE TABLE IF NOT EXISTS admin_actions (
				id SERIAL PRIMARY KEY,
				action TEXT NOT NULL,
				params TEXT NOT NULL,
				admin_key TEXT NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create admin_actions table: %w", err)
		}

		// Insert default settings
		_, err = tx.Exec(`
			INSERT INTO settings (key, value) VALUES 
			('mon_usd_ratio', '100000000000000000'),
			('wallet_limit_percentage', '30')
			ON CONFLICT(key) DO NOTHING
		`)
		if err != nil {
			return fmt.Errorf("failed to insert default settings: %w", err)
		}

		// Set version to 1
		version = 1
	}

	// Migration to add monad_tx_hash column
	if version == 1 {
		// Check if monad_tx_hash column exists
		var columnExists bool
		err = tx.QueryRow(`
			SELECT EXISTS (
				SELECT 1 
				FROM information_schema.columns 
				WHERE table_name = 'transaction_history' AND column_name = 'monad_tx_hash'
			)
		`).Scan(&columnExists)
		if err != nil {
			return fmt.Errorf("failed to check if monad_tx_hash column exists: %w", err)
		}

		// Add monad_tx_hash column if it doesn't exist
		if !columnExists {
			_, err = tx.Exec(`
				ALTER TABLE transaction_history
				ADD COLUMN monad_tx_hash TEXT
			`)
			if err != nil {
				return fmt.Errorf("failed to add monad_tx_hash column: %w", err)
			}
		}

		// Set version to 2
		version = 2
	}

	// Update schema version
	_, err = tx.Exec(`
		INSERT INTO settings (key, value) VALUES ('schema_version', $1)
		ON CONFLICT(key) DO UPDATE SET value = $1, updated_at = CURRENT_TIMESTAMP
	`, fmt.Sprintf("%d", version))
	if err != nil {
		return fmt.Errorf("failed to update schema version: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Info("Database schema initialized/migrated to version %d", schemaVersion)
	return nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}
