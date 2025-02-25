package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

const (
	// Database file name
	dbFileName = "monad_faucet.db"

	// Schema version for migrations
	schemaVersion = 1
)

// DB represents the database connection
type DB struct {
	*sql.DB
}

// New creates a new database connection
func New(dataDir string) (*DB, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, dbFileName)
	db, err := sql.Open("sqlite3", dbPath+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Create a new DB instance
	database := &DB{db}

	// Initialize the database schema
	if err := database.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database schema: %w", err)
	}

	return database, nil
}

// initSchema creates the database tables if they don't exist
func (db *DB) initSchema() error {
	// Check if we need to initialize or migrate the schema
	var version int
	err := db.QueryRow("PRAGMA user_version").Scan(&version)
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
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				deposit_id TEXT NOT NULL,
				wallet_address TEXT NOT NULL,
				amount TEXT NOT NULL,
				currency INTEGER NOT NULL,
				mon_amount TEXT NOT NULL,
				status TEXT NOT NULL,
				tx_hash TEXT,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(deposit_id)
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create transaction_history table: %w", err)
		}

		// Create admin_actions table
		_, err = tx.Exec(`
			CREATE TABLE IF NOT EXISTS admin_actions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
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
	}

	// Update schema version
	_, err = tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion))
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
