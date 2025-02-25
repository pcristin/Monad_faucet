package database

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/pcristin/monad-faucet/pkg/logger"
)

const (
	// Schema version for migrations
	schemaVersion = 1
)

// DB represents the database connection
type DB struct {
	*sql.DB
}

// New creates a new database connection
func New(dataDir string) (*DB, error) {
	// Get database connection string from environment variable
	// If not provided, use the dataDir parameter for backward compatibility
	dbURL := getDBConnectionString()

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
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

// getDBConnectionString gets the PostgreSQL connection string from env vars
func getDBConnectionString() string {
	// Check for the DATABASE_URL environment variable (provided by Render)
	if dbURL := getEnv("DATABASE_URL", ""); dbURL != "" {
		return dbURL
	}

	// Fallback to constructing a connection string from individual parameters
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "postgres")
	password := getEnv("DB_PASSWORD", "postgres")
	dbname := getEnv("DB_NAME", "monad_faucet")
	sslmode := getEnv("DB_SSLMODE", "disable")

	// Format: "host=localhost port=5432 user=postgres password=postgres dbname=monad_faucet sslmode=disable"
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
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
	}

	// Update schema version
	_, err = tx.Exec(`UPDATE schema_version SET version = $1`, schemaVersion)
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
