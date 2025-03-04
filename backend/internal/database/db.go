package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/pcristin/monad-faucet/pkg/logger"
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

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}
