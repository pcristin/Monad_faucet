package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Deposit represents a deposit transaction from Arbitrum
type Deposit struct {
	ID            int64
	DepositID     *big.Int
	WalletAddress common.Address
	Amount        *big.Int
	Currency      CurrencyType
	TxHash        string // Arbitrum transaction hash
	BlockNumber   uint64
	Status        string
	Metadata      string // User-provided metadata for this deposit
	CreatedAt     time.Time
	UpdatedAt     time.Time
	SourceChain   string // Source chain of the deposit (Arbitrum, Base, Optimism)
}

// CreateDeposit creates a new deposit record in the database
func (db *DB) CreateDeposit(deposit *Deposit) error {
	// Use default source chain if not specified
	if deposit.SourceChain == "" {
		deposit.SourceChain = "Arbitrum"
	}

	// Use Postgres ON CONFLICT to handle duplicate deposit IDs
	// This is essentially an UPSERT operation that will do an INSERT if the record doesn't exist,
	// or an UPDATE if it does exist (which will update only the fields we want to update)
	err := db.QueryRow(
		`INSERT INTO deposits 
		(deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, source_chain)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (deposit_id) DO UPDATE SET
		-- Only update fields that may need to be refreshed
		status = CASE
			WHEN deposits.status = 'failed' OR deposits.status = 'pending' THEN $7
			ELSE deposits.status
		END,
		source_chain = $9,
		updated_at = CURRENT_TIMESTAMP
		RETURNING id`,
		deposit.DepositID.String(),
		deposit.WalletAddress.Hex(),
		deposit.Amount.String(),
		deposit.Currency,
		deposit.TxHash,
		deposit.BlockNumber,
		deposit.Status,
		deposit.Metadata,
		deposit.SourceChain,
	).Scan(&deposit.ID)

	if err != nil {
		return fmt.Errorf("failed to create deposit: %w", err)
	}

	logger.Info("Successfully created or updated deposit record for ID %s with status %s, chain %s",
		deposit.DepositID.String(), deposit.Status, deposit.SourceChain)

	return nil
}

// UpdateDepositStatus updates the status of a deposit
func (db *DB) UpdateDepositStatus(depositID *big.Int, status string) error {
	if depositID == nil {
		logger.Error("Cannot update deposit status: deposit ID is nil")
		return fmt.Errorf("deposit ID is nil")
	}

	depositIDStr := depositID.String()
	logger.Info("Updating deposit status for ID %s to %s", depositIDStr, status)

	// Start a transaction for atomicity
	tx, err := db.Begin()
	if err != nil {
		logger.Error("Failed to begin transaction for deposit status update: %v", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use defer with a named error return to handle rollback/commit
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				logger.Error("Failed to rollback transaction: %v", rbErr)
			}
		}
	}()

	// First check if the deposit exists
	var exists bool
	err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM deposits WHERE deposit_id = $1)", depositIDStr).Scan(&exists)
	if err != nil {
		logger.Error("Error checking if deposit exists: %v", err)
		return fmt.Errorf("error checking deposit existence: %w", err)
	}

	if !exists {
		logger.Warn("Deposit ID %s not found in deposits table. Attempting to create from transaction record.", depositIDStr)

		// Try to find the transaction record to get the data we need
		txData, txErr := db.GetTransactionByDepositID(depositID)
		if txErr != nil || txData == nil {
			logger.Error("Could not find transaction record for deposit ID %s: %v", depositIDStr, txErr)
			return fmt.Errorf("deposit does not exist and could not create: %w", txErr)
		}

		// Create the deposit record with data from the transaction, including source_chain
		_, err = tx.Exec(
			`INSERT INTO deposits (deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, source_chain, created_at, updated_at) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			depositIDStr,
			txData.WalletAddress.Hex(),
			txData.Amount.String(),
			txData.Currency,
			txData.TxHash,
			0, // We don't know the block number here
			status,
			txData.Metadata.String, // Use metadata from transaction record
			txData.SourceChain,     // Copy source_chain from transaction
			time.Now(),
			time.Now(),
		)

		if err != nil {
			logger.Error("Failed to create deposit record from transaction: %v", err)
			return fmt.Errorf("failed to create deposit: %w", err)
		}

		logger.Info("Successfully created deposit record with ID %s, status %s, chain %s",
			depositIDStr, status, txData.SourceChain)

		// Commit the transaction
		if err = tx.Commit(); err != nil {
			logger.Error("Failed to commit transaction for deposit creation: %v", err)
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	}

	// For existing deposits, check if we need to update the source_chain from transaction_history
	var updateSourceChain bool
	var txSourceChain string

	// Check if transaction_history has a different source_chain than deposits
	err = tx.QueryRow(`
		SELECT t.source_chain, 
			CASE WHEN (t.source_chain IS NOT NULL AND d.source_chain != t.source_chain) 
				OR d.source_chain IS NULL THEN true ELSE false END
		FROM transaction_history t
		JOIN deposits d ON t.deposit_id = d.deposit_id
		WHERE t.deposit_id = $1
	`, depositIDStr).Scan(&txSourceChain, &updateSourceChain)

	// Only proceed with source_chain check if the query was successful
	if err == nil && updateSourceChain && txSourceChain != "" {
		logger.Info("Updating deposit source_chain from %s to %s for ID %s",
			"unknown/different", txSourceChain, depositIDStr)

		// Update both status and source_chain
		result, err := tx.Exec(
			"UPDATE deposits SET status = $1, source_chain = $2, updated_at = $3 WHERE deposit_id = $4",
			status,
			txSourceChain,
			time.Now(),
			depositIDStr,
		)

		if err != nil {
			logger.Error("Failed to update deposit status and source_chain: %v", err)
			return fmt.Errorf("failed to update deposit: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		logger.Info("Successfully updated status and source_chain for deposit ID %s (%d rows affected)",
			depositIDStr, rowsAffected)

	} else {
		// Just update status (standard path)
		result, err := tx.Exec(
			"UPDATE deposits SET status = $1, updated_at = $2 WHERE deposit_id = $3",
			status,
			time.Now(),
			depositIDStr,
		)

		if err != nil {
			logger.Error("Failed to update deposit status: %v", err)
			return fmt.Errorf("failed to update deposit status: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			logger.Error("Error getting rows affected: %v", err)
		} else if rowsAffected == 0 {
			logger.Warn("No rows affected when updating deposit status for ID %s", depositIDStr)
		} else {
			logger.Info("Successfully updated status for deposit ID %s to %s (%d rows affected)",
				depositIDStr, status, rowsAffected)
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		logger.Error("Failed to commit transaction for deposit update: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetDepositByID retrieves a deposit by its deposit ID
func (db *DB) GetDepositByID(depositID *big.Int) (*Deposit, error) {
	var (
		deposit                                   Deposit
		depositIDStr, walletAddressStr, amountStr string
		currencyInt                               int
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, created_at, updated_at, COALESCE(source_chain, 'Arbitrum') as source_chain
		FROM deposits 
		WHERE deposit_id = $1`,
		depositID.String(),
	).Scan(
		&deposit.ID,
		&depositIDStr,
		&walletAddressStr,
		&amountStr,
		&currencyInt,
		&deposit.TxHash,
		&deposit.BlockNumber,
		&deposit.Status,
		&deposit.Metadata,
		&deposit.CreatedAt,
		&deposit.UpdatedAt,
		&deposit.SourceChain,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get deposit: %w", err)
	}

	// Parse the big.Int and common.Address fields
	deposit.DepositID = new(big.Int)
	deposit.DepositID.SetString(depositIDStr, 10)
	deposit.WalletAddress = common.HexToAddress(walletAddressStr)
	deposit.Amount = new(big.Int)
	deposit.Amount.SetString(amountStr, 10)
	deposit.Currency = CurrencyType(currencyInt)

	return &deposit, nil
}

// GetDepositByTxHash retrieves a deposit by its transaction hash
func (db *DB) GetDepositByTxHash(txHash string) (*Deposit, error) {
	var (
		deposit                                   Deposit
		depositIDStr, walletAddressStr, amountStr string
		currencyInt                               int
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, created_at, updated_at 
		FROM deposits 
		WHERE tx_hash = $1`,
		txHash,
	).Scan(
		&deposit.ID,
		&depositIDStr,
		&walletAddressStr,
		&amountStr,
		&currencyInt,
		&deposit.TxHash,
		&deposit.BlockNumber,
		&deposit.Status,
		&deposit.Metadata,
		&deposit.CreatedAt,
		&deposit.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get deposit: %w", err)
	}

	// Parse the big.Int and common.Address fields
	deposit.DepositID = new(big.Int)
	deposit.DepositID.SetString(depositIDStr, 10)
	deposit.WalletAddress = common.HexToAddress(walletAddressStr)
	deposit.Amount = new(big.Int)
	deposit.Amount.SetString(amountStr, 10)
	deposit.Currency = CurrencyType(currencyInt)

	return &deposit, nil
}

// GetDepositsByWallet retrieves all deposits for a specific wallet
func (db *DB) GetDepositsByWallet(wallet common.Address, limit, offset int) ([]*Deposit, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, created_at, updated_at 
		FROM deposits 
		WHERE wallet_address = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`,
		wallet.Hex(),
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get deposits by wallet: %w", err)
	}
	defer rows.Close()

	var deposits []*Deposit
	for rows.Next() {
		var (
			deposit                                   Deposit
			depositIDStr, walletAddressStr, amountStr string
			currencyInt                               int
		)

		err := rows.Scan(
			&deposit.ID,
			&depositIDStr,
			&walletAddressStr,
			&amountStr,
			&currencyInt,
			&deposit.TxHash,
			&deposit.BlockNumber,
			&deposit.Status,
			&deposit.Metadata,
			&deposit.CreatedAt,
			&deposit.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan deposit: %w", err)
		}

		// Parse the big.Int and common.Address fields
		deposit.DepositID = new(big.Int)
		deposit.DepositID.SetString(depositIDStr, 10)
		deposit.WalletAddress = common.HexToAddress(walletAddressStr)
		deposit.Amount = new(big.Int)
		deposit.Amount.SetString(amountStr, 10)
		deposit.Currency = CurrencyType(currencyInt)

		deposits = append(deposits, &deposit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating deposits: %w", err)
	}

	return deposits, nil
}

// GetRecentDeposits retrieves the most recent deposits
func (db *DB) GetRecentDeposits(limit, offset int) ([]*Deposit, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, created_at, updated_at 
		FROM deposits 
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2`,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent deposits: %w", err)
	}
	defer rows.Close()

	var deposits []*Deposit
	for rows.Next() {
		var (
			deposit                                   Deposit
			depositIDStr, walletAddressStr, amountStr string
			currencyInt                               int
		)

		err := rows.Scan(
			&deposit.ID,
			&depositIDStr,
			&walletAddressStr,
			&amountStr,
			&currencyInt,
			&deposit.TxHash,
			&deposit.BlockNumber,
			&deposit.Status,
			&deposit.Metadata,
			&deposit.CreatedAt,
			&deposit.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan deposit: %w", err)
		}

		// Parse the big.Int and common.Address fields
		deposit.DepositID = new(big.Int)
		deposit.DepositID.SetString(depositIDStr, 10)
		deposit.WalletAddress = common.HexToAddress(walletAddressStr)
		deposit.Amount = new(big.Int)
		deposit.Amount.SetString(amountStr, 10)
		deposit.Currency = CurrencyType(currencyInt)

		deposits = append(deposits, &deposit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating deposits: %w", err)
	}

	return deposits, nil
}

// GetDepositsByStatus retrieves deposits by their status
func (db *DB) GetDepositsByStatus(status string, limit, offset int) ([]*Deposit, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, metadata, created_at, updated_at 
		FROM deposits 
		WHERE status = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`,
		status,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get deposits by status: %w", err)
	}
	defer rows.Close()

	var deposits []*Deposit
	for rows.Next() {
		var (
			deposit                                   Deposit
			depositIDStr, walletAddressStr, amountStr string
			currencyInt                               int
		)

		err := rows.Scan(
			&deposit.ID,
			&depositIDStr,
			&walletAddressStr,
			&amountStr,
			&currencyInt,
			&deposit.TxHash,
			&deposit.BlockNumber,
			&deposit.Status,
			&deposit.Metadata,
			&deposit.CreatedAt,
			&deposit.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan deposit: %w", err)
		}

		// Parse the big.Int and common.Address fields
		deposit.DepositID = new(big.Int)
		deposit.DepositID.SetString(depositIDStr, 10)
		deposit.WalletAddress = common.HexToAddress(walletAddressStr)
		deposit.Amount = new(big.Int)
		deposit.Amount.SetString(amountStr, 10)
		deposit.Currency = CurrencyType(currencyInt)

		deposits = append(deposits, &deposit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating deposits: %w", err)
	}

	return deposits, nil
}

// // GetMetadata retrieves the metadata as a string, returning empty string if NULL
// func (d *Deposit) GetMetadata() string {
// 	if d.Metadata.Valid {
// 		return d.Metadata.String
// 	}
// 	return ""
// }

// BulkUpdateDeposits updates multiple deposit statuses in a single database transaction
func (db *DB) BulkUpdateDeposits(deposits []*Deposit) error {
	if len(deposits) == 0 {
		return nil
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				logger.Error("Failed to rollback transaction: %v", rbErr)
			}
		}
	}()

	// Use prepared statement for better performance
	stmt, err := tx.Prepare(`
		UPDATE deposits 
		SET status = $1, updated_at = $2 
		WHERE deposit_id = $3
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Track successful updates for logging
	successCount := 0
	now := time.Now()

	// Process each deposit update
	for _, deposit := range deposits {
		if deposit.DepositID == nil {
			logger.Warn("Skipping deposit update: deposit ID is nil")
			continue
		}

		depositIDStr := deposit.DepositID.String()
		_, execErr := stmt.Exec(deposit.Status, now, depositIDStr)
		if execErr != nil {
			logger.Error("Failed to update deposit ID %s: %v", depositIDStr, execErr)
			continue
		}
		successCount++
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Info("Successfully bulk updated %d/%d deposits", successCount, len(deposits))
	return nil
}
