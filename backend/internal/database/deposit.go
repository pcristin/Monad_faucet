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
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateDeposit creates a new deposit record in the database
func (db *DB) CreateDeposit(deposit *Deposit) error {
	// Use RETURNING clause to get the inserted ID (PostgreSQL compatible)
	err := db.QueryRow(
		`INSERT INTO deposits 
		(deposit_id, wallet_address, amount, currency, tx_hash, block_number, status) 
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`,
		deposit.DepositID.String(),
		deposit.WalletAddress.Hex(),
		deposit.Amount.String(),
		deposit.Currency,
		deposit.TxHash,
		deposit.BlockNumber,
		deposit.Status,
	).Scan(&deposit.ID)

	if err != nil {
		return fmt.Errorf("failed to create deposit: %w", err)
	}

	return nil
}

// UpdateDepositStatus updates the status of a deposit
func (db *DB) UpdateDepositStatus(depositID *big.Int, status string) error {
	if depositID == nil {
		return fmt.Errorf("deposit ID is nil")
	}

	depositIDStr := depositID.String()
	logger.Info("Updating deposit status for ID %s to %s", depositIDStr, status)

	// First check if the deposit exists
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM deposits WHERE deposit_id = $1)", depositIDStr).Scan(&exists)
	if err != nil {
		logger.Error("Error checking if deposit exists: %v", err)
		// Continue anyway to try the update
	} else if !exists {
		logger.Warn("Deposit ID %s not found in deposits table. Attempting to create from transaction record.", depositIDStr)

		// Try to find the transaction record to get the data we need
		tx, err := db.GetTransactionByDepositID(depositID)
		if err != nil || tx == nil {
			logger.Error("Could not find transaction record for deposit ID %s: %v", depositIDStr, err)
			return fmt.Errorf("deposit does not exist and could not create: %w", err)
		}

		// Create a minimal deposit record
		deposit := &Deposit{
			DepositID:     depositID,
			WalletAddress: tx.WalletAddress,
			Amount:        tx.Amount,
			Currency:      tx.Currency,
			TxHash:        tx.TxHash,
			BlockNumber:   0, // We don't have this info, but it's not critical
			Status:        status,
		}

		logger.Info("Creating deposit record for ID %s with wallet %s, amount %s",
			depositIDStr, deposit.WalletAddress.Hex(), deposit.Amount.String())

		if err := db.CreateDeposit(deposit); err != nil {
			logger.Error("Failed to create deposit record: %v", err)
			// Continue with the update anyway in case the error was due to race condition
		} else {
			logger.Info("Successfully created deposit record for ID %s", depositIDStr)
			return nil // We've created with the correct status, so no need to update
		}
	}

	// Proceed with the update
	// Use a transaction to ensure atomicity
	dbTx, err := db.Begin()
	if err != nil {
		logger.Error("Failed to begin transaction for updating deposit status: %v", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rbErr := dbTx.Rollback(); rbErr != nil {
				logger.Error("Failed to rollback transaction: %v", rbErr)
			}
		}
	}()

	// Get current status for logging
	var currentStatus string
	err = dbTx.QueryRow("SELECT status FROM deposits WHERE deposit_id = $1", depositIDStr).Scan(&currentStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("Deposit ID %s not found when checking current status", depositIDStr)
		} else {
			logger.Error("Error getting current status: %v", err)
		}
		// Continue with update anyway
	} else {
		logger.Info("Current status for deposit ID %s is %s, updating to %s", depositIDStr, currentStatus, status)
	}

	result, err := dbTx.Exec(
		`UPDATE deposits 
		SET status = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $2`,
		status,
		depositIDStr,
	)
	if err != nil {
		logger.Error("Error updating deposit status: %v", err)
		return fmt.Errorf("failed to update deposit status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		logger.Error("Error getting affected rows: %v", err)
	} else if rows == 0 {
		logger.Warn("No rows affected when updating deposit ID %s. Deposit might not exist.", depositIDStr)
	} else {
		logger.Info("Successfully updated status for deposit ID %s to %s (%d rows affected)", depositIDStr, status, rows)
	}

	// Commit the transaction
	if err = dbTx.Commit(); err != nil {
		logger.Error("Failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Double-check the update was successful
	var updatedStatus string
	err = db.QueryRow("SELECT status FROM deposits WHERE deposit_id = $1", depositIDStr).Scan(&updatedStatus)
	if err != nil {
		logger.Error("Failed to verify deposit status update: %v", err)
	} else if updatedStatus != status {
		logger.Error("Status verification failed: expected %s but got %s", status, updatedStatus)
	} else {
		logger.Info("Verified deposit status update: ID %s now has status %s", depositIDStr, updatedStatus)
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
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at 
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

// GetDepositByTxHash retrieves a deposit by its transaction hash
func (db *DB) GetDepositByTxHash(txHash string) (*Deposit, error) {
	var (
		deposit                                   Deposit
		depositIDStr, walletAddressStr, amountStr string
		currencyInt                               int
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at 
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
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at 
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
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at 
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
		`SELECT id, deposit_id, wallet_address, amount, currency, tx_hash, block_number, status, created_at, updated_at 
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
