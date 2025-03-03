package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
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
	fmt.Printf("Updating deposit status for ID %s to %s\n", depositIDStr, status)

	// First check if the deposit exists
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM deposits WHERE deposit_id = $1)", depositIDStr).Scan(&exists)
	if err != nil {
		fmt.Printf("Error checking if deposit exists: %v\n", err)
		// Continue anyway to try the update
	} else if !exists {
		fmt.Printf("Deposit ID %s not found in deposits table. Attempting to create from transaction record.\n", depositIDStr)

		// Try to find the transaction record to get the data we need
		tx, err := db.GetTransactionByDepositID(depositID)
		if err != nil || tx == nil {
			fmt.Printf("Could not find transaction record for deposit ID %s: %v\n", depositIDStr, err)
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

		fmt.Printf("Creating deposit record for ID %s with wallet %s, amount %s\n",
			depositIDStr, deposit.WalletAddress.Hex(), deposit.Amount.String())

		if err := db.CreateDeposit(deposit); err != nil {
			fmt.Printf("Failed to create deposit record: %v\n", err)
			// Continue with the update anyway in case the error was due to race condition
		} else {
			fmt.Printf("Successfully created deposit record for ID %s\n", depositIDStr)
			return nil // We've created with the correct status, so no need to update
		}
	}

	// Proceed with the update
	result, err := db.Exec(
		`UPDATE deposits 
		SET status = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $2`,
		status,
		depositIDStr,
	)
	if err != nil {
		fmt.Printf("Error updating deposit status: %v\n", err)
		return fmt.Errorf("failed to update deposit status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		fmt.Printf("Error getting affected rows: %v\n", err)
	} else if rows == 0 {
		fmt.Printf("Warning: No rows affected when updating deposit ID %s. Deposit might not exist.\n", depositIDStr)
	} else {
		fmt.Printf("Successfully updated status for deposit ID %s to %s (%d rows affected)\n", depositIDStr, status, rows)
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
