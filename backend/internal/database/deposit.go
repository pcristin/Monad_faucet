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
	_, err := db.Exec(
		`UPDATE deposits 
		SET status = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $2`,
		status,
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update deposit status: %w", err)
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
