package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurrencyType represents the type of currency
type CurrencyType int

// Currency types
const (
	CurrencyETH CurrencyType = iota
	CurrencyUSDC
	CurrencyUSDT
)

// Transaction status constants
const (
	StatusPending   = "pending"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusRefunded  = "refunded"
)

// Transaction represents a transaction in the history
type Transaction struct {
	ID            int64
	DepositID     *big.Int
	WalletAddress common.Address
	Amount        *big.Int
	Currency      CurrencyType
	MonAmount     *big.Int
	Status        string
	TxHash        string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateTransaction creates a new transaction record in the database
func (db *DB) CreateTransaction(tx *Transaction) error {
	// Use RETURNING clause to get the inserted ID (PostgreSQL compatible)
	err := db.QueryRow(
		`INSERT INTO transaction_history 
		(deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash) 
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`,
		tx.DepositID.String(),
		tx.WalletAddress.Hex(),
		tx.Amount.String(),
		tx.Currency,
		tx.MonAmount.String(),
		tx.Status,
		tx.TxHash,
	).Scan(&tx.ID)

	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	return nil
}

// UpdateTransactionStatus updates the status of a transaction
func (db *DB) UpdateTransactionStatus(depositID *big.Int, status, txHash string) error {
	_, err := db.Exec(
		`UPDATE transaction_history 
		SET status = $1, tx_hash = $2, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $3`,
		status,
		txHash,
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}
	return nil
}

// GetTransactionByDepositID retrieves a transaction by its deposit ID
func (db *DB) GetTransactionByDepositID(depositID *big.Int) (*Transaction, error) {
	var (
		tx                                                      Transaction
		depositIDStr, walletAddressStr, amountStr, monAmountStr string
		currencyInt                                             int
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, created_at, updated_at 
		FROM transaction_history 
		WHERE deposit_id = $1`,
		depositID.String(),
	).Scan(
		&tx.ID,
		&depositIDStr,
		&walletAddressStr,
		&amountStr,
		&currencyInt,
		&monAmountStr,
		&tx.Status,
		&tx.TxHash,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("transaction not found for deposit ID: %s", depositID.String())
		}
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// Convert strings to appropriate types
	tx.DepositID, _ = new(big.Int).SetString(depositIDStr, 10)
	tx.WalletAddress = common.HexToAddress(walletAddressStr)
	tx.Amount, _ = new(big.Int).SetString(amountStr, 10)
	tx.Currency = CurrencyType(currencyInt)
	tx.MonAmount, _ = new(big.Int).SetString(monAmountStr, 10)

	return &tx, nil
}

// GetTransactionsByWallet retrieves all transactions for a wallet
func (db *DB) GetTransactionsByWallet(wallet common.Address, limit, offset int) ([]*Transaction, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, created_at, updated_at 
		FROM transaction_history 
		WHERE wallet_address = $1 
		ORDER BY created_at DESC 
		LIMIT $2 OFFSET $3`,
		wallet.Hex(),
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []*Transaction
	for rows.Next() {
		var (
			tx                                                      Transaction
			depositIDStr, walletAddressStr, amountStr, monAmountStr string
			currencyInt                                             int
		)

		err := rows.Scan(
			&tx.ID,
			&depositIDStr,
			&walletAddressStr,
			&amountStr,
			&currencyInt,
			&monAmountStr,
			&tx.Status,
			&tx.TxHash,
			&tx.CreatedAt,
			&tx.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}

		// Convert strings to appropriate types
		tx.DepositID, _ = new(big.Int).SetString(depositIDStr, 10)
		tx.WalletAddress = common.HexToAddress(walletAddressStr)
		tx.Amount, _ = new(big.Int).SetString(amountStr, 10)
		tx.Currency = CurrencyType(currencyInt)
		tx.MonAmount, _ = new(big.Int).SetString(monAmountStr, 10)

		transactions = append(transactions, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transaction rows: %w", err)
	}

	return transactions, nil
}

// GetRecentTransactions retrieves recent transactions
func (db *DB) GetRecentTransactions(limit, offset int) ([]*Transaction, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, created_at, updated_at 
		FROM transaction_history 
		ORDER BY created_at DESC 
		LIMIT $1 OFFSET $2`,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []*Transaction
	for rows.Next() {
		var (
			tx                                                      Transaction
			depositIDStr, walletAddressStr, amountStr, monAmountStr string
			currencyInt                                             int
		)

		err := rows.Scan(
			&tx.ID,
			&depositIDStr,
			&walletAddressStr,
			&amountStr,
			&currencyInt,
			&monAmountStr,
			&tx.Status,
			&tx.TxHash,
			&tx.CreatedAt,
			&tx.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}

		// Convert strings to appropriate types
		tx.DepositID, _ = new(big.Int).SetString(depositIDStr, 10)
		tx.WalletAddress = common.HexToAddress(walletAddressStr)
		tx.Amount, _ = new(big.Int).SetString(amountStr, 10)
		tx.Currency = CurrencyType(currencyInt)
		tx.MonAmount, _ = new(big.Int).SetString(monAmountStr, 10)

		transactions = append(transactions, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transaction rows: %w", err)
	}

	return transactions, nil
}

// CreateIndexes creates necessary indexes for performance optimization
func (db *DB) CreateIndexes() error {
	// Index for wallet_address lookups
	_, err := db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_transaction_history_wallet_address 
		ON transaction_history(wallet_address)
	`)
	if err != nil {
		return fmt.Errorf("failed to create wallet_address index: %w", err)
	}

	// Index for created_at sorting (recent transactions)
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_transaction_history_created_at 
		ON transaction_history(created_at DESC)
	`)
	if err != nil {
		return fmt.Errorf("failed to create created_at index: %w", err)
	}

	// Index for status queries
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_transaction_history_status 
		ON transaction_history(status)
	`)
	if err != nil {
		return fmt.Errorf("failed to create status index: %w", err)
	}

	return nil
}

// Ping checks if the database connection is alive
func (db *DB) Ping() error {
	// For SQL databases, we can use the Ping method
	return db.DB.Ping()
}
