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
	TxHash        string // Arbitrum transaction hash
	MonadTxHash   string // Monad transaction hash
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateTransaction creates a new transaction record in the database
func (db *DB) CreateTransaction(tx *Transaction) error {
	// Use RETURNING clause to get the inserted ID (PostgreSQL compatible)
	err := db.QueryRow(
		`INSERT INTO transaction_history 
		(deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`,
		tx.DepositID.String(),
		tx.WalletAddress.Hex(),
		tx.Amount.String(),
		tx.Currency,
		tx.MonAmount.String(),
		tx.Status,
		tx.TxHash,
		tx.MonadTxHash,
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
		SET status = $1, monad_tx_hash = $2, updated_at = CURRENT_TIMESTAMP 
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
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at 
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
		&tx.MonadTxHash,
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

// GetTransactionByArbitrumTxHash retrieves a transaction by its Arbitrum transaction hash
func (db *DB) GetTransactionByArbitrumTxHash(txHash string) (*Transaction, error) {
	// First, check if we have a transaction with this hash as the tx_hash
	var (
		tx                                                      Transaction
		depositIDStr, walletAddressStr, amountStr, monAmountStr string
		currencyInt                                             int
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at 
		FROM transaction_history 
		WHERE tx_hash = $1`,
		txHash,
	).Scan(
		&tx.ID,
		&depositIDStr,
		&walletAddressStr,
		&amountStr,
		&currencyInt,
		&monAmountStr,
		&tx.Status,
		&tx.TxHash,
		&tx.MonadTxHash,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			// Try a more flexible query to find any transaction with this hash
			// This will catch cases where the Arbitrum hash might be stored in a different way
			err := db.QueryRow(
				`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at 
				FROM transaction_history 
				WHERE tx_hash LIKE $1 OR monad_tx_hash LIKE $1`,
				"%"+txHash[2:]+"%", // Search for the hash without 0x prefix
			).Scan(
				&tx.ID,
				&depositIDStr,
				&walletAddressStr,
				&amountStr,
				&currencyInt,
				&monAmountStr,
				&tx.Status,
				&tx.TxHash,
				&tx.MonadTxHash,
				&tx.CreatedAt,
				&tx.UpdatedAt,
			)

			if err != nil {
				if err == sql.ErrNoRows {
					// Try to find it in the transaction_metadata table if it exists
					var depositIDStr string
					err = db.QueryRow(
						`SELECT deposit_id FROM transaction_metadata 
						WHERE key = 'arbitrum_tx_hash' AND value = $1`,
						txHash,
					).Scan(&depositIDStr)

					if err != nil {
						if err == sql.ErrNoRows {
							return nil, fmt.Errorf("transaction not found for Arbitrum tx hash: %s", txHash)
						}
						return nil, fmt.Errorf("failed to query transaction metadata: %w", err)
					}

					// Now get the transaction by deposit ID
					depositID, ok := new(big.Int).SetString(depositIDStr, 10)
					if !ok {
						return nil, fmt.Errorf("invalid deposit ID format in metadata: %s", depositIDStr)
					}

					return db.GetTransactionByDepositID(depositID)
				}
				return nil, fmt.Errorf("failed to get transaction with flexible query: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get transaction: %w", err)
		}
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
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at 
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
			&tx.MonadTxHash,
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
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at 
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
			&tx.MonadTxHash,
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

// StoreArbitrumTransactionTime stores the timestamp of an Arbitrum transaction
func (db *DB) StoreArbitrumTransactionTime(txHash string, timestamp time.Time) error {
	// First, try to get the deposit ID from the transaction history
	var depositID string
	err := db.QueryRow(
		`SELECT deposit_id FROM transaction_history WHERE tx_hash = $1`,
		txHash,
	).Scan(&depositID)

	// If not found in transaction history, use a placeholder
	if err != nil {
		if err == sql.ErrNoRows {
			depositID = "unknown"
		} else {
			return fmt.Errorf("failed to query transaction history: %w", err)
		}
	}

	// Store the transaction timestamp
	_, err = db.Exec(
		`INSERT INTO arbitrum_tx_timestamps (tx_hash, deposit_id, timestamp) 
		VALUES ($1, $2, $3)
		ON CONFLICT(tx_hash) DO UPDATE SET 
		deposit_id = $2, timestamp = $3`,
		txHash, depositID, timestamp,
	)

	if err != nil {
		return fmt.Errorf("failed to store Arbitrum transaction timestamp: %w", err)
	}

	return nil
}

// GetArbitrumTransactionByDepositID retrieves the Arbitrum transaction details by deposit ID
func (db *DB) GetArbitrumTransactionByDepositID(depositID *big.Int) (string, *time.Time, error) {
	var txHash string
	var timestamp time.Time

	err := db.QueryRow(
		`SELECT tx_hash, timestamp FROM arbitrum_tx_timestamps WHERE deposit_id = $1 ORDER BY timestamp DESC LIMIT 1`,
		depositID.String(),
	).Scan(&txHash, &timestamp)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil, fmt.Errorf("no Arbitrum transaction found for deposit ID: %s", depositID.String())
		}
		return "", nil, fmt.Errorf("failed to query Arbitrum transaction: %w", err)
	}

	return txHash, &timestamp, nil
}

// UpdateDepositIDForTransaction updates the deposit ID for a transaction in the timestamps table
func (db *DB) UpdateDepositIDForTransaction(txHash string, depositID *big.Int) error {
	_, err := db.Exec(
		`UPDATE arbitrum_tx_timestamps SET deposit_id = $1 WHERE tx_hash = $2`,
		depositID.String(), txHash,
	)

	if err != nil {
		return fmt.Errorf("failed to update deposit ID for transaction: %w", err)
	}

	return nil
}

// UpdateTransactionHash updates the transaction hash for a specific deposit ID
func (db *DB) UpdateTransactionHash(depositID *big.Int, txHash string) error {
	_, err := db.Exec(
		`UPDATE transaction_history 
		SET tx_hash = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $2`,
		txHash,
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update transaction hash: %w", err)
	}
	return nil
}

// UpdateMonadTransactionHash updates the Monad transaction hash for a specific deposit ID
func (db *DB) UpdateMonadTransactionHash(depositID *big.Int, monadTxHash string) error {
	_, err := db.Exec(
		`UPDATE transaction_history 
		SET monad_tx_hash = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $2`,
		monadTxHash,
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update Monad transaction hash: %w", err)
	}
	return nil
}

// GetTransactionByMonadTxHash retrieves a transaction by Monad transaction hash
func (db *DB) GetTransactionByMonadTxHash(monadTxHash string) (*Transaction, error) {
	var transaction Transaction
	var depositIDStr, walletAddressStr, amountStr, monAmountStr string
	var currencyInt int

	row := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at
		FROM transaction_history 
		WHERE monad_tx_hash = $1 
		LIMIT 1`,
		monadTxHash,
	)

	err := row.Scan(
		&transaction.ID,
		&depositIDStr,
		&walletAddressStr,
		&amountStr,
		&currencyInt,
		&monAmountStr,
		&transaction.Status,
		&transaction.TxHash,
		&transaction.MonadTxHash,
		&transaction.CreatedAt,
		&transaction.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan transaction: %w", err)
	}

	// Parse the big.Int fields
	depositID, success := new(big.Int).SetString(depositIDStr, 10)
	if !success {
		return nil, fmt.Errorf("failed to parse deposit_id: %s", depositIDStr)
	}
	transaction.DepositID = depositID

	// Parse wallet address
	transaction.WalletAddress = common.HexToAddress(walletAddressStr)

	// Parse amount
	amount, success := new(big.Int).SetString(amountStr, 10)
	if !success {
		return nil, fmt.Errorf("failed to parse amount: %s", amountStr)
	}
	transaction.Amount = amount

	// Set currency
	transaction.Currency = CurrencyType(currencyInt)

	// Parse MON amount
	monAmount, success := new(big.Int).SetString(monAmountStr, 10)
	if !success {
		return nil, fmt.Errorf("failed to parse mon_amount: %s", monAmountStr)
	}
	transaction.MonAmount = monAmount

	return &transaction, nil
}
