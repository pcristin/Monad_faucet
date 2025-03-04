package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/pkg/logger"
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
	StatusProcessed = "processed"
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
	if depositID == nil {
		return fmt.Errorf("invalid deposit ID: nil")
	}

	depositIDStr := depositID.String()

	// Add explicit logging for debugging
	logger.Info("Updating transaction status for deposit ID %s: status=%s, txHash=%s",
		depositIDStr, status, txHash)

	// Implement retry logic (3 attempts)
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		// First check if the transaction exists
		exists := false
		err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM transaction_history WHERE deposit_id = $1)",
			depositIDStr).Scan(&exists)

		if err != nil {
			fmt.Printf("Error checking transaction existence (attempt %d/3): %v\n", attempt, err)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
			continue
		}

		if !exists {
			fmt.Printf("Transaction for deposit ID %s does not exist in the database\n", depositIDStr)
			// If transaction doesn't exist, create a minimal record to update later
			if status == "completed" && txHash != "" {
				_, insertErr := db.Exec(
					`INSERT INTO transaction_history 
					(deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash) 
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
					depositIDStr,
					"0x0000000000000000000000000000000000000000", // Placeholder, will be updated later
					"0", // Placeholder
					0,   // Default to ETH
					"0", // Placeholder
					status,
					"", // Will be filled later
					txHash,
				)
				if insertErr != nil {
					fmt.Printf("Error creating placeholder transaction record: %v\n", insertErr)
				} else {
					fmt.Printf("Created placeholder transaction record for deposit ID %s\n", depositIDStr)
					return nil
				}
			}
		}

		// Proceed with update
		result, err := db.Exec(
			`UPDATE transaction_history 
			SET status = $1, monad_tx_hash = $2, updated_at = CURRENT_TIMESTAMP 
			WHERE deposit_id = $3`,
			status,
			txHash,
			depositIDStr,
		)

		if err != nil {
			fmt.Printf("Error updating transaction status (attempt %d/3): %v\n", attempt, err)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
			continue
		}

		// Check if any rows were affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			fmt.Printf("Error getting rows affected: %v\n", err)
		} else if rowsAffected == 0 {
			fmt.Printf("No rows affected when updating transaction status for deposit ID %s\n", depositIDStr)
		} else {
			fmt.Printf("Successfully updated transaction status for deposit ID %s\n", depositIDStr)
			return nil
		}

		time.Sleep(time.Duration(attempt*100) * time.Millisecond)
	}

	return fmt.Errorf("failed to update transaction status after multiple attempts: %w", err)
}

// GetTransactionByDepositID retrieves a transaction by its deposit ID
func (db *DB) GetTransactionByDepositID(depositID *big.Int) (*Transaction, error) {
	if depositID == nil {
		return nil, fmt.Errorf("invalid deposit ID: nil")
	}

	depositIDStr := depositID.String()
	fmt.Printf("Getting transaction for deposit ID %s from database\n", depositIDStr)

	var (
		tx                                        Transaction
		walletAddressStr, amountStr, monAmountStr string
		currencyInt                               int
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at 
		FROM transaction_history 
		WHERE deposit_id = $1`,
		depositIDStr,
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
			fmt.Printf("Transaction not found for deposit ID: %s\n", depositIDStr)
			return nil, fmt.Errorf("transaction not found for deposit ID: %s", depositIDStr)
		}
		fmt.Printf("Error getting transaction: %v\n", err)
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// Convert strings to appropriate types
	var ok bool
	tx.DepositID, ok = new(big.Int).SetString(depositIDStr, 10)
	if !ok {
		fmt.Printf("Failed to parse deposit ID: %s\n", depositIDStr)
	}

	tx.WalletAddress = common.HexToAddress(walletAddressStr)

	tx.Amount, ok = new(big.Int).SetString(amountStr, 10)
	if !ok {
		logger.Warn("Failed to parse amount: %s for deposit ID %s, defaulting to 0",
			amountStr, depositIDStr)
		tx.Amount = big.NewInt(0)
	}

	tx.Currency = CurrencyType(currencyInt)

	// Handle potentially NULL mon_amount better
	if monAmountStr == "<nil>" || monAmountStr == "" {
		logger.Debug("MON amount is NULL for deposit ID %s, defaulting to 0", depositIDStr)
		tx.MonAmount = big.NewInt(0)
	} else {
		tx.MonAmount, ok = new(big.Int).SetString(monAmountStr, 10)
		if !ok {
			logger.Warn("Failed to parse MON amount: %s for deposit ID %s, defaulting to 0",
				monAmountStr, depositIDStr)
			tx.MonAmount = big.NewInt(0)
		}
	}

	logger.Debug("Found transaction for deposit ID %s: status=%s, monadTxHash=%s, monAmount=%s",
		depositIDStr, tx.Status, tx.MonadTxHash, tx.MonAmount.String())

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
					// No transaction found with this hash in any form
					return nil, fmt.Errorf("transaction not found for Arbitrum tx hash: %s", txHash)
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

// AcquireProcessingLock tries to acquire a distributed lock for a deposit ID
// Returns true if the lock was acquired, false if it already exists
func (db *DB) AcquireProcessingLock(depositID *big.Int, instanceID string, duration time.Duration) (bool, error) {
	// First, clean up any expired locks
	_, err := db.Exec(`DELETE FROM processing_locks WHERE expires_at < NOW()`)
	if err != nil {
		return false, fmt.Errorf("failed to clean expired locks: %w", err)
	}

	// Try to insert a new lock record
	result, err := db.Exec(`
		INSERT INTO processing_locks (deposit_id, instance_id, expires_at)
		VALUES ($1, $2, NOW() + $3::INTERVAL)
		ON CONFLICT (deposit_id) DO NOTHING
	`, depositID.String(), instanceID, fmt.Sprintf("%d seconds", int(duration.Seconds())))

	if err != nil {
		return false, fmt.Errorf("failed to acquire processing lock: %w", err)
	}

	// Check if we inserted a new row
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return rowsAffected > 0, nil
}

// ReleaseProcessingLock releases a lock for a deposit ID held by a specific instance
func (db *DB) ReleaseProcessingLock(depositID *big.Int, instanceID string) error {
	_, err := db.Exec(`
		DELETE FROM processing_locks
		WHERE deposit_id = $1 AND instance_id = $2
	`, depositID.String(), instanceID)

	if err != nil {
		return fmt.Errorf("failed to release processing lock: %w", err)
	}

	return nil
}

// RefreshProcessingLock extends the expiration time for an existing lock
func (db *DB) RefreshProcessingLock(depositID *big.Int, instanceID string, duration time.Duration) (bool, error) {
	result, err := db.Exec(`
		UPDATE processing_locks
		SET expires_at = NOW() + $3::INTERVAL
		WHERE deposit_id = $1 AND instance_id = $2
	`, depositID.String(), instanceID, fmt.Sprintf("%d seconds", int(duration.Seconds())))

	if err != nil {
		return false, fmt.Errorf("failed to refresh processing lock: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return rowsAffected > 0, nil
}

// UpdateTransactionStatusWithTx updates the status of a transaction within an existing database transaction
func (db *DB) UpdateTransactionStatusWithTx(tx *sql.Tx, depositID *big.Int, status, txHash string) error {
	_, err := tx.Exec(
		`UPDATE transaction_history 
		SET status = $1, monad_tx_hash = $2, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $3`,
		status,
		txHash,
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update transaction status in transaction: %w", err)
	}
	return nil
}

// GetLockedDeposits returns a list of deposit IDs that are currently locked
func (db *DB) GetLockedDeposits() ([]*big.Int, error) {
	rows, err := db.Query(`
		SELECT deposit_id FROM processing_locks
		WHERE expires_at > NOW()
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query locked deposits: %w", err)
	}
	defer rows.Close()

	var deposits []*big.Int
	for rows.Next() {
		var depositIDStr string
		if err := rows.Scan(&depositIDStr); err != nil {
			return nil, fmt.Errorf("failed to scan deposit ID: %w", err)
		}

		depositID, success := new(big.Int).SetString(depositIDStr, 10)
		if !success {
			return nil, fmt.Errorf("invalid deposit ID format: %s", depositIDStr)
		}

		deposits = append(deposits, depositID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating deposit rows: %w", err)
	}

	return deposits, nil
}

// GetTransactionsByStatus retrieves transactions with the specified status
// limit: maximum number of transactions to return (use 0 for no limit)
// offset: number of transactions to skip (use for pagination)
func (db *DB) GetTransactionsByStatus(status string, limit, offset int) ([]*Transaction, error) {
	query := `
		SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, tx_hash, monad_tx_hash, created_at, updated_at
		FROM transaction_history
		WHERE status = $1
		ORDER BY created_at DESC
	`

	args := []interface{}{status}

	if limit > 0 {
		query += " LIMIT $2"
		args = append(args, limit)

		if offset > 0 {
			query += " OFFSET $3"
			args = append(args, offset)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions by status: %w", err)
	}
	defer rows.Close()

	var transactions []*Transaction
	for rows.Next() {
		var tx Transaction
		var depositIDStr, amountStr, walletAddrStr string
		var monAmountStr sql.NullString // Use sql.NullString to handle NULL values

		err := rows.Scan(
			&tx.ID,
			&depositIDStr,
			&walletAddrStr,
			&amountStr,
			&tx.Currency,
			&monAmountStr, // This will handle NULL values properly
			&tx.Status,
			&tx.TxHash,
			&tx.MonadTxHash,
			&tx.CreatedAt,
			&tx.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}

		// Convert deposit_id string to *big.Int
		depositID, ok := new(big.Int).SetString(depositIDStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to convert deposit_id %s to big.Int", depositIDStr)
		}
		tx.DepositID = depositID

		// Convert amount string to *big.Int
		amount, ok := new(big.Int).SetString(amountStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to convert amount %s to big.Int", amountStr)
		}
		tx.Amount = amount

		// Convert mon_amount string to *big.Int - handle NULL values
		if monAmountStr.Valid {
			monAmount, ok := new(big.Int).SetString(monAmountStr.String, 10)
			if !ok {
				return nil, fmt.Errorf("failed to convert mon_amount %s to big.Int", monAmountStr.String)
			}
			tx.MonAmount = monAmount
		} else {
			// For NULL mon_amount, set to zero
			tx.MonAmount = big.NewInt(0)
			logger.Debug("Transaction ID %d for deposit ID %s has NULL mon_amount, setting to 0",
				tx.ID, depositIDStr)
		}

		// Convert wallet address string to common.Address
		tx.WalletAddress = common.HexToAddress(walletAddrStr)

		transactions = append(transactions, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transaction rows: %w", err)
	}

	return transactions, nil
}
