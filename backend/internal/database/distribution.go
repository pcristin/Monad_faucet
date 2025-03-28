package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Distribution status constants
const (
	DistStatusPending   = "pending"
	DistStatusCompleted = "completed"
	DistStatusFailed    = "failed"
)

// Distribution represents a token distribution transaction on Monad
type Distribution struct {
	ID            int64
	DepositID     *big.Int
	WalletAddress common.Address
	MonAmount     *big.Int
	Status        string
	MonadTxHash   string // Monad transaction hash
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateDistribution creates a new distribution record in the database
func (db *DB) CreateDistribution(dist *Distribution) error {
	// First, explicitly check for nil MonAmount (critical check)
	if dist.MonAmount == nil {
		return fmt.Errorf("mon_amount cannot be nil, required for distribution creation")
	}

	// Make sure we have a valid MonAmount string to avoid SQL NULL issues
	monAmountStr := dist.MonAmount.String()
	if monAmountStr == "" || monAmountStr == "0" {
		return fmt.Errorf("invalid mon_amount value: %s", monAmountStr)
	}

	// Use Postgres ON CONFLICT to handle duplicate deposit IDs
	// This is essentially an UPSERT operation that will do an INSERT if the record doesn't exist,
	// or an UPDATE if it does exist
	err := db.QueryRow(
		`INSERT INTO distributions 
		(deposit_id, wallet_address, mon_amount, status, monad_tx_hash) 
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (deposit_id) DO UPDATE SET
		mon_amount = CASE 
			WHEN $3 IS NOT NULL AND distributions.mon_amount IS NULL THEN $3
			ELSE distributions.mon_amount
		END,
		status = CASE
			WHEN $4 = 'completed' THEN $4 
			WHEN distributions.status = 'pending' AND $4 = 'failed' THEN $4
			ELSE distributions.status
		END,
		monad_tx_hash = CASE
			WHEN $5 <> '' AND (distributions.monad_tx_hash IS NULL OR distributions.monad_tx_hash = '') THEN $5
			ELSE distributions.monad_tx_hash
		END,
		updated_at = CURRENT_TIMESTAMP
		RETURNING id`,
		dist.DepositID.String(),
		dist.WalletAddress.Hex(),
		monAmountStr, // Use the validated string directly
		dist.Status,
		dist.MonadTxHash,
	).Scan(&dist.ID)

	if err != nil {
		return fmt.Errorf("failed to create distribution: %w", err)
	}
	logger.Info("Successfully created or updated distribution record for deposit ID %s", dist.DepositID.String())
	return nil
}

// UpdateDistributionStatus updates the status of a distribution
func (db *DB) UpdateDistributionStatus(depositID *big.Int, status, txHash string) error {
	_, err := db.Exec(
		`UPDATE distributions 
		SET status = $1, monad_tx_hash = $2, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $3`,
		status,
		txHash,
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update distribution status: %w", err)
	}
	return nil
}

// GetDistributionByDepositID retrieves a distribution by its deposit ID
func (db *DB) GetDistributionByDepositID(depositID *big.Int) (*Distribution, error) {
	var (
		dist                                         Distribution
		depositIDStr, walletAddressStr, monAmountStr string
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at 
		FROM distributions 
		WHERE deposit_id = $1`,
		depositID.String(),
	).Scan(
		&dist.ID,
		&depositIDStr,
		&walletAddressStr,
		&monAmountStr,
		&dist.Status,
		&dist.MonadTxHash,
		&dist.CreatedAt,
		&dist.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get distribution: %w", err)
	}

	// Parse the big.Int and common.Address fields
	dist.DepositID = new(big.Int)
	dist.DepositID.SetString(depositIDStr, 10)
	dist.WalletAddress = common.HexToAddress(walletAddressStr)
	dist.MonAmount = new(big.Int)
	dist.MonAmount.SetString(monAmountStr, 10)

	return &dist, nil
}

// GetDistributionByMonadTxHash retrieves a distribution by its Monad transaction hash
func (db *DB) GetDistributionByMonadTxHash(txHash string) (*Distribution, error) {
	var (
		dist                                         Distribution
		depositIDStr, walletAddressStr, monAmountStr string
	)

	err := db.QueryRow(
		`SELECT id, deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at 
		FROM distributions 
		WHERE monad_tx_hash = $1`,
		txHash,
	).Scan(
		&dist.ID,
		&depositIDStr,
		&walletAddressStr,
		&monAmountStr,
		&dist.Status,
		&dist.MonadTxHash,
		&dist.CreatedAt,
		&dist.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get distribution: %w", err)
	}

	// Parse the big.Int and common.Address fields
	dist.DepositID = new(big.Int)
	dist.DepositID.SetString(depositIDStr, 10)
	dist.WalletAddress = common.HexToAddress(walletAddressStr)
	dist.MonAmount = new(big.Int)
	dist.MonAmount.SetString(monAmountStr, 10)

	return &dist, nil
}

// GetDistributionsByWallet retrieves all distributions for a specific wallet
func (db *DB) GetDistributionsByWallet(wallet common.Address, limit, offset int) ([]*Distribution, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at 
		FROM distributions 
		WHERE wallet_address = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`,
		wallet.Hex(),
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get distributions by wallet: %w", err)
	}
	defer rows.Close()

	var distributions []*Distribution
	for rows.Next() {
		var (
			dist                                         Distribution
			depositIDStr, walletAddressStr, monAmountStr string
		)

		err := rows.Scan(
			&dist.ID,
			&depositIDStr,
			&walletAddressStr,
			&monAmountStr,
			&dist.Status,
			&dist.MonadTxHash,
			&dist.CreatedAt,
			&dist.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan distribution: %w", err)
		}

		// Parse the big.Int and common.Address fields
		dist.DepositID = new(big.Int)
		dist.DepositID.SetString(depositIDStr, 10)
		dist.WalletAddress = common.HexToAddress(walletAddressStr)
		dist.MonAmount = new(big.Int)
		dist.MonAmount.SetString(monAmountStr, 10)

		distributions = append(distributions, &dist)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating distributions: %w", err)
	}

	return distributions, nil
}

// GetRecentDistributions retrieves the most recent distributions
func (db *DB) GetRecentDistributions(limit, offset int) ([]*Distribution, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at 
		FROM distributions 
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2`,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent distributions: %w", err)
	}
	defer rows.Close()

	var distributions []*Distribution
	for rows.Next() {
		var (
			dist                                         Distribution
			depositIDStr, walletAddressStr, monAmountStr string
		)

		err := rows.Scan(
			&dist.ID,
			&depositIDStr,
			&walletAddressStr,
			&monAmountStr,
			&dist.Status,
			&dist.MonadTxHash,
			&dist.CreatedAt,
			&dist.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan distribution: %w", err)
		}

		// Parse the big.Int and common.Address fields
		dist.DepositID = new(big.Int)
		dist.DepositID.SetString(depositIDStr, 10)
		dist.WalletAddress = common.HexToAddress(walletAddressStr)
		dist.MonAmount = new(big.Int)
		dist.MonAmount.SetString(monAmountStr, 10)

		distributions = append(distributions, &dist)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating distributions: %w", err)
	}

	return distributions, nil
}

// GetDistributionsByStatus retrieves distributions by their status
func (db *DB) GetDistributionsByStatus(status string, limit, offset int) ([]*Distribution, error) {
	rows, err := db.Query(
		`SELECT id, deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at 
		FROM distributions 
		WHERE status = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`,
		status,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get distributions by status: %w", err)
	}
	defer rows.Close()

	var distributions []*Distribution
	for rows.Next() {
		var (
			dist                                         Distribution
			depositIDStr, walletAddressStr, monAmountStr string
		)

		err := rows.Scan(
			&dist.ID,
			&depositIDStr,
			&walletAddressStr,
			&monAmountStr,
			&dist.Status,
			&dist.MonadTxHash,
			&dist.CreatedAt,
			&dist.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan distribution: %w", err)
		}

		// Parse the big.Int and common.Address fields
		dist.DepositID = new(big.Int)
		dist.DepositID.SetString(depositIDStr, 10)
		dist.WalletAddress = common.HexToAddress(walletAddressStr)
		dist.MonAmount = new(big.Int)
		dist.MonAmount.SetString(monAmountStr, 10)

		distributions = append(distributions, &dist)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating distributions: %w", err)
	}

	return distributions, nil
}

// UpdateDistributionWithAmount updates the status, tx hash and MON amount of a distribution
func (db *DB) UpdateDistributionWithAmount(depositID *big.Int, status, txHash string, monAmount *big.Int) error {
	if monAmount == nil {
		return fmt.Errorf("mon amount is nil")
	}

	_, err := db.Exec(
		`UPDATE distributions 
		SET status = $1, monad_tx_hash = $2, mon_amount = $3, updated_at = CURRENT_TIMESTAMP 
		WHERE deposit_id = $4`,
		status,
		txHash,
		monAmount.String(),
		depositID.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to update distribution with amount: %w", err)
	}
	return nil
}
