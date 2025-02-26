package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// WalletUsage represents a wallet's usage data
type WalletUsage struct {
	WalletAddress common.Address
	TotalAmount   *big.Int
	LastUpdated   time.Time
}

// GetWalletUsage retrieves a wallet's usage data from the database
func (db *DB) GetWalletUsage(wallet common.Address) (*WalletUsage, error) {
	var (
		totalAmount string
		lastUpdated time.Time
	)

	err := db.QueryRow(
		`SELECT total_amount, last_updated FROM wallet_usage WHERE wallet_address = $1`,
		wallet.Hex(),
	).Scan(&totalAmount, &lastUpdated)

	if err != nil {
		if err == sql.ErrNoRows {
			// Return a new wallet usage with zero amount if not found
			return &WalletUsage{
				WalletAddress: wallet,
				TotalAmount:   big.NewInt(0),
				LastUpdated:   time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to get wallet usage: %w", err)
	}

	amount, ok := new(big.Int).SetString(totalAmount, 10)
	if !ok {
		return nil, fmt.Errorf("invalid total amount format: %s", totalAmount)
	}

	return &WalletUsage{
		WalletAddress: wallet,
		TotalAmount:   amount,
		LastUpdated:   lastUpdated,
	}, nil
}

// UpdateWalletUsage updates a wallet's usage data in the database
func (db *DB) UpdateWalletUsage(usage *WalletUsage) error {
	_, err := db.Exec(
		`INSERT INTO wallet_usage (wallet_address, total_amount, last_updated) 
		VALUES ($1, $2, $3) 
		ON CONFLICT(wallet_address) DO UPDATE SET 
		total_amount = $4, last_updated = $5`,
		usage.WalletAddress.Hex(), usage.TotalAmount.String(), usage.LastUpdated,
		usage.TotalAmount.String(), usage.LastUpdated,
	)

	if err != nil {
		return fmt.Errorf("failed to update wallet usage: %w", err)
	}

	return nil
}

// ResetExpiredWalletUsage resets the total amount for wallets that haven't been updated in 24 hours
func (db *DB) ResetExpiredWalletUsage(wallet common.Address) error {
	usage, err := db.GetWalletUsage(wallet)
	if err != nil {
		return err
	}

	// If the wallet hasn't been used or the last usage was more than 24 hours ago,
	// reset the total amount to zero
	if usage.TotalAmount.Sign() > 0 && time.Since(usage.LastUpdated) > 24*time.Hour {
		usage.TotalAmount = big.NewInt(0)
		usage.LastUpdated = time.Now()
		return db.UpdateWalletUsage(usage)
	}

	return nil
}

// AddToWalletUsage adds an amount to a wallet's total usage
func (db *DB) AddToWalletUsage(wallet common.Address, amount *big.Int) error {
	// First, check if we need to reset the wallet usage due to expiration
	if err := db.ResetExpiredWalletUsage(wallet); err != nil {
		return err
	}

	// Get the current wallet usage
	usage, err := db.GetWalletUsage(wallet)
	if err != nil {
		return err
	}

	// Add the new amount to the total
	usage.TotalAmount = new(big.Int).Add(usage.TotalAmount, amount)
	usage.LastUpdated = time.Now()

	// Update the wallet usage in the database
	return db.UpdateWalletUsage(usage)
}
