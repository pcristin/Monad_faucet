package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// TransactionView represents a combined view of deposit and distribution data
// that maintains compatibility with the existing Transaction structure
type TransactionView struct {
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

// GetTransactionViewByDepositID retrieves transaction data by joining deposits and distributions
func (db *DB) GetTransactionViewByDepositID(depositID *big.Int) (*TransactionView, error) {
	if depositID == nil {
		return nil, fmt.Errorf("invalid deposit ID: nil")
	}

	depositIDStr := depositID.String()
	logger.Debug("Getting transaction view for deposit ID %s", depositIDStr)

	// First get the deposit
	deposit, err := db.GetDepositByID(depositID)
	if err != nil {
		return nil, fmt.Errorf("failed to get deposit: %w", err)
	}
	if deposit == nil {
		// Try to fall back to the legacy table
		tx, legacyErr := db.GetTransactionByDepositID(depositID)
		if legacyErr != nil {
			return nil, fmt.Errorf("deposit not found for ID: %s", depositIDStr)
		}
		return convertTransactionToView(tx), nil
	}

	// Then get the distribution
	distribution, err := db.GetDistributionByDepositID(depositID)
	if err != nil {
		logger.Warn("Error getting distribution for deposit ID %s: %v", depositIDStr, err)
	}

	// Create a TransactionView by combining deposit and distribution data
	view := &TransactionView{
		ID:            deposit.ID,
		DepositID:     deposit.DepositID,
		WalletAddress: deposit.WalletAddress,
		Amount:        deposit.Amount,
		Currency:      deposit.Currency,
		Status:        deposit.Status,
		TxHash:        deposit.TxHash,
		CreatedAt:     deposit.CreatedAt,
		UpdatedAt:     deposit.UpdatedAt,
	}

	// Add distribution data if available
	if distribution != nil {
		view.MonAmount = distribution.MonAmount
		view.MonadTxHash = distribution.MonadTxHash

		// Use the later update time between deposit and distribution
		if distribution.UpdatedAt.After(view.UpdatedAt) {
			view.UpdatedAt = distribution.UpdatedAt
		}

		// If the deposit is marked as processed, but the distribution is completed, use completed status
		if view.Status == StatusProcessed && distribution.Status == DistStatusCompleted {
			view.Status = StatusCompleted
		}
	} else {
		// No distribution record, initialize with zero
		view.MonAmount = big.NewInt(0)
	}

	logger.Debug("Found transaction view for deposit ID %s: status=%s, monadTxHash=%s, monAmount=%s",
		depositIDStr, view.Status, view.MonadTxHash, view.MonAmount.String())

	return view, nil
}

// GetTransactionViewByMonadTxHash retrieves transaction data by Monad transaction hash
func (db *DB) GetTransactionViewByMonadTxHash(monadTxHash string) (*TransactionView, error) {
	// First look in distributions table
	distribution, err := db.GetDistributionByMonadTxHash(monadTxHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get distribution: %w", err)
	}
	if distribution == nil {
		// Try to fall back to the legacy table
		tx, legacyErr := db.GetTransactionByMonadTxHash(monadTxHash)
		if legacyErr != nil || tx == nil {
			return nil, nil
		}
		return convertTransactionToView(tx), nil
	}

	// Get the corresponding deposit
	deposit, err := db.GetDepositByID(distribution.DepositID)
	if err != nil {
		return nil, fmt.Errorf("failed to get deposit: %w", err)
	}
	if deposit == nil {
		return nil, fmt.Errorf("deposit not found for distribution with Monad tx hash: %s", monadTxHash)
	}

	// Create a TransactionView by combining deposit and distribution data
	view := &TransactionView{
		ID:            deposit.ID,
		DepositID:     deposit.DepositID,
		WalletAddress: deposit.WalletAddress,
		Amount:        deposit.Amount,
		Currency:      deposit.Currency,
		MonAmount:     distribution.MonAmount,
		Status:        deposit.Status,
		TxHash:        deposit.TxHash,
		MonadTxHash:   distribution.MonadTxHash,
		CreatedAt:     deposit.CreatedAt,
		UpdatedAt:     deposit.UpdatedAt,
	}

	// Use the later update time between deposit and distribution
	if distribution.UpdatedAt.After(view.UpdatedAt) {
		view.UpdatedAt = distribution.UpdatedAt
	}

	// If the deposit is marked as pending, but the distribution is completed, use completed status
	if view.Status == StatusPending && distribution.Status == DistStatusCompleted {
		view.Status = StatusCompleted
	}

	return view, nil
}

// GetTransactionViewsByWallet retrieves all transactions for a wallet by joining deposits and distributions
func (db *DB) GetTransactionViewsByWallet(wallet common.Address, limit, offset int) ([]*TransactionView, error) {
	// Get deposits for this wallet
	deposits, err := db.GetDepositsByWallet(wallet, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get deposits: %w", err)
	}

	// Create a map to store distributions by deposit ID for quick lookups
	distributionMap := make(map[string]*Distribution)

	// Create a slice to collect deposit IDs for querying distributions
	var depositIDs []string
	for _, deposit := range deposits {
		depositIDs = append(depositIDs, deposit.DepositID.String())
	}

	// If we have deposit IDs, get their distributions
	if len(depositIDs) > 0 {
		// Get distributions for these deposit IDs
		// Since we don't have a bulk query function, we'll need to query one by one
		for _, depID := range depositIDs {
			depIDInt, ok := new(big.Int).SetString(depID, 10)
			if !ok {
				continue
			}

			dist, err := db.GetDistributionByDepositID(depIDInt)
			if err != nil {
				logger.Warn("Error getting distribution for deposit ID %s: %v", depID, err)
				continue
			}

			if dist != nil {
				distributionMap[depID] = dist
			}
		}
	}

	// Create transaction views by combining deposits and distributions
	var views []*TransactionView
	for _, deposit := range deposits {
		view := &TransactionView{
			ID:            deposit.ID,
			DepositID:     deposit.DepositID,
			WalletAddress: deposit.WalletAddress,
			Amount:        deposit.Amount,
			Currency:      deposit.Currency,
			Status:        deposit.Status,
			TxHash:        deposit.TxHash,
			CreatedAt:     deposit.CreatedAt,
			UpdatedAt:     deposit.UpdatedAt,
			MonAmount:     big.NewInt(0), // Default to zero
		}

		// Add distribution data if available
		depositIDStr := deposit.DepositID.String()
		if dist, exists := distributionMap[depositIDStr]; exists {
			view.MonAmount = dist.MonAmount
			view.MonadTxHash = dist.MonadTxHash

			// Use the later update time
			if dist.UpdatedAt.After(view.UpdatedAt) {
				view.UpdatedAt = dist.UpdatedAt
			}

			// If the deposit is pending but the distribution is completed, use completed status
			if view.Status == StatusPending && dist.Status == DistStatusCompleted {
				view.Status = StatusCompleted
			}
		}

		views = append(views, view)
	}

	// If we don't have any views, try the legacy table as a fallback
	if len(views) == 0 {
		legacyTxs, legacyErr := db.GetTransactionsByWallet(wallet, limit, offset)
		if legacyErr == nil && len(legacyTxs) > 0 {
			for _, tx := range legacyTxs {
				views = append(views, convertTransactionToView(tx))
			}
		}
	}

	return views, nil
}

// GetRecentTransactionViews retrieves recent transactions by joining deposits and distributions
func (db *DB) GetRecentTransactionViews(limit, offset int) ([]*TransactionView, error) {
	// Get recent deposits
	deposits, err := db.GetRecentDeposits(limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent deposits: %w", err)
	}

	// Create a map to store distributions by deposit ID for quick lookups
	distributionMap := make(map[string]*Distribution)

	// Create a slice to collect deposit IDs for querying distributions
	var depositIDs []string
	for _, deposit := range deposits {
		depositIDs = append(depositIDs, deposit.DepositID.String())
	}

	// If we have deposit IDs, get their distributions
	if len(depositIDs) > 0 {
		// Get distributions for these deposit IDs
		for _, depID := range depositIDs {
			depIDInt, ok := new(big.Int).SetString(depID, 10)
			if !ok {
				continue
			}

			dist, err := db.GetDistributionByDepositID(depIDInt)
			if err != nil {
				logger.Warn("Error getting distribution for deposit ID %s: %v", depID, err)
				continue
			}

			if dist != nil {
				distributionMap[depID] = dist
			}
		}
	}

	// Create transaction views by combining deposits and distributions
	var views []*TransactionView
	for _, deposit := range deposits {
		view := &TransactionView{
			ID:            deposit.ID,
			DepositID:     deposit.DepositID,
			WalletAddress: deposit.WalletAddress,
			Amount:        deposit.Amount,
			Currency:      deposit.Currency,
			Status:        deposit.Status,
			TxHash:        deposit.TxHash,
			CreatedAt:     deposit.CreatedAt,
			UpdatedAt:     deposit.UpdatedAt,
			MonAmount:     big.NewInt(0), // Default to zero
		}

		// Add distribution data if available
		depositIDStr := deposit.DepositID.String()
		if dist, exists := distributionMap[depositIDStr]; exists {
			view.MonAmount = dist.MonAmount
			view.MonadTxHash = dist.MonadTxHash

			// Use the later update time
			if dist.UpdatedAt.After(view.UpdatedAt) {
				view.UpdatedAt = dist.UpdatedAt
			}

			// If the deposit is pending but the distribution is completed, use completed status
			if view.Status == StatusPending && dist.Status == DistStatusCompleted {
				view.Status = StatusCompleted
			}
		}

		views = append(views, view)
	}

	// If we don't have any views, try the legacy table as a fallback
	if len(views) == 0 {
		legacyTxs, legacyErr := db.GetRecentTransactions(limit, offset)
		if legacyErr == nil && len(legacyTxs) > 0 {
			for _, tx := range legacyTxs {
				views = append(views, convertTransactionToView(tx))
			}
		}
	}

	return views, nil
}

// GetTransactionViewsByStatus retrieves transactions with the specified status
func (db *DB) GetTransactionViewsByStatus(status string, limit, offset int) ([]*TransactionView, error) {
	// Get deposits with this status
	deposits, err := db.GetDepositsByStatus(status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get deposits by status: %w", err)
	}

	// Create a map to store distributions by deposit ID for quick lookups
	distributionMap := make(map[string]*Distribution)

	// Create a slice to collect deposit IDs for querying distributions
	var depositIDs []string
	for _, deposit := range deposits {
		depositIDs = append(depositIDs, deposit.DepositID.String())
	}

	// If we have deposit IDs, get their distributions
	if len(depositIDs) > 0 {
		// Get distributions for these deposit IDs
		for _, depID := range depositIDs {
			depIDInt, ok := new(big.Int).SetString(depID, 10)
			if !ok {
				continue
			}

			dist, err := db.GetDistributionByDepositID(depIDInt)
			if err != nil {
				logger.Warn("Error getting distribution for deposit ID %s: %v", depID, err)
				continue
			}

			if dist != nil {
				distributionMap[depID] = dist
			}
		}
	}

	// If we're looking for completed transactions, we should also check for deposits that might
	// be marked as processed but have completed distributions
	if status == StatusCompleted {
		processedDeposits, err := db.GetDepositsByStatus(StatusProcessed, limit, offset)
		if err == nil && len(processedDeposits) > 0 {
			for _, deposit := range processedDeposits {
				depositIDStr := deposit.DepositID.String()
				dist, err := db.GetDistributionByDepositID(deposit.DepositID)
				if err == nil && dist != nil && dist.Status == DistStatusCompleted {
					// This is effectively a completed transaction
					deposits = append(deposits, deposit)
					distributionMap[depositIDStr] = dist
				}
			}
		}
	}

	// Create transaction views by combining deposits and distributions
	var views []*TransactionView
	for _, deposit := range deposits {
		view := &TransactionView{
			ID:            deposit.ID,
			DepositID:     deposit.DepositID,
			WalletAddress: deposit.WalletAddress,
			Amount:        deposit.Amount,
			Currency:      deposit.Currency,
			Status:        deposit.Status,
			TxHash:        deposit.TxHash,
			CreatedAt:     deposit.CreatedAt,
			UpdatedAt:     deposit.UpdatedAt,
			MonAmount:     big.NewInt(0), // Default to zero
		}

		// Add distribution data if available
		depositIDStr := deposit.DepositID.String()
		if dist, exists := distributionMap[depositIDStr]; exists {
			view.MonAmount = dist.MonAmount
			view.MonadTxHash = dist.MonadTxHash

			// Use the later update time
			if dist.UpdatedAt.After(view.UpdatedAt) {
				view.UpdatedAt = dist.UpdatedAt
			}

			// If the deposit is pending but the distribution is completed, use completed status
			if view.Status == StatusPending && dist.Status == DistStatusCompleted {
				view.Status = StatusCompleted
			}
		}

		views = append(views, view)
	}

	// If we don't have any views, try the legacy table as a fallback
	if len(views) == 0 {
		legacyTxs, legacyErr := db.GetTransactionsByStatus(status, limit, offset)
		if legacyErr == nil && len(legacyTxs) > 0 {
			for _, tx := range legacyTxs {
				views = append(views, convertTransactionToView(tx))
			}
		}
	}

	return views, nil
}

// GetTransactionViewByArbitrumTxHash returns a transaction view by Arbitrum transaction hash
func (db *DB) GetTransactionViewByArbitrumTxHash(txHash string) (*TransactionView, error) {
	// First check deposits table
	deposit, err := db.GetDepositByTxHash(txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to check deposits table: %w", err)
	}

	if deposit != nil {
		// Found deposit with this hash, now get full view
		return db.GetTransactionViewByDepositID(deposit.DepositID)
	}

	// If not found in deposits, check legacy table as fallback
	query := `
		SELECT id, deposit_id, wallet_address, amount, currency, mon_amount, status, 
		       tx_hash, monad_tx_hash, created_at, updated_at
		FROM transaction_history
		WHERE tx_hash = $1
		LIMIT 1
	`

	var tx Transaction
	var depositIDStr string
	var walletAddressStr string
	var amountStr string
	var currencyInt int
	var monAmountStr sql.NullString

	err = db.QueryRow(query, txHash).Scan(
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
			// No record found, return nil without error
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query transaction by arbitrum hash: %w", err)
	}

	// Convert strings to appropriate types
	var ok bool
	tx.DepositID, ok = new(big.Int).SetString(depositIDStr, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse deposit ID: %s", depositIDStr)
	}

	tx.WalletAddress = common.HexToAddress(walletAddressStr)

	tx.Amount, ok = new(big.Int).SetString(amountStr, 10)
	if !ok {
		logger.Warn("Failed to parse amount: %s for deposit ID %s, defaulting to 0",
			amountStr, depositIDStr)
		tx.Amount = big.NewInt(0)
	}

	tx.Currency = CurrencyType(currencyInt)

	// Handle NULL mon_amount values using sql.NullString
	if monAmountStr.Valid {
		tx.MonAmount, ok = new(big.Int).SetString(monAmountStr.String, 10)
		if !ok {
			logger.Warn("Failed to parse MON amount: %s for deposit ID %s, defaulting to 0",
				monAmountStr.String, depositIDStr)
			tx.MonAmount = big.NewInt(0)
		}
	} else {
		logger.Debug("MON amount is NULL for deposit ID %s, defaulting to 0", depositIDStr)
		tx.MonAmount = big.NewInt(0)
	}

	return convertTransactionToView(&tx), nil
}

// Helper function to convert a Transaction to a TransactionView
func convertTransactionToView(tx *Transaction) *TransactionView {
	if tx == nil {
		return nil
	}

	return &TransactionView{
		ID:            tx.ID,
		DepositID:     tx.DepositID,
		WalletAddress: tx.WalletAddress,
		Amount:        tx.Amount,
		Currency:      tx.Currency,
		MonAmount:     tx.MonAmount,
		Status:        tx.Status,
		TxHash:        tx.TxHash,
		MonadTxHash:   tx.MonadTxHash,
		CreatedAt:     tx.CreatedAt,
		UpdatedAt:     tx.UpdatedAt,
	}
}
