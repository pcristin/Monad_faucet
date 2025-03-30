package bridge

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/blockchain/listener"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Validation, Confirmation, and Minting ---
//

// validateDepositWithAmount validates a deposit using pre-calculated MON amount.
func (s *BridgeService) validateDepositWithAmount(state *blockchain.ContractState, event listener.DepositEvent, monAmount *big.Int) error {
	if state.IsPaused {
		return fmt.Errorf("bridge is paused")
	}
	if state.MonBalance.Cmp(monAmount) < 0 {
		return fmt.Errorf("insufficient MON balance in distributor")
	}

	return nil
}

// mintTokens mints MON tokens on the Monad blockchain.
func (s *BridgeService) mintTokens(ctx context.Context, recipient common.Address, amount *big.Int, depositId *big.Int) (string, error) {
	depositIDStr := depositId.String()
	logger.Info("Acquiring lock for deposit ID %s in mintTokens", depositIDStr)

	// Validate the amount before proceeding
	if amount == nil || amount.Cmp(big.NewInt(0)) <= 0 {
		return "", fmt.Errorf("invalid amount for minting: %v", amount)
	}

	// Log the current amount for debugging
	logger.Info("Minting amount for deposit ID %s: %s Wei", depositIDStr, amount.String())

	// Safety check - ensure amount is reasonable
	minAmount := big.NewInt(1000000) // 0.001 MON minimum
	if amount.Cmp(minAmount) < 0 {
		logger.Warn("Amount %s is too small, using minimum amount %s", amount.String(), minAmount.String())
		amount = new(big.Int).Set(minAmount)
	}

	// Check for existing transaction before acquiring lock
	if tx, err := s.GetTransactionByDepositID(ctx, depositId); err == nil && tx != nil && tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
		logger.Info("Duplicate prevention: deposit ID %s already processed", depositIDStr)
		return tx.MonadTxHash, nil
	}

	// Acquire lock for processing
	acquired, err := s.acquireLockWithRetries(ctx, depositId)
	if err != nil || !acquired {
		// Retry duplicate check if lock not acquired.
		for retryCount := 0; retryCount < 5; retryCount++ {
			time.Sleep(2 * time.Second)
			if tx, err := s.GetTransactionByDepositID(ctx, depositId); err == nil && tx != nil && tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
				logger.Info("Duplicate prevention: deposit ID %s already processed", depositIDStr)
				return tx.MonadTxHash, nil
			}
			if txHash, _ := s.checkMonadBlockchainForTransaction(ctx, depositId); txHash != "" {
				logger.Info("Duplicate prevention (blockchain): found tx %s for deposit ID %s", txHash, depositIDStr)
				_ = s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, txHash)
				return txHash, nil
			}
		}
		return "", fmt.Errorf("failed to acquire processing lock and verify duplicate: %v", err)
	}
	// Ensure the lock is released when done.
	defer s.releaseLock(depositId)

	if txHash, exists := s.checkExistingTransaction(ctx, depositId); exists {
		logger.Info("Duplicate prevention in mintTokens: deposit ID %s already processed with tx %s", depositIDStr, txHash)
		return txHash, nil
	}

	// Check transaction cache.
	txLookupKey := depositId.String()
	s.txCacheMutex.RLock()
	if cached, found := s.txCache[txLookupKey]; found && cached.Status == database.StatusCompleted && cached.MonadTxHash != "" {
		s.txCacheMutex.RUnlock()
		logger.Info("Using cached tx hash %s for deposit ID %s", cached.MonadTxHash, depositIDStr)
		return cached.MonadTxHash, nil
	}
	s.txCacheMutex.RUnlock()

	// Mark as pending in DB.
	if existingTx, err := s.GetTransactionByDepositID(ctx, depositId); existingTx == nil || existingTx.Status != database.StatusPending {
		if err != nil {
			logger.Error("Failed to get transaction by deposit ID: %v", err)
		}
		if err := s.UpdateTransactionStatus(ctx, depositId, database.StatusPending, ""); err != nil {
			logger.Error("Failed to mark tx as pending: %v", err)
		} else {
			logger.Info("Marked transaction as pending for deposit ID %s", depositIDStr)
		}
		time.Sleep(1 * time.Second) // Allow race conditions to resolve.
		if txHash, _ := s.checkMonadBlockchainForTransaction(ctx, depositId); txHash != "" {
			logger.Info("Race condition check: found existing tx %s for deposit ID %s", txHash, depositIDStr)
			_ = s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, txHash)
			return txHash, nil
		}
	}

	// Proceed with minting.
	transfer := []struct {
		Recipient common.Address `abi:"recipient"`
		Amount    *big.Int       `abi:"amount"`
		Id        *big.Int       `abi:"id"`
	}{
		{Recipient: recipient, Amount: amount, Id: depositId},
	}

	opts, err := s.monadDistributor.GetTransactOpts(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction options: %v", err)
	}

	logger.Info("Submitting Monad tx for deposit ID %s with amount %s", depositIDStr, amount.String())
	// Use our new TransactWithGasBuffer method instead of calling Transact directly
	// This will estimate the gas and add a 20% buffer
	tx, err := s.monadDistributor.TransactWithGasBuffer(opts, "distributeFunds", transfer)
	if err != nil {
		logger.Error("Failed to distribute funds: %v", err)
		// Update status to failed
		_ = s.UpdateTransactionStatus(ctx, depositId, database.StatusFailed, "")
		return "", fmt.Errorf("failed to distribute funds: %v", err)
	}

	txHash := tx.Hash().Hex()
	logger.Info("Waiting for tx %s to be mined for deposit ID %s", txHash, depositIDStr)

	// Update with pending status including the hash
	if err := s.UpdateTransactionStatus(ctx, depositId, database.StatusPending, txHash); err != nil {
		logger.Error("Failed to update tx hash in pending status: %v", err)
	}

	receipt, err := bind.WaitMined(ctx, s.monadDistributor.Client, tx)
	if err != nil {
		logger.Error("Failed to wait for distribution tx: %v", err)
		_ = s.UpdateTransactionStatus(ctx, depositId, database.StatusFailed, txHash)
		return "", fmt.Errorf("failed to wait for distribution tx: %v", err)
	}
	if receipt.Status == 0 {
		logger.Error("Distribution tx failed on blockchain")
		_ = s.UpdateTransactionStatus(ctx, depositId, database.StatusFailed, txHash)
		return "", fmt.Errorf("distribution tx failed")
	}

	// Create distribution record
	err = s.createDistributionRecord(depositId, recipient, amount, database.DistStatusCompleted, txHash)
	if err != nil {
		logger.Error("Failed to create distribution record: %v", err)
		// Continue anyway since the actual transaction was successful
	}

	// Retry updating DB status up to 3 times.
	for i := 0; i < 3; i++ {
		if err := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, txHash); err != nil {
			logger.Error("Failed to update tx status (attempt %d/3): %v", i+1, err)
			if i < 2 {
				time.Sleep(2 * time.Second)
				continue
			}
		} else {
			logger.Info("DB update confirmed for deposit ID %s with tx %s", depositIDStr, txHash)

			// CRITICAL: Always ensure deposit status is updated after transaction update
			logger.Info("Ensuring deposit status is updated to 'processed' for ID %s", depositIDStr)

			// Directly update the deposit status
			if err := s.db.UpdateDepositStatus(depositId, database.StatusProcessed); err != nil {
				logger.Error("Failed to directly update deposit status for ID %s: %v", depositIDStr, err)
			}

			// Double check the deposit status after update
			deposit, err := s.db.GetDepositByID(depositId)
			if err != nil {
				logger.Error("Failed to retrieve deposit after update: %v", err)
			} else if deposit == nil {
				logger.Error("Deposit ID %s not found after update", depositIDStr)
			} else {
				logger.Info("Deposit ID %s current status: %s", depositIDStr, deposit.Status)
			}

			break
		}
	}
	s.updateTxCache(depositId, database.StatusCompleted, txHash)
	logger.Info("Minting complete for deposit ID %s with tx %s and amount %s", depositIDStr, txHash, amount.String())

	// Final verification.
	if updatedTx, err := s.GetTransactionByDepositID(ctx, depositId); err == nil {
		if updatedTx.Status != database.StatusCompleted || updatedTx.MonadTxHash != txHash {
			logger.Error("Tx status verification failed for deposit ID %s", depositIDStr)
		} else {
			logger.Info("Tx status verified for deposit ID %s", depositIDStr)
		}
	} else {
		logger.Error("Failed to verify tx status: %v", err)
	}
	return txHash, nil
}

// createDistributionRecord creates a record in the distributions table
func (s *BridgeService) createDistributionRecord(depositId *big.Int, recipient common.Address, amount *big.Int, status, txHash string) error {
	// First check if a record already exists to avoid duplicates
	existingDist, err := s.db.GetDistributionByDepositID(depositId)
	if err == nil && existingDist != nil {
		logger.Info("Distribution record already exists for deposit ID %s, updating status", depositId.String())
		return s.db.UpdateDistributionStatus(depositId, status, txHash)
	}

	// Create new distribution record
	dist := &database.Distribution{
		DepositID:     depositId,
		WalletAddress: recipient,
		MonAmount:     amount,
		Status:        status,
		MonadTxHash:   txHash,
	}

	logger.Info("Creating distribution record for deposit ID %s with amount %s and status %s",
		depositId.String(), amount.String(), status)
	return s.db.CreateDistribution(dist)
}
