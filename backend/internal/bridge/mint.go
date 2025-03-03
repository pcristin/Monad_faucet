package bridge

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Validation, Confirmation, and Minting ---
//

// validateDepositWithAmount validates a deposit using pre-calculated MON amount.
func (s *BridgeService) validateDepositWithAmount(state *blockchain.ContractState, event blockchain.DepositEvent, monAmount *big.Int) error {
	if state.IsPaused {
		return fmt.Errorf("bridge is paused")
	}
	if state.MonBalance.Cmp(monAmount) < 0 {
		return fmt.Errorf("insufficient MON balance in distributor")
	}

	// Minimum deposit validation removed - this check is already done at the smart contract level.
	// If a deposit event was emitted, it means the minimum amount check has already passed on-chain.

	return nil
}

// waitForConfirmations waits until the target block is reached.
func (s *BridgeService) waitForConfirmations(ctx context.Context, blockNumber uint64, confirmations uint64) error {
	targetBlock := blockNumber + confirmations
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentBlock, err := s.arbDepositor.Client.BlockNumber(ctx)
			if err != nil {
				log.Printf("Error getting block number: %v", err)
				continue
			}
			if currentBlock >= targetBlock {
				return nil
			}
		}
	}
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
		Recipient common.Address
		Amount    big.Int
		Id        big.Int
	}{
		{Recipient: recipient, Amount: *amount, Id: *depositId},
	}

	opts, err := s.monadDistributor.GetTransactOpts(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction options: %v", err)
	}

	logger.Info("Submitting Monad tx for deposit ID %s with amount %s", depositIDStr, amount.String())
	tx, err := s.monadDistributor.BoundContract.Transact(opts, "distributeFunds", transfer)
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
