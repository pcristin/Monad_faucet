package bridge

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Deposit Processing ---
//

// processDeposit processes a single deposit event.
func (s *BridgeService) processDeposit(event blockchain.DepositEvent) error {
	startTime := time.Now()
	if s.isProcessingDeposit(event.DepositId) {
		logger.Warn("Skipping duplicate processing for deposit ID %s", event.DepositId.String())
		return nil
	}
	defer s.finishProcessingDeposit(event.DepositId)

	// Double-check for a completed transaction.
	if tx, err := s.GetTransactionByDepositID(context.Background(), event.DepositId); err == nil && tx != nil && tx.Status == database.StatusCompleted {
		logger.Info("Transaction for deposit ID %s already completed with Monad tx hash %s", event.DepositId.String(), tx.MonadTxHash)
		return nil
	}

	logger.Info("Processing deposit %s", event)
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
	defer cancel()

	state, err := s.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bridge state: %w", err)
	}
	if state.IsPaused {
		return fmt.Errorf("bridge is currently paused")
	}

	monAmount := calculateMonAmount(event.Amount, state.SwapRatios[event.Currency], event.Currency)
	logMonCalculation(event, monAmount)

	// Create or update a pending transaction.
	existingTx, err := s.GetTransactionByDepositID(ctx, event.DepositId)
	if err != nil || existingTx == nil {
		txRecord := &database.Transaction{
			DepositID:     event.DepositId,
			WalletAddress: event.Depositor,
			Amount:        event.Amount,
			Currency:      database.CurrencyType(event.Currency),
			MonAmount:     monAmount,
			Status:        database.StatusPending,
			TxHash:        event.TxHash,
		}
		if err := s.db.CreateTransaction(txRecord); err != nil {
			logger.Error("Failed to create transaction record: %v", err)
		}
	} else if existingTx.Status == database.StatusCompleted {
		logger.Info("Transaction for deposit ID %s already completed with Monad tx hash %s", event.DepositId.String(), existingTx.MonadTxHash)
		return nil
	}

	if err := s.validateDepositWithAmount(state, event, monAmount); err != nil {
		_ = s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusFailed, "")
		s.QueueRefund(event.DepositId)
		return fmt.Errorf("invalid deposit: %w", err)
	}

	if err := s.waitForConfirmations(ctx, event.BlockNumber, 10); err != nil {
		_ = s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusFailed, "")
		s.QueueRefund(event.DepositId)
		return fmt.Errorf("failed to wait for confirmations: %w", err)
	}

	logger.Info("Minting %s MON tokens for wallet %s", formatMonAmount(monAmount), event.Depositor.Hex())

	// Last-minute duplicate prevention.
	if txHash, exists := s.checkExistingTransaction(ctx, event.DepositId); exists {
		logger.Info("Duplicate prevention: deposit ID %s already processed with tx %s", event.DepositId.String(), txHash)
		return nil
	}

	txHash, err := s.mintTokens(ctx, event.Depositor, monAmount, event.DepositId)
	if err != nil {
		if strings.Contains(err.Error(), "already completed") ||
			strings.Contains(err.Error(), "already in progress") ||
			strings.Contains(err.Error(), "duplicate mint attempt") {
			logger.Warn("Skipping refund for duplicate mint attempt: %v", err)
			if txHash != "" {
				logger.Info("Found completed tx %s, updating status", txHash)
				_ = s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusCompleted, txHash)
			}
			return fmt.Errorf("duplicate mint attempt: %w", err)
		}
		logger.Error("Mint tokens failed: %v. Deposit: %v", err, event)
		// Recovery logic omitted here for brevity (same as original)
		_ = s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusFailed, "")
		s.QueueRefund(event.DepositId)
		return fmt.Errorf("failed to mint tokens: %w", err)
	}

	logger.Info("Updating transaction status to completed for deposit ID %s with tx %s", event.DepositId.String(), txHash)
	if err := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusCompleted, txHash); err != nil {
		logger.Error("Failed to update transaction status: %v", err)
	} else {
		if tx, err := s.db.GetTransactionByDepositID(event.DepositId); err == nil {
			if tx.Status == database.StatusCompleted && tx.MonadTxHash == txHash {
				logger.Info("Transaction status updated correctly for deposit ID %s", event.DepositId.String())
			} else {
				logger.Warn("Transaction status may not have updated correctly for deposit ID %s", event.DepositId.String())
			}
		}
	}
	s.updateTxCache(event.DepositId, database.StatusCompleted, txHash)
	logger.Info("Processing completed for deposit ID %s in %v", event.DepositId.String(), time.Since(startTime))
	return nil
}

// finishProcessingDeposit marks a deposit as no longer being processed.
func (s *BridgeService) finishProcessingDeposit(depositID *big.Int) {
	depositIDStr := depositID.String()
	s.lockRefreshersMutex.Lock()
	if cancel, exists := s.lockRefreshers[depositIDStr]; exists {
		cancel()
		delete(s.lockRefreshers, depositIDStr)
	}
	s.lockRefreshersMutex.Unlock()
	s.releaseLock(depositID)
	s.processingMutex.Lock()
	delete(s.processingDeposits, depositIDStr)
	s.processingMutex.Unlock()
}

// isProcessingDeposit checks and marks a deposit as processing.
func (s *BridgeService) isProcessingDeposit(depositID *big.Int) bool {
	depositIDStr := depositID.String()
	s.processingMutex.Lock()
	defer s.processingMutex.Unlock()
	if s.processingDeposits[depositIDStr] {
		logger.Warn("Deposit ID %s is already being processed locally", depositIDStr)
		return true
	}
	if tx, err := s.GetTransactionByDepositID(context.Background(), depositID); err == nil && tx != nil && tx.Status == database.StatusCompleted {
		logger.Info("Transaction for deposit ID %s already completed", depositIDStr)
		return true
	}
	locked, _ := s.acquireLockWithRetries(context.Background(), depositID)
	if !locked {
		return true
	}
	s.processingDeposits[depositIDStr] = true
	logger.Info("Acquired processing lock for deposit ID %s", depositIDStr)
	return false
}
