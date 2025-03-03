package bridge

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Transaction Recovery and Refund ---
//

func (s *BridgeService) RecoverStuckTransactions(ctx context.Context) error {
	pendingTxs, err := s.db.GetTransactionsByStatus(database.StatusPending, 100, 0)
	if err != nil {
		return fmt.Errorf("failed to get pending transactions: %w", err)
	}
	logger.Info("Checking %d pending transactions for recovery", len(pendingTxs))
	for _, tx := range pendingTxs {
		if time.Since(tx.CreatedAt) < 5*time.Minute {
			logger.Info("Skipping recent tx for deposit ID %s", tx.DepositID.String())
			continue
		}
		monadTxHash, status, err := s.FindMonadTransactionByDepositID(ctx, tx.DepositID)
		if err == nil && monadTxHash != "" && status == "success" {
			logger.Info("Recovering tx: deposit ID %s, Monad tx %s", tx.DepositID.String(), monadTxHash)
			if updateErr := s.UpdateTransactionStatus(ctx, tx.DepositID, database.StatusCompleted, monadTxHash); updateErr != nil {
				logger.Error("Failed to update recovered tx: %v", updateErr)
			} else {
				logger.Info("Successfully recovered tx for deposit ID %s", tx.DepositID.String())
			}
			continue
		}
		if time.Since(tx.CreatedAt) > 30*time.Minute {
			logger.Warn("Tx for deposit ID %s pending >30 minutes; marking as failed", tx.DepositID.String())
			if updateErr := s.UpdateTransactionStatus(ctx, tx.DepositID, database.StatusFailed, ""); updateErr != nil {
				logger.Error("Failed to mark tx as failed: %v", updateErr)
			}
		}
	}
	return nil
}

func (s *BridgeService) recoverStuckTransactionsPeriodically() {
	defer s.wg.Done()
	logger.Info("Starting stuck transaction recovery processor...")
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	if err := s.RecoverStuckTransactions(ctx); err != nil {
		logger.Error("Initial transaction recovery error: %v", err)
	}
	cancel()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
			if err := s.RecoverStuckTransactions(ctx); err != nil {
				logger.Error("Transaction recovery error: %v", err)
			}
			cancel()
		}
	}
}

func (s *BridgeService) refundDeposit(ctx context.Context, depositId *big.Int) error {
	// Check if this deposit has already been refunded to prevent duplicates
	tx, err := s.db.GetTransactionByDepositID(depositId)
	if err == nil && (tx.Status == database.StatusRefunded || strings.Contains(tx.Status, "refund")) {
		logger.Info("Deposit ID %s has already been refunded, skipping duplicate refund", depositId.String())
		return nil
	}

	// Update status to "refunding" to prevent concurrent refunds
	if err := s.UpdateTransactionStatus(ctx, depositId, "refunding", ""); err != nil {
		logger.Warn("Failed to update status for deposit ID %s: %v, but continuing with refund", depositId.String(), err)
	}

	logger.Info("Delegating refund for deposit ID %s to ArbitrumDepositor", depositId.String())
	err = s.arbDepositor.RefundDeposit(ctx, depositId)

	// If refund was successful, update status
	if err == nil {
		if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusRefunded, ""); updateErr != nil {
			logger.Warn("Failed to update status after refund for deposit ID %s: %v", depositId.String(), updateErr)
		}
	}

	return err
}
