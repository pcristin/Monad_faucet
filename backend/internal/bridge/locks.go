package bridge

import (
	"context"
	"math/big"
	"strings"
	"time"

	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Distributed Lock & Duplicate Prevention Helpers ---
//

// checkExistingTransaction performs duplicate-checks via DB, cache, and blockchain.
func (s *BridgeService) checkExistingTransaction(ctx context.Context, depositID *big.Int) (string, bool) {
	// Check DB.
	if tx, err := s.GetTransactionByDepositID(ctx, depositID); err == nil && tx != nil && tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
		return tx.MonadTxHash, true
	}
	// Check blockchain.
	if txHash, _ := s.checkMonadBlockchainForTransaction(ctx, depositID); txHash != "" {
		return txHash, true
	}
	// Check cache.
	s.txCacheMutex.RLock()
	if cached, found := s.txCache[depositID.String()]; found && cached.Status == database.StatusCompleted && cached.MonadTxHash != "" {
		s.txCacheMutex.RUnlock()
		return cached.MonadTxHash, true
	}
	s.txCacheMutex.RUnlock()
	return "", false
}

// acquireLockWithRetries wraps distributed lock acquisition with retry logic.
func (s *BridgeService) acquireLockWithRetries(ctx context.Context, depositID *big.Int) (bool, error) {
	depositIDStr := depositID.String()
	acquired, err := s.db.AcquireProcessingLock(depositID, s.instanceID, s.lockDuration)
	if err != nil {
		// On error, attempt a blockchain check before deciding.
		if txHash, _ := s.checkMonadBlockchainForTransaction(ctx, depositID); txHash != "" {
			return true, nil
		}
		// For non-timeout errors, treat as already processing.
		if !strings.Contains(err.Error(), "connection refused") && !strings.Contains(err.Error(), "timeout") {
			logger.Warn("Lock acquisition failed for deposit ID %s: %v", depositIDStr, err)
			return true, err
		}
	} else if !acquired {
		// If not acquired, retry a few times.
		for i := 0; i < 3; i++ {
			time.Sleep(2 * time.Second)
			if tx, err := s.GetTransactionByDepositID(ctx, depositID); err == nil && tx != nil {
				if tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
					return true, nil
				}
			}
			if txHash, _ := s.checkMonadBlockchainForTransaction(ctx, depositID); txHash != "" {
				return true, nil
			}
		}
		logger.Warn("Respecting lock for deposit ID %s after retries", depositIDStr)
		return true, nil
	} else {
		// Start lock refresher.
		s.startLockRefresher(depositID)
	}
	return acquired, nil
}

// releaseLock stops lock refresher and releases the distributed lock.
func (s *BridgeService) releaseLock(depositID *big.Int) {
	depositIDStr := depositID.String()
	s.lockRefreshersMutex.Lock()
	if cancel, exists := s.lockRefreshers[depositIDStr]; exists {
		cancel()
		delete(s.lockRefreshers, depositIDStr)
	}
	s.lockRefreshersMutex.Unlock()
	if err := s.db.ReleaseProcessingLock(depositID, s.instanceID); err != nil {
		logger.Error("Failed to release distributed lock for deposit ID %s: %v", depositIDStr, err)
	} else {
		logger.Info("Released processing lock for deposit ID %s", depositIDStr)
	}
}

// startLockRefresher starts a goroutine to periodically refresh the lock.
func (s *BridgeService) startLockRefresher(depositID *big.Int) {
	depositIDStr := depositID.String()
	refreshCtx, cancel := context.WithCancel(s.ctx)
	s.lockRefreshersMutex.Lock()
	if existingCancel, exists := s.lockRefreshers[depositIDStr]; exists {
		existingCancel()
	}
	s.lockRefreshers[depositIDStr] = cancel
	s.lockRefreshersMutex.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.lockRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-ticker.C:
				refreshed, err := s.db.RefreshProcessingLock(depositID, s.instanceID, s.lockDuration)
				if err != nil {
					logger.Error("Failed to refresh lock for deposit ID %s: %v", depositIDStr, err)
				} else if !refreshed {
					logger.Warn("Lock for deposit ID %s lost during refresh", depositIDStr)
					return
				} else {
					logger.Debug("Refreshed lock for deposit ID %s", depositIDStr)
				}
			}
		}
	}()
}
