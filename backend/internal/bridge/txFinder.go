package bridge

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Transaction Lookup Functions ---
//

func (s *BridgeService) GetDepositIDFromTxHash(ctx context.Context, txHash string) (*big.Int, error) {
	hash := common.HexToHash(txHash)
	if hash == (common.Hash{}) {
		return nil, fmt.Errorf("invalid tx hash format")
	}
	tx, err := s.db.GetTransactionByArbitrumTxHash(hash.Hex())
	if err == nil && tx != nil {
		logger.Info("Found deposit ID %s for tx %s", tx.DepositID.String(), txHash)
		return tx.DepositID, nil
	}
	return nil, fmt.Errorf("transaction not found in database")
}

func (s *BridgeService) GetCachedTransactionByDepositID(ctx context.Context, depositID *big.Int) (*database.Transaction, error) {
	cacheKey := depositID.String()
	s.txCacheMutex.RLock()
	if cached, exists := s.txCache[cacheKey]; exists && cached.Status != database.StatusPending {
		s.txCacheMutex.RUnlock()
		return cached, nil
	}
	s.txCacheMutex.RUnlock()
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction from DB: %w", err)
	}
	if tx.Status != database.StatusPending {
		s.txCacheMutex.Lock()
		s.txCache[cacheKey] = tx
		s.txCacheMutex.Unlock()
		go func(key string, expiration time.Duration) {
			time.Sleep(expiration)
			s.clearTransactionCache(key)
		}(cacheKey, s.txCacheExpiration)
	}
	return tx, nil
}

func (s *BridgeService) UpdateTransactionStatusWithCache(ctx context.Context, depositID *big.Int, status, txHash string) error {
	dbTx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin DB tx: %w", err)
	}
	defer func() {
		if err != nil {
			rollbackErr := dbTx.Rollback()
			if rollbackErr != nil {
				logger.Error("Rollback failed: %v", rollbackErr)
			}
		}
	}()
	err = s.db.UpdateTransactionStatusWithTx(dbTx, depositID, status, txHash)
	if err != nil {
		return fmt.Errorf("failed to update status in DB: %w", err)
	}
	if err = dbTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit DB tx: %w", err)
	}
	if status == database.StatusCompleted {
		s.updateTxCache(depositID, status, txHash)
	} else {
		s.clearTransactionCache(depositID.String())
	}
	return nil
}

func (s *BridgeService) FindMonadTransactionByDepositID(ctx context.Context, depositID *big.Int) (string, string, error) {
	tx, err := s.GetCachedTransactionByDepositID(ctx, depositID)
	if err != nil {
		logger.Error("Error finding transaction: %v", err)
		return "", "", err
	}
	if tx.MonadTxHash != "" {
		logger.Info("Found Monad tx hash %s for deposit ID %s with status %s", tx.MonadTxHash, depositID.String(), tx.Status)
		return tx.Status, tx.MonadTxHash, nil
	}
	if tx.Status == database.StatusPending {
		logger.Info("Transaction pending, checking blockchain for deposit ID %s", depositID.String())
		monadTxHash, err := s.checkMonadBlockchainForTransaction(ctx, depositID)
		if err == nil && monadTxHash != "" {
			logger.Info("Found blockchain tx %s for deposit ID %s", monadTxHash, depositID.String())
			if updateErr := s.UpdateTransactionStatusWithCache(ctx, depositID, database.StatusCompleted, monadTxHash); updateErr != nil {
				logger.Error("Failed to update tx status: %v", updateErr)
			}
			return database.StatusCompleted, monadTxHash, nil
		}
		logger.Info("Standard blockchain search did not find tx; trying fallback for deposit ID %s", depositID.String())
		if scanErr := s.searchAllDistributionEvents(ctx, depositID); scanErr != nil {
			logger.Warn("Fallback search error for deposit ID %s: %v", depositID.String(), scanErr)
		}
		if updatedTx, _ := s.db.GetTransactionByDepositID(depositID); updatedTx != nil && updatedTx.Status == database.StatusCompleted && updatedTx.MonadTxHash != "" {
			logger.Info("Fallback search found tx %s for deposit ID %s", updatedTx.MonadTxHash, depositID.String())
			return database.StatusCompleted, updatedTx.MonadTxHash, nil
		}
	}
	if tx.Status != "" {
		logger.Info("Found transaction for deposit ID %s with status %s but no Monad tx hash", depositID.String(), tx.Status)
		return tx.Status, "", nil
	}
	logger.Warn("Transaction for deposit ID %s exists but lacks Monad tx hash/status", depositID.String())
	return "", "", fmt.Errorf("transaction found but incomplete")
}

func (s *BridgeService) GetDepositIDFromArbitrumTxHash(ctx context.Context, txHash string) (*big.Int, error) {
	tx, err := s.db.GetTransactionByArbitrumTxHash(txHash)
	if err == nil && tx != nil {
		logger.Info("Found deposit ID %s for Arbitrum tx hash %s", tx.DepositID.String(), txHash)
		return tx.DepositID, nil
	}
	depositID, err := s.GetDepositIDFromTxHash(ctx, txHash)
	if err != nil {
		logger.Error("Error getting deposit ID from tx hash: %v", err)
		return nil, err
	}
	if depositID != nil && depositID.Cmp(big.NewInt(0)) > 0 {
		logger.Info("Found deposit ID %s from contract for tx hash %s", depositID.String(), txHash)
		if existingTx, _ := s.db.GetTransactionByDepositID(depositID); existingTx != nil {
			if existingTx.TxHash == "" || existingTx.TxHash != txHash {
				if err := s.db.UpdateTransactionHash(depositID, txHash); err != nil {
					logger.Error("Failed to update transaction hash: %v", err)
				} else {
					logger.Info("Updated Arbitrum tx hash for deposit ID %s", depositID.String())
				}
			}
		}
		return depositID, nil
	}
	return nil, fmt.Errorf("could not find deposit ID for tx hash")
}

func (s *BridgeService) GetMonadTxHashFromArbitrumTxHash(ctx context.Context, txHash string) (string, string, error) {
	depositID, err := s.GetDepositIDFromArbitrumTxHash(ctx, txHash)
	if err != nil {
		logger.Error("Error getting deposit ID from Arbitrum tx hash: %v", err)
		return "", "", err
	}
	status, monadTxHash, err := s.FindMonadTransactionByDepositID(ctx, depositID)
	if err != nil {
		logger.Error("Error finding Monad tx hash by deposit ID: %v", err)
		return "", "", err
	}
	return status, monadTxHash, nil
}
