package bridge

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pcristin/monad-faucet/internal/blockchain"
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

	// First try database
	logger.Info("Looking up transaction by Arbitrum tx hash: %s", txHash)
	tx, err := s.db.GetTransactionByArbitrumTxHash(hash.Hex())
	if err == nil && tx != nil {
		logger.Info("Found deposit ID %s for tx %s in database", tx.DepositID.String(), txHash)
		return tx.DepositID, nil
	}

	// If not found in database, try to get it from the contract logs
	logger.Info("Transaction not found in database, checking contract logs for tx %s", txHash)

	// Look through deposit events to find a matching transaction hash
	// This is a simplified implementation - in a real system you'd want to
	// query the contract directly or use an indexer service
	depositID, err := s.findDepositIDFromContractLogs(ctx, hash)
	if err != nil {
		logger.Error("Error getting deposit ID from contract logs: %v", err)
		return nil, fmt.Errorf("transaction not found: %w", err)
	}

	if depositID != nil && depositID.Cmp(big.NewInt(0)) > 0 {
		logger.Info("Found deposit ID %s from contract for tx hash %s", depositID.String(), txHash)

		// Check if we have a transaction record for this deposit ID
		existingTx, err := s.db.GetTransactionByDepositID(depositID)
		if err == nil && existingTx != nil {
			// Update transaction record with tx hash if needed
			if existingTx.TxHash == "" || existingTx.TxHash != txHash {
				logger.Info("Updating transaction record with Arbitrum tx hash %s", txHash)
				if err := s.db.UpdateTransactionHash(depositID, txHash); err != nil {
					logger.Error("Failed to update transaction hash: %v", err)
				}
			}
		} else {
			// Create minimal transaction record if none exists
			logger.Info("Creating minimal transaction record for deposit ID %s with tx hash %s", depositID.String(), txHash)
			minimalTx := &database.Transaction{
				DepositID: depositID,
				TxHash:    txHash,
				Status:    database.StatusPending,
			}
			if err := s.db.CreateTransaction(minimalTx); err != nil {
				logger.Error("Failed to create minimal transaction record: %v", err)
			}
		}

		return depositID, nil
	}

	return nil, fmt.Errorf("could not find deposit ID for tx hash %s", txHash)
}

func (s *BridgeService) GetCachedTransactionByDepositID(ctx context.Context, depositID *big.Int) (*database.Transaction, error) {
	if depositID == nil {
		return nil, fmt.Errorf("deposit ID is nil")
	}

	cacheKey := depositID.String()

	// Check cache first
	s.txCacheMutex.RLock()
	if cached, exists := s.txCache[cacheKey]; exists && cached.Status != database.StatusPending {
		s.txCacheMutex.RUnlock()
		logger.Info("Found transaction in cache for deposit ID %s with status %s", depositID.String(), cached.Status)
		return cached, nil
	}
	s.txCacheMutex.RUnlock()

	// If not in cache, check database
	logger.Info("Transaction not in cache, checking database for deposit ID %s", depositID.String())
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil {
		logger.Error("Failed to get transaction from database: %v", err)
		return nil, fmt.Errorf("failed to get transaction from DB: %w", err)
	}

	if tx == nil {
		logger.Warn("No transaction found for deposit ID %s", depositID.String())
		return nil, fmt.Errorf("transaction not found for deposit ID %s", depositID.String())
	}

	// If transaction is completed, cache it
	if tx.Status != database.StatusPending {
		s.txCacheMutex.Lock()
		s.txCache[cacheKey] = tx
		s.txCacheMutex.Unlock()

		// Set expiration for cache entry
		go func(key string, expiration time.Duration) {
			time.Sleep(expiration)
			s.clearTransactionCache(key)
		}(cacheKey, s.txCacheExpiration)

		logger.Info("Cached transaction for deposit ID %s with status %s", depositID.String(), tx.Status)
	}

	return tx, nil
}

func (s *BridgeService) UpdateTransactionStatusWithCache(ctx context.Context, depositID *big.Int, status, txHash string) error {
	if depositID == nil {
		return fmt.Errorf("depositID is nil")
	}

	logger.Info("Updating transaction status: depositID=%s, status=%s, txHash=%s", depositID.String(), status, txHash)

	// First check if transaction exists
	existingTx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil || existingTx == nil {
		logger.Error("Error checking for existing transaction: %v", err)
		// Create a placeholder transaction if none exists and we're trying to mark it as completed
		if status == database.StatusCompleted && txHash != "" {
			logger.Info("Creating placeholder transaction for depositID=%s with status=%s", depositID.String(), status)
			// Create a minimal transaction record
			tx := &database.Transaction{
				DepositID:     depositID,
				WalletAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
				Amount:        big.NewInt(0),
				Currency:      database.CurrencyETH,
				MonAmount:     big.NewInt(0),
				Status:        status,
				MonadTxHash:   txHash,
			}
			if err := s.db.CreateTransaction(tx); err != nil {
				logger.Error("Failed to create placeholder transaction: %v", err)
				return fmt.Errorf("failed to create placeholder transaction: %w", err)
			}
			logger.Info("Created placeholder transaction for depositID=%s", depositID.String())
			return nil
		}
	} else if status == database.StatusCompleted && existingTx.Status == database.StatusCompleted {
		// If transaction is already completed, log and return
		logger.Info("Transaction for depositID=%s is already completed with tx hash %s, skipping update",
			depositID.String(), existingTx.MonadTxHash)

		// If the existing record has a different hash but we have a new one, update it
		if existingTx.MonadTxHash == "" && txHash != "" {
			logger.Info("Updating missing Monad tx hash for completed transaction: %s", txHash)
		} else if existingTx.MonadTxHash != txHash && txHash != "" {
			logger.Warn("Different tx hash found for completed transaction: existing=%s, new=%s",
				existingTx.MonadTxHash, txHash)
		} else {
			// Nothing to update
			return nil
		}
	}

	// Begin the database transaction
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

	// Update transaction status
	err = s.db.UpdateTransactionStatusWithTx(dbTx, depositID, status, txHash)
	if err != nil {
		return fmt.Errorf("failed to update status in DB: %w", err)
	}

	// Commit the transaction
	if err = dbTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit DB tx: %w", err)
	}

	// Verify the update was successful
	verifyTx, verifyErr := s.db.GetTransactionByDepositID(depositID)
	if verifyErr != nil {
		logger.Error("Error verifying transaction update: %v", verifyErr)
	} else if verifyTx.Status != status {
		logger.Error("Transaction status update verification failed: expected=%s, actual=%s", status, verifyTx.Status)
	} else if txHash != "" && verifyTx.MonadTxHash != txHash {
		logger.Error("Transaction hash update verification failed: expected=%s, actual=%s", txHash, verifyTx.MonadTxHash)
	} else {
		logger.Info("Transaction update verified successfully: depositID=%s, status=%s, hash=%s",
			depositID.String(), verifyTx.Status, verifyTx.MonadTxHash)
	}

	// Update cache based on status
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
	logger.Info("Looking up Monad transaction for Arbitrum tx hash %s", txHash)

	// Step 1: Get deposit ID from Arbitrum tx hash
	depositID, err := s.GetDepositIDFromArbitrumTxHash(ctx, txHash)
	if err != nil {
		logger.Error("Error getting deposit ID from Arbitrum tx hash: %v", err)
		return "", "", fmt.Errorf("error getting deposit ID: %w", err)
	}

	if depositID == nil {
		logger.Error("No deposit ID found for tx hash %s", txHash)
		return "", "", fmt.Errorf("deposit ID not found for tx hash")
	}

	logger.Info("Found deposit ID %s for Arbitrum tx hash %s", depositID.String(), txHash)

	// Step 2: Get transaction status and Monad tx hash from deposit ID
	status, monadTxHash, err := s.FindMonadTransactionByDepositID(ctx, depositID)
	if err != nil {
		logger.Error("Error finding Monad tx for deposit ID %s: %v", depositID.String(), err)
		return "", "", fmt.Errorf("error finding Monad tx: %w", err)
	}

	if monadTxHash != "" {
		logger.Info("Found Monad tx %s with status %s for Arbitrum tx %s", monadTxHash, status, txHash)
	} else {
		logger.Info("No Monad tx found for Arbitrum tx %s, status: %s", txHash, status)
	}

	return status, monadTxHash, nil
}

func (s *BridgeService) findDepositIDFromContractLogs(ctx context.Context, txHash common.Hash) (*big.Int, error) {
	// Get transaction receipt directly from the blockchain
	logger.Info("Getting transaction receipt for tx hash %s", txHash.Hex())
	receipt, err := s.arbDepositor.Client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction receipt: %w", err)
	}

	// Look for DepositEvent in the logs
	for _, log := range receipt.Logs {
		// Check if this log is from our depositor contract
		if log.Address == s.arbDepositor.Address {
			// Try to parse as DepositEvent
			var depositEvent struct {
				Depositor common.Address
				Amount    *big.Int
				DepositId *big.Int
				Currency  uint8
			}

			// Try to unpack the log as a DepositEvent
			err = s.arbDepositor.BoundContract.UnpackLog(&depositEvent, "DepositEvent", *log)
			if err == nil && depositEvent.DepositId != nil && depositEvent.DepositId.Cmp(big.NewInt(0)) > 0 {
				logger.Info("Found deposit ID %s in transaction logs for tx %s",
					depositEvent.DepositId.String(), txHash.Hex())

				// Get additional data from the event
				walletAddress := depositEvent.Depositor
				amount := depositEvent.Amount
				currency := depositEvent.Currency

				// Log the complete deposit information
				logger.Info("Extracted full deposit data: ID=%s, Wallet=%s, Amount=%s, Currency=%d",
					depositEvent.DepositId.String(), walletAddress.Hex(), amount.String(), currency)

				// Create transaction record if it doesn't exist
				tx, err := s.db.GetTransactionByDepositID(depositEvent.DepositId)
				if err != nil || tx == nil {
					// Record doesn't exist, create a new one
					logger.Info("Creating new transaction record for deposit ID %s from blockchain data",
						depositEvent.DepositId.String())

					// Calculate MON amount if we can access contract state
					var monAmount *big.Int
					state, err := s.GetState(ctx)
					if err == nil && state != nil && state.SwapRatios != nil {
						swapRatio := state.SwapRatios[blockchain.CurrencyType(currency)]
						if swapRatio != nil {
							monAmount = calculateMonAmount(amount, swapRatio, blockchain.CurrencyType(currency))
							logger.Info("Calculated MON amount: %s for deposit amount %s",
								monAmount.String(), amount.String())
						}
					}

					// If we couldn't calculate MON amount, use a placeholder
					if monAmount == nil {
						monAmount = big.NewInt(0)
						logger.Warn("Could not calculate MON amount, using placeholder")
					}

					newTx := &database.Transaction{
						DepositID:     depositEvent.DepositId,
						WalletAddress: walletAddress,
						Amount:        amount,
						Currency:      database.CurrencyType(currency),
						MonAmount:     monAmount,
						Status:        database.StatusPending,
						TxHash:        txHash.Hex(),
					}

					if err := s.db.CreateTransaction(newTx); err != nil {
						logger.Error("Failed to create transaction record: %v", err)
						// Continue despite error, we've found the deposit ID
					} else {
						logger.Info("Successfully created transaction record for deposit ID %s",
							depositEvent.DepositId.String())
					}
				} else if tx.TxHash == "" || tx.TxHash != txHash.Hex() {
					// Update existing transaction with tx hash if needed
					logger.Info("Updating existing transaction record with Arbitrum tx hash %s", txHash.Hex())
					if err := s.db.UpdateTransactionHash(depositEvent.DepositId, txHash.Hex()); err != nil {
						logger.Error("Failed to update transaction hash: %v", err)
					}
				}

				return depositEvent.DepositId, nil
			}
		}
	}

	// If we couldn't find a deposit event in the transaction logs, try using the filter method
	logger.Info("No DepositEvent found in transaction logs, trying to scan past deposits for tx %s", txHash.Hex())

	// Create a RetryClient to handle blockchain queries with retries
	retryClient := blockchain.NewRetryClient(s.arbDepositor.Client)

	// Use a filter query to look for deposit events
	fromBlock := big.NewInt(0)

	// Use a topic based filter for deposit events
	// This approach avoids using the Events field which isn't accessible
	depositEventSignature := []byte("DepositEvent(address,uint256,uint256,uint8)")
	depositEventTopic := crypto.Keccak256Hash(depositEventSignature)

	// Get logs that might contain deposit events
	logs, err := retryClient.FilterLogsWithRetry(ctx, ethereum.FilterQuery{
		FromBlock: fromBlock,
		ToBlock:   big.NewInt(0).Add(fromBlock, big.NewInt(1000000)), // Limit range to avoid timeout
		Addresses: []common.Address{s.arbDepositor.Address},
		Topics:    [][]common.Hash{{depositEventTopic}},
	})

	if err != nil {
		logger.Error("Failed to filter logs for deposit events: %v", err)
		return nil, fmt.Errorf("failed to search deposit events: %w", err)
	}

	logger.Info("Found %d deposit event logs to search through", len(logs))

	for _, eventLog := range logs {
		// Look for logs from the same transaction
		if eventLog.TxHash == txHash {
			var depositEvent struct {
				Depositor common.Address
				Amount    *big.Int
				DepositId *big.Int
				Currency  uint8
			}

			err = s.arbDepositor.BoundContract.UnpackLog(&depositEvent, "DepositEvent", eventLog)
			if err == nil && depositEvent.DepositId != nil && depositEvent.DepositId.Cmp(big.NewInt(0)) > 0 {
				logger.Info("Found deposit ID %s in broad event search for tx %s",
					depositEvent.DepositId.String(), txHash.Hex())
				return depositEvent.DepositId, nil
			}
		}
	}

	return nil, fmt.Errorf("could not find deposit ID for tx hash %s in any deposit events", txHash.Hex())
}

// FindMonadDistributionByDepositID and CheckOrCreateDistributionTransaction functions
// have been moved to events.go
