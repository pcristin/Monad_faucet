package bridge

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Blockchain Event Search Helpers ---
//

// checkMonadBlockchainForTransaction searches for a Distribution event on the Monad blockchain.
func (s *BridgeService) checkMonadBlockchainForTransaction(ctx context.Context, depositId *big.Int) (string, error) {
	client := s.monadDistributor.Client
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get current block number: %w", err)
	}
	lookBackBlocks := uint64(25)
	if currentBlock < lookBackBlocks {
		lookBackBlocks = currentBlock
	}
	startBlock := currentBlock - lookBackBlocks
	depositIdBytes32 := common.BytesToHash(depositId.Bytes())
	distributionEventSignature := []byte("Distribution(address,uint256,uint256)")
	distributionEventTopic := crypto.Keccak256Hash(distributionEventSignature)

	logger.Info("Searching for Distribution event for deposit ID %s (starting at block %d)", depositId.String(), startBlock)
	maxAttempts := 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		filterQuery := ethereum.FilterQuery{
			FromBlock: big.NewInt(int64(startBlock)),
			ToBlock:   big.NewInt(int64(currentBlock)),
			Addresses: []common.Address{s.monadDistributor.Address},
			Topics: [][]common.Hash{
				{distributionEventTopic},
				nil,
				nil,
				{depositIdBytes32},
			},
		}
		logger.Info("Attempt %d/%d: Searching blocks %d to %d", attempt, maxAttempts, startBlock, currentBlock)
		logs, err := client.FilterLogs(ctx, filterQuery)
		if err != nil {
			logger.Error("FilterLogs error (attempt %d): %v", attempt, err)
			if strings.Contains(err.Error(), "Request Entity Too Large") || strings.Contains(err.Error(), "eth_getLogs is limited") {
				lookBackBlocks = lookBackBlocks / 2
				if lookBackBlocks < 5 {
					lookBackBlocks = 5
				}
				if attempt <= 3 {
					startBlock = currentBlock - lookBackBlocks
				} else {
					currentBlock = startBlock - 1
					startBlock = currentBlock - lookBackBlocks
				}
				logger.Info("Reducing block search range to %d blocks", lookBackBlocks)
				time.Sleep(300 * time.Millisecond)
				continue
			} else if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				logger.Info("Falling back to manual event scanning")
				if err := s.searchAllDistributionEvents(ctx, depositId); err != nil {
					logger.Error("Fallback search error: %v", err)
				}
				return "", fmt.Errorf("failed to filter logs after multiple attempts: %w", err)
			}
		}
		logger.Info("Found %d Distribution events for deposit ID %s", len(logs), depositId.String())
		if len(logs) > 0 {
			txHash := logs[len(logs)-1].TxHash.Hex()
			logger.Info("Found Distribution event in tx %s for deposit ID %s", txHash, depositId.String())
			return txHash, nil
		}
		if attempt < maxAttempts {
			newEndBlock := startBlock
			if attempt <= 2 {
				lookBackBlocks = lookBackBlocks * 2
			} else {
				lookBackBlocks = lookBackBlocks * 3
			}
			if lookBackBlocks > 100 {
				lookBackBlocks = 100
			}
			startBlock = newEndBlock - lookBackBlocks
			currentBlock = newEndBlock - 1
			logger.Info("Extending search window: blocks %d to %d", startBlock, currentBlock)
		}
	}
	logger.Info("No Distribution event found via filtering; trying manual decoding")
	if err := s.searchAllDistributionEvents(ctx, depositId); err != nil {
		logger.Error("Final fallback search error: %v", err)
	}
	return "", nil
}

// searchAllDistributionEvents performs a fallback scan of Distribution events.
func (s *BridgeService) searchAllDistributionEvents(ctx context.Context, targetDepositId *big.Int) error {
	client := s.monadDistributor.Client
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}
	lookBackBlocks := uint64(50)
	startBlock := currentBlock - lookBackBlocks
	logger.Info("Fallback scan: blocks %d to %d for deposit ID %s", startBlock, currentBlock, targetDepositId.String())
	distributionEventSignature := []byte("Distribution(address,uint256,uint256)")
	distributionEventTopic := crypto.Keccak256Hash(distributionEventSignature)
	totalEventsChecked := 0
	attemptCount := 0
	maxAttempts := 5
	distributionEventABI := `[{"anonymous":false,"inputs":[{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"id","type":"uint256"}],"name":"Distribution","type":"event"}]`
	parsedABI, err := abi.JSON(strings.NewReader(distributionEventABI))
	if err != nil {
		return fmt.Errorf("failed to parse ABI: %w", err)
	}
	for attemptCount < maxAttempts {
		attemptCount++
		logger.Info("Fallback scan attempt %d/%d: blocks %d to %d", attemptCount, maxAttempts, startBlock, currentBlock)
		filterQuery := ethereum.FilterQuery{
			FromBlock: big.NewInt(int64(startBlock)),
			ToBlock:   big.NewInt(int64(currentBlock)),
			Addresses: []common.Address{s.monadDistributor.Address},
			Topics: [][]common.Hash{
				{distributionEventTopic},
			},
		}
		logs, err := client.FilterLogs(ctx, filterQuery)
		if err != nil {
			if strings.Contains(err.Error(), "Request Entity Too Large") || strings.Contains(err.Error(), "eth_getLogs is limited") {
				lookBackBlocks = lookBackBlocks / 2
				if lookBackBlocks < 5 {
					lookBackBlocks = 5
				}
				startBlock = currentBlock - lookBackBlocks
				logger.Info("Reducing fallback scan range to %d blocks (attempt %d)", lookBackBlocks, attemptCount)
				continue
			} else {
				logger.Error("Fallback scan FilterLogs error (attempt %d): %v", attemptCount, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}
		logger.Info("Fallback scan: found %d events", len(logs))
		totalEventsChecked += len(logs)
		for _, log := range logs {
			if len(log.Topics) == 0 || log.Topics[0] != distributionEventTopic {
				continue
			}
			if len(log.Data) == 0 {
				continue
			}
			decoded, err := parsedABI.Unpack("Distribution", log.Data)
			if err != nil {
				logger.Debug("Failed to decode event data: %v", err)
				continue
			}
			if len(decoded) != 2 {
				logger.Debug("Unexpected parameters in event data")
				continue
			}
			id, ok := decoded[1].(*big.Int)
			if !ok || id == nil {
				logger.Debug("Failed to extract ID from event data")
				continue
			}
			if id.Cmp(targetDepositId) == 0 {
				logger.Info("Fallback scan: found matching deposit ID %s in tx %s", targetDepositId.String(), log.TxHash.Hex())
				if err := s.UpdateTransactionStatus(ctx, targetDepositId, database.StatusCompleted, log.TxHash.Hex()); err != nil {
					logger.Error("Failed to update tx status: %v", err)
				} else {
					s.updateTxCache(targetDepositId, database.StatusCompleted, log.TxHash.Hex())
					return nil
				}
			}
		}
		if currentBlock > 0 && attemptCount < maxAttempts {
			currentBlock = startBlock - 1
			startBlock = currentBlock - lookBackBlocks
			if attemptCount > 2 {
				lookBackBlocks = lookBackBlocks * 2
				if lookBackBlocks > 100 {
					lookBackBlocks = 100
				}
			}
			if currentBlock <= 0 || (currentBlock < 100 && attemptCount > 3) {
				break
			}
		} else {
			break
		}
	}
	logger.Info("Fallback scan complete: no matching deposit ID %s found after checking %d events in %d attempts", targetDepositId.String(), totalEventsChecked, attemptCount)
	return nil
}

// FindMonadDistributionByDepositID searches for Distribution events in the distributor contract by deposit ID
func (s *BridgeService) FindMonadDistributionByDepositID(ctx context.Context, depositID *big.Int) (string, error) {
	if depositID == nil {
		return "", fmt.Errorf("deposit ID is nil")
	}

	logger.Info("Searching for Distribution events for deposit ID %s", depositID.String())

	// Check if we should use QuickNode webhook instead of RPC polling for stage environment
	if s.UseQuickNodeWebhook {
		// When using QuickNode webhooks, we check the database directly instead of polling the blockchain
		// The webhook handler will have already updated the transaction status
		logger.Info("Using QuickNode webhook for distribution events (skipping blockchain polling)")

		// Get transaction from database to check if it's been updated by webhook
		tx, err := s.db.GetTransactionByDepositID(depositID)
		if err != nil {
			logger.Error("Failed to get transaction from database: %v", err)
			return "", err
		}

		// If the transaction has a Monad transaction hash and status is completed, return it
		if tx != nil && tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
			logger.Info("Found completed transaction with Monad tx hash %s from webhook update", tx.MonadTxHash)
			return tx.MonadTxHash, nil
		}

		// If not found or not completed, return empty string with nil error
		// This lets the system continue with other processing without creating an error state
		logger.Info("No completed distribution found for deposit ID %s in database (using webhook mode)", depositID.String())
		return "", nil
	}

	// Traditional RPC polling implementation below
	// Create a filter query for Distribution events with the specified deposit ID
	distributionTopic := crypto.Keccak256Hash([]byte("Distribution(address,uint256,uint256)"))

	// Get current block number to determine search range
	latestBlock, err := s.monadDistributor.Client.BlockNumber(ctx)
	if err != nil {
		logger.Error("Failed to get latest block number: %v", err)
		return "", fmt.Errorf("failed to get latest block number: %w", err)
	}

	// Start with a larger block range to increase chances of finding events
	blockRange := uint64(50000)
	fromBlock := uint64(0)
	if latestBlock > blockRange {
		fromBlock = latestBlock - blockRange
	}

	// Log the search parameters
	logger.Info("Searching for distribution events from block %d to %d for deposit ID %s",
		fromBlock, latestBlock, depositID.String())

	// Create the filter query without specifying the deposit ID in topics
	// Instead, we'll check each event's data to find matching deposit IDs
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(latestBlock)),
		Addresses: []common.Address{s.monadDistributor.Address},
		Topics: [][]common.Hash{
			{distributionTopic}, // Event signature only
		},
	}

	// Create ABI for parsing the event data
	distributionEventABI := `[{"anonymous":false,"inputs":[{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"id","type":"uint256"}],"name":"Distribution","type":"event"}]`
	parsedABI, err := abi.JSON(strings.NewReader(distributionEventABI))
	if err != nil {
		logger.Error("Failed to parse ABI: %v", err)
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	// Implement retry logic for resilience
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		logger.Info("Attempt %d/%d: Querying for Distribution events", attempt, maxAttempts)

		logs, err := s.monadDistributor.Client.FilterLogs(ctx, query)
		if err != nil {
			logger.Error("Failed to filter logs (attempt %d/%d): %v",
				attempt, maxAttempts, err)

			// If we hit request size limits, reduce the block range
			if strings.Contains(err.Error(), "Request Entity Too Large") ||
				strings.Contains(err.Error(), "eth_getLogs is limited") {
				blockRange = blockRange / 2
				if blockRange < 5000 {
					blockRange = 5000
				}

				fromBlock = latestBlock - blockRange
				query.FromBlock = big.NewInt(int64(fromBlock))
				logger.Info("Reduced block range to %d blocks, now searching from %d to %d",
					blockRange, fromBlock, latestBlock)

				// Add a slight delay before retrying
				time.Sleep(300 * time.Millisecond)
				continue
			}

			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			return "", fmt.Errorf("failed to filter logs after %d attempts: %w", maxAttempts, err)
		}

		logger.Info("Found %d Distribution events to check", len(logs))

		// Iterate through all logs to find matching deposit ID
		for _, log := range logs {
			// Make sure it's a Distribution event
			if len(log.Topics) == 0 || log.Topics[0] != distributionTopic {
				continue
			}

			// Parse the event data to extract the deposit ID
			decoded, err := parsedABI.Unpack("Distribution", log.Data)
			if err != nil {
				logger.Debug("Failed to decode event data: %v", err)
				continue
			}

			// The deposit ID should be the second parameter in the decoded data
			if len(decoded) < 2 {
				logger.Debug("Unexpected number of parameters in event data: %d", len(decoded))
				continue
			}

			eventDepositID, ok := decoded[1].(*big.Int)
			if !ok || eventDepositID == nil {
				logger.Debug("Failed to extract deposit ID from event data")
				continue
			}

			logger.Debug("Checking event deposit ID %s against target %s",
				eventDepositID.String(), depositID.String())

			// Compare with our target deposit ID
			if eventDepositID.Cmp(depositID) == 0 {
				logger.Info("Found matching Distribution event for deposit ID %s in tx %s",
					depositID.String(), log.TxHash.Hex())

				// Update transaction status in database
				if err := s.UpdateTransactionStatus(ctx, depositID, database.StatusCompleted, log.TxHash.Hex()); err != nil {
					logger.Error("Failed to update transaction status: %v", err)
				} else {
					// Update the cache
					s.updateTxCache(depositID, database.StatusCompleted, log.TxHash.Hex())
				}

				return log.TxHash.Hex(), nil
			}
		}

		// If we didn't find it with the current range, try a larger range
		if attempt < maxAttempts {
			blockRange = blockRange * 2
			if blockRange > 200000 {
				blockRange = 200000 // Cap at a reasonable maximum
			}

			fromBlock = uint64(0)
			if latestBlock > blockRange {
				fromBlock = latestBlock - blockRange
			}

			query.FromBlock = big.NewInt(int64(fromBlock))
			logger.Info("Expanding search to blocks %d through %d for attempt %d",
				fromBlock, latestBlock, attempt+1)
		}
	}

	logger.Warn("No matching Distribution event found for deposit ID %s after %d attempts",
		depositID.String(), maxAttempts)
	return "", fmt.Errorf("no matching distribution event found for deposit ID %s", depositID.String())
}

// FindMonadDistributionTransactionByDepositID searches for a distribution transaction by finding the associated distribution event
func (s *BridgeService) FindMonadDistributionTransactionByDepositID(ctx context.Context, depositID *big.Int) (string, error) {
	return s.FindMonadDistributionByDepositID(ctx, depositID)
}

// CheckOrCreateDistributionTransaction checks if a distribution transaction exists for a deposit ID and creates a record if needed
func (s *BridgeService) CheckOrCreateDistributionTransaction(ctx context.Context, depositID *big.Int) (*database.Transaction, error) {
	if depositID == nil {
		return nil, fmt.Errorf("deposit ID is nil")
	}

	logger.Info("Checking for distribution transaction: deposit_id=%s", depositID.String())

	// Check if transaction exists
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil {
		logger.Error("Failed to get transaction from database: %v", err)
		return nil, err
	}

	// If transaction already has a Monad tx hash and is marked completed, return it
	if tx != nil && tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
		logger.Info("Found existing completed transaction: monad_tx_hash=%s", tx.MonadTxHash)
		return tx, nil
	}

	// Search for distribution event on blockchain
	txHash, err := s.FindMonadDistributionByDepositID(ctx, depositID)
	if err != nil {
		logger.Warn("Failed to find distribution event: %v", err)
		return tx, err
	}

	// Update transaction with distribution transaction hash
	if tx != nil {
		tx.MonadTxHash = txHash
		tx.Status = database.StatusCompleted

		// Update in database
		err = s.db.UpdateTransactionStatus(depositID, database.StatusCompleted, txHash)
		if err != nil {
			logger.Error("Failed to update transaction status: %v", err)
			return nil, err
		}

		logger.Info("Updated transaction with distribution information: monad_tx_hash=%s, status=%s",
			txHash, database.StatusCompleted)
	} else {
		// Create minimal transaction record if none exists
		logger.Info("Creating minimal transaction record for deposit ID %s with tx hash %s",
			depositID.String(), txHash)

		minimalTx := &database.Transaction{
			DepositID:   depositID,
			MonadTxHash: txHash,
			Status:      database.StatusCompleted,
		}

		if err := s.db.CreateTransaction(minimalTx); err != nil {
			logger.Error("Failed to create minimal transaction record: %v", err)
			return nil, err
		}

		tx = minimalTx
	}

	return tx, nil
}
