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
