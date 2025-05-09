package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/blockchain/listener"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Start initializes the service and starts processing deposits.
func (s *BridgeService) Start() error {
	logger.Info("Starting bridge service...")

	// Initialize and start worker pools for batch processing
	if s.workerPools == nil {
		logger.Info("Initializing bridge worker pools for batch processing")
		s.workerPools = NewBridgeWorkerPools(s)
		s.workerPools.Start()
		logger.Info("Bridge worker pools initialized and started")
	}

	// Start the worker manager if it exists
	if wm := s.GetWorkerManager(); wm != nil {
		wm.StartAll()
	} else {
		logger.Warn("No worker manager set, skipping worker initialization")
	}

	// Start deposit processor goroutine
	s.wg.Add(1)
	go s.processDeposits()

	// Start refund processor goroutine
	s.wg.Add(1)
	go s.processRefunds()

	// Start recovery process for stuck transactions
	s.wg.Add(1)
	go s.recoverStuckTransactionsPeriodically()

	// Start periodic chain state synchronization to keep all chains in sync with Arbitrum
	s.wg.Add(1)
	go s.syncChainStatesPeriodically()

	// Start periodic lock release process for completed transactions
	s.wg.Add(1)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				count, err := s.db.ReleaseLocksForCompletedTransactions()
				if err != nil {
					logger.Error("Error releasing locks for completed transactions: %v", err)
				} else {
					logger.Info("Released %d locks for completed transactions", count)
				}
			case <-s.ctx.Done():
				logger.Info("Lock release process stopped")
				return
			}
		}
	}()

	logger.Info("Bridge service started successfully, all processors running")
	return nil
}

// Stop gracefully shuts down the service.
func (s *BridgeService) Stop() error {
	logger.Info("Stopping bridge service...")
	s.cancel()

	// Stop worker pools
	if s.workerPools != nil {
		logger.Info("Stopping bridge worker pools")
		s.workerPools.Stop()
	}

	// Stop the worker manager if it exists
	if wm := s.GetWorkerManager(); wm != nil {
		wm.StopAll()
	}

	s.wg.Wait()
	logger.Info("Bridge service stopped")
	return nil
}

// HandleDeposit queues a deposit for processing.
func (s *BridgeService) HandleDeposit(event listener.DepositEvent) {
	// First ensure the deposit is recorded in the database immediately
	if err := s.recordDepositImmediately(event); err != nil {
		logger.Error("Failed to immediately record deposit %s: %v", event.DepositId.String(), err)
	}

	// Get combined deposit ID for logging and processing
	combinedDepositID := listener.GenerateCombinedDepositID(event.Chain, event.DepositId)
	networkName, _ := listener.GetChainInfo(event.Chain)

	// Then handle the full processing through the appropriate channel
	// Check if we're using the worker manager or the direct channel approach
	if wm := s.GetWorkerManager(); wm != nil {
		// For worker manager, we need to provide a meaningful task implementation
		// Since DepositTask.Process() is empty, we're bypassing that worker pool
		// and directly queuing the event to the deposit channel for processing
		logger.Debug("Queueing deposit ID %s (%s chain) for full processing",
			combinedDepositID.String(), networkName)

		// Using a separate goroutine to avoid blocking the event listener
		go func() {
			select {
			case s.depositChan <- event:
				logger.Debug("Successfully queued deposit ID %s to processing channel",
					combinedDepositID.String())
			default:
				logger.Error("Deposit processing channel full, deposit ID %s may not be processed",
					combinedDepositID.String())
			}
		}()
	} else {
		// Direct channel approach (fallback)
		logger.Debug("Using direct channel for deposit ID %s", combinedDepositID.String())
		s.depositChan <- event
	}
}

// recordDepositImmediately creates a deposit record in the database as soon as an event is detected
// This ensures we don't miss deposits even if the full processing pipeline fails
func (s *BridgeService) recordDepositImmediately(event listener.DepositEvent) error {
	// Get network name for source chain
	networkName, _ := listener.GetChainInfo(event.Chain)

	// Generate combined deposit ID (chain_id + deposit_id)
	combinedDepositID := listener.GenerateCombinedDepositID(event.Chain, event.DepositId)

	// Check if this deposit has already been recorded with the combined ID
	existing, err := s.db.GetDepositByID(combinedDepositID)
	if err == nil && existing != nil {
		logger.Debug("Deposit ID %s (combined from chain %s, original ID %s) already recorded in database",
			combinedDepositID.String(), networkName, event.DepositId.String())
		return nil
	}

	// Create a new deposit record with pending status and the combined ID
	deposit := &database.Deposit{
		DepositID:     combinedDepositID,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		TxHash:        event.TxHash,
		BlockNumber:   event.BlockNumber,
		Status:        database.StatusPending,
		Metadata:      string(event.Metadata),
		SourceChain:   networkName,
	}

	// Write to database
	networkName, isTestnet := listener.GetChainInfo(event.Chain)
	networkType := "Mainnet"
	if isTestnet {
		networkType = "Testnet"
	}

	logger.Info("Immediately recording deposit with combined ID %s (chain %s, original ID %s) from wallet %s, amount %s, tx %s, chain %s-%s, metadata: '%s'",
		combinedDepositID.String(), networkName, event.DepositId.String(), event.Depositor.Hex(),
		event.Amount.String(), event.TxHash, networkName, networkType, event.Metadata)

	if err := s.db.CreateDeposit(deposit); err != nil {
		return fmt.Errorf("failed to create immediate deposit record: %w", err)
	}

	// Also create a corresponding transaction record so status lookups work
	txRecord := &database.Transaction{
		DepositID:     combinedDepositID,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		Status:        database.StatusPending,
		TxHash:        event.TxHash,
		Metadata:      sql.NullString{String: event.Metadata, Valid: event.Metadata != ""},
		SourceChain:   networkName,
	}

	if err := s.db.CreateTransaction(txRecord); err != nil {
		logger.Warn("Failed to create immediate transaction record for deposit ID %s: %v",
			combinedDepositID.String(), err)
		// Don't return an error here, the deposit is already recorded which is the main goal
	} else {
		logger.Debug("Successfully created transaction record for deposit ID %s", combinedDepositID.String())
	}

	logger.Debug("Successfully recorded deposit ID %s in database", combinedDepositID.String())
	return nil
}

// GetState returns the current state of the contracts.
func (s *BridgeService) GetState(ctx context.Context) (*blockchain.ContractState, error) {
	// Get state from Arbitrum - this is the main/leader contract
	state, err := blockchain.GetContractState(ctx, s.arbDepositor, s.monadDistributor)
	if err != nil {
		return nil, fmt.Errorf("failed to get Arbitrum contract state: %w", err)
	}

	// We only consider Arbitrum's pause status as the authoritative state
	// No need to check other chains, as they should follow Arbitrum's state

	return state, nil
}

//
// --- Other Helpers and Getters ---
//

func (s *BridgeService) CheckBlockchainConnections() error {
	if _, err := s.arbDepositor.Client.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("arbitrum connection failed: %w", err)
	}

	if s.baseDepositor != nil {
		if _, err := s.baseDepositor.Client.BlockNumber(context.Background()); err != nil {
			return fmt.Errorf("base connection failed: %w", err)
		}
	}

	if s.optimismDepositor != nil {
		if _, err := s.optimismDepositor.Client.BlockNumber(context.Background()); err != nil {
			return fmt.Errorf("optimism connection failed: %w", err)
		}
	}

	if _, err := s.monadDistributor.Client.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("monad connection failed: %w", err)
	}
	return nil
}

func (s *BridgeService) GracefulShutdown(ctx context.Context) {
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Bridge service stopped gracefully")
	case <-ctx.Done():
		logger.Warn("Bridge service forced to stop")
	}
}

// GetArbitrumClient returns the Arbitrum blockchain client.
func (s *BridgeService) GetArbitrumClient() *ethclient.Client {
	return s.arbDepositor.Client
}

// GetArbitrumContractAddress returns the Arbitrum contract address.
func (s *BridgeService) GetArbitrumContractAddress() common.Address {
	return s.arbDepositor.Address
}

// GetBaseClient returns the Base blockchain client.
func (s *BridgeService) GetBaseClient() *ethclient.Client {
	if s.baseDepositor == nil {
		return nil
	}
	return s.baseDepositor.Client
}

// GetBaseContractAddress returns the Base contract address.
func (s *BridgeService) GetBaseContractAddress() common.Address {
	if s.baseDepositor == nil {
		return common.Address{}
	}
	return s.baseDepositor.Address
}

// GetOptimismClient returns the Optimism blockchain client.
func (s *BridgeService) GetOptimismClient() *ethclient.Client {
	if s.optimismDepositor == nil {
		return nil
	}
	return s.optimismDepositor.Client
}

// GetOptimismContractAddress returns the Optimism contract address.
func (s *BridgeService) GetOptimismContractAddress() common.Address {
	if s.optimismDepositor == nil {
		return common.Address{}
	}
	return s.optimismDepositor.Address
}

// GetMonadClient returns the Monad blockchain client.
func (s *BridgeService) GetMonadClient() *ethclient.Client {
	return s.monadDistributor.Client
}

// GetMonadContractAddress returns the Monad contract address.
func (s *BridgeService) GetMonadContractAddress() common.Address {
	return s.monadDistributor.Address
}

// GetTransactionByDepositID retrieves a transaction by its deposit ID.
func (s *BridgeService) GetTransactionByDepositID(ctx context.Context, depositID *big.Int) (*database.Transaction, error) {
	// This is for backward compatibility with the old schema
	return s.db.GetTransactionByDepositID(depositID)
}

// UpdateTransactionStatus updates the status of a transaction.
func (s *BridgeService) UpdateTransactionStatus(ctx context.Context, depositID *big.Int, status, txHash string) error {
	logger.Debug("Updating transaction status for deposit ID %s to %s with txHash %s",
		depositID.String(), status, txHash)

	// If this is a completed transaction, handle the distribution record FIRST
	// This ensures the MON amount is available when updating the transaction
	var monAmount *big.Int
	if status == database.StatusCompleted && txHash != "" {
		// Get the deposit to get wallet address
		deposit, err := s.db.GetDepositByID(depositID)
		if err != nil || deposit == nil {
			logger.Error("Failed to get deposit for ID %s: %v", depositID.String(), err)
			// Continue anyway
		} else {
			// Get transaction to get MON amount
			tx, err := s.db.GetTransactionByDepositID(depositID)
			if err != nil || tx == nil {
				logger.Error("Failed to get transaction for deposit ID %s: %v", depositID.String(), err)
				// Continue anyway
			} else {
				monAmount = tx.MonAmount
				if monAmount == nil || monAmount.Cmp(big.NewInt(0)) <= 0 {
					logger.Warn("Transaction has no MON amount, using a default calculation for ID %s", depositID.String())
					// Use a default calculation
					monAmount = new(big.Int).Mul(deposit.Amount, big.NewInt(10))
				}

				// Check if we should use worker pools for distribution record
				if s.workerPools != nil {
					logger.Debug("Using worker pools for distribution record management")

					// Submit distribution creation/update to worker pool
					s.workerPools.DBPool.Submit(&DBWorkerJob{
						JobType: JobCreateDistribution,
						Distribution: &database.Distribution{
							DepositID:     depositID,
							WalletAddress: deposit.WalletAddress,
							MonAmount:     monAmount,
							Status:        database.DistStatusCompleted,
							MonadTxHash:   txHash,
						},
					})
				} else {
					// Direct database operations if worker pools not available
					existingDist, _ := s.db.GetDistributionByDepositID(depositID)
					if existingDist != nil {
						// Update existing distribution
						logger.Debug("Updating existing distribution record for deposit ID %s with txHash %s",
							depositID.String(), txHash)

						if err := s.db.UpdateDistributionStatus(depositID, database.DistStatusCompleted, txHash); err != nil {
							logger.Error("Failed to update distribution status: %v", err)
						} else {
							// Get updated distribution to ensure we have the latest MON amount
							updatedDist, _ := s.db.GetDistributionByDepositID(depositID)
							if updatedDist != nil && updatedDist.MonAmount != nil {
								monAmount = updatedDist.MonAmount
								logger.Debug("Using MON amount %s from updated distribution record", monAmount.String())
							}
						}
					} else {
						// Create new distribution record
						logger.Debug("Creating new distribution record for deposit ID %s with txHash %s and amount %s",
							depositID.String(), txHash, monAmount.String())

						dist := &database.Distribution{
							DepositID:     depositID,
							WalletAddress: deposit.WalletAddress,
							MonAmount:     monAmount,
							Status:        database.DistStatusCompleted,
							MonadTxHash:   txHash,
						}

						if err := s.db.CreateDistribution(dist); err != nil {
							logger.Error("Failed to create distribution record: %v", err)
						}
					}
				}
			}
		}
	}

	// Now, update the transaction with the MON amount
	var err error
	if monAmount != nil && monAmount.Cmp(big.NewInt(0)) > 0 {
		logger.Debug("Updating transaction with explicit MON amount: %s for ID %s",
			monAmount.String(), depositID.String())

		// Custom update that directly sets the MON amount
		err = s.db.UpdateTransactionWithMonAmount(depositID, status, txHash, monAmount)
	} else {
		// Fall back to standard update if we couldn't get the MON amount
		err = s.db.UpdateTransactionStatus(depositID, status, txHash)
	}

	if err != nil {
		logger.Error("Failed to update transaction status: %v", err)
		return err
	}

	// Also update the deposit status in the deposits table
	// Map transaction status to deposit status
	depositStatus := status
	if status == database.StatusCompleted {
		depositStatus = database.StatusProcessed
	} else if status == database.StatusFailed || status == database.StatusRefunded {
		depositStatus = status // Use the same status
	}

	// Submit deposit status update to worker pool if available
	if s.workerPools != nil {
		s.workerPools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: depositID,
				Status:    depositStatus,
			},
		})
		logger.Debug("Queued deposit status update to worker pool for ID %s", depositID.String())
	} else {
		// Direct update if worker pools not available
		if err := s.db.UpdateDepositStatus(depositID, depositStatus); err != nil {
			logger.Error("Failed to update deposit status after updating transaction: %v", err)
			// Don't return error here - we still successfully updated the transaction
		} else {
			logger.Debug("Successfully updated deposit status for ID %s", depositID.String())
		}
	}

	return nil
}

// syncChainStatesPeriodically periodically synchronizes the pause state across all chains.
// It ensures all chains follow the Arbitrum contract's pause state.
func (s *BridgeService) syncChainStatesPeriodically() {
	defer s.wg.Done()

	// Perform initial synchronization at startup
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := s.SyncDepositorPauseStates(ctx); err != nil {
		logger.Error("Initial chain state synchronization failed: %v", err)
	}
	cancel()

	// Synchronize chain states every five minutes
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logger.Debug("Performing periodic chain state synchronization")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := s.SyncDepositorPauseStates(ctx); err != nil {
				logger.Error("Chain state synchronization failed: %v", err)
			}
			cancel()
		case <-s.ctx.Done():
			logger.Debug("Chain state synchronization stopped")
			return
		}
	}
}
