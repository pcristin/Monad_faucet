package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"

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

	// Then handle the full processing through the appropriate channel
	// Check if we're using the worker manager or the direct channel approach
	if wm := s.GetWorkerManager(); wm != nil {
		// For worker manager, we need to provide a meaningful task implementation
		// Since DepositTask.Process() is empty, we're bypassing that worker pool
		// and directly queuing the event to the deposit channel for processing
		logger.Info("Queueing deposit ID %s for full processing", event.DepositId.String())

		// Using a separate goroutine to avoid blocking the event listener
		go func() {
			select {
			case s.depositChan <- event:
				logger.Info("Successfully queued deposit ID %s to processing channel", event.DepositId.String())
			default:
				logger.Error("Deposit processing channel full, deposit ID %s may not be processed",
					event.DepositId.String())
			}
		}()
	} else {
		// Direct channel approach (fallback)
		logger.Info("Using direct channel for deposit ID %s", event.DepositId.String())
		s.depositChan <- event
	}
}

// recordDepositImmediately creates a deposit record in the database as soon as an event is detected
// This ensures we don't miss deposits even if the full processing pipeline fails
func (s *BridgeService) recordDepositImmediately(event listener.DepositEvent) error {
	// Check if this deposit has already been recorded
	existing, err := s.db.GetDepositByID(event.DepositId)
	if err == nil && existing != nil {
		logger.Info("Deposit ID %s already recorded in database", event.DepositId.String())
		return nil
	}

	// Create a new deposit record with pending status
	deposit := &database.Deposit{
		DepositID:     event.DepositId,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		TxHash:        event.TxHash,
		BlockNumber:   event.BlockNumber,
		Status:        database.StatusPending,
		Metadata:      string(event.Metadata),
	}

	// Write to database
	logger.Info("Immediately recording deposit ID %s from wallet %s, amount %s, tx %s, metadata: '%s'",
		event.DepositId.String(), event.Depositor.Hex(), event.Amount.String(), event.TxHash, event.Metadata)

	if err := s.db.CreateDeposit(deposit); err != nil {
		return fmt.Errorf("failed to create immediate deposit record: %w", err)
	}

	// Also create a corresponding transaction record so status lookups work
	txRecord := &database.Transaction{
		DepositID:     event.DepositId,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		Status:        database.StatusPending,
		TxHash:        event.TxHash,
		Metadata:      sql.NullString{String: event.Metadata, Valid: event.Metadata != ""},
	}

	if err := s.db.CreateTransaction(txRecord); err != nil {
		logger.Warn("Failed to create immediate transaction record for deposit ID %s: %v",
			event.DepositId.String(), err)
		// Don't return an error here, the deposit is already recorded which is the main goal
	} else {
		logger.Info("Successfully created transaction record for deposit ID %s", event.DepositId.String())
	}

	logger.Info("Successfully recorded deposit ID %s in database", event.DepositId.String())
	return nil
}

// GetState returns the current state of both contracts.
func (s *BridgeService) GetState(ctx context.Context) (*blockchain.ContractState, error) {
	return blockchain.GetContractState(ctx, s.arbDepositor, s.monadDistributor)
}

//
// --- Other Helpers and Getters ---
//

func (s *BridgeService) CheckBlockchainConnections() error {
	if _, err := s.arbDepositor.Client.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("arbitrum connection failed: %w", err)
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
	logger.Info("Updating transaction status for deposit ID %s to %s with txHash %s",
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
					logger.Info("Using worker pools for distribution record management")

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
						logger.Info("Updating existing distribution record for deposit ID %s with txHash %s",
							depositID.String(), txHash)

						if err := s.db.UpdateDistributionStatus(depositID, database.DistStatusCompleted, txHash); err != nil {
							logger.Error("Failed to update distribution status: %v", err)
						} else {
							// Get updated distribution to ensure we have the latest MON amount
							updatedDist, _ := s.db.GetDistributionByDepositID(depositID)
							if updatedDist != nil && updatedDist.MonAmount != nil {
								monAmount = updatedDist.MonAmount
								logger.Info("Using MON amount %s from updated distribution record", monAmount.String())
							}
						}
					} else {
						// Create new distribution record
						logger.Info("Creating new distribution record for deposit ID %s with txHash %s and amount %s",
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
		logger.Info("Updating transaction with explicit MON amount: %s for ID %s",
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
		logger.Info("Queued deposit status update to worker pool for ID %s", depositID.String())
	} else {
		// Direct update if worker pools not available
		if err := s.db.UpdateDepositStatus(depositID, depositStatus); err != nil {
			logger.Error("Failed to update deposit status after updating transaction: %v", err)
			// Don't return error here - we still successfully updated the transaction
		} else {
			logger.Info("Successfully updated deposit status for ID %s", depositID.String())
		}
	}

	return nil
}
