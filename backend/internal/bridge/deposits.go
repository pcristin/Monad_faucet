package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/blockchain/listener"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Deposit Processing ---
//

// processDeposit processes a single deposit event.
func (s *BridgeService) processDeposit(event listener.DepositEvent) error {
	startTime := time.Now()
	logger.Info("Starting deposit processing for ID %s, amount %s", event.DepositId.String(), event.Amount.String())

	// Check if we've already processed this deposit
	alreadyProcessing := s.isProcessingDeposit(event.DepositId)
	if alreadyProcessing {
		logger.Warn("Skipping duplicate processing for deposit ID %s", event.DepositId.String())
		return nil
	}

	// Check if a transaction record already exists and is completed
	tx, err := s.db.GetTransactionByDepositID(event.DepositId)
	if err == nil && tx != nil && tx.Status == database.StatusCompleted {
		logger.Info("Transaction for deposit ID %s already completed with Monad tx hash %s", event.DepositId.String(), tx.MonadTxHash)
		return nil
	}

	// NEW CODE: Check if the deposit is already refunded or in the process of refunding
	// This prevents the token minting after a refund has been initiated
	if err == nil && tx != nil && (tx.Status == database.StatusRefunded || tx.Status == "refunding") {
		logger.Warn("Deposit ID %s is already refunded or in process of refunding (status: %s). Skipping token minting.",
			event.DepositId.String(), tx.Status)
		return fmt.Errorf("deposit already refunded or refunding")
	}

	// Log the deposit details
	logger.Info("Processing deposit %s from wallet %s, amount %s, currency %s",
		event.DepositId.String(),
		event.Depositor.Hex(),
		event.Amount.String(),
		blockchain.CurrencyTypeToString(event.Currency))

	// Get bridge state
	logger.Debug("Getting bridge state for deposit ID %s", event.DepositId.String())
	state, err := s.GetState(context.Background())
	if err != nil {
		logger.Error("Failed to get bridge state: %v", err)
		return err
	}

	// Check if the bridge is paused
	if state.IsPaused {
		logger.Error("Bridge is paused, rejecting deposit ID %s", event.DepositId.String())
		return fmt.Errorf("bridge is paused")
	}

	// Get the swap ratio for this currency
	swapRatio := state.SwapRatios[event.Currency]
	if swapRatio == nil {
		logger.Error("Invalid swap ratio for currency %s: %v",
			blockchain.CurrencyTypeToString(event.Currency), swapRatio)
		return fmt.Errorf("invalid swap ratio")
	}

	// Calculate MON amount
	logger.Debug("Calculating MON amount for deposit ID %s", event.DepositId.String())
	monAmount := calculateMonAmount(event.Amount, swapRatio, event.Currency)
	logger.Info("Calculated MON amount: %s from deposit amount: %s %s",
		formatMonAmount(monAmount),
		formatBigIntAsFloat(event.Amount, blockchain.GetCurrencyDecimals(event.Currency)),
		blockchain.CurrencyTypeToString(event.Currency))

	// Create a database task to ensure transaction record exists
	dbTask := workers.NewDatabaseTask("create_deposit", map[string]interface{}{
		"deposit": map[string]interface{}{
			"deposit_id":     event.DepositId.String(),
			"wallet_address": event.Depositor.Hex(),
			"amount":         event.Amount.String(),
			"currency":       int(event.Currency),
			"tx_hash":        event.TxHash,
			"block_number":   event.BlockNumber,
			"status":         database.StatusPending,
			"metadata":       event.Metadata,
		},
	})

	if !s.SubmitDatabaseTask(dbTask) {
		// Fall back to direct DB operation if worker pool submission fails
		logger.Warn("Failed to submit database task, falling back to direct DB operation")
		_, err = s.ensureTransactionRecord(event, monAmount)
		if err != nil {
			logger.Error("Failed to ensure transaction record: %v", err)
			return err
		}
	}

	logger.Info("Validating deposit ID %s", event.DepositId.String())
	if err := s.validateDepositWithAmount(state, event, monAmount); err != nil {
		logger.Error("Deposit validation failed for ID %s: %v", event.DepositId.String(), err)

		// Update status via worker pool
		updateStatusTask := workers.NewDatabaseTask("update_deposit_status", map[string]interface{}{
			"deposit_id": event.DepositId.String(),
			"status":     database.StatusFailed,
		})
		s.SubmitDatabaseTask(updateStatusTask)

		logger.Info("Queueing refund for deposit ID %s", event.DepositId.String())
		s.QueueRefund(event.DepositId)
		return fmt.Errorf("invalid deposit: %w", err)
	}

	logger.Info("Waiting for confirmations for deposit ID %s, block %d", event.DepositId.String(), event.BlockNumber)

	logger.Info("Minting %s MON tokens for wallet %s (deposit ID %s)", formatMonAmount(monAmount), event.Depositor.Hex(), event.DepositId.String())

	// Last-minute duplicate prevention.
	logger.Info("Checking for existing transaction for deposit ID %s", event.DepositId.String())
	if txHash, exists := s.checkExistingTransaction(context.Background(), event.DepositId); exists {
		logger.Info("Duplicate prevention: deposit ID %s already processed with tx %s", event.DepositId.String(), txHash)
		return nil
	}

	// Create a distribution task for parallel processing
	distributionTask := workers.NewDistributionTask(
		event.DepositId.String(),
		event.DepositId.String(), // Generate a unique distribution ID
		[]string{event.Depositor.Hex()},
		[]string{monAmount.String()},
	)

	// Submit the distribution task to the worker pool
	if !s.SubmitDistributionTask(distributionTask) {
		// Check if we can use worker pools directly for batching
		if s.workerPools != nil {
			logger.Info("Using worker pools batching for deposit ID %s", event.DepositId.String())

			// Create a distribution job
			distJob := &DistributionJob{
				DepositID:     event.DepositId,
				WalletAddress: event.Depositor,
				MonAmount:     monAmount,
			}

			// Add the job to the batch processing system
			s.workerPools.addToDistributionBatch(distJob)
			logger.Info("Added deposit ID %s to batch processing via worker pools", event.DepositId.String())
		} else {
			// Last resort: Fall back to direct minting if worker pools are not available
			logger.Warn("Failed to submit distribution task and worker pools not available, falling back to direct minting")

			logger.Info("Initiating token minting for deposit ID %s", event.DepositId.String())
			txHash, err := s.mintTokens(context.Background(), event.Depositor, monAmount, event.DepositId)
			if err != nil {
				if strings.Contains(err.Error(), "already completed") ||
					strings.Contains(err.Error(), "already in progress") ||
					strings.Contains(err.Error(), "duplicate mint attempt") {
					logger.Warn("Skipping refund for duplicate mint attempt: %v", err)
					if txHash != "" {
						logger.Info("Found completed tx %s, updating status for deposit ID %s", txHash, event.DepositId.String())
						_ = s.UpdateTransactionStatus(context.Background(), event.DepositId, database.StatusCompleted, txHash)
					}
					return fmt.Errorf("duplicate mint attempt: %w", err)
				}
				logger.Error("Mint tokens failed for deposit ID %s: %v", event.DepositId.String(), err)
				logger.Info("Updating transaction status to failed for deposit ID %s", event.DepositId.String())
				_ = s.UpdateTransactionStatus(context.Background(), event.DepositId, database.StatusFailed, "")
				logger.Info("Queueing refund for deposit ID %s", event.DepositId.String())
				s.QueueRefund(event.DepositId)
				return fmt.Errorf("failed to mint tokens: %w", err)
			}

			logger.Info("Successfully minted %s MON for wallet %s. Updating transaction status to completed with tx %s",
				formatMonAmount(monAmount), event.Depositor.Hex(), txHash)

			// Update transaction status directly
			if err := s.UpdateTransactionStatus(context.Background(), event.DepositId, database.StatusCompleted, txHash); err != nil {
				logger.Error("Failed to update transaction status: %v", err)
			}

			// Create distribution record directly
			err = s.createDistributionRecord(event.DepositId, event.Depositor, monAmount, database.DistStatusCompleted, txHash)
			if err != nil {
				logger.Error("Failed to create distribution record: %v", err)
			}
		}
	} else {
		logger.Info("Distribution task successfully submitted to worker pool for deposit ID %s", event.DepositId.String())
	}

	logger.Info("Processing completed for deposit ID %s in %v", event.DepositId.String(), time.Since(startTime))
	return nil
}

// ensureTransactionRecord ensures that a transaction record exists in the database for the given deposit event.
// If a record exists, it returns it. If not, it creates a new record.
func (s *BridgeService) ensureTransactionRecord(event listener.DepositEvent, monAmount *big.Int) (*database.Transaction, error) {
	// First check if the transaction already exists
	logger.Info("Checking for existing transaction record for deposit ID %s", event.DepositId.String())
	existingTx, err := s.db.GetTransactionByDepositID(event.DepositId)
	if err == nil && existingTx != nil {
		logger.Info("Found existing transaction record for deposit ID %s: status=%s",
			event.DepositId.String(), existingTx.Status)
		return existingTx, nil
	} else if err != nil {
		logger.Warn("Error looking up existing transaction for deposit ID %s: %v (will create new record)",
			event.DepositId.String(), err)
	}

	// Create a new transaction record
	logger.Info("Creating new transaction record for deposit ID %s from wallet %s, amount %s, metadata: '%s'",
		event.DepositId.String(), event.Depositor.Hex(), event.Amount.String(), event.Metadata)

	txRecord := &database.Transaction{
		DepositID:     event.DepositId,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		MonAmount:     monAmount,
		Status:        database.StatusPending,
		TxHash:        event.TxHash,
		Metadata:      sql.NullString{String: event.Metadata, Valid: event.Metadata != ""},
	}

	// Try multiple times to create the transaction
	var createErr error
	for attempt := 1; attempt <= 3; attempt++ {
		logger.Info("Attempt %d/3: Creating transaction record for deposit ID %s",
			attempt, event.DepositId.String())

		if err := s.db.CreateTransaction(txRecord); err != nil {
			createErr = err
			logger.Error("Failed to create transaction record (attempt %d/3): %v",
				attempt, err)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
			continue
		}

		// Creation successful
		createErr = nil
		break
	}

	if createErr != nil {
		logger.Error("All attempts to create transaction record failed for deposit ID %s: %v",
			event.DepositId.String(), createErr)
		return nil, fmt.Errorf("failed to create transaction record: %w", createErr)
	}

	// Verify the transaction was created
	logger.Info("Verifying transaction record was created for deposit ID %s", event.DepositId.String())
	verifyTx, err := s.db.GetTransactionByDepositID(event.DepositId)
	if err != nil {
		logger.Error("Failed to verify transaction creation: %v", err)
		return txRecord, fmt.Errorf("failed to verify transaction creation: %w", err)
	} else if verifyTx == nil {
		logger.Error("Transaction not found after creation for deposit ID %s", event.DepositId.String())
		return txRecord, fmt.Errorf("transaction not found after creation")
	}

	logger.Info("Transaction record successfully created and verified for deposit ID %s: ID=%d",
		event.DepositId.String(), verifyTx.ID)
	return verifyTx, nil
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
