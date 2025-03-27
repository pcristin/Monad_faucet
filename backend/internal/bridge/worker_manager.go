package bridge

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// QueueRefund adds a deposit ID to the refund channel for processing
func (s *BridgeService) QueueRefund(depositID *big.Int) {
	select {
	case s.refundChan <- depositID:
		logger.Info("Queued refund for deposit ID: %s", depositID.String())
	default:
		logger.Error("Failed to queue refund for deposit ID: %s - channel full", depositID.String())
	}
}

// SetWorkerManager sets the worker manager for the bridge service
func (s *BridgeService) SetWorkerManager(manager *workers.Manager) {
	s.workerManager = manager
	logger.Info("Worker manager set for bridge service")
}

// GetWorkerManager returns the worker manager
func (s *BridgeService) GetWorkerManager() *workers.Manager {
	return s.workerManager
}

// SubmitDepositTask submits a deposit task to the worker pool
func (s *BridgeService) SubmitDepositTask(task *workers.DepositTask) bool {
	wm := s.GetWorkerManager()
	if wm == nil {
		logger.Error("Worker manager not set")
		return false
	}

	// Set up a custom processor to handle this task when it's processed by a worker
	task.SetCustomProcessor(func(t interface{}) error {
		depositTask, ok := t.(*workers.DepositTask)
		if !ok {
			return fmt.Errorf("invalid task type")
		}
		return s.HandleDepositTask(depositTask)
	})

	return wm.SubmitTask(workers.DepositPool, task)
}

// SubmitCalculationTask submits a calculation task to the worker pool
func (s *BridgeService) SubmitCalculationTask(task *workers.CalculationTask) bool {
	wm := s.GetWorkerManager()
	if wm == nil {
		logger.Error("Worker manager not set")
		return false
	}

	return wm.SubmitTask(workers.CalculationPool, task)
}

// SubmitDistributionTask submits a distribution task to the worker pool
func (s *BridgeService) SubmitDistributionTask(task *workers.DistributionTask) bool {
	wm := s.GetWorkerManager()
	if wm == nil {
		logger.Error("Worker manager not set")
		return false
	}

	// Set up a custom processor
	task.SetCustomProcessor(func(t interface{}) error {
		distTask, ok := t.(*workers.DistributionTask)
		if !ok {
			return fmt.Errorf("invalid task type")
		}
		return s.HandleDistributionTask(distTask)
	})

	return wm.SubmitTask(workers.DistributionPool, task)
}

// SubmitDatabaseTask submits a database task to the worker pool
func (s *BridgeService) SubmitDatabaseTask(task *workers.DatabaseTask) bool {
	wm := s.GetWorkerManager()
	if wm == nil {
		logger.Error("Worker manager not set")
		return false
	}

	// Set up a custom processor
	task.SetCustomProcessor(func(t interface{}) error {
		dbTask, ok := t.(*workers.DatabaseTask)
		if !ok {
			return fmt.Errorf("invalid task type")
		}
		return s.HandleDatabaseTask(dbTask)
	})

	return wm.SubmitTask(workers.DatabasePool, task)
}

// HandleDepositTask processes deposit tasks from the worker pool
func (s *BridgeService) HandleDepositTask(task *workers.DepositTask) error {
	if task.EventData == nil {
		return fmt.Errorf("no event data in deposit task")
	}

	// Cast the event data back to the original event type
	event, ok := task.EventData.(blockchain.DepositEvent)
	if !ok {
		return fmt.Errorf("invalid event data type in deposit task")
	}

	// Process the deposit using the existing method
	start := time.Now()
	err := s.processDeposit(event)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate mint attempt") {
			logger.Warn("Skipping refund for duplicate mint: %v", err)
			return nil
		}
		logger.Error("Error processing deposit from worker pool: %v", err)
		s.QueueRefund(event.DepositId)
		return err
	}

	logger.Info("Worker pool processing time for deposit %s: %v",
		task.DepositID, time.Since(start))
	return nil
}

// HandleDistributionTask processes distribution tasks from the worker pool
func (s *BridgeService) HandleDistributionTask(task *workers.DistributionTask) error {
	logger.Info("Handling distribution task %s for deposit %s", task.DistributionID, task.DepositID)

	// Convert the task data to the format needed for minting tokens
	depositID, ok := new(big.Int).SetString(task.DepositID, 10)
	if !ok {
		return fmt.Errorf("invalid deposit ID: %s", task.DepositID)
	}

	// For now, we only support single recipient distributions
	if len(task.Recipients) != 1 || len(task.Amounts) != 1 {
		return fmt.Errorf("expected 1 recipient and amount, got %d recipients and %d amounts",
			len(task.Recipients), len(task.Amounts))
	}

	recipient := common.HexToAddress(task.Recipients[0])
	amount := new(big.Int)
	if _, ok := amount.SetString(task.Amounts[0], 10); !ok {
		return fmt.Errorf("invalid amount: %s", task.Amounts[0])
	}

	// Use the existing mintTokens method to perform the distribution
	start := time.Now()
	txHash, err := s.mintTokens(context.Background(), recipient, amount, depositID)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate mint attempt") {
			logger.Warn("Skipping duplicate mint: %v", err)
			return nil
		}
		logger.Error("Error processing distribution from worker pool: %v", err)
		return err
	}

	// Update the transaction status
	if err := s.UpdateTransactionStatus(context.Background(), depositID, database.StatusCompleted, txHash); err != nil {
		logger.Error("Failed to update transaction status: %v", err)
	}

	// Update the distribution record
	if err := s.createDistributionRecord(depositID, recipient, amount, database.DistStatusCompleted, txHash); err != nil {
		logger.Error("Failed to create/update distribution record: %v", err)
	}

	logger.Info("Worker pool distribution time for deposit %s: %v",
		task.DepositID, time.Since(start))
	return nil
}

// HandleDatabaseTask processes database tasks from the worker pool
func (s *BridgeService) HandleDatabaseTask(task *workers.DatabaseTask) error {
	logger.Info("Handling database task with operation %s", task.Operation)

	switch task.Operation {
	case "create_deposit":
		// Extract deposit data from the task
		depositData, ok := task.Data["deposit"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid deposit data format")
		}

		// Create a deposit object from the data
		deposit := &database.Deposit{}

		// Extract and convert deposit ID
		if depositIDStr, ok := depositData["deposit_id"].(string); ok {
			depositID := new(big.Int)
			depositID.SetString(depositIDStr, 10)
			deposit.DepositID = depositID
		} else {
			return fmt.Errorf("invalid deposit ID")
		}

		// Extract other fields (simplified for example)
		if walletStr, ok := depositData["wallet_address"].(string); ok {
			deposit.WalletAddress = common.HexToAddress(walletStr)
		}

		if status, ok := depositData["status"].(string); ok {
			deposit.Status = status
		} else {
			deposit.Status = database.StatusPending
		}

		// Create the deposit
		return s.db.CreateDeposit(deposit)

	case "update_deposit_status":
		// Extract deposit ID and status
		depositIDStr, ok := task.Data["deposit_id"].(string)
		if !ok {
			return fmt.Errorf("missing deposit ID")
		}

		status, ok := task.Data["status"].(string)
		if !ok {
			return fmt.Errorf("missing status")
		}

		// Convert deposit ID
		depositID := new(big.Int)
		depositID.SetString(depositIDStr, 10)

		// Update the deposit status
		return s.db.UpdateDepositStatus(depositID, status)

	case "create_distribution":
		// Similar implementation to create_deposit but for distributions
		distributionData, ok := task.Data["distribution"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid distribution data format")
		}

		// Create a distribution object and populate it
		dist := &database.Distribution{}

		// Extract and convert deposit ID
		if depositIDStr, ok := distributionData["deposit_id"].(string); ok {
			depositID := new(big.Int)
			depositID.SetString(depositIDStr, 10)
			dist.DepositID = depositID
		} else {
			return fmt.Errorf("invalid deposit ID")
		}

		// Extract other fields
		if walletStr, ok := distributionData["wallet_address"].(string); ok {
			dist.WalletAddress = common.HexToAddress(walletStr)
		}

		if amountStr, ok := distributionData["mon_amount"].(string); ok {
			amount := new(big.Int)
			amount.SetString(amountStr, 10)
			dist.MonAmount = amount
		}

		if status, ok := distributionData["status"].(string); ok {
			dist.Status = status
		} else {
			dist.Status = database.DistStatusPending
		}

		if txHash, ok := distributionData["monad_tx_hash"].(string); ok {
			dist.MonadTxHash = txHash
		}

		// Create the distribution
		return s.db.CreateDistribution(dist)

	case "update_distribution_status":
		// Extract necessary fields
		depositIDStr, ok := task.Data["deposit_id"].(string)
		if !ok {
			return fmt.Errorf("missing deposit ID")
		}

		status, ok := task.Data["status"].(string)
		if !ok {
			return fmt.Errorf("missing status")
		}

		txHash, _ := task.Data["monad_tx_hash"].(string)

		// Convert deposit ID
		depositID := new(big.Int)
		depositID.SetString(depositIDStr, 10)

		// Update the distribution status
		return s.db.UpdateDistributionStatus(depositID, status, txHash)

	default:
		return fmt.Errorf("unknown database operation: %s", task.Operation)
	}
}
