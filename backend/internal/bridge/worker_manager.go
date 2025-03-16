package bridge

import (
	"math/big"

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

	return wm.SubmitTask(workers.DistributionPool, task)
}

// SubmitDatabaseTask submits a database task to the worker pool
func (s *BridgeService) SubmitDatabaseTask(task *workers.DatabaseTask) bool {
	wm := s.GetWorkerManager()
	if wm == nil {
		logger.Error("Worker manager not set")
		return false
	}

	return wm.SubmitTask(workers.DatabasePool, task)
}
