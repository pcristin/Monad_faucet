package bridge

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Start initializes the service and starts processing deposits.
func (s *BridgeService) Start() error {
	logger.Info("Starting bridge service...")

	// Start the worker manager if it exists
	if wm := s.GetWorkerManager(); wm != nil {
		wm.StartAll()
	} else {
		logger.Warn("No worker manager set, skipping worker initialization")
	}

	// Start recovery process for stuck transactions
	s.wg.Add(1)
	go s.recoverStuckTransactionsPeriodically()

	return nil
}

// Stop gracefully shuts down the service.
func (s *BridgeService) Stop() error {
	logger.Info("Stopping bridge service...")
	s.cancel()

	// Stop the worker manager if it exists
	if wm := s.GetWorkerManager(); wm != nil {
		wm.StopAll()
	}

	s.wg.Wait()
	logger.Info("Bridge service stopped")
	return nil
}

// HandleDeposit queues a deposit for processing.
func (s *BridgeService) HandleDeposit(event blockchain.DepositEvent) {
	// Create a deposit task and submit it to the worker pool
	depositTask := &workers.DepositTask{
		BaseTask:    workers.NewBaseTask("deposit"),
		DepositID:   event.DepositId.String(),
		UserAddress: event.Depositor.Hex(),
		Amount:      event.Amount.String(),
		TxHash:      event.TxHash,
	}

	if wm := s.GetWorkerManager(); wm != nil {
		wm.SubmitTask(workers.DepositPool, depositTask)
	} else {
		// Fallback to the old direct channel approach if no worker manager
		s.depositChan <- event
	}
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
	// This is for backward compatibility with the old schema
	return s.db.UpdateTransactionStatus(depositID, status, txHash)
}
