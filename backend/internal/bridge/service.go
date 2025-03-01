package bridge

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Start initializes the service and starts processing deposits.
func (s *BridgeService) Start() error {
	logger.Info("Starting bridge service...")
	s.wg.Add(1)
	go s.processDeposits()
	s.wg.Add(1)
	go s.processRefunds()
	s.wg.Add(1)
	go s.recoverStuckTransactionsPeriodically()
	return nil
}

// Stop gracefully shuts down the service.
func (s *BridgeService) Stop() error {
	logger.Info("Stopping bridge service...")
	s.cancel()
	s.wg.Wait()
	logger.Info("Bridge service stopped")
	return nil
}

// HandleDeposit queues a deposit for processing.
func (s *BridgeService) HandleDeposit(event blockchain.DepositEvent) {
	select {
	case s.depositChan <- event:
		logger.Info("Queued deposit: %s", event.String())
	default:
		logger.Warn("Deposit channel full, dropping event: %s", event.String())
	}
}

// QueueRefund queues a deposit ID for refund after checking safety.
func (s *BridgeService) QueueRefund(depositId *big.Int) {
	depositIDStr := depositId.String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check database.
	tx, err := s.GetTransactionByDepositID(ctx, depositId)
	if err == nil && tx != nil && tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
		logger.Warn("⚠️ REFUND PREVENTED: Deposit ID %s already completed with tx %s", depositIDStr, tx.MonadTxHash)
		return
	}

	// Check blockchain.
	txHash, err := s.checkMonadBlockchainForTransaction(ctx, depositId)
	if err == nil && txHash != "" {
		logger.Warn("⚠️ REFUND PREVENTED: Found tx %s on blockchain for deposit ID %s", txHash, depositIDStr)
		if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, txHash); updateErr != nil {
			logger.Error("Failed to update tx status during refund prevention: %v", updateErr)
		}
		return
	}

	select {
	case s.refundChan <- depositId:
		logger.Info("Queued refund for deposit ID: %s", depositIDStr)
	default:
		logger.Warn("Refund channel full, dropping deposit ID: %s", depositIDStr)
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
		logger.Info("All in-progress transactions completed")
	case <-ctx.Done():
		logger.Warn("Shutdown timed out, some transactions may not have completed")
	}
}

func (s *BridgeService) GetArbitrumClient() *ethclient.Client {
	return s.arbDepositor.Client
}

func (s *BridgeService) GetArbitrumContractAddress() common.Address {
	return s.arbDepositor.Address
}
