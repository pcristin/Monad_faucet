package bridge

import (
	"context"
	"strings"
	"time"

	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Queue Processing (Deposits & Refunds) ---
//

// processDeposits reads deposit events from the channel and processes them.
func (s *BridgeService) processDeposits() {
	defer s.wg.Done()
	logger.Info("Starting deposit processor...")

	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-s.depositChan:
			// Instead of processing the deposit directly, submit it to the worker pool
			depositTask := workers.NewDepositTask(
				event.DepositId.String(),
				event.Depositor.Hex(),
				event.Amount.String(),
				event.TxHash,
			)

			// Add the event data to the task
			depositTask.SetEventData(event)

			if !s.SubmitDepositTask(depositTask) {
				logger.Error("Failed to submit deposit task for ID %s to worker pool", event.DepositId.String())
				// Try to process it directly as fallback
				start := time.Now()
				if err := s.processDeposit(event); err != nil {
					if strings.Contains(err.Error(), "duplicate mint attempt") {
						logger.Warn("Skipping refund for duplicate mint: %v", err)
					} else {
						logger.Error("Error processing deposit: %v", err)
						s.QueueRefund(event.DepositId)
					}
				}
				logger.Debug("Direct processing time: %v", time.Since(start))
			} else {
				logger.Debug("Successfully queued deposit ID %s to worker pool", event.DepositId.String())
			}
		}
	}
}

// processRefunds reads refund requests and processes them.
func (s *BridgeService) processRefunds() {
	defer s.wg.Done()
	logger.Info("Starting refund processor...")
	for {
		select {
		case <-s.ctx.Done():
			return
		case depositId := <-s.refundChan:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
			if err := s.refundDeposit(ctx, depositId); err != nil {
				logger.Error("Error processing refund for deposit ID %s: %v", depositId.String(), err)
			} else {
				logger.Info("Successfully refunded deposit ID: %s", depositId.String())
			}
			cancel()
		}
	}
}
