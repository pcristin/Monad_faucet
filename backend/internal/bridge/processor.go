package bridge

import (
	"context"
	"strings"
	"time"

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
			start := time.Now()
			if err := s.processDeposit(event); err != nil {
				if strings.Contains(err.Error(), "duplicate mint attempt") {
					logger.Warn("Skipping refund for duplicate mint: %v", err)
				} else {
					logger.Error("Error processing deposit: %v", err)
					s.QueueRefund(event.DepositId)
				}
			}
			logger.Info("Processing time: %v", time.Since(start))
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
