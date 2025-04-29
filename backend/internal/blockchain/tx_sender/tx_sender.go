package tx_sender

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// SendTransaction queues a transaction to be sent and returns result chanel
func (s *TransactionSender) SendTransaction(ctx context.Context, method string, params []interface{}, opts *bind.TransactOpts) <-chan *TransactionResult {
	resultCh := make(chan *TransactionResult, 1)

	req := &TransactionRequest{
		Method:   method,
		Params:   params,
		Opts:     opts,
		ResultCh: resultCh,
	}

	// Try to send the request with timeout from context
	select {
	case s.requestCh <- req:
		// Successfully queued
	case <-ctx.Done():
		// Context cancelled
		resultCh <- &TransactionResult{nil, ctx.Err()}
		close(resultCh)
	}

	return resultCh
}

// processTransactions is the main goroutine that processes transactions sequentially
func (s *TransactionSender) processTransactions() {
	defer s.wg.Done()

	// Keep track of the current nonce
	var currentNonce uint64
	var nonceInitialized bool

	for {
		select {
		case req := <-s.requestCh:
			// Initialize nonce if not already done
			if !nonceInitialized {
				nonce, err := s.client.PendingNonceAt(context.Background(), s.fromAddress)
				if err != nil {
					req.ResultCh <- &TransactionResult{nil, fmt.Errorf("failed to get initial nonce: %w", err)}
					close(req.ResultCh)
					continue
				}
				currentNonce = nonce
				nonceInitialized = true
				logger.Debug("Transaction sender initialized with nonce %d", currentNonce)
			}

			// Set up the nonce in the transaction options
			req.Opts.Nonce = new(big.Int).SetUint64(currentNonce)
			logger.Debug("Using nonce: %d for transaction", currentNonce)

			// Send the transaction
			tx, err := s.contract.Transact(req.Opts, req.Method, req.Params...)

			// Increment nonce on success
			if err == nil {
				currentNonce++
				logger.Debug("Transaction sent successfully, incremented nonce to %d", currentNonce)
			} else {
				// On certain errors, we might need to refresh the nonce
				logger.Error("Transaction failed: %v", err)

				// Refresh the nonce if error is due too low nonce
				if strings.Contains(err.Error(), "nonce too low") {
					nonce, nErr := s.client.PendingNonceAt(context.Background(), s.fromAddress)
					if nErr == nil {
						currentNonce = nonce
						logger.Debug("Reset the nonce to %d", currentNonce)
					}
				}
			}

			// Send the result
			req.ResultCh <- &TransactionResult{tx, err}
			close(req.ResultCh)

		case <-s.quitCh:
			return
		}
	}
}

// Stop gracefully stops the transaction sender
func (s *TransactionSender) Stop() {
	close(s.quitCh)
	s.wg.Wait()
}
