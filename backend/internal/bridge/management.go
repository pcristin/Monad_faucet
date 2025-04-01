package bridge

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Pause/Resume Deposits and wallet limit percentage (using a helper) ---
//

// callDepositorMethod calls a method on the Arbitrum depositor contract.
func (s *BridgeService) callDepositorMethod(ctx context.Context, method string) error {
	publicKey := s.arbDepositor.PrivateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	input, err := blockchain.DepositorABI.Pack(method)
	if err != nil {
		return fmt.Errorf("failed to pack %s data: %v", method, err)
	}
	gasPrice, err := s.arbDepositor.Client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))
	msg := ethereum.CallMsg{From: fromAddress, To: &s.arbDepositor.Address, Data: input}
	gasLimit, err := s.arbDepositor.Client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}
	gasLimit = gasLimit * 12 / 10
	nonce, err := s.arbDepositor.Client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.arbDepositor.Address,
		Value:    big.NewInt(0),
		Data:     input,
	})
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.arbDepositor.ChainID), s.arbDepositor.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to sign tx: %v", err)
	}
	if err = s.arbDepositor.Client.SendTransaction(ctx, signedTx); err != nil {
		return fmt.Errorf("failed to send %s tx: %v", method, err)
	}
	receipt, err := bind.WaitMined(ctx, s.arbDepositor.Client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for %s tx: %v", method, err)
	}
	if receipt.Status == 0 {
		return fmt.Errorf("%s tx failed", method)
	}
	logger.Info("Successfully executed %s on Arbitrum (tx: %s)", method, signedTx.Hash().Hex())
	return nil
}

// callBaseDepositorMethod calls a method on the Base depositor contract.
func (s *BridgeService) callBaseDepositorMethod(ctx context.Context, method string) error {
	if s.baseDepositor == nil {
		logger.Warn("Base depositor not initialized, skipping %s call", method)
		return nil
	}

	publicKey := s.baseDepositor.PrivateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	input, err := blockchain.DepositorABI.Pack(method)
	if err != nil {
		return fmt.Errorf("failed to pack %s data: %v", method, err)
	}
	gasPrice, err := s.baseDepositor.Client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))
	msg := ethereum.CallMsg{From: fromAddress, To: &s.baseDepositor.Address, Data: input}
	gasLimit, err := s.baseDepositor.Client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}
	gasLimit = gasLimit * 12 / 10
	nonce, err := s.baseDepositor.Client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.baseDepositor.Address,
		Value:    big.NewInt(0),
		Data:     input,
	})
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.baseDepositor.ChainID), s.baseDepositor.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to sign tx: %v", err)
	}
	if err = s.baseDepositor.Client.SendTransaction(ctx, signedTx); err != nil {
		return fmt.Errorf("failed to send %s tx: %v", method, err)
	}
	receipt, err := bind.WaitMined(ctx, s.baseDepositor.Client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for %s tx: %v", method, err)
	}
	if receipt.Status == 0 {
		return fmt.Errorf("%s tx failed", method)
	}
	logger.Info("Successfully executed %s on Base (tx: %s)", method, signedTx.Hash().Hex())
	return nil
}

// callOptimismDepositorMethod calls a method on the Optimism depositor contract.
func (s *BridgeService) callOptimismDepositorMethod(ctx context.Context, method string) error {
	if s.optimismDepositor == nil {
		logger.Warn("Optimism depositor not initialized, skipping %s call", method)
		return nil
	}

	publicKey := s.optimismDepositor.PrivateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	input, err := blockchain.DepositorABI.Pack(method)
	if err != nil {
		return fmt.Errorf("failed to pack %s data: %v", method, err)
	}
	gasPrice, err := s.optimismDepositor.Client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))
	msg := ethereum.CallMsg{From: fromAddress, To: &s.optimismDepositor.Address, Data: input}
	gasLimit, err := s.optimismDepositor.Client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}
	gasLimit = gasLimit * 12 / 10
	nonce, err := s.optimismDepositor.Client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.optimismDepositor.Address,
		Value:    big.NewInt(0),
		Data:     input,
	})
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.optimismDepositor.ChainID), s.optimismDepositor.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to sign tx: %v", err)
	}
	if err = s.optimismDepositor.Client.SendTransaction(ctx, signedTx); err != nil {
		return fmt.Errorf("failed to send %s tx: %v", method, err)
	}
	receipt, err := bind.WaitMined(ctx, s.optimismDepositor.Client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for %s tx: %v", method, err)
	}
	if receipt.Status == 0 {
		return fmt.Errorf("%s tx failed", method)
	}
	logger.Info("Successfully executed %s on Optimism (tx: %s)", method, signedTx.Hash().Hex())
	return nil
}

// PauseDeposits pauses deposit functionality on Arbitrum (leader chain) and then syncs to other chains.
func (s *BridgeService) PauseDeposits(ctx context.Context) error {
	// First pause Arbitrum (the leader/main chain)
	err := s.callDepositorMethod(ctx, "pauseDeposits")
	if err != nil {
		return fmt.Errorf("failed to pause Arbitrum deposits: %v", err)
	}

	logger.Info("Successfully paused Arbitrum deposits")

	// Then sync the state to other chains
	if err := s.SyncDepositorPauseStates(ctx); err != nil {
		logger.Error("Failed to fully sync pause state to all chains: %v", err)
		// Continue despite errors since Arbitrum was successfully paused
	}

	return nil
}

// ResumeDeposits resumes deposit functionality on Arbitrum (leader chain) and then syncs to other chains.
func (s *BridgeService) ResumeDeposits(ctx context.Context) error {
	// First resume Arbitrum (the leader/main chain)
	err := s.callDepositorMethod(ctx, "resumeDeposits")
	if err != nil {
		return fmt.Errorf("failed to resume Arbitrum deposits: %v", err)
	}

	logger.Info("Successfully resumed Arbitrum deposits")

	// Then sync the state to other chains
	if err := s.SyncDepositorPauseStates(ctx); err != nil {
		logger.Error("Failed to fully sync resume state to all chains: %v", err)
		// Continue despite errors since Arbitrum was successfully resumed
	}

	return nil
}

// UpdateWalletLimitPercentage updates the wallet limit percentage.
func (s *BridgeService) UpdateWalletLimitPercentage(newPercentage int64) error {
	if newPercentage < 0 {
		return fmt.Errorf("wallet limit percentage cannot be negative")
	}
	if newPercentage > 100 {
		return fmt.Errorf("wallet limit percentage cannot exceed 100%%")
	}

	// Update the database if available.
	if s.db != nil {
		if err := s.db.SetIntSetting("wallet_limit_percentage", int(newPercentage)); err != nil {
			logger.Error("Failed to update wallet limit percentage in database: %v", err)
			return err
		}
	}

	logger.Info("Wallet limit percentage updated to %d%%", newPercentage)

	return nil
}

// SyncDepositorPauseStates synchronizes the pause states of all chain depositors with Arbitrum.
// If Arbitrum is paused, all other chains will be paused. If Arbitrum is active, all other chains will be resumed.
func (s *BridgeService) SyncDepositorPauseStates(ctx context.Context) error {
	// Check Arbitrum's pause state
	var out []interface{}
	client := blockchain.NewRetryClient(s.arbDepositor.Client)
	err := client.CallWithRetry(ctx, s.arbDepositor.BoundContract, &out, "paused")
	if err != nil {
		return fmt.Errorf("failed to check Arbitrum pause status: %v", err)
	}

	arbIsPaused := out[0].(bool)
	logger.Info("Arbitrum depositor pause status: %v, synchronizing other chains", arbIsPaused)

	// Channels for collecting errors from goroutines
	errChan := make(chan error, 2)

	// Sync Base if available
	if s.baseDepositor != nil {
		go func() {
			// First check if Base is already in the desired state
			var baseOut []interface{}
			baseClient := blockchain.NewRetryClient(s.baseDepositor.Client)
			err := baseClient.CallWithRetry(ctx, s.baseDepositor.BoundContract, &baseOut, "paused")
			if err != nil {
				errChan <- fmt.Errorf("failed to check Base pause status: %v", err)
				return
			}

			baseIsPaused := baseOut[0].(bool)
			// Only take action if states don't match
			if baseIsPaused != arbIsPaused {
				if arbIsPaused {
					// Arbitrum is paused, so pause Base
					err = s.callBaseDepositorMethod(ctx, "pauseDeposits")
					if err != nil {
						errChan <- fmt.Errorf("failed to pause Base depositor: %v", err)
						return
					}
					logger.Info("Successfully paused Base depositor to match Arbitrum")
				} else {
					// Arbitrum is active, so resume Base
					err = s.callBaseDepositorMethod(ctx, "resumeDeposits")
					if err != nil {
						errChan <- fmt.Errorf("failed to resume Base depositor: %v", err)
						return
					}
					logger.Info("Successfully resumed Base depositor to match Arbitrum")
				}
			} else {
				logger.Info("Base depositor already in sync with Arbitrum (paused=%v)", baseIsPaused)
			}
			errChan <- nil
		}()
	} else {
		// Skip Base if not available
		errChan <- nil
	}

	// Sync Optimism if available
	if s.optimismDepositor != nil {
		go func() {
			// First check if Optimism is already in the desired state
			var optimismOut []interface{}
			optimismClient := blockchain.NewRetryClient(s.optimismDepositor.Client)
			err := optimismClient.CallWithRetry(ctx, s.optimismDepositor.BoundContract, &optimismOut, "paused")
			if err != nil {
				errChan <- fmt.Errorf("failed to check Optimism pause status: %v", err)
				return
			}

			optimismIsPaused := optimismOut[0].(bool)
			// Only take action if states don't match
			if optimismIsPaused != arbIsPaused {
				if arbIsPaused {
					// Arbitrum is paused, so pause Optimism
					err = s.callOptimismDepositorMethod(ctx, "pauseDeposits")
					if err != nil {
						errChan <- fmt.Errorf("failed to pause Optimism depositor: %v", err)
						return
					}
					logger.Info("Successfully paused Optimism depositor to match Arbitrum")
				} else {
					// Arbitrum is active, so resume Optimism
					err = s.callOptimismDepositorMethod(ctx, "resumeDeposits")
					if err != nil {
						errChan <- fmt.Errorf("failed to resume Optimism depositor: %v", err)
						return
					}
					logger.Info("Successfully resumed Optimism depositor to match Arbitrum")
				}
			} else {
				logger.Info("Optimism depositor already in sync with Arbitrum (paused=%v)", optimismIsPaused)
			}
			errChan <- nil
		}()
	} else {
		// Skip Optimism if not available
		errChan <- nil
	}

	// Collect errors
	var errs []error
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to sync some depositor states: %v", errs)
	}

	logger.Info("Successfully synchronized all depositor chains with Arbitrum (paused=%v)", arbIsPaused)
	return nil
}
