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
	logger.Info("Successfully executed %s (tx: %s)", method, signedTx.Hash().Hex())
	return nil
}

// PauseDeposits pauses deposit functionality.
func (s *BridgeService) PauseDeposits(ctx context.Context) error {
	return s.callDepositorMethod(ctx, "pauseDeposits")
}

// ResumeDeposits resumes deposit functionality.
func (s *BridgeService) ResumeDeposits(ctx context.Context) error {
	return s.callDepositorMethod(ctx, "resumeDeposits")
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
