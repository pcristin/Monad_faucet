package blockchain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// BridgeService handles the business logic for the bridge operations
type BridgeService struct {
	arbDepositor     *ArbitrumDepositor
	monadDistributor *MonadDistributor
	depositChan      chan DepositEvent
	refundChan       chan *big.Int
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewBridgeService creates a new instance of BridgeService
func NewBridgeService(
	arbDepositor *ArbitrumDepositor,
	monadDistributor *MonadDistributor,
) *BridgeService {
	ctx, cancel := context.WithCancel(context.Background())
	return &BridgeService{
		arbDepositor:     arbDepositor,
		monadDistributor: monadDistributor,
		depositChan:      make(chan DepositEvent, 100),
		refundChan:       make(chan *big.Int, 100),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Start initializes the service and starts processing deposits
func (s *BridgeService) Start() error {
	logger.Info("Starting bridge service...")
	s.wg.Add(1)
	go s.processDeposits()
	s.wg.Add(1)
	go s.processRefunds()
	return nil
}

// Stop gracefully shuts down the service
func (s *BridgeService) Stop() {
	logger.Info("Stopping bridge service...")
	s.cancel()
	s.wg.Wait()
	logger.Info("Bridge service stopped")
}

// HandleDeposit queues a deposit for processing
func (s *BridgeService) HandleDeposit(event DepositEvent) {
	select {
	case s.depositChan <- event:
		logger.Info("Queued deposit: %s", event.String())
	default:
		logger.Warn("Deposit channel full, dropping event: %s", event.String())
	}
}

// QueueRefund queues a deposit ID for refund
func (s *BridgeService) QueueRefund(depositId *big.Int) {
	select {
	case s.refundChan <- depositId:
		logger.Info("Queued refund for deposit ID: %s", depositId.String())
	default:
		logger.Warn("Refund channel full, dropping deposit ID: %s", depositId.String())
	}
}

// GetState returns the current state of both contracts
func (s *BridgeService) GetState(ctx context.Context) (*ContractState, error) {
	return GetContractState(ctx, s.arbDepositor, s.monadDistributor)
}

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
				logger.Error("Error processing deposit: %v", err)
				s.QueueRefund(event.DepositId)
			}
			logger.Info("Processing time: %v", time.Since(start))
		}
	}
}

func (s *BridgeService) processRefunds() {
	defer s.wg.Done()
	logger.Info("Starting refund processor...")

	for {
		select {
		case <-s.ctx.Done():
			return
		case depositId := <-s.refundChan:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
			if err := s.arbDepositor.RefundDeposit(ctx, depositId); err != nil {
				logger.Error("Error processing refund for deposit ID %s: %v", depositId.String(), err)
			} else {
				logger.Info("Successfully refunded deposit ID: %s", depositId.String())
			}
			cancel()
		}
	}
}

func (s *BridgeService) processDeposit(event DepositEvent) error {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
	defer cancel()

	state, err := s.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %v", err)
	}

	if err := s.validateDeposit(state, event); err != nil {
		return fmt.Errorf("deposit validation failed: %v", err)
	}

	monAmount := calculateMonAmount(event.Amount, state.SwapRatios[event.Currency])

	if err := s.waitForConfirmations(ctx, event.BlockNumber, 10); err != nil {
		return fmt.Errorf("failed to wait for confirmations: %v", err)
	}

	if err := s.mintTokens(ctx, event.Depositor, monAmount, event.DepositId); err != nil {
		return fmt.Errorf("failed to mint tokens: %v", err)
	}

	logger.Info("Successfully processed deposit %s", event.String())
	return nil
}

func (s *BridgeService) validateDeposit(state *ContractState, event DepositEvent) error {
	if state.IsPaused {
		return fmt.Errorf("bridge is paused")
	}

	monAmount := calculateMonAmount(event.Amount, state.SwapRatios[event.Currency])
	if state.MonBalance.Cmp(monAmount) < 0 {
		return fmt.Errorf("insufficient MON balance in distributor (required: %s MON, available: %s MON)",
			new(big.Float).Quo(
				new(big.Float).SetInt(monAmount),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			).Text('f', 6),
			new(big.Float).Quo(
				new(big.Float).SetInt(state.MonBalance),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			).Text('f', 6),
		)
	}

	var out []interface{}
	var method string
	switch event.Currency {
	case CurrencyETH:
		method = "minEthDeposit"
	case CurrencyUSDC:
		method = "minUsdcDeposit"
	case CurrencyUSDT:
		method = "minUsdtDeposit"
	default:
		return fmt.Errorf("unsupported currency type")
	}

	err := s.arbDepositor.BoundContract.Call(&bind.CallOpts{Context: s.ctx}, &out, method)
	if err != nil {
		return fmt.Errorf("failed to get min amount for %s: %v", CurrencyTypeToString(event.Currency), err)
	}

	minAmount := out[0].(*big.Int)
	if event.Amount.Cmp(minAmount) < 0 {
		return fmt.Errorf("deposit amount below minimum for %s", CurrencyTypeToString(event.Currency))
	}

	return nil
}

func (s *BridgeService) waitForConfirmations(ctx context.Context, blockNumber uint64, confirmations uint64) error {
	targetBlock := blockNumber + confirmations
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentBlock, err := s.arbDepositor.client.BlockNumber(ctx)
			if err != nil {
				log.Printf("Error getting current block number: %v", err)
				continue
			}

			if currentBlock >= targetBlock {
				return nil
			}
		}
	}
}

func (s *BridgeService) mintTokens(ctx context.Context, recipient common.Address, amount *big.Int, depositId *big.Int) error {
	transfer := []struct {
		Recipient common.Address `abi:"recipient"`
		Amount    *big.Int       `abi:"amount"`
		ID        *big.Int       `abi:"id"`
	}{
		{
			Recipient: recipient,
			Amount:    amount,
			ID:        depositId,
		},
	}

	opts, err := s.monadDistributor.GetTransactOpts(ctx)
	if err != nil {
		return fmt.Errorf("failed to get transaction options: %v", err)
	}

	tx, err := s.monadDistributor.BoundContract.Transact(opts, "distributeFunds", transfer)
	if err != nil {
		return fmt.Errorf("failed to distribute funds: %v", err)
	}

	receipt, err := bind.WaitMined(ctx, s.monadDistributor.client, tx)
	if err != nil {
		return fmt.Errorf("failed to wait for distribution transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("distribution transaction failed")
	}

	logger.Info("✅ Distributed %s MON to %s (tx: %s)",
		new(big.Float).Quo(
			new(big.Float).SetInt(amount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6),
		recipient.Hex(),
		tx.Hash().Hex(),
	)
	return nil
}

func calculateMonAmount(depositAmount *big.Int, swapRatio *big.Int) *big.Int {
	result := new(big.Int).Mul(depositAmount, swapRatio)
	result = new(big.Int).Div(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	return result
}

// PauseDeposits pauses deposit functionality
func (s *BridgeService) PauseDeposits(ctx context.Context) error {
	// Get transaction options
	publicKey := s.arbDepositor.privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Pack the method call
	input, err := DepositorABI.Pack("pauseDeposits")
	if err != nil {
		return fmt.Errorf("failed to pack pauseDeposits data: %v", err)
	}

	// Get current gas price with a small buffer (20% increase)
	gasPrice, err := s.arbDepositor.client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))

	// Estimate gas
	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &s.arbDepositor.address,
		Data: input,
	}
	gasLimit, err := s.arbDepositor.client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add 20% buffer to gas limit
	gasLimit = gasLimit * 12 / 10

	// Create transaction
	nonce, err := s.arbDepositor.client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.arbDepositor.address,
		Value:    big.NewInt(0),
		Data:     input,
	})

	// Sign and send transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.arbDepositor.chainID), s.arbDepositor.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	err = s.arbDepositor.client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send pause transaction: %v", err)
	}

	// Wait for transaction receipt
	receipt, err := bind.WaitMined(ctx, s.arbDepositor.client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for pause transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("pause transaction failed")
	}

	logger.Info("Successfully paused deposits (tx: %s)", signedTx.Hash().Hex())
	return nil
}

// ResumeDeposits resumes deposit functionality
func (s *BridgeService) ResumeDeposits(ctx context.Context) error {
	// Get transaction options
	publicKey := s.arbDepositor.privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Pack the method call
	input, err := DepositorABI.Pack("resumeDeposits")
	if err != nil {
		return fmt.Errorf("failed to pack resumeDeposits data: %v", err)
	}

	// Get current gas price with a small buffer (20% increase)
	gasPrice, err := s.arbDepositor.client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))

	// Estimate gas
	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &s.arbDepositor.address,
		Data: input,
	}
	gasLimit, err := s.arbDepositor.client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add 20% buffer to gas limit
	gasLimit = gasLimit * 12 / 10

	// Create transaction
	nonce, err := s.arbDepositor.client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.arbDepositor.address,
		Value:    big.NewInt(0),
		Data:     input,
	})

	// Sign and send transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.arbDepositor.chainID), s.arbDepositor.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	err = s.arbDepositor.client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send resume transaction: %v", err)
	}

	// Wait for transaction receipt
	receipt, err := bind.WaitMined(ctx, s.arbDepositor.client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for resume transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("resume transaction failed")
	}

	logger.Info("Successfully resumed deposits (tx: %s)", signedTx.Hash().Hex())
	return nil
}
