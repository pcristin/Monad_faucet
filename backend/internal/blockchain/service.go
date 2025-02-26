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
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// WalletLimitPercentage is the percentage of total MON balance that can be distributed to a single wallet per transaction
var WalletLimitPercentage int64 = 30 // 30% of total distributor MON balance

// UpdateWalletLimitPercentage updates the wallet limit percentage
func UpdateWalletLimitPercentage(newPercentage int64) error {
	if newPercentage < 0 {
		return fmt.Errorf("wallet limit percentage cannot be negative")
	}
	if newPercentage > 100 {
		return fmt.Errorf("wallet limit percentage cannot exceed 100%%")
	}

	WalletLimitPercentage = newPercentage
	logger.Info("Wallet limit percentage updated to %d%%", newPercentage)

	// Update the database if available
	if dbInstance != nil {
		if err := dbInstance.SetIntSetting("wallet_limit_percentage", int(newPercentage)); err != nil {
			logger.Error("Failed to update wallet limit percentage in database: %v", err)
			return err
		}
	}

	return nil
}

// BridgeService handles the business logic for the bridge operations
type BridgeService struct {
	arbDepositor      *ArbitrumDepositor
	monadDistributor  *MonadDistributor
	depositChan       chan DepositEvent
	refundChan        chan *big.Int
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	walletUsage       map[common.Address]*WalletUsage  // Track wallet usage
	walletMutex       sync.RWMutex                     // Mutex for thread-safe access to walletUsage
	db                *database.DB                     // Database connection
	txCache           map[string]*database.Transaction // Cache for transaction status
	txCacheMutex      sync.RWMutex
	txCacheExpiration time.Duration
}

// NewBridgeService creates a new instance of BridgeService
func NewBridgeService(
	arbDepositor *ArbitrumDepositor,
	monadDistributor *MonadDistributor,
	db *database.DB,
) *BridgeService {
	ctx, cancel := context.WithCancel(context.Background())

	// Set the database instance for the contracts package
	SetDatabase(db)

	return &BridgeService{
		arbDepositor:      arbDepositor,
		monadDistributor:  monadDistributor,
		depositChan:       make(chan DepositEvent),
		refundChan:        make(chan *big.Int),
		wg:                sync.WaitGroup{},
		ctx:               ctx,
		cancel:            cancel,
		walletUsage:       make(map[common.Address]*WalletUsage),
		walletMutex:       sync.RWMutex{},
		db:                db,
		txCache:           make(map[string]*database.Transaction),
		txCacheExpiration: 5 * time.Minute, // Cache transactions for 5 minutes
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
func (s *BridgeService) Stop() error {
	logger.Info("Stopping bridge service...")
	s.cancel()
	s.wg.Wait()
	logger.Info("Bridge service stopped")
	return nil
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
			if err := s.refundDeposit(ctx, depositId); err != nil {
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

	// Create a pending transaction record in the database
	monAmount := calculateMonAmount(event.Amount, state.SwapRatios[event.Currency])
	txRecord := &database.Transaction{
		DepositID:     event.DepositId,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		MonAmount:     monAmount,
		Status:        database.StatusPending,
		TxHash:        event.TxHash, // Arbitrum transaction hash
	}

	if err := s.db.CreateTransaction(txRecord); err != nil {
		logger.Error("Failed to create transaction record: %v", err)
		// Continue processing even if DB record creation fails
	}

	if err := s.validateDeposit(state, event); err != nil {
		// Update transaction status to failed
		if err2 := s.db.UpdateTransactionStatus(event.DepositId, database.StatusFailed, event.TxHash); err2 != nil {
			logger.Error("Failed to update transaction status to failed: %v", err2)
		}
		return fmt.Errorf("deposit validation failed: %v", err)
	}

	if err := s.waitForConfirmations(ctx, event.BlockNumber, 10); err != nil {
		// Update transaction status to failed
		if err2 := s.db.UpdateTransactionStatus(event.DepositId, database.StatusFailed, event.TxHash); err2 != nil {
			logger.Error("Failed to update transaction status to failed: %v", err2)
		}
		return fmt.Errorf("failed to wait for confirmations: %v", err)
	}

	// Attempt to mint tokens
	monadTxHash, err := s.mintTokens(ctx, event.Depositor, monAmount, event.DepositId)
	if err != nil {
		// Update transaction status to failed
		if err2 := s.db.UpdateTransactionStatus(event.DepositId, database.StatusFailed, event.TxHash); err2 != nil {
			logger.Error("Failed to update transaction status to failed: %v", err2)
		}
		return fmt.Errorf("failed to mint tokens: %v", err)
	}

	// Update transaction status to completed with the Monad transaction hash
	if err := s.db.UpdateTransactionStatus(event.DepositId, database.StatusCompleted, monadTxHash); err != nil {
		logger.Error("Failed to update transaction status to completed: %v", err)
	}

	// Update wallet usage after successful mint
	s.updateWalletUsage(event.Depositor, monAmount)

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

	// Check wallet limit
	if err := s.checkWalletLimit(monAmount, state.MonBalance); err != nil {
		return err
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

func (s *BridgeService) mintTokens(ctx context.Context, recipient common.Address, amount *big.Int, depositId *big.Int) (string, error) {
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
		return "", fmt.Errorf("failed to get transaction options: %v", err)
	}

	tx, err := s.monadDistributor.BoundContract.Transact(opts, "distributeFunds", transfer)
	if err != nil {
		return "", fmt.Errorf("failed to distribute funds: %v", err)
	}

	receipt, err := bind.WaitMined(ctx, s.monadDistributor.client, tx)
	if err != nil {
		return "", fmt.Errorf("failed to wait for distribution transaction: %v", err)
	}

	if receipt.Status == 0 {
		return "", fmt.Errorf("distribution transaction failed")
	}

	logger.Info("✅ Distributed %s MON to %s (tx: %s)",
		new(big.Float).Quo(
			new(big.Float).SetInt(amount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6),
		recipient.Hex(),
		tx.Hash().Hex(),
	)
	return tx.Hash().Hex(), nil
}

func calculateMonAmount(depositAmount *big.Int, swapRatio *big.Int) *big.Int {
	result := new(big.Int).Mul(depositAmount, swapRatio)
	result = new(big.Int).Div(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	// Ensure minimum amount (1 wei)
	// If the calculation results in zero due to tiny input or rounding,
	// return at least 1 wei to avoid the "Transfer amount must be greater than zero" error
	if result.Sign() <= 0 {
		logger.Warn("Calculated MON amount is zero, setting to minimum (1 wei)")
		return big.NewInt(1) // Minimum of 1 wei
	}

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

// checkWalletLimit checks if a transaction exceeds the per-transaction wallet limit
// Returns nil if the wallet is within limits, otherwise returns an error
func (s *BridgeService) checkWalletLimit(requestedAmount *big.Int, totalMonBalance *big.Int) error {
	// If limit is set to 0, there are no limits
	if WalletLimitPercentage == 0 {
		return nil
	}

	// Calculate the maximum allowed amount (percentage of total MON balance)
	maxAllowedAmount := new(big.Int).Mul(totalMonBalance, big.NewInt(WalletLimitPercentage))
	maxAllowedAmount = new(big.Int).Div(maxAllowedAmount, big.NewInt(100))

	// Check if the current request exceeds the limit
	if requestedAmount.Cmp(maxAllowedAmount) > 0 {
		// Format amounts for logging
		maxAllowedFormatted := new(big.Float).Quo(
			new(big.Float).SetInt(maxAllowedAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6)
		requestedFormatted := new(big.Float).Quo(
			new(big.Float).SetInt(requestedAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6)

		return fmt.Errorf("requested amount (%s MON) exceeds wallet limit (%s MON per transaction)",
			requestedFormatted, maxAllowedFormatted)
	}

	return nil
}

// updateWalletUsage updates the usage record for a wallet
// This is kept for compatibility but no longer tracks usage over time
func (s *BridgeService) updateWalletUsage(wallet common.Address, amount *big.Int) {
	// No longer needed for per-transaction limits, but kept for compatibility
	// with existing code structure
}

// GetDepositIDFromTxHash retrieves the deposit ID from a transaction hash
func (s *BridgeService) GetDepositIDFromTxHash(ctx context.Context, txHash string) (*big.Int, error) {
	// Parse the transaction hash
	hash := common.HexToHash(txHash)
	if hash == (common.Hash{}) {
		return nil, fmt.Errorf("invalid transaction hash format")
	}

	// Try to find the transaction in the database
	tx, err := s.db.GetTransactionByArbitrumTxHash(hash.Hex())
	if err == nil && tx != nil {
		logger.Info("Found transaction with deposit ID %s for tx %s in database", tx.DepositID.String(), txHash)
		return tx.DepositID, nil
	}

	// If not in database, return an error
	return nil, fmt.Errorf("transaction not found in database")
}

// GetTransactionByDepositID retrieves transaction details by deposit ID
func (s *BridgeService) GetTransactionByDepositID(ctx context.Context, depositID *big.Int) (*database.Transaction, error) {
	// Check cache first
	cacheKey := depositID.String()
	s.txCacheMutex.RLock()
	cachedTx, exists := s.txCache[cacheKey]
	s.txCacheMutex.RUnlock()

	// If transaction is in cache and not pending, return it
	if exists && cachedTx.Status != database.StatusPending {
		return cachedTx, nil
	}

	// Query the database for the transaction
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction from database: %w", err)
	}

	// Cache the transaction if it's not pending
	if tx.Status != database.StatusPending {
		s.txCacheMutex.Lock()
		s.txCache[cacheKey] = tx
		s.txCacheMutex.Unlock()

		// Start a goroutine to remove the transaction from cache after expiration
		go func(key string, expiration time.Duration) {
			time.Sleep(expiration)
			s.txCacheMutex.Lock()
			delete(s.txCache, key)
			s.txCacheMutex.Unlock()
		}(cacheKey, s.txCacheExpiration)
	}

	return tx, nil
}

// UpdateTransactionStatus updates the status of a transaction
func (s *BridgeService) UpdateTransactionStatus(ctx context.Context, depositID *big.Int, status, txHash string) error {
	// Update the transaction in the database
	err := s.db.UpdateTransactionStatus(depositID, status, txHash)
	if err != nil {
		return fmt.Errorf("failed to update transaction status in database: %w", err)
	}

	// Clear the cache for this transaction
	s.clearTransactionCache(depositID.String())

	return nil
}

// clearTransactionCache removes a transaction from the cache
func (s *BridgeService) clearTransactionCache(depositID string) {
	s.txCacheMutex.Lock()
	delete(s.txCache, depositID)
	s.txCacheMutex.Unlock()
}

// CheckDatabaseConnection verifies the database connection is working
func (s *BridgeService) CheckDatabaseConnection() error {
	// Simple ping to verify database connection
	return s.db.Ping()
}

// CheckBlockchainConnections verifies connections to blockchain nodes
func (s *BridgeService) CheckBlockchainConnections() error {
	// Check Arbitrum connection by attempting to get the latest block number
	arbClient := s.arbDepositor.client
	if _, err := arbClient.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("arbitrum connection failed: %w", err)
	}

	// Check Monad connection by attempting to get the latest block number
	monadClient := s.monadDistributor.client
	if _, err := monadClient.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("monad connection failed: %w", err)
	}

	return nil
}

// GracefulShutdown waits for in-progress operations to complete
func (s *BridgeService) GracefulShutdown(ctx context.Context) {
	// Signal that we're shutting down
	s.cancel()

	// Create a channel to signal completion
	done := make(chan struct{})

	// Wait for processing to complete in a goroutine
	go func() {
		s.wg.Wait()
		close(done)
	}()

	// Wait for processing to complete or context to timeout
	select {
	case <-done:
		// Processing completed normally
		logger.Info("All in-progress transactions completed")
	case <-ctx.Done():
		// Timeout occurred
		logger.Warn("Shutdown timed out, some transactions may not have completed")
	}
}

// GetTransactionCounts returns counts of transactions by status
func (s *BridgeService) GetTransactionCounts() (pending, completed, failed, refunded int, err error) {
	// Query the database for transaction counts by status
	rows, err := s.db.Query(`
		SELECT status, COUNT(*) 
		FROM transaction_history 
		GROUP BY status
	`)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to query transaction counts: %w", err)
	}
	defer rows.Close()

	// Process the results
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("failed to scan transaction count row: %w", err)
		}

		// Increment the appropriate counter
		switch status {
		case database.StatusPending:
			pending = count
		case database.StatusCompleted:
			completed = count
		case database.StatusFailed:
			failed = count
		case database.StatusRefunded:
			refunded = count
		}
	}

	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("error iterating transaction count rows: %w", err)
	}

	return pending, completed, failed, refunded, nil
}

// GetCacheSize returns the current size of the transaction cache
func (s *BridgeService) GetCacheSize() int {
	s.txCacheMutex.RLock()
	defer s.txCacheMutex.RUnlock()
	return len(s.txCache)
}

// GetDB returns the database connection
func (s *BridgeService) GetDB() *database.DB {
	return s.db
}

// GetArbitrumClient returns the Arbitrum client
func (s *BridgeService) GetArbitrumClient() *ethclient.Client {
	return s.arbDepositor.client
}

// GetArbitrumContractAddress returns the Arbitrum contract address
func (s *BridgeService) GetArbitrumContractAddress() common.Address {
	return s.arbDepositor.address
}

// FindMonadTransactionByDepositID looks for a transaction on the Monad blockchain by deposit ID
// Returns the transaction hash, status, and any error that occurred
func (s *BridgeService) FindMonadTransactionByDepositID(ctx context.Context, depositID *big.Int) (string, string, error) {
	logger.Info("Looking for Monad transaction with deposit ID: %s", depositID.String())

	// Try looking in the database as a first option
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err == nil && tx != nil {
		logger.Info("Found transaction in database: deposit_id=%s, status=%s, tx_hash=%s",
			depositID.String(), tx.Status, tx.TxHash)

		// If there's a transaction hash in the database, use it
		if tx.TxHash != "" {
			return tx.TxHash, string(tx.Status), nil
		}

		// If status is completed but no hash, it might be special handling
		if tx.Status == database.StatusCompleted {
			logger.Info("Transaction marked as completed in database: %s", depositID.String())
			return "", "success", nil
		}
	}

	// Not found in database
	logger.Info("No Monad transaction found for deposit ID: %s", depositID.String())
	return "", "", nil
}

func (s *BridgeService) refundDeposit(ctx context.Context, depositId *big.Int) error {
	logger.Info("Delegating refund of deposit ID %s to ArbitrumDepositor", depositId.String())
	return s.arbDepositor.RefundDeposit(ctx, depositId)
}
