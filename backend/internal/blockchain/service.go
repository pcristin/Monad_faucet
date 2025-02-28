package blockchain

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
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
	arbDepositor        *ArbitrumDepositor
	monadDistributor    *MonadDistributor
	depositChan         chan DepositEvent
	refundChan          chan *big.Int
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
	walletUsage         map[common.Address]*WalletUsage  // Track wallet usage
	walletMutex         sync.RWMutex                     // Mutex for thread-safe access to walletUsage
	db                  *database.DB                     // Database connection
	txCache             map[string]*database.Transaction // Cache for transaction status
	txCacheMutex        sync.RWMutex
	txCacheExpiration   time.Duration
	processingMutex     sync.Mutex                    // Mutex for the processing map
	processingDeposits  map[string]bool               // Track deposit IDs currently being processed
	instanceID          string                        // Unique ID for this service instance
	lockDuration        time.Duration                 // How long to hold distributed locks
	lockRefreshInterval time.Duration                 // How often to refresh distributed locks
	lockRefreshers      map[string]context.CancelFunc // Map of running lock refreshers
	lockRefreshersMutex sync.Mutex                    // Mutex for the lock refreshers map
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

	// Generate a unique instance ID
	instanceID := fmt.Sprintf("instance-%d", time.Now().UnixNano())

	return &BridgeService{
		arbDepositor:        arbDepositor,
		monadDistributor:    monadDistributor,
		depositChan:         make(chan DepositEvent, 100),
		refundChan:          make(chan *big.Int, 100),
		wg:                  sync.WaitGroup{},
		ctx:                 ctx,
		cancel:              cancel,
		walletUsage:         make(map[common.Address]*WalletUsage),
		walletMutex:         sync.RWMutex{},
		db:                  db,
		txCache:             make(map[string]*database.Transaction),
		txCacheExpiration:   time.Hour * 24,
		processingDeposits:  make(map[string]bool),
		instanceID:          instanceID,
		lockDuration:        time.Minute * 5, // 5 minute lock by default
		lockRefreshInterval: time.Minute * 1, // Refresh lock every minute
		lockRefreshers:      make(map[string]context.CancelFunc),
	}
}

// Start initializes the service and starts processing deposits
func (s *BridgeService) Start() error {
	logger.Info("Starting bridge service...")
	s.wg.Add(1)
	go s.processDeposits()
	s.wg.Add(1)
	go s.processRefunds()

	// Add recovery process
	s.wg.Add(1)
	go s.recoverStuckTransactionsPeriodically()

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
				// Check if this is a duplicate mint error, which shouldn't trigger a refund
				if strings.Contains(err.Error(), "duplicate mint attempt") {
					logger.Warn("Skipping refund for duplicate mint attempt: %v", err)
				} else {
					// Only queue a refund for non-duplicate errors
					logger.Error("Error processing deposit: %v", err)
					s.QueueRefund(event.DepositId)
				}
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

// isProcessingDeposit checks if a deposit is already being processed and marks it as processing if not
// Now with distributed locking support
func (s *BridgeService) isProcessingDeposit(depositID *big.Int) bool {
	s.processingMutex.Lock()
	defer s.processingMutex.Unlock()

	// Check if this deposit is already being processed locally
	depositIDStr := depositID.String()
	if s.processingDeposits[depositIDStr] {
		logger.Warn("⚠️ Deposit ID %s is already being processed locally, skipping duplicate attempt", depositIDStr)
		return true
	}

	// Try to acquire a distributed lock
	lockAcquired, err := s.db.AcquireProcessingLock(depositID, s.instanceID, s.lockDuration)
	if err != nil {
		logger.Error("Failed to check distributed lock: %v", err)
		// Fall back to local locking in case of database errors
	} else if !lockAcquired {
		logger.Warn("⚠️ Deposit ID %s is locked by another instance, skipping duplicate attempt", depositIDStr)
		return true
	} else {
		// Start a goroutine to refresh the lock periodically
		s.startLockRefresher(depositID)
	}

	// Mark this deposit as processing locally
	s.processingDeposits[depositIDStr] = true
	logger.Info("🔒 Acquired processing lock for deposit ID %s", depositIDStr)
	return false
}

// startLockRefresher starts a goroutine that periodically refreshes the lock
func (s *BridgeService) startLockRefresher(depositID *big.Int) {
	depositIDStr := depositID.String()

	// Create a new context for this refresher
	refreshCtx, cancel := context.WithCancel(s.ctx)

	s.lockRefreshersMutex.Lock()
	// Cancel any existing refresher for this deposit ID
	if existingCancel, exists := s.lockRefreshers[depositIDStr]; exists {
		existingCancel()
	}
	// Store the new cancel function
	s.lockRefreshers[depositIDStr] = cancel
	s.lockRefreshersMutex.Unlock()

	// Start the refresher goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.lockRefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-ticker.C:
				refreshed, err := s.db.RefreshProcessingLock(depositID, s.instanceID, s.lockDuration)
				if err != nil {
					logger.Error("Failed to refresh lock for deposit ID %s: %v", depositIDStr, err)
				} else if !refreshed {
					logger.Warn("Lock for deposit ID %s could not be refreshed, may have been lost", depositIDStr)
					// The lock was lost, we should stop refreshing
					return
				} else {
					logger.Debug("Refreshed lock for deposit ID %s", depositIDStr)
				}
			}
		}
	}()
}

// finishProcessingDeposit marks a deposit as no longer being processed
func (s *BridgeService) finishProcessingDeposit(depositID *big.Int) {
	depositIDStr := depositID.String()

	// Stop the lock refresher
	s.lockRefreshersMutex.Lock()
	if cancel, exists := s.lockRefreshers[depositIDStr]; exists {
		cancel()
		delete(s.lockRefreshers, depositIDStr)
	}
	s.lockRefreshersMutex.Unlock()

	// Release the distributed lock
	if err := s.db.ReleaseProcessingLock(depositID, s.instanceID); err != nil {
		logger.Error("Failed to release distributed lock for deposit ID %s: %v", depositIDStr, err)
	} else {
		logger.Info("🔓 Released processing lock for deposit ID %s", depositIDStr)
	}

	// Release the local lock
	s.processingMutex.Lock()
	delete(s.processingDeposits, depositIDStr)
	s.processingMutex.Unlock()
}

// processDeposit processes a deposit
func (s *BridgeService) processDeposit(event DepositEvent) error {
	startTime := time.Now()

	// First, check if this deposit is already being processed
	if s.isProcessingDeposit(event.DepositId) {
		// This is a duplicate attempt, just return without error
		logger.Warn("Skipping duplicate processing attempt for deposit ID %s", event.DepositId.String())
		return nil
	}

	// Always mark the deposit as finished processing when we're done
	defer s.finishProcessingDeposit(event.DepositId)

	// Double-check if this transaction was already completed
	existingTx, err := s.GetTransactionByDepositID(context.Background(), event.DepositId)
	if err == nil && existingTx != nil && existingTx.Status == database.StatusCompleted {
		logger.Info("✅ Transaction for deposit ID %s was already completed with Monad tx hash %s",
			event.DepositId.String(), existingTx.MonadTxHash)
		return nil
	}

	logger.Info("Processing deposit %s", event)

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
	defer cancel()

	// Get current state of the bridge
	state, err := s.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bridge state: %w", err)
	}

	// Check if bridge is paused
	if state.IsPaused {
		return fmt.Errorf("bridge is currently paused")
	}

	// Process the deposit
	monAmount := calculateMonAmount(event.Amount, state.SwapRatios[event.Currency], event.Currency)

	// Early log of MON amount calculation for debugging purposes
	logMonCalculation(event, monAmount)

	// Check if a transaction record already exists
	existingTx, err = s.GetTransactionByDepositID(ctx, event.DepositId)
	if err != nil || existingTx == nil {
		// Create a pending transaction record in the database
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
	} else if existingTx.Status == database.StatusCompleted {
		// Double check again to avoid race conditions
		logger.Info("✅ Transaction for deposit ID %s was already completed with Monad tx hash %s",
			event.DepositId.String(), existingTx.MonadTxHash)
		return nil
	}

	// Validate the deposit with the pre-calculated MON amount
	if err := s.validateDepositWithAmount(state, event, monAmount); err != nil {
		// Update status to failed
		updateErr := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusFailed, "")
		if updateErr != nil {
			logger.Error("Failed to update transaction status: %v", updateErr)
		}

		// Queue a refund
		s.QueueRefund(event.DepositId)

		return fmt.Errorf("invalid deposit: %w", err)
	}

	// Wait for confirmations
	if err := s.waitForConfirmations(ctx, event.BlockNumber, 10); err != nil {
		// Update transaction status to failed
		updateErr := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusFailed, "")
		if updateErr != nil {
			logger.Error("Failed to update transaction status: %v", updateErr)
		}

		// Queue a refund
		s.QueueRefund(event.DepositId)

		return fmt.Errorf("failed to wait for confirmations: %w", err)
	}

	// Mint tokens on Monad
	logger.Info("Minting %s MON tokens for wallet %s", formatMonAmount(monAmount), event.Depositor.Hex())
	txHash, err := s.mintTokens(ctx, event.Depositor, monAmount, event.DepositId)
	if err != nil {
		// Check if this is a duplicate transaction error
		if strings.Contains(err.Error(), "already completed") ||
			strings.Contains(err.Error(), "already in progress") ||
			strings.Contains(err.Error(), "duplicate mint attempt") {
			logger.Warn("Skipping refund for duplicate transaction attempt: %v", err)

			// Check if we got a txHash back despite the error - this means we found a completed transaction
			if txHash != "" {
				logger.Info("Found completed transaction with hash %s, updating status", txHash)
				// Update status to completed if we have a valid transaction hash
				updateErr := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusCompleted, txHash)
				if updateErr != nil {
					logger.Error("Failed to update transaction status: %v", updateErr)
				}
			}

			return fmt.Errorf("duplicate mint attempt: %w", err)
		}

		logger.Error("Mint tokens failed: %v. Deposit: %v", err, event)

		// For other errors, update status and queue a refund
		updateErr := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusFailed, "")
		if updateErr != nil {
			logger.Error("Failed to update transaction status: %v", updateErr)
		}

		// Queue a refund
		s.QueueRefund(event.DepositId)

		return fmt.Errorf("failed to mint tokens: %w", err)
	}

	// Update transaction status to completed with the Monad transaction hash
	logger.Info("🔄 Updating transaction status to completed for deposit ID %s with Monad tx hash %s",
		event.DepositId.String(), txHash)
	if err := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusCompleted, txHash); err != nil {
		logger.Error("Failed to update transaction status to completed: %v", err)
		// Even though the update failed, the tokens were minted successfully
		// Let's log the Monad tx hash to make it easier to find in case of issues
		logger.Error("IMPORTANT: Transaction status update failed, but tokens were minted! Deposit ID: %s, Monad tx hash: %s",
			event.DepositId.String(), txHash)
	} else {
		// Verify the update was successful by querying the database
		tx, err := s.db.GetTransactionByDepositID(event.DepositId)
		if err != nil {
			logger.Error("Failed to verify transaction update: %v", err)
		} else {
			if tx.Status == database.StatusCompleted && tx.MonadTxHash == txHash {
				logger.Info("✅ Transaction status updated correctly: deposit_id=%s, status=%s, monad_tx_hash=%s",
					event.DepositId.String(), tx.Status, tx.MonadTxHash)
			} else {
				logger.Warn("⚠️ Transaction status may not have updated correctly: deposit_id=%s, current_status=%s, monad_tx_hash=%s (expected: %s)",
					event.DepositId.String(), tx.Status, tx.MonadTxHash, txHash)
			}
		}
	}

	// Also update the cache to reflect the completed transaction
	s.txCacheMutex.Lock()
	s.txCache[event.DepositId.String()] = &database.Transaction{
		DepositID:   event.DepositId,
		Status:      database.StatusCompleted,
		MonadTxHash: txHash,
	}
	s.txCacheMutex.Unlock()

	logger.Info("✅ Processing completed for deposit ID %s in %v", event.DepositId.String(), time.Since(startTime))
	return nil
}

// validateDepositWithAmount validates a deposit with a precalculated MON amount
// to avoid redundant calculations
func (s *BridgeService) validateDepositWithAmount(state *ContractState, event DepositEvent, monAmount *big.Int) error {
	if state.IsPaused {
		return fmt.Errorf("bridge is paused")
	}

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

// mintTokens mints MON tokens on the Monad blockchain
func (s *BridgeService) mintTokens(ctx context.Context, recipient common.Address, amount *big.Int, depositId *big.Int) (string, error) {
	// First check if this transaction is already completed in database
	existingTx, err := s.GetTransactionByDepositID(ctx, depositId)
	if err == nil && existingTx != nil && existingTx.Status == database.StatusCompleted && existingTx.MonadTxHash != "" {
		logger.Info("⚠️ DUPLICATE PREVENTION: Deposit ID %s already has a completed transaction with Monad hash %s",
			depositId.String(), existingTx.MonadTxHash)
		return existingTx.MonadTxHash, nil
	}

	// For pending transactions, actively check on the Monad blockchain if it's already been processed
	if err == nil && existingTx != nil && existingTx.Status == database.StatusPending {
		// Check Monad blockchain directly
		monadTxHash, err := s.checkMonadBlockchainForTransaction(ctx, depositId)
		if err == nil && monadTxHash != "" {
			logger.Info("🔍 Found existing transaction on Monad blockchain: %s for deposit ID %s",
				monadTxHash, depositId.String())

			// Update database with Monad tx hash
			if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, monadTxHash); updateErr != nil {
				logger.Error("Failed to update transaction status: %v", updateErr)
			}

			return monadTxHash, nil
		}

		// If we get here, the transaction is actually pending and needs processing
		// DO NOT throw an error - we need to process this transaction
		logger.Info("Transaction for deposit ID %s is pending and needs processing", depositId.String())
	}

	// Check transaction cache as well
	txLookupKey := depositId.String()
	s.txCacheMutex.RLock()
	cachedTx, found := s.txCache[txLookupKey]
	s.txCacheMutex.RUnlock()

	if found && cachedTx.Status == database.StatusCompleted && cachedTx.MonadTxHash != "" {
		return cachedTx.MonadTxHash, nil
	}

	// Proceed with the regular minting process
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

	// Log that we're about to submit the transaction
	logger.Info("⏳ Submitting Monad transaction for deposit ID %s", depositId.String())

	tx, err := s.monadDistributor.BoundContract.Transact(opts, "distributeFunds", transfer)
	if err != nil {
		return "", fmt.Errorf("failed to distribute funds: %v", err)
	}

	logger.Info("⏳ Waiting for Monad transaction %s for deposit ID %s to be mined",
		tx.Hash().Hex(), depositId.String())

	receipt, err := bind.WaitMined(ctx, s.monadDistributor.client, tx)
	if err != nil {
		return "", fmt.Errorf("failed to wait for distribution transaction: %v", err)
	}

	if receipt.Status == 0 {
		return "", fmt.Errorf("distribution transaction failed")
	}

	// Format amount for logging
	amountStr := new(big.Float).Quo(
		new(big.Float).SetInt(amount),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
	).Text('f', 6)

	// Log the successful transaction
	logger.Info("✅ Successfully distributed %s MON to %s (tx: %s)",
		amountStr,
		recipient.Hex(),
		tx.Hash().Hex())

	// CRITICAL: Immediately update the transaction status in database
	if err := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, tx.Hash().Hex()); err != nil {
		// Log the error but don't fail - tokens were still distributed
		logger.Error("CRITICAL: Failed to update transaction status: %v", err)
		logger.Error("IMPORTANT: Transaction status update failed, but tokens were minted! Deposit ID: %s, Monad tx hash: %s",
			depositId.String(), tx.Hash().Hex())
	} else {
		// Verify the update
		verifyTx, verifyErr := s.db.GetTransactionByDepositID(depositId)
		if verifyErr != nil {
			logger.Error("Failed to verify transaction update: %v", verifyErr)
		} else if verifyTx.Status != database.StatusCompleted || verifyTx.MonadTxHash != tx.Hash().Hex() {
			logger.Error("CRITICAL: Transaction verify failed - current status=%s, monad_tx_hash=%s",
				verifyTx.Status, verifyTx.MonadTxHash)
		} else {
			logger.Info("✅ Database update confirmed for deposit ID %s with Monad tx hash %s",
				depositId.String(), tx.Hash().Hex())
		}
	}

	return tx.Hash().Hex(), nil
}

// checkMonadBlockchainForTransaction attempts to find a transaction on the Monad blockchain
// for a given deposit ID. This is used for recovery purposes.
func (s *BridgeService) checkMonadBlockchainForTransaction(ctx context.Context, depositId *big.Int) (string, error) {
	client := s.monadDistributor.client

	// First try to query recent events from the contract
	// We'd ideally look for TransferSingle events from the ERC1155 contract
	// or specific DistributeFunds events from our distributor contract

	// Get current block number
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get current block number: %w", err)
	}

	// Look back at most 500 blocks
	// Adjust as needed based on your expected transaction volume and confirmation time
	lookBackBlocks := uint64(500)
	if currentBlock < lookBackBlocks {
		lookBackBlocks = currentBlock
	}

	startBlock := currentBlock - lookBackBlocks

	// Create a filter query
	filterQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startBlock)),
		ToBlock:   big.NewInt(int64(currentBlock)),
		Addresses: []common.Address{s.monadDistributor.address},
	}

	logs, err := client.FilterLogs(ctx, filterQuery)
	if err != nil {
		logger.Error("Failed to filter logs: %v", err)
		return "", err
	}

	logger.Info("Searching %d logs for deposit ID %s", len(logs), depositId.String())

	// Search logs for events involving our deposit ID
	for _, log := range logs {
		// Check if there's a transaction receipt
		receipt, err := client.TransactionReceipt(ctx, log.TxHash)
		if err != nil {
			continue
		}

		// If the transaction was successful
		if receipt.Status == 1 {
			// Get the transaction
			tx, _, err := client.TransactionByHash(ctx, log.TxHash)
			if err != nil {
				continue
			}

			// Try to decode input data to see if it matches our deposit ID
			// This is a simple approach that just checks for the presence of the deposit ID
			// in the transaction data
			data := tx.Data()
			depositIdHex := fmt.Sprintf("%x", depositId)
			if len(depositIdHex)%2 != 0 {
				depositIdHex = "0" + depositIdHex
			}

			// Search for the deposit ID in the transaction data (with padding)
			if strings.Contains(hex.EncodeToString(data), depositIdHex) {
				logger.Info("Found matching transaction %s for deposit ID %s",
					log.TxHash.Hex(), depositId.String())
				return log.TxHash.Hex(), nil
			}
		}
	}

	return "", nil
}

func calculateMonAmount(depositAmount *big.Int, swapRatio *big.Int, currencyType CurrencyType) *big.Int {
	// For stablecoins (USDC/USDT), we need to adjust for decimal differences
	if currencyType == CurrencyUSDC || currencyType == CurrencyUSDT {
		// Stablecoins have 6 decimals, need to adjust by 12 (18-6)
		var decimalAdjustment int64 = 12

		// Adjust deposit amount for decimal difference
		adjustedDepositAmount := new(big.Int).Mul(
			depositAmount,
			new(big.Int).Exp(big.NewInt(10), big.NewInt(decimalAdjustment), nil),
		)

		// Calculate MON amount using the swap ratio
		result := new(big.Int).Mul(adjustedDepositAmount, swapRatio)
		result = new(big.Int).Div(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

		// Convert to human-readable values for logging and additional calculations
		depositValueFloat := new(big.Float).Quo(
			new(big.Float).SetInt(depositAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)), // 6 decimals for stablecoins
		)

		resultFloat := new(big.Float).Quo(
			new(big.Float).SetInt(result),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)), // 18 decimals for MON
		)

		depositValue, _ := depositValueFloat.Float64() // USD value
		resultValue, _ := resultFloat.Float64()        // MON value

		// Log the calculation
		logger.Info("Stablecoin to MON calculation: %s units ($%.6f), adjusted=%s, swapRatio=%s, result=%s MON wei (%.6f MON)",
			depositAmount.String(),
			depositValue,
			adjustedDepositAmount.String(),
			swapRatio.String(),
			result.String(),
			resultValue)

		// Handle small deposits (when calculated MON is very low or zero)
		if (result.Sign() <= 0 && depositAmount.Sign() > 0) || resultValue < 0.00001 {
			// For stablecoins, the USD value is directly the deposit amount
			// We need to use the current MON/USD ratio rather than a hardcoded multiplier

			// Get MON/USD ratio from the global setting
			monUsdRatio := GetMonUsdRatio()

			// Convert MON/USD ratio to float for easier math
			monUsdRatioFloat := new(big.Float).Quo(
				new(big.Float).SetInt(monUsdRatio),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			)
			monRatioValue, _ := monUsdRatioFloat.Float64() // e.g., 0.17

			// Calculate expected MON amount from USD value (depositValue / monRatioValue)
			// For example: $0.17 / 0.17 = 1 MON
			expectedMon := new(big.Float).Quo(
				depositValueFloat,
				monUsdRatioFloat,
			)

			// Convert to wei (multiply by 10^18)
			expectedMonWei := new(big.Float).Mul(
				expectedMon,
				new(big.Float).SetFloat64(1e18),
			)

			// Convert to integer with proper rounding
			expectedMonWeiInt, _ := expectedMonWei.Int(nil)

			// Base minimum MON amount (in wei) for any valid deposit
			minMonWei := big.NewInt(1000000000000000) // 0.001 MON (10^15 wei)

			// Ensure we provide at least the minimum MON amount for any valid deposit
			if expectedMonWeiInt.Cmp(minMonWei) < 0 {
				expectedMonWeiInt = new(big.Int).Set(minMonWei)
			}

			// For very small deposits (under $0.001), ensure they still get something if valid
			if depositValue < 0.001 && depositAmount.Sign() > 0 {
				// Ensure minimum amount of MON wei for extremely small deposits
				microMon := big.NewInt(10000000000000) // 0.00001 MON (10^13 wei)
				if expectedMonWeiInt.Cmp(microMon) < 0 {
					expectedMonWeiInt = microMon
				}
			}

			humanReadableMon := new(big.Float).Quo(
				new(big.Float).SetInt(expectedMonWeiInt),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			).Text('f', 6)

			logger.Info("Small stablecoin deposit ($%.6f with MON/USD ratio $%.6f), allocating %s MON (%s wei)",
				depositValue, monRatioValue, humanReadableMon, expectedMonWeiInt.String())

			return expectedMonWeiInt
		}

		return result
	}

	// For ETH deposits, we need to:
	// 1. Get ETH/USD price directly from Chainlink (8 decimal precision)
	// 2. Get MON/USD ratio (18 decimal precision)
	// 3. Calculate: depositAmount * ethUsdPrice / monUsdRatio (with proper decimal adjustments)

	// Get direct ETH/USD price from Chainlink (8 decimal precision)
	ethUsdPrice := GetEthUsdPrice() // This should return the Chainlink price feed value

	// Get MON/USD ratio with 18 decimal precision
	monUsdRatio := GetMonUsdRatio()

	// Convert to human-readable values for logging
	depositEthFloat := new(big.Float).Quo(
		new(big.Float).SetInt(depositAmount),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
	)

	ethUsdPriceFloat := new(big.Float).Quo(
		new(big.Float).SetInt(ethUsdPrice),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)),
	)

	monUsdRatioFloat := new(big.Float).Quo(
		new(big.Float).SetInt(monUsdRatio),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
	)

	// Calculate USD value: depositAmount * ethUsdPrice / 10^(18+8-18) = depositAmount * ethUsdPrice / 10^8
	// Using big.Rat for precise calculation
	depositRat := new(big.Rat).SetInt(depositAmount)
	ethUsdPriceRat := new(big.Rat).SetInt(ethUsdPrice)
	usdValueRat := new(big.Rat).Mul(depositRat, ethUsdPriceRat)
	// Adjust for Chainlink decimals (8)
	usdValueRat = new(big.Rat).Quo(usdValueRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)))
	// Further adjust for ETH decimals (18)
	usdValueRat = new(big.Rat).Quo(usdValueRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))

	// Calculate MON amount: usdValue * 10^18 / monUsdRatio
	monAmountRat := new(big.Rat).Mul(usdValueRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	monAmountRat = new(big.Rat).Quo(monAmountRat, new(big.Rat).SetInt(monUsdRatio))

	// Convert to floats for logging
	usdValueFloat, _ := usdValueRat.Float64()
	monAmountFloat, _ := new(big.Rat).Quo(monAmountRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))).Float64()

	// Calculate final MON wei amount (rounding properly)
	monWeiRat := new(big.Rat).Add(monAmountRat, new(big.Rat).SetFrac(big.NewInt(1), big.NewInt(2))) // Add 0.5 for rounding
	monWeiInt := new(big.Int)
	monWeiRat.Num().Div(monWeiRat.Num(), monWeiRat.Denom())
	monWeiInt.Set(monWeiRat.Num())

	// Log the calculation
	ethUsdPriceValue, _ := ethUsdPriceFloat.Float64()
	monUsdRatioValue, _ := monUsdRatioFloat.Float64()

	// Handle small deposits (when calculated MON is very low or zero)
	if (monWeiInt.Sign() <= 0 && depositAmount.Sign() > 0) || monAmountFloat < 0.00001 {
		// Base minimum MON amount (in wei) for any valid deposit
		minMonWei := big.NewInt(1000000000000000) // 0.001 MON (10^15 wei)

		// Get MON/USD ratio for direct calculation
		monUsdRatio := GetMonUsdRatio()
		monUsdRatioFloat := new(big.Float).Quo(
			new(big.Float).SetInt(monUsdRatio),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		)
		monRatioValue, _ := monUsdRatioFloat.Float64()

		// Calculate expected MON amount in proper proportion to USD value
		// For an ETH deposit worth $0.02 with MON/USD = $0.17, we should get 0.02/0.17 = 0.1176 MON
		expectedMon := new(big.Float).Quo(
			new(big.Float).SetFloat64(usdValueFloat),
			monUsdRatioFloat,
		)

		// Convert to wei (multiply by 10^18)
		expectedMonWei := new(big.Float).Mul(
			expectedMon,
			new(big.Float).SetFloat64(1e18),
		)

		// Convert to integer with proper rounding
		expectedMonWeiInt, _ := expectedMonWei.Int(nil)

		// Ensure we provide at least the minimum MON amount for any valid deposit
		if expectedMonWeiInt.Cmp(minMonWei) < 0 {
			expectedMonWeiInt = new(big.Int).Set(minMonWei)
		}

		// For very small deposits (under $0.001), ensure they still get something if valid
		if usdValueFloat < 0.001 && depositAmount.Sign() > 0 {
			// Ensure minimum amount of MON wei for extremely small deposits
			microMon := big.NewInt(10000000000000) // 0.00001 MON (10^13 wei)
			if expectedMonWeiInt.Cmp(microMon) < 0 {
				expectedMonWeiInt = microMon
			}
		}

		humanReadableMon := new(big.Float).Quo(
			new(big.Float).SetInt(expectedMonWeiInt),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6)

		logger.Info("ETH to MON calculation: %s ETH ≈ $%.6f USD (ETH price: $%.2f) / $%.6f per MON = %s MON (%s wei)",
			depositEthFloat.Text('f', 18),
			usdValueFloat,
			ethUsdPriceValue,
			monRatioValue,
			humanReadableMon,
			expectedMonWeiInt.String())

		return expectedMonWeiInt
	}

	logger.Info("ETH to MON calculation: %s ETH ≈ $%.6f USD (ETH price: $%.2f) / $%.6f per MON = %.6f MON (%s wei)",
		depositEthFloat.Text('f', 18),
		usdValueFloat,
		ethUsdPriceValue,
		monUsdRatioValue,
		monAmountFloat,
		monWeiInt.String())

	return monWeiInt
}

// GetEthUsdPrice returns the current ETH/USD price from Chainlink oracle
// This price is in 8 decimal precision as per Chainlink standard
func GetEthUsdPrice() *big.Int {
	// Use the Chainlink ETH/USD price feed
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Parse the Chainlink price feed ABI
	priceFeedAbi, err := abi.JSON(strings.NewReader(PriceFeedABI))
	if err != nil {
		logger.Error("Failed to parse price feed ABI: %v", err)
		// Return fallback value
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // ~$3000 with 8 decimals
	}

	// Get a client to use for the price feed
	// We'll use the ArbitrumDepositor's client if available, or create a temporary one if needed
	var client *ethclient.Client

	// Try to get a client from a globally available service
	clients := getAvailableClients()
	if len(clients) > 0 {
		client = clients[0]
	} else {
		// No clients available, log the error and return default
		logger.Error("No Ethereum clients available for Chainlink price feed")
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // ~$3000 with 8 decimals
	}

	// Create a price feed contract binding
	priceFeed := bind.NewBoundContract(common.HexToAddress(ChainlinkEthUsdFeed), priceFeedAbi, client, client, client)

	// Call the latestRoundData function
	var out []interface{}
	err = priceFeed.Call(&bind.CallOpts{Context: ctx}, &out, "latestRoundData")
	if err != nil {
		logger.Error("Failed to get ETH/USD price from Chainlink: %v", err)
		// Return fallback value
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // ~$3000 with 8 decimals
	}

	// Extract the price value (second return value, index 1)
	ethUsdPrice := out[1].(*big.Int)

	// Log the price for debugging
	ethUsdPriceFloat := new(big.Float).Quo(
		new(big.Float).SetInt(ethUsdPrice),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)),
	)
	ethUsdPriceValue, _ := ethUsdPriceFloat.Float64()
	logger.Info("Current ETH/USD price from Chainlink: $%.2f", ethUsdPriceValue)

	return ethUsdPrice
}

// getAvailableClients returns available ethclient.Client instances
// This is a helper function to get any available client for price feed queries
func getAvailableClients() []*ethclient.Client {
	var clients []*ethclient.Client

	// Get global variables or singletons that might have clients
	// This is a placeholder - in a real implementation, you would access
	// your application's client pool or global client instances

	// For now, let's create a temporary client
	rpcURL := "https://arb1.arbitrum.io/rpc" // Arbitrum One RPC URL
	client, err := ethclient.Dial(rpcURL)
	if err == nil && client != nil {
		clients = append(clients, client)
	}

	return clients
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
	// Begin a database transaction for atomic operations
	dbTx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use deferred function to ensure we either commit or rollback
	defer func() {
		if err != nil {
			// If we have an error, roll back the transaction
			rollbackErr := dbTx.Rollback()
			if rollbackErr != nil {
				logger.Error("Failed to rollback transaction: %v", rollbackErr)
			}
		}
	}()

	// Update the transaction status within the database transaction
	err = s.db.UpdateTransactionStatusWithTx(dbTx, depositID, status, txHash)
	if err != nil {
		return fmt.Errorf("failed to update transaction status in database: %w", err)
	}

	// Commit the database transaction
	if err = dbTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Successfully committed, now update the cache
	if status == database.StatusCompleted {
		s.txCacheMutex.Lock()
		s.txCache[depositID.String()] = &database.Transaction{
			DepositID:   depositID,
			Status:      status,
			MonadTxHash: txHash,
		}
		s.txCacheMutex.Unlock()
	} else {
		// For other statuses, just clear the cache
		s.clearTransactionCache(depositID.String())
	}

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

// FindMonadTransactionByDepositID attempts to find a Monad transaction hash for a given deposit ID
func (s *BridgeService) FindMonadTransactionByDepositID(ctx context.Context, depositID *big.Int) (string, string, error) {
	// First check the database for a transaction with this deposit ID
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil {
		logger.Error("Error finding transaction by deposit ID: %v", err)
		return "", "", err
	}

	// If the transaction exists and has a Monad tx hash, return it
	if tx.MonadTxHash != "" {
		// Return both the status and the Monad transaction hash
		logger.Info("Found existing Monad transaction hash %s for deposit ID %s with status %s",
			tx.MonadTxHash, depositID.String(), tx.Status)
		return tx.Status, tx.MonadTxHash, nil
	}

	// If there's no Monad tx hash but there is a status, at least return the status
	if tx.Status != "" {
		logger.Info("Found transaction for deposit ID %s with status %s but no Monad tx hash",
			depositID.String(), tx.Status)
		return tx.Status, "", nil
	}

	// If we get here, the transaction exists but doesn't have a Monad tx hash or status
	logger.Warn("Transaction for deposit ID %s exists but has no Monad tx hash or status", depositID.String())
	return "", "", fmt.Errorf("transaction found but has no Monad tx hash or status")
}

// GetDepositIDFromArbitrumTxHash attempts to find a deposit ID from an Arbitrum tx hash
func (s *BridgeService) GetDepositIDFromArbitrumTxHash(ctx context.Context, txHash string) (*big.Int, error) {
	// First try to get the transaction by Arbitrum tx hash
	tx, err := s.db.GetTransactionByArbitrumTxHash(txHash)
	if err == nil && tx != nil {
		logger.Info("Found deposit ID %s for Arbitrum tx hash %s", tx.DepositID.String(), txHash)
		return tx.DepositID, nil
	}

	// If we couldn't find it in the transaction table, try to get the deposit ID from the contract
	depositID, err := s.GetDepositIDFromTxHash(ctx, txHash)
	if err != nil {
		logger.Error("Error getting deposit ID from tx hash: %v", err)
		return nil, err
	}

	// If we found a deposit ID, save the mapping for future lookups
	if depositID != nil && depositID.Cmp(big.NewInt(0)) > 0 {
		logger.Info("Found deposit ID %s from contract for Arbitrum tx hash %s", depositID.String(), txHash)

		// Try to get the transaction first to see if it exists
		existingTx, _ := s.db.GetTransactionByDepositID(depositID)
		if existingTx != nil {
			// Update the tx hash if needed
			if existingTx.TxHash == "" || existingTx.TxHash != txHash {
				if err := s.db.UpdateTransactionHash(depositID, txHash); err != nil {
					logger.Error("Failed to update transaction hash: %v", err)
				} else {
					logger.Info("Updated Arbitrum tx hash for deposit ID %s", depositID.String())
				}
			}
		}

		return depositID, nil
	}

	return nil, fmt.Errorf("could not find deposit ID for tx hash")
}

// GetMonadTxHashFromArbitrumTxHash attempts to find a Monad tx hash from an Arbitrum tx hash
func (s *BridgeService) GetMonadTxHashFromArbitrumTxHash(ctx context.Context, txHash string) (string, string, error) {
	// First try to get the deposit ID from the Arbitrum tx hash
	depositID, err := s.GetDepositIDFromArbitrumTxHash(ctx, txHash)
	if err != nil {
		logger.Error("Error getting deposit ID from Arbitrum tx hash: %v", err)
		return "", "", err
	}

	// Then try to get the Monad tx hash from the deposit ID
	status, monadTxHash, err := s.FindMonadTransactionByDepositID(ctx, depositID)
	if err != nil {
		logger.Error("Error finding Monad tx hash by deposit ID: %v", err)
		return "", "", err
	}

	return status, monadTxHash, nil
}

// formatMonAmount formats a MON amount in wei to a human-readable string
func formatMonAmount(amount *big.Int) string {
	f := new(big.Float).SetInt(amount)
	f = new(big.Float).Quo(f, new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	return f.Text('f', 6)
}

// logMonCalculation logs the MON amount calculation for debugging purposes
func logMonCalculation(event DepositEvent, monAmount *big.Int) {
	// Get current ETH/USD price for reference
	ethUsdPrice := GetEthUsdPrice()
	ethUsdPriceFloat := new(big.Float).Quo(
		new(big.Float).SetInt(ethUsdPrice),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)),
	)

	// Convert deposit amount to ETH (assuming wei/satoshi/etc)
	var decimals int64
	var currencySymbol string
	switch event.Currency {
	case CurrencyETH:
		decimals = 18
		currencySymbol = "ETH"
	case CurrencyUSDC, CurrencyUSDT:
		decimals = 6
		currencySymbol = CurrencyTypeToString(event.Currency)
	default:
		decimals = 18
		currencySymbol = "UNKNOWN"
	}

	// Format deposit amount as human-readable
	depositAmountFloat := new(big.Float).Quo(
		new(big.Float).SetInt(event.Amount),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil)),
	)
	depositAmountStr := depositAmountFloat.Text('f', 12)

	// Calculate USD value of deposit
	usdValue := new(big.Float).Mul(depositAmountFloat, ethUsdPriceFloat)

	// Calculate MON/USD ratio
	monUsdRatio := GetMonUsdRatio()
	monUsdRatioFloat := new(big.Float).Quo(
		new(big.Float).SetInt(monUsdRatio),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
	)

	// Log the calculation
	if event.Currency == CurrencyETH {
		logger.Info("%s to MON calculation: %s %s ≈ $%s USD (ETH price: $%s) / $%s per MON = %s MON (%s wei)",
			currencySymbol,
			depositAmountStr,
			currencySymbol,
			usdValue.Text('f', 6),
			ethUsdPriceFloat.Text('f', 2),
			monUsdRatioFloat.Text('f', 6),
			formatMonAmount(monAmount),
			monAmount.String(),
		)
	} else {
		logger.Info("%s to MON calculation: %s %s / $%s per MON = %s MON (%s wei)",
			currencySymbol,
			depositAmountStr,
			currencySymbol,
			monUsdRatioFloat.Text('f', 6),
			formatMonAmount(monAmount),
			monAmount.String(),
		)
	}
}

// RecoverStuckTransactions checks for transactions stuck in pending state
// and updates them if they're actually completed on Monad
func (s *BridgeService) RecoverStuckTransactions(ctx context.Context) error {
	// Query all pending transactions
	pendingTxs, err := s.db.GetTransactionsByStatus(database.StatusPending, 100, 0)
	if err != nil {
		return fmt.Errorf("failed to get pending transactions: %w", err)
	}

	logger.Info("Checking %d pending transactions for recovery", len(pendingTxs))

	for _, tx := range pendingTxs {
		// Skip very recent transactions (less than 5 minutes old)
		if time.Since(tx.CreatedAt) < 5*time.Minute {
			logger.Info("Skipping recent transaction for deposit ID %s (created %s ago)",
				tx.DepositID.String(), time.Since(tx.CreatedAt).Round(time.Second))
			continue
		}

		// Try to find if this transaction exists on Monad
		monadTxHash, status, err := s.FindMonadTransactionByDepositID(ctx, tx.DepositID)
		if err == nil && monadTxHash != "" && status == "success" {
			logger.Info("📌 Recovering transaction: deposit ID %s has completed Monad transaction %s",
				tx.DepositID.String(), monadTxHash)

			// Update the transaction status
			if updateErr := s.UpdateTransactionStatus(ctx, tx.DepositID, database.StatusCompleted, monadTxHash); updateErr != nil {
				logger.Error("Failed to update recovered transaction: %v", updateErr)
			} else {
				logger.Info("✅ Successfully recovered transaction for deposit ID %s", tx.DepositID.String())
			}
			continue
		}

		// If it's been pending for more than 30 minutes, mark as failed
		if time.Since(tx.CreatedAt) > 30*time.Minute {
			logger.Warn("Transaction for deposit ID %s has been pending for more than 30 minutes, marking as failed",
				tx.DepositID.String())
			if updateErr := s.UpdateTransactionStatus(ctx, tx.DepositID, database.StatusFailed, ""); updateErr != nil {
				logger.Error("Failed to mark timed out transaction as failed: %v", updateErr)
			}
		}
	}

	return nil
}

func (s *BridgeService) recoverStuckTransactionsPeriodically() {
	defer s.wg.Done()
	logger.Info("Starting stuck transaction recovery processor...")

	// Run immediately on startup to fix any existing issues
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	if err := s.RecoverStuckTransactions(ctx); err != nil {
		logger.Error("Error in initial transaction recovery: %v", err)
	}
	cancel()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
			if err := s.RecoverStuckTransactions(ctx); err != nil {
				logger.Error("Error in transaction recovery: %v", err)
			}
			cancel()
		}
	}
}

// refundDeposit delegates the refund operation to the Arbitrum depositor
func (s *BridgeService) refundDeposit(ctx context.Context, depositId *big.Int) error {
	logger.Info("Delegating refund of deposit ID %s to ArbitrumDepositor", depositId.String())
	return s.arbDepositor.RefundDeposit(ctx, depositId)
}
