package blockchain

import (
	"context"
	"crypto/ecdsa"
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

	depositIDStr := depositID.String()

	// FIRST: Check if this deposit is already being processed locally
	if s.processingDeposits[depositIDStr] {
		logger.Warn("⚠️ Deposit ID %s is already being processed locally, skipping duplicate attempt", depositIDStr)
		return true
	}

	// SECOND: Check if the transaction is already completed in the database
	existingTx, txErr := s.GetTransactionByDepositID(context.Background(), depositID)
	if txErr == nil && existingTx != nil && existingTx.Status == database.StatusCompleted {
		logger.Info("Transaction for deposit ID %s is already completed with hash %s, skipping processing",
			depositIDStr, existingTx.MonadTxHash)
		return true
	}

	// THIRD: Try to acquire a distributed lock
	lockAcquired, err := s.db.AcquireProcessingLock(depositID, s.instanceID, s.lockDuration)
	if err != nil {
		logger.Error("Failed to check distributed lock: %v", err)

		// On lock error, be extra cautious and check blockchain directly
		// (expensive but safer than potential duplicate transactions)
		txHash, blockchainErr := s.checkMonadBlockchainForTransaction(context.Background(), depositID)
		if blockchainErr == nil && txHash != "" {
			logger.Info("Found transaction %s on blockchain for deposit ID %s during lock error handling",
				txHash, depositIDStr)

			// Update the database if possible
			if updateErr := s.UpdateTransactionStatus(context.Background(), depositID, database.StatusCompleted, txHash); updateErr != nil {
				logger.Error("Failed to update transaction status: %v", updateErr)
			}

			return true
		}

		// If we can't verify on the blockchain, we should err on the side of caution
		// For lock errors, still mark as processed (returning true) if we're not very confident
		if !strings.Contains(err.Error(), "connection refused") && !strings.Contains(err.Error(), "timeout") {
			logger.Warn("Lock acquisition failed with non-connection error, treating as already processing: %v", err)
			return true
		}
	} else if !lockAcquired {
		logger.Warn("⚠️ Deposit ID %s is locked by another instance, skipping duplicate attempt", depositIDStr)

		// When lock is not acquired, we MUST verify if there's already a transaction for this deposit ID
		// Check repeatedly as the other process might still be working on it
		for i := 0; i < 3; i++ {
			time.Sleep(2 * time.Second)

			// Check database first (faster)
			tx, err := s.GetTransactionByDepositID(context.Background(), depositID)
			if err == nil && tx != nil {
				if tx.Status == database.StatusCompleted && tx.MonadTxHash != "" {
					logger.Info("Found completed transaction for deposit ID %s with Monad hash %s after lock failure",
						depositIDStr, tx.MonadTxHash)
					return true
				} else if tx.Status == database.StatusPending && time.Since(tx.CreatedAt) < 5*time.Minute {
					// If a pending transaction exists and is recent, trust the lock and return true
					logger.Info("Found recent pending transaction for deposit ID %s, respecting lock", depositIDStr)
					return true
				}
				// If the transaction is old or failed, we'll fall through and try to process it again
			}

			// Check blockchain directly as a fallback
			txHash, _ := s.checkMonadBlockchainForTransaction(context.Background(), depositID)
			if txHash != "" {
				logger.Info("Found transaction %s on blockchain for deposit ID %s after lock failure",
					txHash, depositIDStr)

				// Update the database
				if updateErr := s.UpdateTransactionStatus(context.Background(), depositID, database.StatusCompleted, txHash); updateErr != nil {
					logger.Error("Failed to update transaction status: %v", updateErr)
				}

				return true
			}
		}

		// After our checks, if we still can't confirm that the transaction is already processed,
		// we should respect the lock and return true (indicating it's being processed)
		logger.Warn("Could not verify transaction status after lock acquisition failure, respecting lock for deposit ID %s", depositIDStr)
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

	// CRITICAL SECTION: Multiple checks to prevent duplicate transactions
	// This is the last line of defense against duplicate transactions

	// 1. Check database for completed transactions
	finalTx, finalErr := s.GetTransactionByDepositID(ctx, event.DepositId)
	if finalErr == nil && finalTx != nil && finalTx.Status == database.StatusCompleted && finalTx.MonadTxHash != "" {
		logger.Info("⚠️ LAST-MINUTE DUPLICATE PREVENTION: Deposit ID %s already has a completed transaction with Monad hash %s",
			event.DepositId.String(), finalTx.MonadTxHash)
		return nil // Successfully handled, just return
	}

	// 2. Check blockchain for existing transactions
	existingTxHash, _ := s.checkMonadBlockchainForTransaction(ctx, event.DepositId)
	if existingTxHash != "" {
		logger.Info("⚠️ LAST-MINUTE BLOCKCHAIN CHECK: Found existing transaction %s for deposit ID %s",
			existingTxHash, event.DepositId.String())

		// Update the database
		if err := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusCompleted, existingTxHash); err != nil {
			logger.Error("Failed to update transaction with found tx hash: %v", err)
		}

		return nil // Successfully handled, just return
	}

	// 3. Final check with retry - this is critical for high-value transactions
	// We'll check multiple times with a short delay to catch any race conditions
	for i := 0; i < 3; i++ {
		// Skip first iteration delay
		if i > 0 {
			time.Sleep(2 * time.Second)
		}

		// Check database again
		retryTx, retryErr := s.GetTransactionByDepositID(ctx, event.DepositId)
		if retryErr == nil && retryTx != nil && retryTx.Status == database.StatusCompleted && retryTx.MonadTxHash != "" {
			logger.Info("⚠️ RETRY CHECK FOUND DUPLICATE: Deposit ID %s already has a completed transaction with Monad hash %s",
				event.DepositId.String(), retryTx.MonadTxHash)
			return nil
		}

		// Check blockchain again
		retryTxHash, _ := s.checkMonadBlockchainForTransaction(ctx, event.DepositId)
		if retryTxHash != "" {
			logger.Info("⚠️ RETRY BLOCKCHAIN CHECK: Found existing transaction %s for deposit ID %s",
				retryTxHash, event.DepositId.String())

			// Update the database
			if err := s.UpdateTransactionStatus(ctx, event.DepositId, database.StatusCompleted, retryTxHash); err != nil {
				logger.Error("Failed to update transaction with found tx hash: %v", err)
			}

			return nil
		}
	}

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
	// We need to be extremely careful about duplicate transactions
	// Use a distributed lock based on deposit ID to prevent concurrent processing of the same deposit
	depositIDStr := depositId.String()
	logger.Info("🔒 Attempting to acquire lock for deposit ID %s", depositIDStr)

	// Try to acquire a distributed lock for 5 minutes
	acquired, err := s.db.AcquireProcessingLock(depositId, s.instanceID, 5*time.Minute)
	if err != nil {
		logger.Error("Error trying to acquire processing lock: %v", err)
		// Check for existing transactions before giving up
		for retryCount := 0; retryCount < 5; retryCount++ {
			// Check in database first (faster than blockchain check)
			existingTx, dbErr := s.GetTransactionByDepositID(ctx, depositId)
			if dbErr == nil && existingTx != nil && existingTx.Status == database.StatusCompleted && existingTx.MonadTxHash != "" {
				logger.Info("⚠️ DUPLICATE PREVENTION: Deposit ID %s already has a completed transaction with Monad hash %s",
					depositIDStr, existingTx.MonadTxHash)
				return existingTx.MonadTxHash, nil
			}

			// Check blockchain as a fallback
			monadTxHash, blockchainErr := s.checkMonadBlockchainForTransaction(ctx, depositId)
			if blockchainErr == nil && monadTxHash != "" {
				logger.Info("🔍 Found existing transaction on Monad blockchain: %s for deposit ID %s",
					monadTxHash, depositId.String())

				// Update database with Monad tx hash
				if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, monadTxHash); updateErr != nil {
					logger.Error("Failed to update transaction status: %v", updateErr)
				}

				return monadTxHash, nil
			}

			time.Sleep(2 * time.Second) // Brief pause between retries
		}

		// After retries, if we still can't confirm a completed transaction, fail safely
		return "", fmt.Errorf("failed to acquire processing lock and could not verify existing transactions: %v", err)
	} else if !acquired {
		logger.Warn("⚠️ Another instance appears to be processing deposit ID %s", depositIDStr)

		// If lock acquisition failed, check repeatedly for the result of the other process
		for retryCount := 0; retryCount < 5; retryCount++ {
			time.Sleep(2 * time.Second) // Give other process time to complete

			// Check in database
			existingTx, dbErr := s.GetTransactionByDepositID(ctx, depositId)
			if dbErr == nil && existingTx != nil && existingTx.Status == database.StatusCompleted && existingTx.MonadTxHash != "" {
				logger.Info("⚠️ DUPLICATE PREVENTION: Deposit ID %s already has a completed transaction with Monad hash %s",
					depositIDStr, existingTx.MonadTxHash)
				return existingTx.MonadTxHash, nil
			}

			// Check blockchain as a fallback
			monadTxHash, blockchainErr := s.checkMonadBlockchainForTransaction(ctx, depositId)
			if blockchainErr == nil && monadTxHash != "" {
				logger.Info("🔍 Found existing transaction on Monad blockchain: %s for deposit ID %s",
					monadTxHash, depositId.String())

				// Update database with Monad tx hash
				if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, monadTxHash); updateErr != nil {
					logger.Error("Failed to update transaction status: %v", updateErr)
				}

				return monadTxHash, nil
			}
		}

		// After maximum retries, MUST abort to prevent duplicate transactions
		return "", fmt.Errorf("another instance is processing deposit ID %s, and we could not verify completion", depositIDStr)
	} else {
		// Don't forget to release the lock when we're done
		defer func() {
			if releaseErr := s.db.ReleaseProcessingLock(depositId, s.instanceID); releaseErr != nil {
				logger.Error("Failed to release processing lock: %v", releaseErr)
			} else {
				logger.Info("Released processing lock for deposit ID %s", depositIDStr)
			}
		}()
	}

	// FIRST DEFENSE: Check if this transaction is already completed in database
	existingTx, err := s.GetTransactionByDepositID(ctx, depositId)
	if err == nil && existingTx != nil && existingTx.Status == database.StatusCompleted && existingTx.MonadTxHash != "" {
		logger.Info("⚠️ DUPLICATE PREVENTION: Deposit ID %s already has a completed transaction with Monad hash %s",
			depositIDStr, existingTx.MonadTxHash)
		return existingTx.MonadTxHash, nil
	}

	// SECOND DEFENSE: Check if there's a pending transaction and verify on blockchain
	if err == nil && existingTx != nil && existingTx.Status == database.StatusPending {
		// Check Monad blockchain directly by looking for Distribution events
		monadTxHash, err := s.checkMonadBlockchainForTransaction(ctx, depositId)
		if err == nil && monadTxHash != "" {
			logger.Info("🔍 Found existing transaction on Monad blockchain: %s for deposit ID %s",
				monadTxHash, depositId.String())

			// Update database with Monad tx hash
			if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, monadTxHash); updateErr != nil {
				logger.Error("Failed to update transaction status: %v", updateErr)
			} else {
				logger.Info("✅ Updated pending transaction to completed for deposit ID %s with Monad tx hash %s",
					depositId.String(), monadTxHash)
			}

			return monadTxHash, nil
		}

		// If we get here, the transaction is actually pending and needs processing
		logger.Info("Transaction for deposit ID %s is pending and needs processing", depositId.String())
	}

	// THIRD DEFENSE: Check transaction cache as well
	txLookupKey := depositId.String()
	s.txCacheMutex.RLock()
	cachedTx, found := s.txCache[txLookupKey]
	s.txCacheMutex.RUnlock()

	if found && cachedTx.Status == database.StatusCompleted && cachedTx.MonadTxHash != "" {
		logger.Info("⚠️ DUPLICATE PREVENTION (CACHE): Using cached Monad hash %s for deposit ID %s",
			cachedTx.MonadTxHash, depositId.String())
		return cachedTx.MonadTxHash, nil
	}

	// FOURTH DEFENSE: One final check on the blockchain before proceeding
	// This catches cases where the transaction exists but we don't have it in our DB
	monadTxHash, err := s.checkMonadBlockchainForTransaction(ctx, depositId)
	if err == nil && monadTxHash != "" {
		logger.Info("🔍 FINAL CHECK: Found existing transaction on Monad blockchain: %s for deposit ID %s",
			monadTxHash, depositId.String())

		// Update database with Monad tx hash
		if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, monadTxHash); updateErr != nil {
			logger.Error("Failed to update transaction status: %v", updateErr)
		} else {
			logger.Info("✅ Created/updated transaction record for deposit ID %s with Monad tx hash %s",
				depositId.String(), monadTxHash)
		}

		return monadTxHash, nil
	}

	// FIFTH DEFENSE: Register intent to process this transaction in database BEFORE submitting
	// This creates a race condition barrier that helps prevent duplicate submissions
	if existingTx == nil || existingTx.Status != database.StatusPending {
		// Create or update transaction with pending status
		if err := s.UpdateTransactionStatus(ctx, depositId, database.StatusPending, ""); err != nil {
			logger.Error("Failed to update transaction to pending status: %v", err)
			// Continue anyway, but log the error
		} else {
			logger.Info("Updated transaction status to pending for deposit ID %s", depositId.String())
		}

		// After marking as pending, do one more check on the blockchain to catch any race conditions
		// This is a critical section to prevent duplicate transactions
		time.Sleep(1 * time.Second) // Give other instances a moment to potentially complete
		monadTxHash, _ := s.checkMonadBlockchainForTransaction(ctx, depositId)
		if monadTxHash != "" {
			logger.Info("🔍 RACE CONDITION CHECK: Found existing transaction %s after marking as pending", monadTxHash)

			// Update database with Monad tx hash
			if updateErr := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, monadTxHash); updateErr != nil {
				logger.Error("Failed to update transaction status: %v", updateErr)
			}

			return monadTxHash, nil
		}
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

	// CRITICAL: Immediately update the transaction status in database using a transaction
	txHash := tx.Hash().Hex()
	for i := 0; i < 3; i++ { // Retry up to 3 times
		if err := s.UpdateTransactionStatus(ctx, depositId, database.StatusCompleted, txHash); err != nil {
			// Log the error but don't fail - tokens were still distributed
			logger.Error("CRITICAL: Failed to update transaction status (attempt %d/3): %v", i+1, err)

			if i < 2 {
				// Wait a moment before retrying
				time.Sleep(time.Second * 2)
				continue
			}
		} else {
			logger.Info("✅ Database update confirmed for deposit ID %s with Monad tx hash %s",
				depositId.String(), txHash)
			break
		}
	}

	// Update the transaction cache
	s.txCacheMutex.Lock()
	s.txCache[txLookupKey] = &database.Transaction{
		DepositID:   depositId,
		Status:      database.StatusCompleted,
		MonadTxHash: txHash,
	}
	s.txCacheMutex.Unlock()

	logger.Info("🔄 Updating transaction status to completed for deposit ID %s with Monad tx hash %s",
		depositId.String(), txHash)

	// Double-check that the status was actually updated
	updatedTx, err := s.GetTransactionByDepositID(ctx, depositId)
	if err != nil {
		logger.Error("Failed to verify transaction status update: %v", err)
	} else if updatedTx.Status != database.StatusCompleted || updatedTx.MonadTxHash != txHash {
		logger.Error("⚠️ Transaction status verification failed! Expected status=%s, hash=%s but got status=%s, hash=%s",
			database.StatusCompleted, txHash, updatedTx.Status, updatedTx.MonadTxHash)
	} else {
		logger.Info("✅ Transaction status updated correctly: deposit_id=%s, status=%s, monad_tx_hash=%s",
			depositId.String(), updatedTx.Status, updatedTx.MonadTxHash)
	}

	return txHash, nil
}

// checkMonadBlockchainForTransaction attempts to find a transaction on the Monad blockchain
// for a given deposit ID. This is used for recovery purposes.
func (s *BridgeService) checkMonadBlockchainForTransaction(ctx context.Context, depositId *big.Int) (string, error) {
	client := s.monadDistributor.client

	// Get current block number
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get current block number: %w", err)
	}

	// Look back a maximum of 50 blocks at a time to avoid RPC errors
	// Adjust as needed based on your expected transaction volume and confirmation time
	lookBackBlocks := uint64(50)
	if currentBlock < lookBackBlocks {
		lookBackBlocks = currentBlock
	}

	startBlock := currentBlock - lookBackBlocks

	// Create a filter for the Distribution event specifically
	// This event is emitted when funds are distributed via the distributeFunds function
	distributionEventSignature := []byte("Distribution(address,uint256,uint256)")
	distributionEventTopic := crypto.Keccak256Hash(distributionEventSignature)

	// IMPORTANT: The depositId is the third parameter in the event but since the first is indexed,
	// it appears as the third topic (index 2) in the event logs
	depositIdBytes32 := common.BytesToHash(depositId.Bytes())

	logger.Info("🔍 Looking for Distribution event with deposit ID %s (bytes32: %s)",
		depositId.String(), depositIdBytes32.Hex())

	// Create a filter query specifically for Distribution events with our deposit ID
	filterQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startBlock)),
		ToBlock:   big.NewInt(int64(currentBlock)),
		Addresses: []common.Address{s.monadDistributor.address},
		Topics: [][]common.Hash{
			{distributionEventTopic}, // Event signature
			nil,                      // Any recipient address
			nil,                      // Any amount
			{depositIdBytes32},       // Our deposit ID
		},
	}

	logger.Info("Searching for Distribution events with deposit ID %s between blocks %d and %d",
		depositId.String(), startBlock, currentBlock)

	logs, err := client.FilterLogs(ctx, filterQuery)
	if err != nil {
		logger.Error("Failed to filter logs: %v", err)

		// If we got a 'request entity too large' error, we need to try a smaller range
		if strings.Contains(err.Error(), "Request Entity Too Large") ||
			strings.Contains(err.Error(), "eth_getLogs is limited") {
			// Reduce the block range and try again
			lookBackBlocks = lookBackBlocks / 2
			if lookBackBlocks < 10 {
				lookBackBlocks = 10 // Minimum 10 blocks
			}
			logger.Info("Reducing block search range to %d blocks due to RPC limits", lookBackBlocks)

			// Update the query with smaller range
			startBlock = currentBlock - lookBackBlocks
			filterQuery.FromBlock = big.NewInt(int64(startBlock))
			filterQuery.ToBlock = big.NewInt(int64(currentBlock))

			// Try again with smaller range
			logs, err = client.FilterLogs(ctx, filterQuery)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	logger.Info("Found %d matching Distribution events for deposit ID %s", len(logs), depositId.String())

	// If we found matching events
	if len(logs) > 0 {
		// Return the hash of the most recent transaction
		txHash := logs[len(logs)-1].TxHash.Hex()
		logger.Info("🔍 Found existing Distribution event in transaction %s for deposit ID %s",
			txHash, depositId.String())
		return txHash, nil
	}

	// If no events found in the recent blocks, try a few more epochs
	// going back in history (up to 8 more epochs with exponential backoff)
	for i := 1; i <= 8; i++ {
		newEndBlock := startBlock
		newStartBlock := newEndBlock - lookBackBlocks
		if newStartBlock < 0 {
			newStartBlock = 0
		}

		// Update the filter query with new block range
		filterQuery.FromBlock = big.NewInt(int64(newStartBlock))
		filterQuery.ToBlock = big.NewInt(int64(newEndBlock - 1))

		logger.Info("Extending search to blocks %d-%d for deposit ID %s",
			newStartBlock, newEndBlock-1, depositId.String())

		logs, err := client.FilterLogs(ctx, filterQuery)
		if err != nil {
			logger.Error("Failed to filter logs in extended range: %v", err)

			// If we get an RPC error, reduce the range by half
			if strings.Contains(err.Error(), "Request Entity Too Large") ||
				strings.Contains(err.Error(), "eth_getLogs is limited") {
				lookBackBlocks = lookBackBlocks / 2
				if lookBackBlocks < 10 {
					// Give up if we're already down to 10 blocks
					break
				}
				logger.Info("Reducing block search range to %d blocks due to RPC limits", lookBackBlocks)
				// Restart the current iteration
				i--
				continue
			} else {
				// For other errors, just continue to the next range
				continue
			}
		}

		if len(logs) > 0 {
			// Return the hash of the most recent transaction
			txHash := logs[len(logs)-1].TxHash.Hex()
			logger.Info("🔍 Found existing Distribution event in transaction %s for deposit ID %s (extended search)",
				txHash, depositId.String())
			return txHash, nil
		}

		// Update for next iteration
		startBlock = newStartBlock

		// Increase the lookback window for each iteration to search further back
		// but cap it to avoid RPC issues
		lookBackBlocks = lookBackBlocks * 2
		if lookBackBlocks > 200 {
			lookBackBlocks = 200
		}
	}

	// As a last resort, search for all Distribution events and decode them manually
	// This is less efficient but can handle cases where the topic filtering is problematic
	if err := s.searchAllDistributionEvents(ctx, depositId); err != nil {
		logger.Error("Error during fallback search: %v", err)
	}

	return "", nil
}

// Custom context key type to avoid collisions
type contextKey string

// Define a constant for the fallback depth key
const fallbackDepthKey contextKey = "fallbackDepth"

// searchAllDistributionEvents scans recent Distribution events and manually checks for our deposit ID
// This is a fallback mechanism when the topic filtering doesn't work
func (s *BridgeService) searchAllDistributionEvents(ctx context.Context, targetDepositId *big.Int) error {
	client := s.monadDistributor.client

	// Get current block number
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	// Look back a reasonable number of blocks (last day or so)
	// Try to keep within RPC limits
	lookBackBlocks := uint64(1000)
	startBlock := currentBlock - lookBackBlocks
	if startBlock < 0 {
		startBlock = 0
	}

	logger.Info("🧠 FALLBACK SEARCH: Scanning all Distribution events between blocks %d and %d for deposit ID %s",
		startBlock, currentBlock, targetDepositId.String())

	// Create a filter for just the Distribution event
	distributionEventSignature := []byte("Distribution(address,uint256,uint256)")
	distributionEventTopic := crypto.Keccak256Hash(distributionEventSignature)

	// Query without deposit ID filter - just get all Distribution events
	filterQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startBlock)),
		ToBlock:   big.NewInt(int64(currentBlock)),
		Addresses: []common.Address{s.monadDistributor.address},
		Topics: [][]common.Hash{
			{distributionEventTopic}, // Event signature only
		},
	}

	logs, err := client.FilterLogs(ctx, filterQuery)
	if err != nil {
		// If we got a 'request entity too large' error, try with smaller range
		if strings.Contains(err.Error(), "Request Entity Too Large") ||
			strings.Contains(err.Error(), "eth_getLogs is limited") {
			lookBackBlocks = lookBackBlocks / 5 // Try with much smaller range
			startBlock = currentBlock - lookBackBlocks

			// Update the query with smaller range
			filterQuery.FromBlock = big.NewInt(int64(startBlock))
			logger.Info("Reducing block search range to %d blocks due to RPC limits", lookBackBlocks)

			// Try again with smaller range
			logs, err = client.FilterLogs(ctx, filterQuery)
			if err != nil {
				return fmt.Errorf("failed to filter logs even with reduced range: %w", err)
			}
		} else {
			return fmt.Errorf("failed to filter logs in fallback search: %w", err)
		}
	}

	logger.Info("FALLBACK SEARCH: Found %d Distribution events to check for deposit ID %s",
		len(logs), targetDepositId.String())

	// Define the ABI for the Distribution event
	// This exactly matches what's in the smart contract
	distributionEventABI := `[{"anonymous":false,"inputs":[{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"id","type":"uint256"}],"name":"Distribution","type":"event"}]`
	parsedABI, err := abi.JSON(strings.NewReader(distributionEventABI))
	if err != nil {
		return fmt.Errorf("failed to parse ABI: %w", err)
	}

	// Check each event
	matchCount := 0
	for _, log := range logs {
		// First, verify this is indeed a Distribution event
		if len(log.Topics) == 0 || log.Topics[0] != distributionEventTopic {
			continue
		}

		// Try to decode the event data
		// The event has: recipient (indexed), amount, id
		// For indexed fields, we get them from Topics
		// For non-indexed fields, they're in the Data field
		if len(log.Data) == 0 {
			continue // Skip events with no data
		}

		// Parse the non-indexed fields from data
		// We expect two uint256 values: amount and id
		var id *big.Int

		// Try to decode the data
		decoded, err := parsedABI.Unpack("Distribution", log.Data)
		if err != nil {
			logger.Debug("Failed to decode event data: %v", err)
			continue
		}

		// Ensure we have 2 parameters (amount and id)
		if len(decoded) != 2 {
			logger.Debug("Unexpected number of parameters in event data: %v", decoded)
			continue
		}

		// Extract the id parameter
		id, ok := decoded[1].(*big.Int)
		if !ok || id == nil {
			logger.Debug("Failed to extract ID from event data")
			continue
		}

		// Check if this event matches our deposit ID
		if id.Cmp(targetDepositId) == 0 {
			matchCount++
			logger.Info("🔍 FALLBACK SEARCH: Found matching deposit ID %s in transaction %s (block %d)",
				targetDepositId.String(), log.TxHash.Hex(), log.BlockNumber)

			// Update the database
			if updateErr := s.UpdateTransactionStatus(ctx, targetDepositId, database.StatusCompleted, log.TxHash.Hex()); updateErr != nil {
				logger.Error("Failed to update transaction status: %v", updateErr)
			} else {
				logger.Info("✅ RECOVERY: Updated transaction for deposit ID %s with Monad tx hash %s",
					targetDepositId.String(), log.TxHash.Hex())

				// Update the cache as well
				s.txCacheMutex.Lock()
				s.txCache[targetDepositId.String()] = &database.Transaction{
					DepositID:   targetDepositId,
					Status:      database.StatusCompleted,
					MonadTxHash: log.TxHash.Hex(),
				}
				s.txCacheMutex.Unlock()

				// We found what we were looking for
				return nil
			}
		}
	}

	// Try other date ranges if nothing found and time permits
	if matchCount == 0 && currentBlock > lookBackBlocks*2 {
		// Try an earlier time period
		newEnd := startBlock
		newStart := newEnd - lookBackBlocks
		if newStart < 0 {
			newStart = 0
		}

		logger.Info("FALLBACK SEARCH: Extending to blocks %d-%d for deposit ID %s",
			newStart, newEnd, targetDepositId.String())

		filterQuery.FromBlock = big.NewInt(int64(newStart))
		filterQuery.ToBlock = big.NewInt(int64(newEnd))

		// Recursively call to check earlier blocks
		// But limit to 2 recursive calls
		if ctx.Value(fallbackDepthKey) == nil {
			newCtx := context.WithValue(ctx, fallbackDepthKey, 1)
			return s.searchAllDistributionEvents(newCtx, targetDepositId)
		} else if depth, ok := ctx.Value(fallbackDepthKey).(int); ok && depth < 2 {
			newCtx := context.WithValue(ctx, fallbackDepthKey, depth+1)
			return s.searchAllDistributionEvents(newCtx, targetDepositId)
		}
	}

	logger.Info("FALLBACK SEARCH: No matching deposit ID %s found after checking %d events",
		targetDepositId.String(), len(logs))
	return nil
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
