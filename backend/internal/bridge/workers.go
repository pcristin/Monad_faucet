package bridge

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// WorkerPool represents a generic worker pool
type WorkerPool struct {
	name       string
	numWorkers int
	jobChan    chan interface{}
	quit       chan struct{}
	wg         sync.WaitGroup
}

// NewWorkerPool creates a new worker pool with specified number of workers
func NewWorkerPool(name string, numWorkers, jobChannelSize int) *WorkerPool {
	return &WorkerPool{
		name:       name,
		numWorkers: numWorkers,
		jobChan:    make(chan interface{}, jobChannelSize),
		quit:       make(chan struct{}),
	}
}

// Start starts the worker pool
func (pool *WorkerPool) Start(processFunc func(context.Context, interface{})) {
	pool.wg.Add(pool.numWorkers)

	for i := 0; i < pool.numWorkers; i++ {
		workerID := i
		go func() {
			defer pool.wg.Done()
			logger.Info("[%s] Worker %d started", pool.name, workerID)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			for {
				select {
				case job, ok := <-pool.jobChan:
					if !ok {
						logger.Info("[%s] Worker %d stopping: job channel closed", pool.name, workerID)
						return
					}
					processFunc(ctx, job)
				case <-pool.quit:
					logger.Info("[%s] Worker %d stopping: quit signal received", pool.name, workerID)
					return
				}
			}
		}()
	}
}

// Stop stops the worker pool
func (pool *WorkerPool) Stop() {
	logger.Info("[%s] Stopping worker pool", pool.name)
	close(pool.quit)
	pool.wg.Wait()
	logger.Info("[%s] Worker pool stopped", pool.name)
}

// Submit submits a job to the worker pool
func (pool *WorkerPool) Submit(job interface{}) {
	select {
	case pool.jobChan <- job:
		// Job submitted successfully
	default:
		logger.Warn("[%s] Job channel full, dropping job", pool.name)
	}
}

// Deposit event worker pool

// DepositJob represents a job for deposit event processing
type DepositJob struct {
	Event blockchain.DepositEvent
}

// CalculationJob represents a job for MON amount calculation
type CalculationJob struct {
	Deposit     *database.Deposit
	State       *blockchain.ContractState
	DepositChan chan<- CalculatedDeposit
}

// CalculatedDeposit contains deposit info with calculated MON amount
type CalculatedDeposit struct {
	Deposit   *database.Deposit
	MonAmount *big.Int
}

// DistributionJob represents a job for distribution processing
type DistributionJob struct {
	DepositID     *big.Int
	WalletAddress common.Address
	MonAmount     *big.Int
}

// BatchDistributionJob represents a job for batch distribution processing
type BatchDistributionJob struct {
	Distributions []*DistributionJob
}

// DBWorkerJob represents a job for database operations
type DBWorkerJob struct {
	JobType      string
	Deposit      *database.Deposit
	Distribution *database.Distribution
}

// JobTypes for database worker
const (
	JobCreateDeposit            = "create_deposit"
	JobUpdateDepositStatus      = "update_deposit_status"
	JobCreateDistribution       = "create_distribution"
	JobUpdateDistributionStatus = "update_distribution_status"
)

// BridgeWorkerPools contains all worker pools for the bridge service
type BridgeWorkerPools struct {
	DepositPool        *WorkerPool
	CalculationPool    *WorkerPool
	DistributionPool   *WorkerPool
	DBPool             *WorkerPool
	calculationChannel chan CalculatedDeposit
	service            *BridgeService
	mu                 sync.Mutex
	processingDeposits map[string]bool

	// Batch processing fields
	distributionBatch []*DistributionJob
	batchMutex        sync.Mutex
	batchTimer        *time.Timer
	mergeDelay        time.Duration
	maxDeposits       int
}

// NewBridgeWorkerPools creates a new set of worker pools
func NewBridgeWorkerPools(service *BridgeService) *BridgeWorkerPools {
	return &BridgeWorkerPools{
		DepositPool:        NewWorkerPool("deposit", 2, 1000),
		CalculationPool:    NewWorkerPool("calculation", 2, 1000),
		DistributionPool:   NewWorkerPool("distribution", 2, 1000),
		DBPool:             NewWorkerPool("database", 2, 1000),
		calculationChannel: make(chan CalculatedDeposit, 1000),
		service:            service,
		processingDeposits: make(map[string]bool),
		distributionBatch:  make([]*DistributionJob, 0, 100),
		mergeDelay:         30 * time.Second, // Very short delay to encourage faster batching
		maxDeposits:        100,              // Small batch size to encourage more frequent batches
	}
}

// Start starts all worker pools
func (pools *BridgeWorkerPools) Start() {
	// Start deposit workers
	pools.DepositPool.Start(func(ctx context.Context, job interface{}) {
		pools.processDepositJob(ctx, job.(*DepositJob))
	})

	// Start calculation workers
	pools.CalculationPool.Start(func(ctx context.Context, job interface{}) {
		pools.processCalculationJob(ctx, job.(*CalculationJob))
	})

	// Start distribution workers
	pools.DistributionPool.Start(func(ctx context.Context, job interface{}) {
		switch j := job.(type) {
		case *DistributionJob:
			pools.processDistributionJob(ctx, j)
		case *BatchDistributionJob:
			pools.processBatchDistributionJob(ctx, j)
		}
	})

	// Start database workers
	pools.DBPool.Start(func(ctx context.Context, job interface{}) {
		pools.processDBJob(ctx, job.(*DBWorkerJob))
	})

	// Start calculation result consumer
	go pools.consumeCalculationResults()
}

// Stop stops all worker pools
func (pools *BridgeWorkerPools) Stop() {
	pools.DepositPool.Stop()
	pools.CalculationPool.Stop()
	pools.DistributionPool.Stop()
	pools.DBPool.Stop()
	close(pools.calculationChannel)

	// Stop batch timer if active
	pools.batchMutex.Lock()
	if pools.batchTimer != nil {
		pools.batchTimer.Stop()
	}
	pools.batchMutex.Unlock()
}

// SubmitDepositEvent submits a deposit event for processing
func (pools *BridgeWorkerPools) SubmitDepositEvent(event blockchain.DepositEvent) {
	// Check if deposit is already being processed
	depositIDStr := event.DepositId.String()

	pools.mu.Lock()
	if pools.processingDeposits[depositIDStr] {
		pools.mu.Unlock()
		logger.Warn("Skipping duplicate processing for deposit ID %s", depositIDStr)
		return
	}
	pools.processingDeposits[depositIDStr] = true
	pools.mu.Unlock()

	pools.DepositPool.Submit(&DepositJob{Event: event})
}

// finishProcessingDeposit marks a deposit as done processing
func (pools *BridgeWorkerPools) finishProcessingDeposit(depositID *big.Int) {
	depositIDStr := depositID.String()
	pools.mu.Lock()
	defer pools.mu.Unlock()
	delete(pools.processingDeposits, depositIDStr)
}

// processDepositJob processes a deposit job
func (pools *BridgeWorkerPools) processDepositJob(ctx context.Context, job *DepositJob) {
	defer pools.finishProcessingDeposit(job.Event.DepositId)

	// 1. Check if this deposit has already been processed
	deposit, err := pools.service.db.GetDepositByID(job.Event.DepositId)
	if err == nil && deposit != nil && deposit.Status == database.StatusProcessed {
		logger.Info("Deposit ID %s already processed", job.Event.DepositId.String())
		return
	}

	// 2. Create a new deposit record
	newDeposit := &database.Deposit{
		DepositID:     job.Event.DepositId,
		WalletAddress: job.Event.Depositor,
		Amount:        job.Event.Amount,
		Currency:      database.CurrencyType(job.Event.Currency),
		TxHash:        job.Event.TxHash,
		BlockNumber:   job.Event.BlockNumber,
		Status:        database.StatusPending,
		Metadata:      job.Event.Metadata,
	}

	// 3. Submit database job to create the deposit
	pools.DBPool.Submit(&DBWorkerJob{
		JobType: JobCreateDeposit,
		Deposit: newDeposit,
	})

	// 4. Get the current contract state
	state, err := pools.service.GetState(ctx)
	if err != nil {
		logger.Error("Failed to get bridge state: %v", err)
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.Event.DepositId,
				Status:    database.StatusFailed,
			},
		})
		return
	}

	if state.IsPaused {
		logger.Warn("Bridge is currently paused, deposit %s will not be processed", job.Event.DepositId.String())
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.Event.DepositId,
				Status:    database.StatusFailed,
			},
		})
		return
	}

	// 5. Wait for confirmations and mark as processed if successful
	if err := pools.service.waitForConfirmations(ctx, job.Event.BlockNumber, 10); err != nil {
		logger.Error("Failed to wait for confirmations: %v", err)
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.Event.DepositId,
				Status:    database.StatusFailed,
			},
		})
		return
	} else {
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.Event.DepositId,
				Status:    database.StatusProcessed,
			},
		})
	}

	// 6. Submit to calculation pool
	pools.CalculationPool.Submit(&CalculationJob{
		Deposit:     newDeposit,
		State:       state,
		DepositChan: pools.calculationChannel,
	})
}

// processCalculationJob calculates MON amount for a deposit
func (pools *BridgeWorkerPools) processCalculationJob(ctx context.Context, job *CalculationJob) {
	// Calculate MON amount based on deposit amount and exchange rate
	monAmount := calculateMonAmount(job.Deposit.Amount, job.State.SwapRatios[blockchain.CurrencyType(job.Deposit.Currency)], blockchain.CurrencyType(job.Deposit.Currency))

	// Validate the deposit and amount
	if err := pools.service.validateDepositWithAmount(job.State, blockchain.DepositEvent{
		DepositId:   job.Deposit.DepositID,
		Depositor:   job.Deposit.WalletAddress,
		Amount:      job.Deposit.Amount,
		Currency:    blockchain.CurrencyType(job.Deposit.Currency),
		BlockNumber: job.Deposit.BlockNumber,
	}, monAmount); err != nil {
		logger.Error("Invalid deposit: %v", err)
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.Deposit.DepositID,
				Status:    database.StatusFailed,
			},
		})
		return
	}

	// Send calculated deposit to the channel
	select {
	case job.DepositChan <- CalculatedDeposit{
		Deposit:   job.Deposit,
		MonAmount: monAmount,
	}:
		// Successfully sent
	case <-ctx.Done():
		logger.Error("Context cancelled while submitting calculated deposit")
	}
}

// consumeCalculationResults consumes calculation results and forwards to distribution pool
func (pools *BridgeWorkerPools) consumeCalculationResults() {
	for calcResult := range pools.calculationChannel {
		deposit := calcResult.Deposit
		monAmount := calcResult.MonAmount

		// Create distribution record
		distribution := &database.Distribution{
			DepositID:     deposit.DepositID,
			WalletAddress: deposit.WalletAddress,
			MonAmount:     monAmount,
			Status:        database.DistStatusPending,
		}

		// Submit to database pool to create distribution record
		pools.DBPool.Submit(&DBWorkerJob{
			JobType:      JobCreateDistribution,
			Distribution: distribution,
		})

		// Create distribution job
		distJob := &DistributionJob{
			DepositID:     deposit.DepositID,
			WalletAddress: deposit.WalletAddress,
			MonAmount:     monAmount,
		}

		// Add to batch or process individually
		pools.addToDistributionBatch(distJob)
	}
}

// addToDistributionBatch adds a distribution job to the current batch
func (pools *BridgeWorkerPools) addToDistributionBatch(job *DistributionJob) {
	pools.batchMutex.Lock()
	defer pools.batchMutex.Unlock()

	depositIDStr := job.DepositID.String()
	logger.Info("BATCH-TRACKING: Adding deposit ID %s to distribution batch", depositIDStr)

	// Add job to the batch
	pools.distributionBatch = append(pools.distributionBatch, job)

	logger.Info("BATCH-TRACKING: Added distribution job for deposit ID %s to batch (current batch size: %d/%d)",
		depositIDStr, len(pools.distributionBatch), pools.maxDeposits)

	// If this is the first job in the batch, start the batch timer
	if len(pools.distributionBatch) == 1 {
		logger.Info("BATCH-TRACKING: Starting batch timer with %d second delay for deposit ID %s",
			int(pools.mergeDelay.Seconds()), depositIDStr)
		pools.startBatchTimer()
	}

	// Check if batch is full
	if len(pools.distributionBatch) >= pools.maxDeposits {
		logger.Info("BATCH-TRACKING: Batch is full (%d/%d distributions), submitting now",
			len(pools.distributionBatch), pools.maxDeposits)
		pools.submitBatch()
	}
}

// startBatchTimer starts a timer to process the batch after mergeDelay
func (pools *BridgeWorkerPools) startBatchTimer() {
	// Stop existing timer if any
	if pools.batchTimer != nil {
		pools.batchTimer.Stop()
	}

	// Start new timer
	pools.batchTimer = time.AfterFunc(pools.mergeDelay, func() {
		pools.processBatchOnTimeout()
	})
}

// processBatchOnTimeout processes the current batch due to timeout
func (pools *BridgeWorkerPools) processBatchOnTimeout() {
	pools.batchMutex.Lock()
	defer pools.batchMutex.Unlock()

	// Only process if there are jobs in the batch
	if len(pools.distributionBatch) > 0 {
		logger.Info("BATCH-TRACKING: Processing distribution batch of %d deposits due to timeout (merge delay of %d seconds reached)",
			len(pools.distributionBatch), int(pools.mergeDelay.Seconds()))
		pools.submitBatch()
	} else {
		logger.Info("BATCH-TRACKING: Batch timer fired but no distributions to process")
	}
}

// submitBatch submits the current batch for processing
func (pools *BridgeWorkerPools) submitBatch() {
	// Stop timer
	if pools.batchTimer != nil {
		pools.batchTimer.Stop()
		pools.batchTimer = nil
	}

	// Create a copy of the current batch
	batchCopy := make([]*DistributionJob, len(pools.distributionBatch))
	copy(batchCopy, pools.distributionBatch)

	batchCount := len(batchCopy)
	batchId := fmt.Sprintf("batch-%d", time.Now().Unix())

	// Get deposit IDs for logging
	var depositIDs []string
	for _, job := range batchCopy {
		depositIDs = append(depositIDs, job.DepositID.String())
	}
	depositIDsStr := strings.Join(depositIDs, ", ")

	// Clear the batch
	pools.distributionBatch = make([]*DistributionJob, 0, pools.maxDeposits)

	// Submit batch job
	logger.Info("BATCH-TRACKING: Submitting batch %s with %d distributions: [%s]",
		batchId, batchCount, depositIDsStr)

	pools.DistributionPool.Submit(&BatchDistributionJob{
		Distributions: batchCopy,
	})
}

// processBatchDistributionJob handles a batch of distributions
func (pools *BridgeWorkerPools) processBatchDistributionJob(ctx context.Context, batch *BatchDistributionJob) {
	if len(batch.Distributions) == 0 {
		logger.Warn("Received empty distribution batch")
		return
	}

	batchID := fmt.Sprintf("batch-%d", time.Now().Unix())
	logger.Info("[Batch %s] Processing batch of %d distributions", batchID, len(batch.Distributions))

	// Log all deposit IDs in this batch
	var depositIDs []string
	for _, dist := range batch.Distributions {
		depositIDs = append(depositIDs, dist.DepositID.String())
	}
	logger.Info("[Batch %s] Contains deposit IDs: %s", batchID, strings.Join(depositIDs, ", "))

	// Attempt to process as a batch first
	success, failedJobs := pools.processBatchMint(ctx, batch.Distributions)

	// If batch processing was completely successful, we're done
	if success && len(failedJobs) == 0 {
		logger.Info("[Batch %s] Successfully processed all %d distributions in batch", batchID, len(batch.Distributions))
		return
	}

	// If batch processing failed completely or partially, fall back to individual processing
	if len(failedJobs) > 0 {
		logger.Warn("[Batch %s] Batch processing partially failed, falling back to individual processing for %d failed jobs",
			batchID, len(failedJobs))

		// Log the failed deposit IDs
		var failedIDs []string
		for _, job := range failedJobs {
			failedIDs = append(failedIDs, job.DepositID.String())
		}
		logger.Warn("[Batch %s] Failed deposit IDs: %s", batchID, strings.Join(failedIDs, ", "))

		for _, job := range failedJobs {
			pools.processDistributionJob(ctx, job)
		}
	} else {
		logger.Warn("[Batch %s] Batch processing completely failed, falling back to individual processing for all %d jobs",
			batchID, len(batch.Distributions))
		for _, job := range batch.Distributions {
			pools.processDistributionJob(ctx, job)
		}
	}
}

// processBatchMint attempts to process multiple distributions in a single transaction
// Returns true if the batch was attempted (even if some distributions failed)
// Also returns any failed distribution jobs that should be retried individually
func (pools *BridgeWorkerPools) processBatchMint(ctx context.Context, distributions []*DistributionJob) (bool, []*DistributionJob) {
	if len(distributions) == 0 {
		return false, nil
	}

	// Prepare arrays for batch mint call
	addresses := make([]common.Address, len(distributions))
	amounts := make([]*big.Int, len(distributions))
	depositIDs := make([]*big.Int, len(distributions))

	for i, dist := range distributions {
		addresses[i] = dist.WalletAddress
		amounts[i] = dist.MonAmount
		depositIDs[i] = dist.DepositID
	}

	// Log the batch mint attempt
	logger.Info("Attempting batch mint of %d distributions", len(distributions))

	// Check if any distributions are already completed
	for i, dist := range distributions {
		// Check blockchain for existing transaction
		txHash, err := pools.service.checkMonadBlockchainForTransaction(ctx, dist.DepositID)
		if err == nil && txHash != "" {
			logger.Info("Distribution for deposit ID %s already exists on blockchain with tx %s",
				dist.DepositID.String(), txHash)

			// Update database records
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobUpdateDistributionStatus,
				Distribution: &database.Distribution{
					DepositID:   dist.DepositID,
					Status:      database.DistStatusCompleted,
					MonadTxHash: txHash,
				},
			})

			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobUpdateDepositStatus,
				Deposit: &database.Deposit{
					DepositID: dist.DepositID,
					Status:    database.StatusProcessed,
				},
			})

			// Remove this distribution from the batch
			// We do this by setting the address to zero address and amount to zero
			// The distributor contract should skip these
			addresses[i] = common.Address{}
			amounts[i] = big.NewInt(0)
		}
	}

	// Attempt to do a batch mint for all distributions at once using individual mintTokens
	// This is a fallback approach since we don't have batchMintTokens implemented yet
	txHash, err := pools.service.mintTokensBatch(ctx, addresses, amounts, depositIDs)

	if err != nil {
		logger.Error("Batch mint transaction failed: %v", err)
		return false, nil // Complete failure, retry all individually
	}

	// Process successful batch mint
	for i, dist := range distributions {
		// Skip distributions with zero amount (already processed)
		if amounts[i].Cmp(big.NewInt(0)) <= 0 {
			continue
		}

		// This distribution succeeded in the batch
		depositIDStr := dist.DepositID.String()
		logger.Info("Successfully minted %s MON for deposit ID %s in batch tx %s",
			formatMonAmount(dist.MonAmount), depositIDStr, txHash)

		// Update database records for successful mint
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDistributionStatus,
			Distribution: &database.Distribution{
				DepositID:   dist.DepositID,
				Status:      database.DistStatusCompleted,
				MonadTxHash: txHash,
			},
		})

		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: dist.DepositID,
				Status:    database.StatusProcessed,
			},
		})

		// Create a custom DB job to update transaction_history table
		// This ensures all tables are updated consistently through the worker pool
		txHistoryJob := &DBWorkerJob{
			JobType: "update_transaction_history",
			Distribution: &database.Distribution{
				DepositID:   dist.DepositID,
				Status:      database.DistStatusCompleted,
				MonadTxHash: txHash,
				MonAmount:   dist.MonAmount, // Include the MON amount for the transaction history
			},
		}

		pools.DBPool.Submit(txHistoryJob)
		logger.Info("Queued transaction history update for deposit ID %s with tx hash %s and MON amount %s",
			depositIDStr, txHash, formatMonAmount(dist.MonAmount))
	}

	return true, nil // All processed successfully in batch
}

// mintTokensBatch mints MON tokens for multiple recipients in a single transaction.
func (s *BridgeService) mintTokensBatch(ctx context.Context, recipients []common.Address, amounts []*big.Int, depositIds []*big.Int) (string, error) {
	if len(recipients) != len(amounts) || len(recipients) != len(depositIds) {
		return "", fmt.Errorf("recipients, amounts, and depositIds must have the same length")
	}

	if len(recipients) == 0 {
		return "", fmt.Errorf("empty batch")
	}

	// Count valid transfers
	validCount := 0
	for _, amount := range amounts {
		if amount.Cmp(big.NewInt(0)) > 0 {
			validCount++
		}
	}

	if validCount == 0 {
		return "", fmt.Errorf("no valid transfers in batch")
	}

	// Create a typed array of TransferData structs for the contract call
	// The struct must match the contract's TransferData struct: {recipient, amount, id}
	type TransferData struct {
		Recipient common.Address `abi:"recipient"`
		Amount    *big.Int       `abi:"amount"`
		Id        *big.Int       `abi:"id"`
	}

	transfers := make([]TransferData, 0, validCount)

	// Add only valid transfers (with non-zero amounts)
	for i, amount := range amounts {
		if amount.Cmp(big.NewInt(0)) > 0 {
			transfer := TransferData{
				Recipient: recipients[i],
				Amount:    amounts[i],
				Id:        depositIds[i],
			}
			transfers = append(transfers, transfer)
		}
	}

	logger.Info("Creating batch mint transaction for %d recipients", len(transfers))

	// Log each transfer for debugging
	for i, transfer := range transfers {
		logger.Info("Batch transfer #%d: %s MON to %s (deposit ID: %s)",
			i+1,
			formatMonAmount(transfer.Amount),
			transfer.Recipient.Hex(),
			transfer.Id.String())
	}

	// Get transaction options
	opts, err := s.monadDistributor.GetTransactOpts(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction options: %v", err)
	}

	// Execute the batch distribution transaction
	// The contract function expects an array of TransferData structs
	tx, err := s.monadDistributor.TransactWithGasBuffer(opts, "distributeFunds", transfers)
	if err != nil {
		logger.Error("Failed to distribute funds in batch: %v", err)
		return "", fmt.Errorf("failed to distribute funds in batch: %v", err)
	}

	txHash := tx.Hash().Hex()
	logger.Info("Batch mint transaction submitted: %s", txHash)

	// Wait for transaction to be mined
	receipt, err := bind.WaitMined(ctx, s.monadDistributor.Client, tx)
	if err != nil {
		logger.Error("Failed to wait for batch mint transaction: %v", err)
		return txHash, fmt.Errorf("failed to wait for transaction confirmation: %v", err)
	}

	if receipt.Status == 0 {
		logger.Error("Batch mint transaction failed on-chain")
		return txHash, fmt.Errorf("transaction failed on-chain")
	}

	logger.Info("Batch mint transaction confirmed with %d distributions", len(transfers))
	return txHash, nil
}

// processDistributionJob processes a distribution job
func (pools *BridgeWorkerPools) processDistributionJob(ctx context.Context, job *DistributionJob) {
	depositIDStr := job.DepositID.String()

	// Create a unique processing key for this distribution job
	processingKey := "distribution:" + depositIDStr

	// Attempt to acquire lock for this deposit ID
	pools.mu.Lock()
	if _, exists := pools.processingDeposits[processingKey]; exists {
		pools.mu.Unlock()
		logger.Info("Distribution for deposit ID %s is already being processed", depositIDStr)
		return
	}

	// Mark as processing and release global lock
	pools.processingDeposits[processingKey] = true
	pools.mu.Unlock()

	// Ensure we clean up when we're done
	defer func() {
		pools.mu.Lock()
		delete(pools.processingDeposits, processingKey)
		pools.mu.Unlock()
	}()

	// First, check blockchain for existing transaction
	// This is the most authoritative source
	txHash, err := pools.service.checkMonadBlockchainForTransaction(ctx, job.DepositID)
	if err == nil && txHash != "" {
		logger.Info("Distribution already exists on blockchain with tx %s for deposit ID %s", txHash, depositIDStr)

		// Update records to match blockchain state
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDistributionStatus,
			Distribution: &database.Distribution{
				DepositID:   job.DepositID,
				Status:      database.DistStatusCompleted,
				MonadTxHash: txHash,
			},
		})
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.DepositID,
				Status:    database.StatusProcessed,
			},
		})
		return
	}

	// Second, check if the distribution exists in the database
	dist, err := pools.service.db.GetDistributionByDepositID(job.DepositID)
	if err == nil && dist != nil {
		if dist.Status == database.DistStatusCompleted && dist.MonadTxHash != "" {
			logger.Info("Distribution for deposit ID %s already completed with tx hash %s in database",
				depositIDStr, dist.MonadTxHash)

			// Double-check blockchain for this transaction
			if txHash, err := pools.service.checkMonadBlockchainForTransaction(ctx, job.DepositID); err == nil && txHash != "" {
				if txHash != dist.MonadTxHash {
					logger.Warn("Database tx hash %s doesn't match blockchain tx hash %s for deposit ID %s",
						dist.MonadTxHash, txHash, depositIDStr)
				}
			}

			// Ensure deposit status is also updated
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobUpdateDepositStatus,
				Deposit: &database.Deposit{
					DepositID: job.DepositID,
					Status:    database.StatusProcessed,
				},
			})
			return
		}
	}

	// Verify we have valid amount
	if job.MonAmount == nil || job.MonAmount.Cmp(big.NewInt(0)) <= 0 {
		logger.Error("Invalid MON amount for distribution: %v", job.MonAmount)
		return
	}

	// Log the amount for debugging
	logger.Info("Processing distribution of %s MON for deposit ID %s to wallet %s",
		formatMonAmount(job.MonAmount), depositIDStr, job.WalletAddress.Hex())

	// Execute the mint transaction
	txHash, err = pools.service.mintTokens(ctx, job.WalletAddress, job.MonAmount, job.DepositID)

	if err != nil {
		// Check if this is a duplicate error - mint function should handle this internally
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "already") {
			logger.Info("Duplicate mint detected: %v", err)

			// If we got a transaction hash despite the error, update records
			if txHash != "" {
				pools.DBPool.Submit(&DBWorkerJob{
					JobType: JobUpdateDistributionStatus,
					Distribution: &database.Distribution{
						DepositID:   job.DepositID,
						Status:      database.DistStatusCompleted,
						MonadTxHash: txHash,
					},
				})
				pools.DBPool.Submit(&DBWorkerJob{
					JobType: JobUpdateDepositStatus,
					Deposit: &database.Deposit{
						DepositID: job.DepositID,
						Status:    database.StatusProcessed,
					},
				})
			}
		} else {
			// Real error occurred
			logger.Error("Mint transaction failed: %v", err)
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobUpdateDistributionStatus,
				Distribution: &database.Distribution{
					DepositID: job.DepositID,
					Status:    database.DistStatusFailed,
				},
			})
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobUpdateDepositStatus,
				Deposit: &database.Deposit{
					DepositID: job.DepositID,
					Status:    database.StatusFailed,
				},
			})
		}
		return
	}

	// Successfully minted tokens
	logger.Info("Successfully minted %s MON for deposit ID %s with tx %s",
		formatMonAmount(job.MonAmount), depositIDStr, txHash)

	pools.DBPool.Submit(&DBWorkerJob{
		JobType: JobUpdateDistributionStatus,
		Distribution: &database.Distribution{
			DepositID:   job.DepositID,
			Status:      database.DistStatusCompleted,
			MonadTxHash: txHash,
		},
	})

	pools.DBPool.Submit(&DBWorkerJob{
		JobType: JobUpdateDepositStatus,
		Deposit: &database.Deposit{
			DepositID: job.DepositID,
			Status:    database.StatusProcessed,
		},
	})
}

// processDBJob processes database operations
func (pools *BridgeWorkerPools) processDBJob(ctx context.Context, job *DBWorkerJob) {
	switch job.JobType {
	case JobCreateDeposit:
		if err := pools.service.db.CreateDeposit(job.Deposit); err != nil {
			logger.Error("Failed to create deposit record: %v", err)
		}

	case JobUpdateDepositStatus:
		logger.Info("Processing JobUpdateDepositStatus for deposit ID %s to status %s",
			job.Deposit.DepositID.String(), job.Deposit.Status)

		// First check if the deposit exists and get current status
		deposit, _ := pools.service.db.GetDepositByID(job.Deposit.DepositID)
		if deposit != nil {
			logger.Info("Current deposit status before update for ID %s: %s",
				job.Deposit.DepositID.String(), deposit.Status)
		}

		if err := pools.service.db.UpdateDepositStatus(job.Deposit.DepositID, job.Deposit.Status); err != nil {
			logger.Error("Failed to update deposit status: %v", err)
		} else {
			logger.Info("Successfully updated deposit status for ID %s to %s",
				job.Deposit.DepositID.String(), job.Deposit.Status)

			// Verify the update was successful by checking the database
			deposit, err := pools.service.db.GetDepositByID(job.Deposit.DepositID)
			if err != nil {
				logger.Error("Failed to verify deposit status update: %v", err)
			} else if deposit == nil {
				logger.Error("Failed to verify deposit status update: deposit not found")
			} else {
				logger.Info("Verified deposit status for ID %s: current status is %s",
					job.Deposit.DepositID.String(), deposit.Status)
			}
		}

	case JobCreateDistribution:
		if err := pools.service.db.CreateDistribution(job.Distribution); err != nil {
			logger.Error("Failed to create distribution record: %v", err)
		}

	case JobUpdateDistributionStatus:
		if err := pools.service.db.UpdateDistributionStatus(job.Distribution.DepositID, job.Distribution.Status, job.Distribution.MonadTxHash); err != nil {
			logger.Error("Failed to update distribution status: %v", err)
		}

	case "update_transaction_history":
		// Update the transaction_history table with the Monad tx hash and MON amount
		depositID := job.Distribution.DepositID
		depositIDStr := depositID.String()
		txHash := job.Distribution.MonadTxHash

		logger.Info("Processing update_transaction_history for deposit ID %s with tx hash %s",
			depositIDStr, txHash)

		// Use UpdateTransactionStatus which will find the MON amount in the distributions table
		if err := pools.service.UpdateTransactionStatus(ctx, depositID, database.StatusCompleted, txHash); err != nil {
			logger.Error("Failed to update transaction history for deposit ID %s: %v", depositIDStr, err)
		} else {
			logger.Info("Successfully updated transaction history for deposit ID %s with tx hash %s",
				depositIDStr, txHash)
		}

	default:
		logger.Error("Unknown database job type: %s", job.JobType)
	}
}
