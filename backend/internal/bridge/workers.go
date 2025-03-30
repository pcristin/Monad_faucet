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
	"github.com/pcristin/monad-faucet/internal/blockchain/listener"
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
			logger.Debug("[%s] Worker %d started", pool.name, workerID)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			for {
				select {
				case job, ok := <-pool.jobChan:
					if !ok {
						logger.Debug("[%s] Worker %d stopping: job channel closed", pool.name, workerID)
						return
					}
					processFunc(ctx, job)
				case <-pool.quit:
					logger.Debug("[%s] Worker %d stopping: quit signal received", pool.name, workerID)
					return
				}
			}
		}()
	}
}

// Stop stops the worker pool
func (pool *WorkerPool) Stop() {
	logger.Debug("[%s] Stopping worker pool", pool.name)
	close(pool.quit)
	pool.wg.Wait()
	logger.Debug("[%s] Worker pool stopped", pool.name)
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
	Event listener.DepositEvent
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
	txHash        string
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
	BulkData     *BulkDBData // New field for bulk operations
}

// BulkDBData contains collections of records for bulk operations
type BulkDBData struct {
	Distributions []*database.Distribution
	Deposits      []*database.Deposit
}

// JobTypes for database worker
const (
	JobCreateDeposit            = "create_deposit"
	JobUpdateDepositStatus      = "update_deposit_status"
	JobCreateDistribution       = "create_distribution"
	JobUpdateDistributionStatus = "update_distribution_status"
	JobBulkUpdateDistributions  = "bulk_update_distributions"
	JobBulkUpdateDeposits       = "bulk_update_deposits"
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
	batchStartTime    time.Time // Tracks when the current batch started forming
	mergeDelay        time.Duration
	maxDeposits       int
}

// NewBridgeWorkerPools creates a new set of worker pools
func NewBridgeWorkerPools(service *BridgeService) *BridgeWorkerPools {
	return &BridgeWorkerPools{
		DepositPool:        NewWorkerPool("deposit", 1, 500),
		CalculationPool:    NewWorkerPool("calculation", 1, 500),
		DistributionPool:   NewWorkerPool("distribution", 1, 500),
		DBPool:             NewWorkerPool("database", 1, 500),
		calculationChannel: make(chan CalculatedDeposit, 500),
		service:            service,
		processingDeposits: make(map[string]bool),
		distributionBatch:  make([]*DistributionJob, 0, 100),
		mergeDelay:         20 * time.Second, // Very short delay to encourage faster batching
		maxDeposits:        200,              // Small batch size to encourage more frequent batches
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

// SubmitDepositEvent adds a deposit event to the deposit worker pool
func (pools *BridgeWorkerPools) SubmitDepositEvent(event listener.DepositEvent) {
	depositId := event.DepositId.String()
	startTime := time.Now()

	// Prevent duplicate processing
	pools.mu.Lock()
	if pools.processingDeposits[depositId] {
		logger.Info("Skipping duplicate deposit ID %s", depositId)
		pools.mu.Unlock()
		return
	}
	pools.processingDeposits[depositId] = true
	pools.mu.Unlock()

	logger.Info("PIPELINE-FLOW: Received deposit event ID %s in submission pipeline", depositId)

	// Create deposit job and submit to pool
	depositJob := &DepositJob{
		Event: event,
	}

	// Submit the job to the deposit worker pool
	submitStart := time.Now()
	pools.DepositPool.Submit(depositJob)
	logger.Info("TIMING: SubmitDepositEvent to DepositPool for ID %s took %v (total handling: %v)",
		depositId, time.Since(submitStart), time.Since(startTime))
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
	startTime := time.Now()
	defer func() {
		logger.Info("TIMING: processDepositJob for ID %s took %v", job.Event.DepositId.String(), time.Since(startTime))
	}()

	depositId := job.Event.DepositId.String()
	logger.Info("PIPELINE-FLOW: Processing deposit job for ID %s", depositId)

	defer pools.finishProcessingDeposit(job.Event.DepositId)

	// 1. Check if this deposit has already been processed
	getDepositStart := time.Now()
	deposit, err := pools.service.db.GetDepositByID(job.Event.DepositId)
	logger.Info("TIMING: GetDepositByID for ID %s took %v", job.Event.DepositId.String(), time.Since(getDepositStart))

	if err == nil && deposit != nil && deposit.Status == database.StatusProcessed {
		logger.Info("Deposit ID %s already processed", job.Event.DepositId.String())
		return
	}

	// 2. Create deposit record if it doesn't exist yet
	if err != nil || deposit == nil {
		logger.Info("Creating new deposit record for deposit ID %s", depositId)

		// Extract metadata from the event if available
		metadata := job.Event.Metadata
		if metadata == "" {
			metadata = "No metadata provided"
		}

		// Create new deposit
		depositCreateStart := time.Now()
		deposit = &database.Deposit{
			DepositID:     job.Event.DepositId,
			WalletAddress: job.Event.Depositor,
			Amount:        job.Event.Amount,
			Currency:      database.CurrencyType(job.Event.Currency),
			BlockNumber:   job.Event.BlockNumber,
			Status:        database.StatusPending,
			TxHash:        job.Event.TxHash,
			Metadata:      metadata,
		}

		// Submit to database pool
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobCreateDeposit,
			Deposit: deposit,
		})
		logger.Info("TIMING: Creating deposit record and submitting to DB pool for ID %s took %v",
			depositId, time.Since(depositCreateStart))
	} else if deposit.Status == database.StatusFailed {
		logger.Warn("Failed deposit being reprocessed: %s", depositId)
		// Update status to retry
		updateStart := time.Now()
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobUpdateDepositStatus,
			Deposit: &database.Deposit{
				DepositID: job.Event.DepositId,
				Status:    database.StatusPending,
			},
		})
		logger.Info("TIMING: Updating previously failed deposit for ID %s took %v",
			depositId, time.Since(updateStart))
	}

	// 3. Get the current contract state for validation
	stateStart := time.Now()
	state, err := pools.service.GetState(ctx)
	if err != nil {
		logger.Error("Failed to get contract state: %v", err)
		return
	}
	logger.Info("TIMING: GetState for ID %s took %v", depositId, time.Since(stateStart))

	// 4. Submit calculation job
	calcStart := time.Now()
	pools.CalculationPool.Submit(&CalculationJob{
		Deposit:     deposit,
		State:       state,
		DepositChan: pools.calculationChannel,
	})
	logger.Info("PIPELINE-FLOW: Deposit ID %s submitted to calculation pool at %v",
		depositId, time.Now().Format(time.RFC3339))
	logger.Info("TIMING: Submitting to CalculationPool for ID %s took %v",
		depositId, time.Since(calcStart))
}

// processCalculationJob calculates MON amount for a deposit
func (pools *BridgeWorkerPools) processCalculationJob(ctx context.Context, job *CalculationJob) {
	startTime := time.Now()
	defer func() {
		logger.Info("TIMING: processCalculationJob for ID %s took %v", job.Deposit.DepositID.String(), time.Since(startTime))
	}()

	// Calculate MON amount based on deposit amount and exchange rate
	calcStart := time.Now()
	monAmount := calculateMonAmount(job.Deposit.Amount, job.State.SwapRatios[blockchain.CurrencyType(job.Deposit.Currency)], blockchain.CurrencyType(job.Deposit.Currency))
	logger.Info("TIMING: calculateMonAmount for ID %s took %v", job.Deposit.DepositID.String(), time.Since(calcStart))

	// Validate the deposit and amount
	validateStart := time.Now()
	err := pools.service.validateDepositWithAmount(job.State, monAmount)
	logger.Info("TIMING: validateDepositWithAmount for ID %s took %v", job.Deposit.DepositID.String(), time.Since(validateStart))

	if err != nil {
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
	channelStart := time.Now()
	select {
	case job.DepositChan <- CalculatedDeposit{
		Deposit:   job.Deposit,
		MonAmount: monAmount,
	}:
		logger.Info("TIMING: Sending to calculationChannel for ID %s took %v", job.Deposit.DepositID.String(), time.Since(channelStart))
	case <-ctx.Done():
		logger.Error("Context cancelled while submitting calculated deposit")
	}
}

// consumeCalculationResults consumes calculation results and forwards to distribution pool
func (pools *BridgeWorkerPools) consumeCalculationResults() {
	batchCount := 0
	batchStartTime := time.Now()
	lastConsumeTime := time.Time{}

	for calcResult := range pools.calculationChannel {
		processStart := time.Now()
		batchCount++

		deposit := calcResult.Deposit
		monAmount := calcResult.MonAmount

		depositIDStr := deposit.DepositID.String()

		// Log the timing and inflow rate to the batch system
		if !lastConsumeTime.IsZero() {
			timeSinceLast := time.Since(lastConsumeTime)
			logger.Info("PIPELINE-FLOW: Calculation result for ID %s received after %v from previous result",
				depositIDStr, timeSinceLast)

			if timeSinceLast > 5*time.Second {
				logger.Warn("PIPELINE-FLOW: Long gap of %v between calculation results! Previous: %s, Current: %s",
					timeSinceLast, lastConsumeTime.Format(time.RFC3339), time.Now().Format(time.RFC3339))
			}
		} else {
			logger.Info("PIPELINE-FLOW: First calculation result received at %v",
				time.Now().Format(time.RFC3339))
		}
		lastConsumeTime = time.Now()

		logger.Info("Processing calculation result for deposit ID %s with MON amount %s",
			depositIDStr, formatMonAmount(monAmount))

		// First check if deposit exists and is valid
		dbCheckStart := time.Now()
		existingDeposit, err := pools.service.db.GetDepositByID(deposit.DepositID)
		logger.Info("TIMING: GetDepositByID in consumeCalculationResults for ID %s took %v", depositIDStr, time.Since(dbCheckStart))

		if err != nil || existingDeposit == nil {
			logger.Error("Failed to verify deposit %s exists: %v", depositIDStr, err)
			logger.Info("Attempting to recreate deposit record for ID %s", depositIDStr)

			// Try to create/update the deposit record directly
			createStart := time.Now()
			if err := pools.service.db.CreateDeposit(deposit); err != nil {
				logger.Error("Failed to create/update deposit record directly: %v", err)
				// Even if this fails, we'll still continue with distribution creation
			} else {
				logger.Info("Successfully created/updated deposit record for ID %s", depositIDStr)
			}
			logger.Info("TIMING: CreateDeposit in consumeCalculationResults for ID %s took %v", depositIDStr, time.Since(createStart))
		}

		// Check if distribution already exists to avoid duplicate creation
		distCheckStart := time.Now()
		existingDist, err := pools.service.db.GetDistributionByDepositID(deposit.DepositID)
		logger.Info("TIMING: GetDistributionByDepositID for ID %s took %v", depositIDStr, time.Since(distCheckStart))

		if err == nil && existingDist != nil {
			logger.Info("Distribution record already exists for deposit ID %s, updating if needed", depositIDStr)

			// Update distribution with latest data if needed
			if existingDist.Status != database.DistStatusCompleted ||
				existingDist.MonadTxHash == "" ||
				(existingDist.MonAmount == nil && monAmount != nil) {

				// Use the better MON amount between existing and new
				distMonAmount := monAmount
				if existingDist.MonAmount != nil && existingDist.MonAmount.Cmp(big.NewInt(0)) > 0 {
					distMonAmount = existingDist.MonAmount
				}

				distribution := &database.Distribution{
					DepositID:     deposit.DepositID,
					WalletAddress: deposit.WalletAddress,
					MonAmount:     distMonAmount,
					Status:        existingDist.Status,
					MonadTxHash:   existingDist.MonadTxHash,
				}

				// Submit to database pool to update distribution record
				pools.DBPool.Submit(&DBWorkerJob{
					JobType:      JobUpdateDistributionStatus,
					Distribution: distribution,
				})

				// Create distribution job only if it's not completed yet
				if existingDist.Status != database.DistStatusCompleted {
					distJob := &DistributionJob{
						DepositID:     deposit.DepositID,
						WalletAddress: deposit.WalletAddress,
						MonAmount:     distMonAmount,
					}

					// Add to batch or process individually
					batchAddStart := time.Now()
					pools.addToDistributionBatch(distJob)
					logger.Info("TIMING: addToDistributionBatch for ID %s took %v", depositIDStr, time.Since(batchAddStart))
				}
			}
		} else {
			// Create new distribution record
			logger.Info("Creating new distribution record for deposit ID %s", depositIDStr)

			// CRITICAL: Ensure monAmount is valid before proceeding
			if monAmount == nil || monAmount.Cmp(big.NewInt(0)) <= 0 {
				logger.Error("Invalid MON amount for deposit ID %s (nil or zero)", depositIDStr)

				// Try to calculate a minimum amount if deposit amount is available
				var safeMonAmount *big.Int
				if deposit.Amount != nil && deposit.Amount.Cmp(big.NewInt(0)) > 0 {
					// Use 10x deposit amount as a safe default (matches typical calculation)
					safeMonAmount = new(big.Int).Mul(deposit.Amount, big.NewInt(10))
					logger.Warn("Calculated fallback MON amount %s for deposit ID %s",
						safeMonAmount.String(), depositIDStr)
				} else {
					// Absolute minimum fallback (0.001 MON)
					safeMonAmount = big.NewInt(1000000000000000)
					logger.Warn("Using minimum MON amount %s for deposit ID %s",
						safeMonAmount.String(), depositIDStr)
				}
				monAmount = safeMonAmount
			}

			// Now that we have a guaranteed valid monAmount, proceed with distribution creation
			logger.Info("Using MON amount %s for distribution record of deposit ID %s",
				monAmount.String(), depositIDStr)

			distCreateStart := time.Now()
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
			logger.Info("TIMING: Creating and submitting distribution job for ID %s took %v", depositIDStr, time.Since(distCreateStart))

			// Create distribution job
			jobCreateStart := time.Now()
			distJob := &DistributionJob{
				DepositID:     deposit.DepositID,
				WalletAddress: deposit.WalletAddress,
				MonAmount:     monAmount,
			}

			// Add to batch or process individually
			batchAddStart := time.Now()
			pools.addToDistributionBatch(distJob)
			logger.Info("TIMING: addToDistributionBatch for ID %s took %v", depositIDStr, time.Since(batchAddStart))
			logger.Info("TIMING: Total job creation and batch addition for ID %s took %v", depositIDStr, time.Since(jobCreateStart))
		}

		logger.Info("TIMING: Total consumeCalculationResults processing for ID %s took %v", depositIDStr, time.Since(processStart))
		logger.Info("PIPELINE-FLOW: Calculation result for ID %s added to batch at %v",
			depositIDStr, time.Now().Format(time.RFC3339))

		// Log throughput stats every 5 transactions or 10 seconds
		if batchCount%5 == 0 || time.Since(batchStartTime) > 10*time.Second {
			elapsed := time.Since(batchStartTime)
			rate := float64(batchCount) / elapsed.Seconds()
			logger.Info("THROUGHPUT: Processed %d deposits in %v (%.2f deposits/sec)",
				batchCount, elapsed, rate)

			// Reset counters
			batchCount = 0
			batchStartTime = time.Now()
		}
	}
}

// addToDistributionBatch adds a distribution job to the current batch
func (pools *BridgeWorkerPools) addToDistributionBatch(job *DistributionJob) {
	lockStart := time.Now()
	pools.batchMutex.Lock()
	lockTime := time.Since(lockStart)
	defer pools.batchMutex.Unlock()

	depositIDStr := job.DepositID.String()
	logger.Info("BATCH-TRACKING: Adding deposit ID %s to distribution batch (lock acquisition took %v)",
		depositIDStr, lockTime)

	// Add job to the batch
	currentBatchSize := len(pools.distributionBatch)
	isFirstItem := currentBatchSize == 0

	pools.distributionBatch = append(pools.distributionBatch, job)
	currentBatchSize = len(pools.distributionBatch)

	// If this is the first job in the batch, start the batch timer and record start time
	if isFirstItem {
		pools.batchStartTime = time.Now()
		timerStart := time.Now()

		// Stop existing timer if any
		if pools.batchTimer != nil {
			pools.batchTimer.Stop()
		}

		logger.Info("BATCH-TRACKING: Starting batch timer with %d second delay for deposit ID %s",
			int(pools.mergeDelay.Seconds()), depositIDStr)

		// Start new timer
		pools.batchTimer = time.AfterFunc(pools.mergeDelay, func() {
			pools.processBatchOnTimeout()
		})
		logger.Info("TIMING: Timer setup for batch took %v", time.Since(timerStart))
	}

	batchAge := time.Since(pools.batchStartTime)
	ratePerSecond := float64(currentBatchSize) / batchAge.Seconds()

	logger.Info("BATCH-TRACKING: Added distribution job for deposit ID %s to batch (current batch size: %d/%d, batch age: %v, rate: %.2f/sec)",
		depositIDStr, currentBatchSize, pools.maxDeposits, batchAge, ratePerSecond)

	// Check if batch is full
	if currentBatchSize >= pools.maxDeposits {
		submitStart := time.Now()
		logger.Info("BATCH-TRACKING: Batch is full (%d/%d distributions), submitting now (batch formed in %v at %.2f items/sec)",
			currentBatchSize, pools.maxDeposits, batchAge, ratePerSecond)
		pools.submitBatch()
		logger.Info("TIMING: submitBatch call took %v", time.Since(submitStart))
	}
}

// processBatchOnTimeout processes the current batch due to timeout
func (pools *BridgeWorkerPools) processBatchOnTimeout() {
	startTime := time.Now()
	logger.Info("TIMING: processBatchOnTimeout called by timer after %d seconds",
		int(pools.mergeDelay.Seconds()))

	lockStart := time.Now()
	pools.batchMutex.Lock()
	logger.Info("TIMING: Lock acquisition in processBatchOnTimeout took %v", time.Since(lockStart))
	defer pools.batchMutex.Unlock()

	// Only process if there are jobs in the batch
	currentBatchSize := len(pools.distributionBatch)
	if currentBatchSize > 0 {
		batchAge := time.Since(pools.batchStartTime)
		ratePerSecond := float64(currentBatchSize) / batchAge.Seconds()

		logger.Info("BATCH-TRACKING: Processing distribution batch of %d deposits due to timeout "+
			"(merge delay of %d seconds reached, actual batch age: %v, rate: %.2f items/sec)",
			currentBatchSize, int(pools.mergeDelay.Seconds()), batchAge, ratePerSecond)

		// Reset timer to nil before submitting to avoid race conditions
		pools.batchTimer = nil

		// Create a copy of the current batch
		copyStart := time.Now()
		batchCopy := make([]*DistributionJob, currentBatchSize)
		copy(batchCopy, pools.distributionBatch)
		logger.Info("TIMING: Batch copy operation for %d items took %v", currentBatchSize, time.Since(copyStart))

		batchId := fmt.Sprintf("batch-%d", time.Now().Unix())

		// Get deposit IDs for logging
		var depositIDs []string
		for _, job := range batchCopy {
			depositIDs = append(depositIDs, job.DepositID.String())
		}
		depositIDsStr := strings.Join(depositIDs, ", ")

		// Clear the batch
		pools.distributionBatch = make([]*DistributionJob, 0, pools.maxDeposits)

		// Submit batch job (outside the lock using goroutine)
		go func() {
			logger.Info("BATCH-TRACKING: Submitting batch %s with %d distributions: [%s]",
				batchId, currentBatchSize, depositIDsStr)

			submitStart := time.Now()
			pools.DistributionPool.Submit(&BatchDistributionJob{
				Distributions: batchCopy,
			})
			logger.Info("TIMING: DistributionPool.Submit in processBatchOnTimeout took %v",
				time.Since(submitStart))
		}()
	} else {
		logger.Info("BATCH-TRACKING: Batch timer fired but no distributions to process")
		pools.batchTimer = nil
	}

	logger.Info("TIMING: Total processBatchOnTimeout took %v", time.Since(startTime))
}

// submitBatch submits the current batch for processing
func (pools *BridgeWorkerPools) submitBatch() {
	// Lock is already held by the caller (addToDistributionBatch or processBatchOnTimeout)

	// Stop timer if active
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

	// Submit batch job (outside the lock using goroutine)
	go func() {
		logger.Info("BATCH-TRACKING: Submitting batch %s with %d distributions: [%s]",
			batchId, batchCount, depositIDsStr)

		pools.DistributionPool.Submit(&BatchDistributionJob{
			Distributions: batchCopy,
		})
	}()
}

// processBatchMint attempts to process multiple distributions in a single transaction
// Returns true if the batch was attempted (even if some distributions failed)
// Also returns any failed distribution jobs that should be retried individually
func (pools *BridgeWorkerPools) processBatchMint(ctx context.Context, distributions []*DistributionJob) (bool, []*DistributionJob) {
	startTime := time.Now()
	defer func() {
		logger.Info("TIMING: Total processBatchMint for %d distributions took %v",
			len(distributions), time.Since(startTime))
	}()

	if len(distributions) == 0 {
		return false, nil
	}

	// Prepare arrays for batch mint call
	prepStart := time.Now()
	addresses := make([]common.Address, len(distributions))
	amounts := make([]*big.Int, len(distributions))
	depositIDs := make([]*big.Int, len(distributions))

	for i, dist := range distributions {
		addresses[i] = dist.WalletAddress
		amounts[i] = dist.MonAmount
		depositIDs[i] = dist.DepositID
	}
	logger.Info("TIMING: Array preparation for batch mint took %v", time.Since(prepStart))

	// Log the batch mint attempt
	logger.Info("Attempting batch mint of %d distributions", len(distributions))

	// Collections for skipped/already processed distributions
	alreadyProcessed := make(map[string]string) // depositID -> txHash
	var distributionsToUpdate []*database.Distribution
	var depositsToUpdate []*database.Deposit

	// Check if any distributions are already completed
	checkStart := time.Now()
	for i, dist := range distributions {
		// Check blockchain for existing transaction
		bcStart := time.Now()
		txHash, err := pools.service.checkMonadBlockchainForTransaction(ctx, dist.DepositID)
		logger.Info("TIMING: blockchain check for deposit ID %s took %v",
			dist.DepositID.String(), time.Since(bcStart))

		if err == nil && txHash != "" {
			depositIDStr := dist.DepositID.String()
			logger.Info("Distribution for deposit ID %s already exists on blockchain with tx %s",
				depositIDStr, txHash)

			// Add to bulk update collections
			distributionsToUpdate = append(distributionsToUpdate, &database.Distribution{
				DepositID:     dist.DepositID,
				Status:        database.DistStatusCompleted,
				MonadTxHash:   txHash,
				MonAmount:     dist.MonAmount,
				WalletAddress: dist.WalletAddress,
			})

			depositsToUpdate = append(depositsToUpdate, &database.Deposit{
				DepositID: dist.DepositID,
				Status:    database.StatusProcessed,
			})

			// Store in map for tracking
			alreadyProcessed[depositIDStr] = txHash

			// Remove this distribution from the batch
			// We do this by setting the address to zero address and amount to zero
			// The distributor contract should skip these
			addresses[i] = common.Address{}
			amounts[i] = big.NewInt(0)
		}
	}
	logger.Info("TIMING: Checking existing transactions for %d distributions took %v",
		len(distributions), time.Since(checkStart))

	// If we found already processed distributions, update them in bulk
	if len(distributionsToUpdate) > 0 {
		dbStart := time.Now()
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobBulkUpdateDistributions,
			BulkData: &BulkDBData{
				Distributions: distributionsToUpdate,
			},
		})

		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobBulkUpdateDeposits,
			BulkData: &BulkDBData{
				Deposits: depositsToUpdate,
			},
		})
		logger.Info("TIMING: Database job submission for %d already processed distributions took %v",
			len(distributionsToUpdate), time.Since(dbStart))

		logger.Info("Submitted bulk update for %d already processed distributions",
			len(distributionsToUpdate))
	}

	// Attempt to do a batch mint for all distributions at once using individual mintTokens
	// This is a fallback approach since we don't have batchMintTokens implemented yet
	mintStart := time.Now()
	txHash, err := pools.service.mintTokensBatch(ctx, addresses, amounts, depositIDs)
	logger.Info("TIMING: mintTokensBatch blockchain call took %v", time.Since(mintStart))

	if err != nil {
		logger.Error("Batch mint transaction failed: %v", err)
		return false, nil // Complete failure, retry all individually
	}

	// Process successful batch mint
	processStart := time.Now()
	distributionsToUpdate = nil
	depositsToUpdate = nil

	for i, dist := range distributions {
		// Skip distributions with zero amount (already processed) or those in our already processed map
		depositIDStr := dist.DepositID.String()
		if amounts[i].Cmp(big.NewInt(0)) <= 0 || alreadyProcessed[depositIDStr] != "" {
			continue
		}

		// Assign the transaction hash to the dist job for later use in database updates
		dist.txHash = txHash

		// This distribution succeeded in the batch
		logger.Info("Successfully minted %s MON for deposit ID %s in batch tx %s",
			formatMonAmount(dist.MonAmount), depositIDStr, txHash)

		// CRITICAL: First check if distribution record exists, create if it doesn't
		existingDist, distErr := pools.service.db.GetDistributionByDepositID(dist.DepositID)
		if distErr != nil || existingDist == nil {
			logger.Info("Creating initial distribution record for deposit ID %s before updating status", depositIDStr)

			// Create the distribution record directly to ensure it exists
			initialDist := &database.Distribution{
				DepositID:     dist.DepositID,
				WalletAddress: dist.WalletAddress,
				MonAmount:     dist.MonAmount,
				Status:        database.DistStatusCompleted,
				MonadTxHash:   txHash,
			}

			if err := pools.service.db.CreateDistribution(initialDist); err != nil {
				logger.Error("Failed to create initial distribution record for deposit ID %s: %v", depositIDStr, err)
			} else {
				logger.Info("Successfully created distribution record for deposit ID %s", depositIDStr)
			}
		} else {
			// Add to bulk update collections
			distributionsToUpdate = append(distributionsToUpdate, &database.Distribution{
				DepositID:     dist.DepositID,
				Status:        database.DistStatusCompleted,
				MonadTxHash:   txHash,
				MonAmount:     dist.MonAmount,
				WalletAddress: dist.WalletAddress,
			})

			depositsToUpdate = append(depositsToUpdate, &database.Deposit{
				DepositID: dist.DepositID,
				Status:    database.StatusProcessed,
			})
		}

		// Create a custom DB job to update transaction_history table with the MON amount
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

	// Submit bulk updates if any
	if len(distributionsToUpdate) > 0 {
		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobBulkUpdateDistributions,
			BulkData: &BulkDBData{
				Distributions: distributionsToUpdate,
			},
		})

		pools.DBPool.Submit(&DBWorkerJob{
			JobType: JobBulkUpdateDeposits,
			BulkData: &BulkDBData{
				Deposits: depositsToUpdate,
			},
		})

		logger.Info("Submitted bulk update for %d newly processed distributions",
			len(distributionsToUpdate))
	}
	logger.Info("TIMING: Post-processing for successful batch mint took %v", time.Since(processStart))

	return true, nil // All processed successfully in batch
}

// processBatchDistributionJob handles a batch of distributions
func (pools *BridgeWorkerPools) processBatchDistributionJob(ctx context.Context, batch *BatchDistributionJob) {
	startTime := time.Now()
	defer func() {
		logger.Info("TIMING: Total processBatchDistributionJob for %d distributions took %v",
			len(batch.Distributions), time.Since(startTime))
	}()

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
	batchMintStart := time.Now()
	success, failedJobs := pools.processBatchMint(ctx, batch.Distributions)
	logger.Info("TIMING: processBatchMint call took %v", time.Since(batchMintStart))

	// If batch processing was completely successful, we're done
	if success && len(failedJobs) == 0 {
		logger.Info("[Batch %s] Successfully processed all %d distributions in batch", batchID, len(batch.Distributions))

		// Get the transaction hash from the first distribution (all should have the same)
		var txHash string
		if len(batch.Distributions) > 0 && batch.Distributions[0].txHash != "" {
			txHash = batch.Distributions[0].txHash
		}

		// Prepare bulk updates
		var distributions []*database.Distribution
		var deposits []*database.Deposit

		for _, dist := range batch.Distributions {
			// Skip distributions with zero amount (already processed)
			if dist.MonAmount.Cmp(big.NewInt(0)) <= 0 {
				continue
			}

			// Add to bulk update collections
			distributions = append(distributions, &database.Distribution{
				DepositID:     dist.DepositID,
				Status:        database.DistStatusCompleted,
				MonadTxHash:   txHash, // Use the transaction hash from the batch
				MonAmount:     dist.MonAmount,
				WalletAddress: dist.WalletAddress,
			})

			deposits = append(deposits, &database.Deposit{
				DepositID: dist.DepositID,
				Status:    database.StatusProcessed,
			})
		}

		// Use bulk operations if we have data
		if len(distributions) > 0 {
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobBulkUpdateDistributions,
				BulkData: &BulkDBData{
					Distributions: distributions,
				},
			})
			logger.Info("[Batch %s] Submitted bulk update for %d distributions",
				batchID, len(distributions))
		}

		if len(deposits) > 0 {
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: JobBulkUpdateDeposits,
				BulkData: &BulkDBData{
					Deposits: deposits,
				},
			})
			logger.Info("[Batch %s] Submitted bulk update for %d deposits",
				batchID, len(deposits))
		}

		// Individually update transaction history for better error tracking
		for _, dist := range batch.Distributions {
			// Skip distributions with zero amount
			if dist.MonAmount.Cmp(big.NewInt(0)) <= 0 {
				continue
			}

			// Update transaction history
			pools.DBPool.Submit(&DBWorkerJob{
				JobType: "update_transaction_history",
				Distribution: &database.Distribution{
					DepositID:   dist.DepositID,
					Status:      database.DistStatusCompleted,
					MonadTxHash: txHash,
					MonAmount:   dist.MonAmount,
				},
			})
		}

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

	// CRITICAL: First check if distribution record exists, create if it doesn't
	existingDist, distErr := pools.service.db.GetDistributionByDepositID(job.DepositID)
	if distErr != nil || existingDist == nil {
		logger.Info("Creating initial distribution record for deposit ID %s before updating status", depositIDStr)

		// Create the distribution record directly to ensure it exists
		initialDist := &database.Distribution{
			DepositID:     job.DepositID,
			WalletAddress: job.WalletAddress,
			MonAmount:     job.MonAmount,
			Status:        database.DistStatusCompleted,
			MonadTxHash:   txHash,
		}

		if err := pools.service.db.CreateDistribution(initialDist); err != nil {
			logger.Error("Failed to create initial distribution record for deposit ID %s: %v", depositIDStr, err)
			// Continue with update anyway - it might succeed if the record was created by another worker
		} else {
			logger.Info("Successfully created distribution record for deposit ID %s", depositIDStr)
		}
	}

	// Now submit the update job to the worker pool
	pools.DBPool.Submit(&DBWorkerJob{
		JobType: JobUpdateDistributionStatus,
		Distribution: &database.Distribution{
			DepositID:   job.DepositID,
			Status:      database.DistStatusCompleted,
			MonadTxHash: txHash,
			MonAmount:   job.MonAmount, // Include MON amount for updating distribution
		},
	})

	pools.DBPool.Submit(&DBWorkerJob{
		JobType: JobUpdateDepositStatus,
		Deposit: &database.Deposit{
			DepositID: job.DepositID,
			Status:    database.StatusProcessed,
		},
	})

	// Create a custom DB job to update transaction_history table with the MON amount
	txHistoryJob := &DBWorkerJob{
		JobType: "update_transaction_history",
		Distribution: &database.Distribution{
			DepositID:   job.DepositID,
			Status:      database.DistStatusCompleted,
			MonadTxHash: txHash,
			MonAmount:   job.MonAmount, // Include the MON amount for the transaction history
		},
	}

	pools.DBPool.Submit(txHistoryJob)
	logger.Info("Queued transaction history update for deposit ID %s with tx hash %s and MON amount %s",
		depositIDStr, txHash, formatMonAmount(job.MonAmount))
}

// processDBJob processes database operations
func (pools *BridgeWorkerPools) processDBJob(ctx context.Context, job *DBWorkerJob) {
	startTime := time.Now()
	defer func() {
		jobIdentifier := "unknown"
		if job.Deposit != nil && job.Deposit.DepositID != nil {
			jobIdentifier = job.Deposit.DepositID.String()
		} else if job.Distribution != nil && job.Distribution.DepositID != nil {
			jobIdentifier = job.Distribution.DepositID.String()
		}
		logger.Info("TIMING: processDBJob %s for ID %s took %v",
			job.JobType, jobIdentifier, time.Since(startTime))
	}()

	switch job.JobType {
	case JobCreateDeposit:
		opStart := time.Now()
		if err := pools.service.db.CreateDeposit(job.Deposit); err != nil {
			logger.Error("Failed to create deposit record: %v", err)
		}
		logger.Info("TIMING: CreateDeposit DB operation for ID %s took %v",
			job.Deposit.DepositID.String(), time.Since(opStart))

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
		depositID := job.Distribution.DepositID
		depositIDStr := depositID.String()
		logger.Info("Processing JobCreateDistribution for deposit ID %s", depositIDStr)

		// CRITICAL: Directly verify MonAmount is not nil before proceeding
		if job.Distribution.MonAmount == nil {
			logger.Error("MonAmount is nil for deposit ID %s, cannot create distribution record", depositIDStr)

			// Try to retrieve MonAmount from transaction history or use default minimum
			tx, txErr := pools.service.db.GetTransactionByDepositID(depositID)
			if txErr == nil && tx != nil && tx.MonAmount != nil && tx.MonAmount.Cmp(big.NewInt(0)) > 0 {
				// Use MON amount from transaction_history
				logger.Info("Retrieved MonAmount %s from transaction_history for deposit ID %s",
					tx.MonAmount.String(), depositIDStr)
				job.Distribution.MonAmount = tx.MonAmount
			} else {
				// Fall back to a safe minimum value (1000000000000000 = 0.001 MON)
				job.Distribution.MonAmount = big.NewInt(1000000000000000)
				logger.Warn("Using fallback minimum MonAmount for deposit ID %s: %s",
					depositIDStr, job.Distribution.MonAmount.String())
			}
		}

		// Check if the distribution record already exists
		existingDist, err := pools.service.db.GetDistributionByDepositID(depositID)
		if err == nil && existingDist != nil {
			logger.Info("Distribution record already exists for deposit ID %s, updating with new data", depositIDStr)

			// Prepare the update data
			updateNeeded := false
			newStatus := job.Distribution.Status
			newMonAmount := job.Distribution.MonAmount
			newTxHash := job.Distribution.MonadTxHash

			// Only update fields that have values and differ from existing record
			if newStatus != "" && newStatus != existingDist.Status {
				updateNeeded = true
			}
			if newMonAmount != nil && (existingDist.MonAmount == nil || newMonAmount.Cmp(existingDist.MonAmount) != 0) {
				updateNeeded = true
			}
			if newTxHash != "" && newTxHash != existingDist.MonadTxHash {
				updateNeeded = true
			}

			// Update if needed
			if updateNeeded {
				// If we have a tx hash, use it, otherwise keep existing
				txHashToUse := existingDist.MonadTxHash
				if newTxHash != "" {
					txHashToUse = newTxHash
				}

				// If we have a status, use it, otherwise keep existing
				statusToUse := existingDist.Status
				if newStatus != "" {
					statusToUse = newStatus
				}

				// Use updateDistributionWithAmount instead if we have a MON amount
				if newMonAmount != nil && newMonAmount.Cmp(big.NewInt(0)) > 0 {
					if err := pools.service.db.UpdateDistributionWithAmount(
						depositID, statusToUse, txHashToUse, newMonAmount); err != nil {
						logger.Error("Failed to update existing distribution record with amount: %v", err)
					} else {
						logger.Info("Successfully updated distribution record with MON amount for deposit ID %s",
							depositIDStr)
					}
				} else {
					// Fall back to standard update without MON amount
					if err := pools.service.db.UpdateDistributionStatus(depositID, statusToUse, txHashToUse); err != nil {
						logger.Error("Failed to update existing distribution record: %v", err)
					} else {
						logger.Info("Successfully updated distribution record for deposit ID %s: status=%s, txHash=%s",
							depositIDStr, statusToUse, txHashToUse)
					}
				}
			} else {
				logger.Info("No changes needed for distribution record %s", depositIDStr)
			}
		} else {
			// Try to create new distribution record
			logger.Info("Creating new distribution record for deposit ID %s with MON amount %s",
				depositIDStr, job.Distribution.MonAmount.String())

			if err := pools.service.db.CreateDistribution(job.Distribution); err != nil {
				logger.Error("Failed to create distribution record: %v", err)

				// If creation failed, try getting distribution again and update if it exists
				// (it might have been created by another worker in the meantime)
				retryDist, retryErr := pools.service.db.GetDistributionByDepositID(depositID)
				if retryErr == nil && retryDist != nil {
					logger.Info("Distribution record now exists for deposit ID %s, attempting update instead", depositIDStr)

					// Use the UpdateDistributionWithAmount to ensure MON amount is set
					if job.Distribution.MonAmount != nil && job.Distribution.MonAmount.Cmp(big.NewInt(0)) > 0 {
						if updateErr := pools.service.db.UpdateDistributionWithAmount(
							depositID, job.Distribution.Status, job.Distribution.MonadTxHash, job.Distribution.MonAmount); updateErr != nil {
							logger.Error("Also failed to update existing distribution record with amount: %v", updateErr)
						} else {
							logger.Info("Successfully updated distribution record with MON amount for deposit ID %s",
								depositIDStr)
						}
					} else {
						// Fall back to standard update without MON amount (shouldn't happen due to our earlier check)
						if updateErr := pools.service.db.UpdateDistributionStatus(
							depositID, job.Distribution.Status, job.Distribution.MonadTxHash); updateErr != nil {
							logger.Error("Also failed to update existing distribution record: %v", updateErr)
						} else {
							logger.Info("Successfully updated distribution record after failed creation for deposit ID %s",
								depositIDStr)
						}
					}
				}
			} else {
				logger.Info("Successfully created new distribution record for deposit ID %s with MON amount %s",
					depositIDStr, job.Distribution.MonAmount.String())
			}
		}

	case JobUpdateDistributionStatus:
		if job.Distribution.MonAmount != nil && job.Distribution.MonAmount.Cmp(big.NewInt(0)) > 0 {
			// Use the new method to update status, hash and amount in one operation
			if err := pools.service.db.UpdateDistributionWithAmount(
				job.Distribution.DepositID,
				job.Distribution.Status,
				job.Distribution.MonadTxHash,
				job.Distribution.MonAmount); err != nil {
				logger.Error("Failed to update distribution with amount: %v", err)
			} else {
				logger.Info("Updated distribution for ID %s with status %s, txHash %s, and MON amount %s",
					job.Distribution.DepositID.String(),
					job.Distribution.Status,
					job.Distribution.MonadTxHash,
					formatMonAmount(job.Distribution.MonAmount))
			}
		} else {
			// Fall back to the standard update method if we don't have a MON amount
			if err := pools.service.db.UpdateDistributionStatus(
				job.Distribution.DepositID,
				job.Distribution.Status,
				job.Distribution.MonadTxHash); err != nil {
				logger.Error("Failed to update distribution status: %v", err)
			}
		}

	case JobBulkUpdateDistributions:
		// Handle bulk update of distributions
		if job.BulkData == nil || len(job.BulkData.Distributions) == 0 {
			logger.Warn("Received empty bulk distributions update")
			return
		}

		startTime := time.Now()
		count := len(job.BulkData.Distributions)
		logger.Info("Processing bulk update of %d distributions", count)

		opStart := time.Now()
		if err := pools.service.db.BulkUpdateDistributions(job.BulkData.Distributions); err != nil {
			logger.Error("Failed to bulk update distributions: %v", err)
		} else {
			opTime := time.Since(opStart)
			totalTime := time.Since(startTime)
			opsPerSecond := float64(count) / opTime.Seconds()
			logger.Info("TIMING: BulkUpdateDistributions DB operation for %d items took %v (%0.2f items/sec)",
				count, opTime, opsPerSecond)
			logger.Info("Successfully bulk updated %d distributions in %v", count, totalTime)
		}

	case JobBulkUpdateDeposits:
		// Handle bulk update of deposits
		if job.BulkData == nil || len(job.BulkData.Deposits) == 0 {
			logger.Warn("Received empty bulk deposits update")
			return
		}

		startTime := time.Now()
		count := len(job.BulkData.Deposits)
		logger.Info("Processing bulk update of %d deposits", count)

		opStart := time.Now()
		if err := pools.service.db.BulkUpdateDeposits(job.BulkData.Deposits); err != nil {
			logger.Error("Failed to bulk update deposits: %v", err)
		} else {
			opTime := time.Since(opStart)
			totalTime := time.Since(startTime)
			opsPerSecond := float64(count) / opTime.Seconds()
			logger.Info("TIMING: BulkUpdateDeposits DB operation for %d items took %v (%0.2f items/sec)",
				count, opTime, opsPerSecond)
			logger.Info("Successfully bulk updated %d deposits in %v", count, totalTime)
		}

	case "update_transaction_history":
		// Update the transaction_history table with the Monad tx hash and MON amount
		depositID := job.Distribution.DepositID
		depositIDStr := depositID.String()
		txHash := job.Distribution.MonadTxHash

		logger.Info("Processing update_transaction_history for deposit ID %s with tx hash %s",
			depositIDStr, txHash)

		// Check if we have a MON amount directly in the job, which is preferred
		if job.Distribution.MonAmount != nil && job.Distribution.MonAmount.Cmp(big.NewInt(0)) > 0 {
			// Use direct update with the MON amount from the job
			logger.Info("Using MON amount %s from job for transaction history update",
				job.Distribution.MonAmount.String())

			if err := pools.service.db.UpdateTransactionWithMonAmount(
				depositID, database.StatusCompleted, txHash, job.Distribution.MonAmount); err != nil {
				logger.Error("Failed to update transaction history with MON amount for deposit ID %s: %v",
					depositIDStr, err)
			} else {
				logger.Info("Successfully updated transaction history for deposit ID %s with tx hash %s and MON amount %s",
					depositIDStr, txHash, job.Distribution.MonAmount.String())
			}
		} else {
			// Fall back to the original method which tries to find MON amount in distributions table
			logger.Warn("No MON amount in job for deposit ID %s, falling back to distribution table lookup",
				depositIDStr)

			if err := pools.service.UpdateTransactionStatus(ctx, depositID, database.StatusCompleted, txHash); err != nil {
				logger.Error("Failed to update transaction history for deposit ID %s: %v", depositIDStr, err)
			} else {
				logger.Info("Successfully updated transaction history for deposit ID %s with tx hash %s",
					depositIDStr, txHash)
			}
		}

	default:
		logger.Error("Unknown database job type: %s", job.JobType)
	}
}

// mintTokensBatch mints MON tokens for multiple recipients in a single transaction.
func (s *BridgeService) mintTokensBatch(ctx context.Context, recipients []common.Address, amounts []*big.Int, depositIds []*big.Int) (string, error) {
	startTime := time.Now()
	defer func() {
		logger.Info("TIMING: Total mintTokensBatch for %d recipients took %v",
			len(recipients), time.Since(startTime))
	}()

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
	prepStart := time.Now()
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
	logger.Info("TIMING: Transaction data preparation took %v", time.Since(prepStart))

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
	optsStart := time.Now()
	opts, err := s.monadDistributor.GetTransactOpts(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction options: %v", err)
	}
	logger.Info("TIMING: Getting transaction options took %v", time.Since(optsStart))

	// Execute the batch distribution transaction
	// The contract function expects an array of TransferData structs
	txStart := time.Now()
	tx, err := s.monadDistributor.TransactWithGasBuffer(opts, "distributeFunds", transfers)
	txTime := time.Since(txStart)
	logger.Info("TIMING: Transaction submission took %v", txTime)

	if err != nil {
		logger.Error("Failed to distribute funds in batch: %v", err)
		return "", fmt.Errorf("failed to distribute funds in batch: %v", err)
	}

	txHash := tx.Hash().Hex()
	logger.Info("Batch mint transaction submitted: %s", txHash)

	// Wait for transaction to be mined
	waitStart := time.Now()
	receipt, err := bind.WaitMined(ctx, s.monadDistributor.Client, tx)
	logger.Info("TIMING: Waiting for transaction confirmation took %v", time.Since(waitStart))

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
