package bridge

import (
	"context"
	"math/big"
	"strings"
	"sync"

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
}

// NewBridgeWorkerPools creates a new set of worker pools
func NewBridgeWorkerPools(service *BridgeService) *BridgeWorkerPools {
	return &BridgeWorkerPools{
		DepositPool:        NewWorkerPool("deposit", 5, 100),
		CalculationPool:    NewWorkerPool("calculation", 3, 50),
		DistributionPool:   NewWorkerPool("distribution", 5, 100),
		DBPool:             NewWorkerPool("database", 2, 200),
		calculationChannel: make(chan CalculatedDeposit, 50),
		service:            service,
		processingDeposits: make(map[string]bool),
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
		pools.processDistributionJob(ctx, job.(*DistributionJob))
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

// isProcessingDeposit checks if a deposit is already being processed
func (pools *BridgeWorkerPools) isProcessingDeposit(depositID *big.Int) bool {
	depositIDStr := depositID.String()
	pools.mu.Lock()
	defer pools.mu.Unlock()
	return pools.processingDeposits[depositIDStr]
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

	// 5. Wait for confirmations
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

		// Submit to distribution pool for processing
		pools.DistributionPool.Submit(&DistributionJob{
			DepositID:     deposit.DepositID,
			WalletAddress: deposit.WalletAddress,
			MonAmount:     monAmount,
		})
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
		if err := pools.service.db.UpdateDepositStatus(job.Deposit.DepositID, job.Deposit.Status); err != nil {
			logger.Error("Failed to update deposit status: %v", err)
		}

	case JobCreateDistribution:
		if err := pools.service.db.CreateDistribution(job.Distribution); err != nil {
			logger.Error("Failed to create distribution record: %v", err)
		}

	case JobUpdateDistributionStatus:
		if err := pools.service.db.UpdateDistributionStatus(job.Distribution.DepositID, job.Distribution.Status, job.Distribution.MonadTxHash); err != nil {
			logger.Error("Failed to update distribution status: %v", err)
		}

	default:
		logger.Error("Unknown database job type: %s", job.JobType)
	}
}
