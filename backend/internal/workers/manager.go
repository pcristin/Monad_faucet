package workers

import (
	"sync"

	"github.com/pcristin/monad-faucet/pkg/logger"
)

// PoolType represents the type of worker pool
type PoolType string

const (
	// DepositPool processes deposit events
	DepositPool PoolType = "deposit"

	// CalculationPool processes calculation tasks
	CalculationPool PoolType = "calculation"

	// DistributionPool processes distribution tasks
	DistributionPool PoolType = "distribution"

	// DatabasePool processes database operations
	DatabasePool PoolType = "database"
)

// Manager coordinates all worker pools
type Manager struct {
	pools     map[PoolType]*WorkerPool
	config    *PoolConfig
	mu        sync.RWMutex
	isRunning bool
}

// NewManager creates a new worker pool manager
func NewManager(cfg *PoolConfig) *Manager {
	return &Manager{
		pools:     make(map[PoolType]*WorkerPool),
		config:    cfg,
		isRunning: false,
	}
}

// Initialize sets up all worker pools
func (m *Manager) Initialize() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create deposit worker pool
	m.pools[DepositPool] = NewWorkerPool(
		string(DepositPool),
		m.config.DepositWorkers,
		m.config.QueueSize,
	)

	// Create calculation worker pool
	m.pools[CalculationPool] = NewWorkerPool(
		string(CalculationPool),
		m.config.CalculationWorkers,
		m.config.QueueSize,
	)

	// Create distribution worker pool
	m.pools[DistributionPool] = NewWorkerPool(
		string(DistributionPool),
		m.config.DistributionWorkers,
		m.config.QueueSize,
	)

	// Create database worker pool
	m.pools[DatabasePool] = NewWorkerPool(
		string(DatabasePool),
		m.config.DatabaseWorkers,
		m.config.QueueSize,
	)

	logger.Info("Worker pools initialized")
}

// StartAll starts all worker pools
func (m *Manager) StartAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isRunning {
		logger.Warn("Worker pools already running")
		return
	}

	for poolType, pool := range m.pools {
		logger.Info("Starting %s worker pool", poolType)
		pool.Start()
	}

	m.isRunning = true
	logger.Info("All worker pools started")
}

// StopAll stops all worker pools
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		logger.Warn("Worker pools not running")
		return
	}

	for poolType, pool := range m.pools {
		logger.Info("Stopping %s worker pool", poolType)
		pool.Stop()
	}

	m.isRunning = false
	logger.Info("All worker pools stopped")
}

// GetPool returns a worker pool by type
func (m *Manager) GetPool(poolType PoolType) *WorkerPool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.pools[poolType]
}

// SubmitTask submits a task to the specified pool
func (m *Manager) SubmitTask(poolType PoolType, task Task) bool {
	m.mu.RLock()
	pool, exists := m.pools[poolType]
	m.mu.RUnlock()

	if !exists {
		logger.Error("Worker pool %s does not exist", poolType)
		return false
	}

	return pool.Submit(task)
}

// Status returns the status of all worker pools
func (m *Manager) Status() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]interface{})

	for poolType, pool := range m.pools {
		poolStatus := map[string]interface{}{
			"running":    pool.IsRunning(),
			"queue_size": pool.QueueSize(),
			"workers":    0,
		}

		switch poolType {
		case DepositPool:
			poolStatus["workers"] = m.config.DepositWorkers
		case CalculationPool:
			poolStatus["workers"] = m.config.CalculationWorkers
		case DistributionPool:
			poolStatus["workers"] = m.config.DistributionWorkers
		case DatabasePool:
			poolStatus["workers"] = m.config.DatabaseWorkers
		}

		status[string(poolType)] = poolStatus
	}

	return status
}

// IsRunning returns whether all worker pools are running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.isRunning
}
