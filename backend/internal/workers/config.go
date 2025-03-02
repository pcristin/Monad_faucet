package workers

// PoolConfig contains configuration for worker pools
type PoolConfig struct {
	// DepositWorkers is the number of workers for processing deposits
	DepositWorkers int

	// CalculationWorkers is the number of workers for calculations
	CalculationWorkers int

	// DistributionWorkers is the number of workers for distributions
	DistributionWorkers int

	// DatabaseWorkers is the number of workers for database operations
	DatabaseWorkers int

	// QueueSize is the size of the task queue for each worker pool
	QueueSize int
}
