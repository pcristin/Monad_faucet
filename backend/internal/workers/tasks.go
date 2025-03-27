package workers

import (
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// BaseTask provides common functionality for all tasks
type BaseTask struct {
	id            string
	taskType      string
	createdAt     time.Time
	customProcess func(interface{}) error
}

// NewBaseTask creates a new base task
func NewBaseTask(taskType string) BaseTask {
	return BaseTask{
		id:        uuid.New().String(),
		taskType:  taskType,
		createdAt: time.Now(),
	}
}

// ID returns the task ID
func (t *BaseTask) ID() string {
	return t.id
}

// Type returns the task type
func (t *BaseTask) Type() string {
	return t.taskType
}

// CreatedAt returns when the task was created
func (t *BaseTask) CreatedAt() time.Time {
	return t.createdAt
}

// SetCustomProcessor sets a custom processor function for this task
func (t *BaseTask) SetCustomProcessor(fn func(interface{}) error) {
	t.customProcess = fn
}

// HasCustomProcessor returns true if a custom processor is set
func (t *BaseTask) HasCustomProcessor() bool {
	return t.customProcess != nil
}

// RunCustomProcessor runs the custom processor if one is set
func (t *BaseTask) RunCustomProcessor(task interface{}) error {
	if t.customProcess == nil {
		return fmt.Errorf("no custom processor set")
	}
	return t.customProcess(task)
}

// DepositTask represents a task to process a deposit
type DepositTask struct {
	BaseTask
	DepositID   string
	UserAddress string
	Amount      string
	TxHash      string
	EventData   interface{} // Store the original event data
}

// NewDepositTask creates a new deposit task
func NewDepositTask(depositID, userAddress, amount, txHash string) *DepositTask {
	return &DepositTask{
		BaseTask:    NewBaseTask("deposit"),
		DepositID:   depositID,
		UserAddress: userAddress,
		Amount:      amount,
		TxHash:      txHash,
	}
}

// SetEventData stores the original blockchain event data
func (t *DepositTask) SetEventData(event interface{}) {
	t.EventData = event
}

// Process handles the deposit task
func (t *DepositTask) Process() error {
	logger.Info("Processing deposit task %s for user %s, amount %s, tx %s",
		t.DepositID, t.UserAddress, t.Amount, t.TxHash)

	// Check if we have event data and a bridge service reference
	if t.EventData == nil {
		logger.Error("No event data provided for deposit task %s", t.DepositID)
		return fmt.Errorf("missing event data for deposit")
	}

	// The EventData should be handled by the service that implements the
	// processing of this task. When this task is submitted to a worker pool,
	// the pool should have a handler registered that knows how to process
	// this specific task type and can access the EventData properly.
	//
	// In our case, this will be handled by a custom ProcessorFunc that
	// will be registered with the BridgeService.

	logger.Info("Deposit task %s processed - event data will be handled by registered handler",
		t.DepositID)

	return nil
}

// CalculationTask represents a task to calculate distribution amounts
type CalculationTask struct {
	BaseTask
	DepositID string
	BatchSize int
}

// NewCalculationTask creates a new calculation task
func NewCalculationTask(depositID string, batchSize int) *CalculationTask {
	return &CalculationTask{
		BaseTask:  NewBaseTask("calculation"),
		DepositID: depositID,
		BatchSize: batchSize,
	}
}

// Process handles the calculation task
func (t *CalculationTask) Process() error {
	logger.Info("Processing calculation task for deposit %s with batch size %d",
		t.DepositID, t.BatchSize)

	// TODO: Implement actual calculation processing

	return nil
}

// DistributionTask represents a task to distribute tokens
type DistributionTask struct {
	BaseTask
	DepositID      string
	DistributionID string
	Recipients     []string
	Amounts        []string
}

// NewDistributionTask creates a new distribution task
func NewDistributionTask(depositID, distributionID string, recipients []string, amounts []string) *DistributionTask {
	return &DistributionTask{
		BaseTask:       NewBaseTask("distribution"),
		DepositID:      depositID,
		DistributionID: distributionID,
		Recipients:     recipients,
		Amounts:        amounts,
	}
}

// Process handles the distribution task
func (t *DistributionTask) Process() error {
	if len(t.Recipients) != len(t.Amounts) {
		return fmt.Errorf("recipients and amounts length mismatch: %d vs %d",
			len(t.Recipients), len(t.Amounts))
	}

	logger.Info("Processing distribution task %s for deposit %s with %d recipients",
		t.DistributionID, t.DepositID, len(t.Recipients))

	// Check if a custom processor is set via RunCustomProcessor
	if t.HasCustomProcessor() {
		return t.RunCustomProcessor(t)
	}

	// Convert values for processing (for validation)
	_, ok := new(big.Int).SetString(t.DepositID, 10)
	if !ok {
		return fmt.Errorf("invalid deposit ID: %s", t.DepositID)
	}

	// The actual distribution should be handled by the service that
	// registered a custom processor for this task.
	// When this task is submitted to a worker pool, that handler
	// will use the BridgeService.mintTokens method to perform the actual
	// blockchain transaction.

	logger.Info("Distribution task %s processed for deposit %s with %d recipients",
		t.DistributionID, t.DepositID, len(t.Recipients))

	return nil
}

// DatabaseTask represents a task to perform database operations
type DatabaseTask struct {
	BaseTask
	Operation string
	Data      map[string]interface{}
}

// NewDatabaseTask creates a new database task
func NewDatabaseTask(operation string, data map[string]interface{}) *DatabaseTask {
	return &DatabaseTask{
		BaseTask:  NewBaseTask("database"),
		Operation: operation,
		Data:      data,
	}
}

// Process handles the database task
func (t *DatabaseTask) Process() error {
	logger.Info("Processing database task with operation %s", t.Operation)

	// Check if a custom processor is set via RunCustomProcessor
	if t.HasCustomProcessor() {
		return t.RunCustomProcessor(t)
	}

	// Define common operations that database tasks might perform
	switch t.Operation {
	case "create_deposit":
		logger.Info("Database task: Creating deposit record")
	case "update_deposit_status":
		logger.Info("Database task: Updating deposit status")
	case "create_distribution":
		logger.Info("Database task: Creating distribution record")
	case "update_distribution_status":
		logger.Info("Database task: Updating distribution status")
	default:
		logger.Warn("Unknown database operation: %s", t.Operation)
	}

	// The actual database operation should be handled by the service
	// that registered a custom processor for this task.
	// This allows the bridge service to handle the database operations
	// with its own DB connection and transaction management.

	logger.Info("Database task with operation %s processed", t.Operation)

	return nil
}
