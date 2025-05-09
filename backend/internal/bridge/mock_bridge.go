package bridge

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/interfaces"
)

var _ interfaces.BridgeServiceInterface = (*MockBridgeService)(nil)

// MockBridgeService implements BridgeServiceInterface for testing
type MockBridgeService struct {
	// Configuration
	config *MockConfig

	// State
	deposits      map[string]*DepositData
	distributions map[string]*DistributionData
	transactions  map[string]*TransactionData

	// Channels for simulating events
	depositEventCh chan interfaces.DepositEvent
	subscribersMu  sync.RWMutex
	subscribers    []chan<- interfaces.DepositEvent

	// Control
	ctx             context.Context
	cancelFunc      context.CancelFunc
	wg              sync.WaitGroup
	running         bool
	runningMu       sync.RWMutex
	processingDelay time.Duration
}

type MockConfig struct {
	ProcessingDelay time.Duration
	FailureRate     float64 // 0.0-1.0 rate of random failures
	SimulateLatency bool
	MaxLatency      time.Duration
}

type DepositData struct {
	DepositID   *big.Int
	UserAddress common.Address
	Amount      *big.Int
	TxHash      string
	Status      string // "pending", "processing", "completed", "failed"
	Timestamp   uint64
}

type DistributionData struct {
	DepositID   string
	UserAddress string
	Amount      string
	TxHash      string
	Status      string
}

type TransactionData struct {
	DepositID   *big.Int
	UserAddress common.Address
	Amount      *big.Int
	TxHash      string
	Status      string
	Timestamp   uint64
}

// NewMockBridgeService creates a new mock bridge service
func NewMockBridgeService(config *MockConfig) *MockBridgeService {
	if config == nil {
		config = &MockConfig{
			ProcessingDelay: 10 * time.Millisecond,
			FailureRate:     0.0,
			SimulateLatency: false,
			MaxLatency:      100 * time.Millisecond,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MockBridgeService{
		config:          config,
		deposits:        make(map[string]*DepositData),
		distributions:   make(map[string]*DistributionData),
		transactions:    make(map[string]*TransactionData),
		depositEventCh:  make(chan interfaces.DepositEvent, 1000),
		ctx:             ctx,
		cancelFunc:      cancel,
		processingDelay: config.ProcessingDelay,
	}
}

// Start begins mock service operations
func (m *MockBridgeService) Start(ctx context.Context) error {
	m.runningMu.Lock()
	defer m.runningMu.Unlock()

	if m.running {
		return fmt.Errorf("mock bridge service already running")
	}

	m.running = true
	m.wg.Add(1)

	// Start event processor
	go m.processEvents()

	return nil
}

// Stop halts mock service operations
func (m *MockBridgeService) Stop() error {
	m.runningMu.Lock()
	defer m.runningMu.Unlock()

	if !m.running {
		return nil
	}

	m.cancelFunc()
	m.wg.Wait()
	m.running = false

	return nil
}

// SimulateDeposit generates a mock deposit event
func (m *MockBridgeService) SimulateDeposit(depositID *big.Int, userAddress common.Address, amount *big.Int, txHash string) {
	event := interfaces.DepositEvent{
		DepositID:   depositID,
		UserAddress: userAddress,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: uint64(12345 + depositID.Int64()),
		SourceChain: "Arbitrum",
		Timestamp:   uint64(time.Now().Unix()),
	}

	// Queue the event
	m.depositEventCh <- event
}

// ProcessDeposit handles a deposit
func (m *MockBridgeService) ProcessDeposit(depositID *big.Int, userAddress common.Address, amount *big.Int, txHash string) error {
	// Simulate processing delay
	if m.config.SimulateLatency {
		time.Sleep(time.Duration(float64(m.config.MaxLatency) * float64(time.Now().UnixNano()%100) / 100.0))
	} else {
		time.Sleep(m.processingDelay)
	}

	depositKey := depositID.String()

	// Add to tracked deposits
	m.deposits[depositKey] = &DepositData{
		DepositID:   depositID,
		UserAddress: userAddress,
		Amount:      amount,
		TxHash:      txHash,
		Status:      "processing",
		Timestamp:   uint64(time.Now().Unix()),
	}

	return nil
}

// GetDepositStatus returns the status of a deposit
func (m *MockBridgeService) GetDepositStatus(depositID *big.Int) (string, error) {
	depositKey := depositID.String()

	if deposit, exists := m.deposits[depositKey]; exists {
		return deposit.Status, nil
	}

	return "", fmt.Errorf("deposit %s not found", depositKey)
}

// CreateDistribution creates a distribution record
func (m *MockBridgeService) CreateDistribution(depositID string, userAddress string, amount string) error {
	m.distributions[depositID] = &DistributionData{
		DepositID:   depositID,
		UserAddress: userAddress,
		Amount:      amount,
		Status:      "pending",
	}

	return nil
}

// CompleteDistribution marks a distribution as complete
func (m *MockBridgeService) CompleteDistribution(depositID string, txHash string) error {
	if dist, exists := m.distributions[depositID]; exists {
		dist.Status = "completed"
		dist.TxHash = txHash
		return nil
	}

	return fmt.Errorf("distribution for deposit %s not found", depositID)
}

// SubmitTransaction submits a mock transaction
func (m *MockBridgeService) SubmitTransaction(depositID *big.Int, userAddress common.Address, amount *big.Int) (string, error) {
	// Simulate processing
	time.Sleep(m.processingDelay)

	// Generate mock tx hash
	txHash := fmt.Sprintf("0x%064x", depositID.Int64())

	// Store transaction
	m.transactions[txHash] = &TransactionData{
		DepositID:   depositID,
		UserAddress: userAddress,
		Amount:      amount,
		TxHash:      txHash,
		Status:      "pending",
		Timestamp:   uint64(time.Now().Unix()),
	}

	// Update deposit status
	depositKey := depositID.String()
	if deposit, exists := m.deposits[depositKey]; exists {
		deposit.Status = "processing"
	}

	return txHash, nil
}

// GetTransactionStatus returns the status of a transaction
func (m *MockBridgeService) GetTransactionStatus(txHash string) (string, error) {
	if tx, exists := m.transactions[txHash]; exists {
		return tx.Status, nil
	}

	return "", fmt.Errorf("transaction %s not found", txHash)
}

// SubscribeToDepositEvents subscribes to deposit events
func (m *MockBridgeService) SubscribeToDepositEvents(ch chan<- interfaces.DepositEvent) error {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	m.subscribers = append(m.subscribers, ch)
	return nil
}

// UnsubscribeFromDepositEvents unsubscribes from deposit events
func (m *MockBridgeService) UnsubscribeFromDepositEvents(ch chan<- interfaces.DepositEvent) error {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	for i, sub := range m.subscribers {
		if sub == ch {
			// Remove by swapping with the last element and trimming
			m.subscribers[i] = m.subscribers[len(m.subscribers)-1]
			m.subscribers = m.subscribers[:len(m.subscribers)-1]
			return nil
		}
	}

	return fmt.Errorf("subscription not found")
}

// processEvents handles distributing events to subscribers
func (m *MockBridgeService) processEvents() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return

		case event := <-m.depositEventCh:
			// Broadcast to all subscribers
			m.subscribersMu.RLock()
			for _, ch := range m.subscribers {
				select {
				case ch <- event:
					// Event sent successfully
				default:
					// Skip if channel is full/blocked (non-blocking send)
				}
			}
			m.subscribersMu.RUnlock()

			// Process the deposit automatically
			m.ProcessDeposit(event.DepositID, event.UserAddress, event.Amount, event.TxHash)
		}
	}
}

// SimulateTransactionConfirmation simulates a transaction being confirmed
func (m *MockBridgeService) SimulateTransactionConfirmation(txHash string) error {
	if tx, exists := m.transactions[txHash]; exists {
		tx.Status = "completed"

		// Find and update the corresponding deposit
		for id, deposit := range m.deposits {
			if deposit.TxHash == txHash {
				deposit.Status = "completed"

				// Complete any distributions
				if dist, exists := m.distributions[id]; exists {
					dist.Status = "completed"
					dist.TxHash = txHash
				}

				break
			}
		}

		return nil
	}

	return fmt.Errorf("transaction %s not found", txHash)
}

// SimulateTransactionFailure simulates a transaction failing
func (m *MockBridgeService) SimulateTransactionFailure(txHash string) error {
	if tx, exists := m.transactions[txHash]; exists {
		tx.Status = "failed"

		// Find and update the corresponding deposit
		for id, deposit := range m.deposits {
			if deposit.TxHash == txHash {
				deposit.Status = "failed"

				// Complete any distributions
				if dist, exists := m.distributions[id]; exists {
					dist.Status = "failed"
				}

				break
			}
		}

		return nil
	}

	return fmt.Errorf("transaction %s not found", txHash)
}
