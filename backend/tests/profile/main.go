// backend/tests/profile/main.go
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pcristin/monad-faucet/internal/bridge"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/interfaces"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to run: %v\n", err)
	}
}

func run() error {
	// Create a new profile directory if not exists
	profileDir := "../../profiles"
	if _, err := os.Stat(profileDir); os.IsNotExist(err) {
		if err := os.MkdirAll(profileDir, 0755); err != nil {
			return fmt.Errorf("failed to create profile directory: %w", err)
		}
	}
	return runBenchMarksAndProfile(filepath.Join(profileDir, "profile.pprof"))
}

func runBenchMarksAndProfile(profilePath string) error {
	// Force garbage collection to get up-to-date statistics
	runtime.GC()

	// Create profile file
	f, err := os.Create(profilePath)
	if err != nil {
		return fmt.Errorf("failed to create profile file: %w", err)
	}
	defer f.Close()

	// Create CPU profile
	if err := pprof.StartCPUProfile(f); err != nil {
		return fmt.Errorf("failed to start CPU profile: %w", err)
	}
	defer pprof.StopCPUProfile()

	return simulateFullDepositProcessing()
}

func simulateFullDepositProcessing() error {
	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := setupDatabase(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup database: %w", err)
	}
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			fmt.Printf("Failed to terminate postgres container: %v\n", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to get connection string: %s", err)
	}

	// Initialize database
	db, err := database.NewDB(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create mock bridge service
	mockBridge := bridge.NewMockBridgeService(&bridge.MockConfig{
		ProcessingDelay: 5 * time.Millisecond, // Fast for testing
		SimulateLatency: true,
		MaxLatency:      20 * time.Second,
	})

	// Start the mock bridge
	if err := mockBridge.Start(ctx); err != nil {
		return fmt.Errorf("failed to start mock bridge: %w", err)
	}
	defer mockBridge.Stop()

	// Create bridge worker pools
	// NOTE: We can't directly access bridge.BridgeWorkerPools because of unexported fields
	// Instead, we will simulate batch behavior by throttling our deposit submissions
	// and allowing time for batch processing as in production

	// Create worker pool manager with the mock bridge and real DB
	workerPoolConfig := &workers.PoolConfig{
		DepositWorkers:      5,
		CalculationWorkers:  5,
		DistributionWorkers: 5,
		DatabaseWorkers:     5,
		QueueSize:           100,
	}

	// Create a worker manager that uses our mock bridge and real DB
	mockInterfaceBridge := convertToInterfaceMock(mockBridge)
	workerManager := workers.NewMockManager(workerPoolConfig, mockInterfaceBridge, db)

	// Define a function to handle deposits
	processDeposit := func(task interface{}) error {
		depositTask, ok := task.(*workers.DepositTask)
		if !ok {
			return fmt.Errorf("invalid task type")
		}

		// Extract the event data from the task
		event, ok := depositTask.EventData.(interfaces.DepositEvent)
		if !ok {
			return fmt.Errorf("invalid event data type")
		}

		// Process the deposit by creating a deposit record in the database
		deposit := &database.Deposit{
			DepositID:     event.DepositID,
			WalletAddress: event.UserAddress,
			Amount:        event.Amount,
			Currency:      database.CurrencyETH, // Assuming ETH for simplicity
			TxHash:        event.TxHash,
			BlockNumber:   event.BlockNumber,
			Status:        "processing", // Set as processing, not completed, to allow distribution phase
			SourceChain:   event.SourceChain,
		}

		err := db.CreateDeposit(deposit)
		if err != nil {
			fmt.Printf("Failed to create deposit: %v\n", err)
			return err
		}

		// Calculate MON amount (simple 1:1 conversion for testing)
		monAmount := new(big.Int).Set(event.Amount)

		// Create a distribution for this deposit
		distribution := &database.Distribution{
			DepositID:     event.DepositID,
			WalletAddress: event.UserAddress,
			MonAmount:     monAmount,
			Status:        "pending",
		}

		// Save distribution to database
		err = db.CreateDistribution(distribution)
		if err != nil {
			fmt.Printf("Failed to create distribution: %v\n", err)
			return err
		}

		fmt.Printf("Successfully processed deposit %s and created distribution\n", event.DepositID.String())
		return nil
	}

	// We need to also simulate the distribution handler
	// In production, distributions would be processed in batches - we're manually handling this
	// through database updates after waiting for the batch delay

	workerManager.Initialize()
	workerManager.StartAll()
	defer workerManager.StopAll()

	// Set up deposit event channel
	depositEventCh := make(chan interfaces.DepositEvent, 10000)
	mockBridge.SubscribeToDepositEvents(depositEventCh)
	defer mockBridge.UnsubscribeFromDepositEvents(depositEventCh)

	// Process events from the channel and submit them to the worker pool
	go func() {
		for event := range depositEventCh {
			// Create a deposit task from the event
			depositTask := workers.NewDepositTask(
				event.DepositID.String(),
				event.UserAddress.Hex(),
				event.Amount.String(),
				event.TxHash,
			)
			depositTask.SetEventData(event)

			// Set the custom processor
			depositTask.SetCustomProcessor(processDeposit)

			// Submit the task to the worker pool
			if !workerManager.SubmitTask(workers.DepositPool, depositTask) {
				fmt.Printf("Failed to submit deposit task %s to worker pool\n", event.DepositID.String())
			} else {
				fmt.Printf("Successfully submitted deposit task %s to worker pool\n", event.DepositID.String())
			}
		}
	}()

	// Create and simulate test deposits
	numDeposits := 1000 // Reduced from 10000 for better testing
	testDeposits := createTestDeposits(numDeposits)

	fmt.Printf("Starting profile test with %d deposits\n", numDeposits)
	startTime := time.Now()

	// Submit deposits to the mock bridge at a controlled rate
	// to better simulate real-world behavior and allow for proper batching
	const batchSize = 50 // Submit in smaller batches at a time
	const batchDelay = 100 * time.Millisecond

	for i := 0; i < numDeposits; i += batchSize {
		end := i + batchSize
		if end > numDeposits {
			end = numDeposits
		}

		// Submit a batch of deposits
		for j := i; j < end; j++ {
			deposit := testDeposits[j]
			mockBridge.SimulateDeposit(
				big.NewInt(int64(j)),
				common.HexToAddress(deposit.UserAddress),
				big.NewInt(int64(j+1)*1000000000000000000), // j+1 ETH in wei
				fmt.Sprintf("0x%064x", j),
			)
		}

		// Wait a bit to allow for processing and to not overwhelm the system
		time.Sleep(batchDelay)
	}

	// Wait a reasonable time for processing all deposits and distributions
	// This includes time for batches to form (20 seconds) plus processing time
	fmt.Println("Waiting for all deposits and distributions to process...")

	// Wait for initial deposit processing
	time.Sleep(5 * time.Second)

	// Wait for distribution batching (should be at least the merge delay)
	fmt.Println("Waiting for batch processing delay (20 seconds) to complete...")
	time.Sleep(20 * time.Second)

	// After the batch delay, let's simulate batch completion
	fmt.Println("Simulating batch distribution completion...")

	// Update all distributions to completed
	_, err = db.Exec("UPDATE distributions SET status = 'completed', monad_tx_hash = 'batch-tx-1' WHERE status = 'pending'")
	if err != nil {
		fmt.Printf("Failed to update distributions: %v\n", err)
	}

	// Update all deposits to completed
	_, err = db.Exec("UPDATE deposits SET status = 'completed' WHERE status = 'processing'")
	if err != nil {
		fmt.Printf("Failed to update deposits: %v\n", err)
	}

	// Simulate transaction confirmations
	fmt.Println("Simulating transaction confirmations...")
	for i := 0; i < numDeposits; i++ {
		txHash := fmt.Sprintf("0x%064x", i)
		mockBridge.SimulateTransactionConfirmation(txHash)
	}

	// Wait for confirmation processing
	time.Sleep(5 * time.Second)

	duration := time.Since(startTime)

	// Query final state from DB for verification
	var processingCount, completedCount int
	err = db.QueryRow("SELECT COUNT(*) FROM deposits WHERE status = 'processing'").Scan(&processingCount)
	if err != nil {
		return fmt.Errorf("failed to get processing deposit count: %w", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM deposits WHERE status = 'completed'").Scan(&completedCount)
	if err != nil {
		return fmt.Errorf("failed to get completed deposit count: %w", err)
	}

	fmt.Printf("Deposit status: %d processing, %d completed out of %d total\n",
		processingCount, completedCount, numDeposits)
	fmt.Printf("Total test duration: %s (avg %s per deposit)\n",
		duration, duration/time.Duration(numDeposits))

	// Query distribution stats
	var pendingCount, distributionCompletedCount int
	err = db.QueryRow("SELECT COUNT(*) FROM distributions WHERE status = 'pending'").Scan(&pendingCount)
	if err == nil {
		fmt.Printf("Pending distributions: %d\n", pendingCount)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM distributions WHERE status = 'completed'").Scan(&distributionCompletedCount)
	if err == nil {
		fmt.Printf("Completed distributions: %d\n", distributionCompletedCount)
	}

	// Memory profiling after processing
	memProfilePath := filepath.Join("../../profiles", "memory.pprof")
	memFile, err := os.Create(memProfilePath)
	if err != nil {
		return fmt.Errorf("failed to create memory profile: %w", err)
	}
	defer memFile.Close()

	if err := pprof.WriteHeapProfile(memFile); err != nil {
		return fmt.Errorf("failed to write memory profile: %w", err)
	}

	fmt.Printf("Memory profile written to %s\n", memProfilePath)

	return nil
}

// Fix for your profile/main.go code
func setupDatabase(ctx context.Context) (*postgres.PostgresContainer, error) {
	// Create SQL init script for testing (keep your existing SQL)
	const testInitSQL = `
    -- Create tables for testing without relying on migrations
    CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
    INSERT INTO schema_version (version) VALUES (999) ON CONFLICT DO NOTHING;
    
    CREATE TABLE IF NOT EXISTS transaction_history (
        id SERIAL PRIMARY KEY,
        deposit_id VARCHAR(100),
        wallet_address VARCHAR(42),
        amount VARCHAR(78),
        currency INTEGER,
        mon_amount VARCHAR(78),
        status VARCHAR(20),
        tx_hash VARCHAR(66),
        monad_tx_hash VARCHAR(66),
        metadata TEXT,
        source_chain VARCHAR(20),
        refund_tx_hash VARCHAR(66),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS deposits (
        id SERIAL PRIMARY KEY,
        deposit_id VARCHAR(100) NOT NULL,
        wallet_address VARCHAR(42) NOT NULL,
        amount VARCHAR(78) NOT NULL,
        currency INTEGER NOT NULL,
        tx_hash VARCHAR(66) NOT NULL,
		metadata TEXT,
        block_number BIGINT NOT NULL,
        status VARCHAR(20) NOT NULL,
        source_chain VARCHAR(20) DEFAULT 'arbitrum',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT deposit_id_unique_deposits UNIQUE (deposit_id)
    );
    `

	// Use the newer approach with Run function
	pgContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(10*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres container: %w", err)
	}

	// Execute the SQL to initialize the test database - FIXED to capture all return values
	exitCode, stdout, err := pgContainer.Exec(ctx, []string{"psql", "-U", "postgres", "-d", "testdb", "-c", testInitSQL})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize test database: %w", err)
	}
	if exitCode != 0 {
		return nil, fmt.Errorf("failed to initialize test database, exit code: %d, output: %s", exitCode, stdout)
	}

	return pgContainer, nil
}

type TestDeposit struct {
	DepositID   string
	UserAddress string
	Amount      string
	TxHash      string
}

func createTestDeposits(numDeposits int) []TestDeposit {
	deposits := make([]TestDeposit, numDeposits)
	for i := 0; i < numDeposits; i++ {
		deposits[i] = TestDeposit{
			DepositID:   fmt.Sprintf("deposit-%d", i),
			UserAddress: fmt.Sprintf("0x%040x", i),     // Mock Ethereum address
			Amount:      fmt.Sprintf("%d", (i+1)*1e18), // i+1 ETH in wei
			TxHash:      fmt.Sprintf("0x%064x", i),     // Mock transaction hash
		}
	}
	return deposits
}

// convertToInterfaceMock creates an interfaces.MockBridgeService from a bridge.MockBridgeService
func convertToInterfaceMock(bridgeMock *bridge.MockBridgeService) *interfaces.MockBridgeService {
	return &interfaces.MockBridgeService{
		StartFunc:                           bridgeMock.Start,
		StopFunc:                            bridgeMock.Stop,
		ProcessDepositFunc:                  bridgeMock.ProcessDeposit,
		SimulateDepositFunc:                 bridgeMock.SimulateDeposit,
		SubscribeToDepositEventsFunc:        bridgeMock.SubscribeToDepositEvents,
		UnsubscribeFromDepositEventsFunc:    bridgeMock.UnsubscribeFromDepositEvents,
		SimulateTransactionConfirmationFunc: bridgeMock.SimulateTransactionConfirmation,
	}
}
