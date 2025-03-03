package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"

	"github.com/pcristin/monad-faucet/config"
	"github.com/pcristin/monad-faucet/internal/api"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/bridge"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		logger.Fatal("Configuration validation failed: %v", err)
	}

	// Create application context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database
	db, err := database.NewDB(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Fatal("Failed to connect to database: %v", err)
	}
	defer db.Close()

	logger.Info("Database initialized successfully")

	// Set the database instance for the blockchain package so it can load settings
	blockchain.SetDatabase(db)
	logger.Info("Blockchain settings loaded from database")

	// Parse private key
	privateKey, err := crypto.HexToECDSA(cfg.WalletPrivateKey)
	if err != nil {
		logger.Fatal("Failed to parse private key: %v", err)
	}

	// Create blockchain clients
	arbClient, err := blockchain.NewClient(cfg.ArbRpcURL)
	if err != nil {
		logger.Fatal("Failed to connect to Arbitrum network: %v", err)
	}

	monadClient, err := blockchain.NewClient(cfg.MonadRpcURL)
	if err != nil {
		logger.Fatal("Failed to connect to Monad network: %v", err)
	}

	// Create contract instances
	arbDepositor, err := blockchain.NewArbitrumDepositor(
		arbClient,
		common.HexToAddress(cfg.ArbDepositorAddr),
		privateKey,
	)
	if err != nil {
		logger.Fatal("Failed to create Arbitrum depositor: %v", err)
	}

	monadDistributor, err := blockchain.NewMonadDistributor(
		monadClient,
		common.HexToAddress(cfg.MonadDistributorAddr),
		privateKey,
	)
	if err != nil {
		logger.Fatal("Failed to create Monad distributor: %v", err)
	}

	// Initialize worker pool manager with default configuration
	workerPoolConfig := &workers.PoolConfig{
		DepositWorkers:      5, // Default to 5 workers per pool
		CalculationWorkers:  5,
		DistributionWorkers: 5,
		DatabaseWorkers:     5,
		QueueSize:           100, // Default queue size
	}

	workerManager := workers.NewManager(workerPoolConfig)
	workerManager.Initialize()
	workerManager.StartAll()
	defer workerManager.StopAll()

	logger.Info("Worker pools started successfully")

	// Create bridge service
	bridgeService := bridge.NewBridgeService(
		arbDepositor,
		monadDistributor,
		db,
	)

	// Add worker manager to bridge service
	bridgeService.SetWorkerManager(workerManager)

	if err := bridgeService.Start(); err != nil {
		logger.Fatal("Failed to start bridge service: %v", err)
	}
	defer bridgeService.Stop()

	// Create event listener for Arbitrum deposits
	logger.Info("Creating event listener for Arbitrum deposits with contract address: %s", cfg.ArbDepositorAddr)
	listener, err := blockchain.NewEventListener(cfg.ArbRpcURL, common.HexToAddress(cfg.ArbDepositorAddr))
	if err != nil {
		logger.Fatal("Failed to create event listener: %v", err)
	}
	defer listener.Close()

	// Start listening for deposit events
	go func() {
		logger.Info("Starting to listen for deposit events from Arbitrum contract: %s", cfg.ArbDepositorAddr)
		// Get deposit events channel
		depositChan, errChan := listener.ListenToDeposits(ctx)

		// Process deposit events
		for {
			select {
			case deposit := <-depositChan:
				logger.Info("Deposit event received: ID=%s, Amount=%s, Wallet=%s, Currency=%s",
					deposit.DepositId.String(), deposit.Amount.String(), deposit.Depositor.Hex(),
					blockchain.CurrencyTypeToString(deposit.Currency))

				// Forward the deposit to the bridge service for processing
				logger.Info("Forwarding deposit ID=%s to bridge service for immediate database recording and processing",
					deposit.DepositId.String())
				bridgeService.HandleDeposit(deposit)

			case err := <-errChan:
				logger.Error("Error listening for deposits: %v", err)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Create API server
	router := gin.Default()

	// Create API handler
	mainHandler := api.NewHandler(db, bridgeService)

	// Configure API routes with worker pool support
	api.SetupWorkerPoolRoutes(router, mainHandler, db)

	// Create HTTP server
	server := &http.Server{
		Addr:    cfg.ServerAddr,
		Handler: router,
	}

	// Start HTTP server in a goroutine
	go func() {
		logger.Info("Starting HTTP server on %s", cfg.ServerAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server error: %v", err)
		}
	}()

	// Create a channel to receive shutdown signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Wait for a shutdown signal
	<-quit
	logger.Info("Shutting down server...")

	// Create a deadline context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Stop HTTP server
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("Server forced to shutdown: %v", err)
	}

	// Stop bridge service (already deferred)
	bridgeService.GracefulShutdown(shutdownCtx)

	logger.Info("Server exited properly")
}
