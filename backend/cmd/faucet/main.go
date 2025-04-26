package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gin-gonic/gin"

	"github.com/pcristin/monad-faucet/config"
	"github.com/pcristin/monad-faucet/internal/api"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/blockchain/listener"
	"github.com/pcristin/monad-faucet/internal/bridge"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

func main() {
	runtime.GOMAXPROCS(0)
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		logger.Fatal("Configuration validation failed: %v", err)
	}

	// Determine if we're in production mode
	isProduction := true
	if os.Getenv("PRODUCTION") == "false" {
		isProduction = false
		logger.Info("Running in STAGING mode - will use testnet networks")
	} else {
		logger.Info("Running in PRODUCTION mode - will use mainnet networks")
	}

	// Set up chain types based on environment
	var arbChain, baseChain, optimismChain listener.ChainType
	if isProduction {
		arbChain = listener.ChainArbitrumMainnet
		baseChain = listener.ChainBaseMainnet
		optimismChain = listener.ChainOptimismMainnet
		logger.Info("Using mainnet networks: Arbitrum, Base, Optimism")
	} else {
		arbChain = listener.ChainArbitrumSepolia
		baseChain = listener.ChainBaseSepolia
		optimismChain = listener.ChainOptimismSepolia
		logger.Info("Using testnet networks: Arbitrum Sepolia, Base Sepolia, Optimism Sepolia")
	}

	// Set production mode for logging if running in production environment
	if os.Getenv("PRODUCTION") == "true" {
		logger.SetProduction(true)
		logger.Info("Running in production mode - verbose logging disabled")
	} else {
		logger.Info("Running in development mode - verbose logging enabled")
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
	arbClient, err := ethclient.Dial(cfg.ArbRpcURL)
	if err != nil {
		logger.Fatal("Failed to connect to Arbitrum network: %v", err)
	}

	optimismClient, err := ethclient.Dial(cfg.OptimismRpcURL)
	if err != nil {
		logger.Fatal("Failed to connect to Optimism network: %v", err)
	}

	baseClient, err := ethclient.Dial(cfg.BaseRpcURL)
	if err != nil {
		logger.Fatal("Failed to connect to Base network: %v", err)
	}

	monadClient, err := ethclient.Dial(cfg.MonadRpcURL)
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

	optimismDepositor, err := blockchain.NewOptimismDepositor(
		optimismClient,
		common.HexToAddress(cfg.OptimismDepositorAddr),
		privateKey,
	)
	if err != nil {
		logger.Fatal("Failed to create Optimism distributor: %v", err)
	}

	baseDepositor, err := blockchain.NewBaseDepositor(
		baseClient,
		common.HexToAddress(cfg.BaseDepositorAddr),
		privateKey,
	)
	if err != nil {
		logger.Fatal("Failed to create Base distributor: %v", err)
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
		optimismDepositor,
		baseDepositor,
		monadDistributor,
		db,
	)

	// Set webhook configuration from config
	bridgeService.SetWebhookConfig(cfg.UseWebhook, cfg.WebhookProvider)
	logger.Info("Webhook configuration: enabled=%v, provider=%s", cfg.UseWebhook, cfg.WebhookProvider)

	// Add worker manager to bridge service
	bridgeService.SetWorkerManager(workerManager)

	if err := bridgeService.Start(); err != nil {
		logger.Fatal("Failed to start bridge service: %v", err)
	}
	defer bridgeService.Stop()

	// Create event listener for Arbitrum deposits
	logger.Info("Creating event listener for Arbitrum deposits with contract address: %s", cfg.ArbDepositorAddr)
	arbListener, err := listener.NewEventListener(cfg.ArbRpcURL, common.HexToAddress(cfg.ArbDepositorAddr), arbChain)
	if err != nil {
		logger.Fatal("Failed to create Arbitrum event listener: %v", err)
	}
	defer arbListener.Close()

	// Create event listeners for Base and Optimism if configured
	var baseListener, optimismListener *listener.EventListener

	if cfg.BaseRpcURL != "" && cfg.BaseDepositorAddr != "" {
		logger.Info("Creating event listener for Base deposits with contract address: %s", cfg.BaseDepositorAddr)
		baseListener, err = listener.NewEventListener(cfg.BaseRpcURL, common.HexToAddress(cfg.BaseDepositorAddr), baseChain)
		if err != nil {
			logger.Fatal("Failed to create Base event listener: %v", err)
		}
		defer baseListener.Close()
	}

	if cfg.OptimismRpcURL != "" && cfg.OptimismDepositorAddr != "" {
		logger.Info("Creating event listener for Optimism deposits with contract address: %s", cfg.OptimismDepositorAddr)
		optimismListener, err = listener.NewEventListener(cfg.OptimismRpcURL, common.HexToAddress(cfg.OptimismDepositorAddr), optimismChain)
		if err != nil {
			logger.Fatal("Failed to create Optimism event listener: %v", err)
		}
		defer optimismListener.Close()
	}

	// Log network status for all configured networks
	networkUrls := map[string]string{
		"Arbitrum": cfg.ArbRpcURL,
		"Base":     cfg.BaseRpcURL,
		"Optimism": cfg.OptimismRpcURL,
		"Monad":    cfg.MonadRpcURL,
	}

	contractAddresses := map[string]string{
		"Arbitrum_Depositor": cfg.ArbDepositorAddr,
		"Base_Depositor":     cfg.BaseDepositorAddr,
		"Optimism_Depositor": cfg.OptimismDepositorAddr,
		"Monad_Distributor":  cfg.MonadDistributorAddr,
	}

	// Log environment and network type information
	logger.Info("=== Environment Configuration ===")
	if isProduction {
		logger.Info("Environment: PRODUCTION - Using mainnet networks")
	} else {
		logger.Info("Environment: STAGING - Using testnet networks")
	}
	logger.Info("Arbitrum chain: %s", listener.ChainTypeToString(arbChain))
	logger.Info("Base chain: %s", listener.ChainTypeToString(baseChain))
	logger.Info("Optimism chain: %s", listener.ChainTypeToString(optimismChain))

	// Log status of all configured networks and contracts
	blockchain.LogNetworkStatus(networkUrls, contractAddresses)

	// Start listening for deposit events from Arbitrum
	go startListener(ctx, arbListener, bridgeService)

	// Start listening for deposit events from Base if configured
	if baseListener != nil {
		go startListener(ctx, baseListener, bridgeService)
	}

	// Start listening for deposit events from Optimism if configured
	if optimismListener != nil {
		go startListener(ctx, optimismListener, bridgeService)
	}

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

// Helper function to start a listener
func startListener(ctx context.Context, eventListener *listener.EventListener, bridgeService *bridge.BridgeService) {
	chain := eventListener.GetChain()
	networkName, isTestnet := listener.GetChainInfo(chain)
	networkType := "Mainnet"
	if isTestnet {
		networkType = "Testnet"
	}

	logger.Info("Starting to listen for deposit events from %s-%s contract", networkName, networkType)

	// Get deposit events channel
	depositChan, errChan := eventListener.ListenToDeposits(ctx)

	// Process deposit events
	for {
		select {
		case deposit := <-depositChan:
			networkName, isTestnet := listener.GetChainInfo(deposit.Chain)
			networkLabel := networkName
			if isTestnet {
				networkLabel = networkName + "-Testnet"
			}

			logger.Info("[%s] Deposit event received: ID=%s, Amount=%s, Wallet=%s, Currency=%s",
				networkLabel, deposit.DepositId.String(), deposit.Amount.String(), deposit.Depositor.Hex(),
				blockchain.CurrencyTypeToString(deposit.Currency))

			// Forward the deposit to the bridge service for processing
			logger.Info("[%s] Forwarding deposit ID=%s to bridge service for immediate database recording and processing",
				networkLabel, deposit.DepositId.String())
			bridgeService.HandleDeposit(deposit)

		case err := <-errChan:
			logger.Error("[%s-%s] Error listening for deposits: %v", networkName, networkType, err)
		case <-ctx.Done():
			return
		}
	}
}
