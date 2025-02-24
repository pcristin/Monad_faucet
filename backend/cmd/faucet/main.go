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
	"github.com/pcristin/monad-faucet/pkg/logger"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", err)
	}

	// Parse private key
	privateKey, err := crypto.HexToECDSA(cfg.WalletPrivateKey)
	if err != nil {
		logger.Fatal("Failed to parse private key: %v", err)
	}

	// Create event listener for Arbitrum
	listener, err := blockchain.NewEventListener(cfg.ArbRpcURL)
	if err != nil {
		logger.Fatal("Failed to create event listener: %v", err)
	}
	defer listener.Close()

	// Create contract instances
	arbDepositor, err := blockchain.NewArbitrumDepositor(
		listener.GetClient(),
		common.HexToAddress(cfg.ArbDepositorAddr),
		privateKey,
	)
	if err != nil {
		logger.Fatal("Failed to create Arbitrum depositor: %v", err)
	}

	monadClient, err := blockchain.NewClient(cfg.MonadRpcURL)
	if err != nil {
		logger.Fatal("Failed to connect to Monad network: %v", err)
	}

	monadDistributor, err := blockchain.NewMonadDistributor(
		monadClient,
		common.HexToAddress(cfg.MonadDistributorAddr),
		privateKey,
	)
	if err != nil {
		logger.Fatal("Failed to create Monad distributor: %v", err)
	}

	// Create bridge service
	bridgeService := blockchain.NewBridgeService(arbDepositor, monadDistributor)
	if err := bridgeService.Start(); err != nil {
		logger.Fatal("Failed to start bridge service: %v", err)
	}
	defer bridgeService.Stop()

	// Create a channel to receive shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create a context that we'll cancel on shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start listening for events in a goroutine
	events, errors := listener.ListenToDeposits(ctx)
	go func() {
		for {
			select {
			case event := <-events:
				logger.Info("🎉 %s", event.String())
				bridgeService.HandleDeposit(event)
			case err := <-errors:
				if ctx.Err() == nil { // Only log errors if context is not cancelled
					logger.Error("❌ Error from event listener: %v", err)
				}
			case <-ctx.Done():
				logger.Info("Event processing goroutine shutting down...")
				return
			}
		}
	}()

	// Setup HTTP server
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// CORS middleware
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// Register API routes
	handler := api.NewHandler(bridgeService)
	handler.RegisterRoutes(r)

	// Create HTTP server
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	// Start HTTP server in a goroutine
	go func() {
		logger.Info("Starting HTTP server on port %s...", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error: %v", err)
		}
	}()

	logger.Info("✨ Server and event listener started. HTTP server on port %s", cfg.Port)
	logger.Info("Press Ctrl+C to shutdown...")

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutdown signal received...")

	// Create a timeout context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// First cancel the event listener context
	cancel()

	// Then shutdown the HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server forced to shutdown: %v", err)
	}

	logger.Info("Server shutdown complete.")
}
