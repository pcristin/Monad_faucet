package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/monad-labs/monad-faucet/internal/blockchain"
)

func init() {
	// Configure logging to write to stdout with timestamp and file info
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	log.Println("Starting Monad Faucet backend...")

	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found")
	}

	// Get RPC URL from environment
	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		log.Fatal("RPC_URL environment variable is required")
	}
	log.Printf("Using RPC URL: %s", rpcURL)

	// Create event listener
	listener, err := blockchain.NewEventListener(rpcURL)
	if err != nil {
		log.Fatalf("Failed to create event listener: %v", err)
	}
	defer listener.Close()

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
				log.Printf("🎉 %s", event.String())
			case err := <-errors:
				if ctx.Err() == nil { // Only log errors if context is not cancelled
					log.Printf("❌ Error from event listener: %v", err)
				}
			case <-ctx.Done():
				log.Println("Event processing goroutine shutting down...")
				return
			}
		}
	}()

	// Setup HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	gin.SetMode(gin.ReleaseMode) // Switch to release mode
	r := gin.New()               // Use New() instead of Default() for more control
	r.Use(gin.Recovery())        // Add recovery middleware

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

	// Health check endpoint
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	// Create HTTP server
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("Starting HTTP server on port %s...", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("✨ Server and event listener started. HTTP server on port %s", port)
	log.Println("Press Ctrl+C to shutdown...")

	// Wait for shutdown signal
	<-sigChan
	log.Println("\nShutdown signal received...")

	// Create a timeout context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// First cancel the event listener context
	cancel()

	// Then shutdown the HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server forced to shutdown: %v", err)
	}

	log.Println("Server shutdown complete.")
	os.Exit(0)
}
