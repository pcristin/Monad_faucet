package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                 string
	ArbRpcURL            string
	MonadRpcURL          string
	ArbDepositorAddr     string
	MonadDistributorAddr string
	WalletPrivateKey     string
	AdminAPIKeys         []string
	DataDir              string // Directory to store SQLite database
}

func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: .env file not found\n")
	}

	// Determine the appropriate data directory
	dataDir := getEnvOrDefault("DATA_DIR", "")
	if dataDir == "" {
		// Default locations based on environment
		if os.Getenv("RENDER") != "" {
			// If running on Render, use /var/data if available (requires paid plan with disk)
			if _, err := os.Stat("/var/data"); err == nil {
				dataDir = "/var/data"
			} else {
				// For free tier, use /tmp which is ephemeral
				dataDir = "/tmp"
				fmt.Printf("Warning: Using ephemeral /tmp directory for database on Render free tier\n")
				fmt.Printf("Note: Database will be reset on service restart\n")
			}
		} else {
			// Local development - use a data directory in the current working directory
			dataDir = "./data"
		}
	}

	cfg := &Config{
		Port:                 getEnvOrDefault("PORT", "8080"),
		ArbRpcURL:            getEnvOrFatal("ARB_RPC_URL"),
		MonadRpcURL:          getEnvOrFatal("MONAD_RPC_URL"),
		ArbDepositorAddr:     getEnvOrDefault("ARB_DEPOSITOR_ADDRESS", "0xYourDepositorAddressHere"),
		MonadDistributorAddr: getEnvOrDefault("MONAD_DISTRIBUTOR_ADDRESS", "0xYourDistributorAddressHere"),
		WalletPrivateKey:     strings.TrimPrefix(getEnvOrFatal("WALLET_PRIVATE_KEY"), "0x"),
		AdminAPIKeys: []string{
			getEnvOrFatal("ADMIN_API_KEY_1"),
			getEnvOrDefault("ADMIN_API_KEY_2", ""), // Optional second key
		},
		DataDir: dataDir,
	}

	return cfg, nil
}

func getEnvOrFatal(key string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	fmt.Printf("Error: %s environment variable is required\n", key)
	os.Exit(1)
	return ""
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
