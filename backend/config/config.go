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
}

func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: .env file not found\n")
	}

	cfg := &Config{
		Port:                 getEnvOrDefault("PORT", "8080"),
		ArbRpcURL:            getEnvOrFatal("ARB_RPC_URL"),
		MonadRpcURL:          getEnvOrFatal("MONAD_RPC_URL"),
		ArbDepositorAddr:     getEnvOrFatal("ARB_DEPOSITOR_ADDRESS"),
		MonadDistributorAddr: getEnvOrFatal("MONAD_DISTRIBUTOR_ADDRESS"),
		WalletPrivateKey:     strings.TrimPrefix(getEnvOrFatal("WALLET_PRIVATE_KEY"), "0x"),
		AdminAPIKeys: []string{
			getEnvOrFatal("ADMIN_API_KEY_1"),
			getEnvOrDefault("ADMIN_API_KEY_2", ""), // Optional second key
		},
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
