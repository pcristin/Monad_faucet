package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all application configuration
type Config struct {
	ServerAddr           string           // HTTP server address with port
	DatabaseURL          string           // PostgreSQL connection string
	ArbRpcURL            string           // Arbitrum RPC URL
	MonadRpcURL          string           // Monad RPC URL
	ArbDepositorAddr     string           // Arbitrum depositor contract address
	MonadDistributorAddr string           // Monad distributor contract address
	ChainlinkEthUsdFeed  string           // Chainlink ETH/USD price feed contract address
	WalletPrivateKey     string           // Private key for transaction signing
	AdminAPIKeys         []string         // API keys for admin endpoints
	AdminPasswords       []string         // Passwords for admin auth
	LogLevel             string           // Logging level
	WorkerPoolConfig     WorkerPoolConfig // Configuration for worker pools
	UseQuickNodeWebhook  bool             // Flag to use QuickNode webhook for distribution events instead of polling
}

// WorkerPoolConfig holds configuration for all worker pools
type WorkerPoolConfig struct {
	DepositWorkers      int // Number of deposit processing workers
	CalculationWorkers  int // Number of calculation workers
	DistributionWorkers int // Number of distribution workers
	DBWorkers           int // Number of database workers
}

// Load reads configuration from environment variables
func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: .env file not found\n")
	}

	// Get database URL - check multiple environment variables for flexibility
	dbURL := getEnvOrDefault("DATABASE_URL", "")
	if dbURL == "" {
		// Try alternative environment variables
		dbURL = getEnvOrDefault("POSTGRES_URL", "")
	}
	if dbURL == "" {
		// Build from individual components if available
		host := getEnvOrDefault("DB_HOST", "localhost")
		port := getEnvOrDefault("DB_PORT", "5432")
		user := getEnvOrDefault("DB_USER", "postgres")
		password := getEnvOrDefault("DB_PASSWORD", "postgres")
		dbname := getEnvOrDefault("DB_NAME", "bridgedb")
		sslmode := getEnvOrDefault("DB_SSLMODE", "disable")

		dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
			user, password, host, port, dbname, sslmode)
	}

	// Get worker pool configuration
	depositWorkers := getEnvAsIntOrDefault("DEPOSIT_WORKERS", 5)
	calculationWorkers := getEnvAsIntOrDefault("CALCULATION_WORKERS", 3)
	distributionWorkers := getEnvAsIntOrDefault("DISTRIBUTION_WORKERS", 5)
	dbWorkers := getEnvAsIntOrDefault("DB_WORKERS", 2)

	// Get server address with port
	port := getEnvOrDefault("PORT", "8080")
	serverAddr := getEnvOrDefault("SERVER_ADDR", ":"+port)

	// Admin API keys and passwords
	adminAPIKeys := []string{
		getEnvOrDefault("ADMIN_API_KEY_1", ""),
		getEnvOrDefault("ADMIN_API_KEY_2", ""), // Optional second key
	}

	// Filter out empty API keys
	var filteredAPIKeys []string
	for _, key := range adminAPIKeys {
		if key != "" {
			filteredAPIKeys = append(filteredAPIKeys, key)
		}
	}

	// Admin passwords
	adminPasswords := []string{
		getEnvOrDefault("ADMIN_PASSWORD_1", ""),
		getEnvOrDefault("ADMIN_PASSWORD_2", ""), // Optional second password
	}

	// Filter out empty passwords
	var filteredPasswords []string
	for _, pw := range adminPasswords {
		if pw != "" {
			filteredPasswords = append(filteredPasswords, pw)
		}
	}

	cfg := &Config{
		ServerAddr:           serverAddr,
		DatabaseURL:          dbURL,
		ArbRpcURL:            getEnvOrFatal("ARB_RPC_URL"),
		MonadRpcURL:          getEnvOrFatal("MONAD_RPC_URL"),
		ArbDepositorAddr:     getEnvOrDefault("ARB_DEPOSITOR_ADDRESS", "0x487177C3278FAA36dd317DBB4CA97425a4F4Ee31"),
		MonadDistributorAddr: getEnvOrDefault("MONAD_DISTRIBUTOR_ADDRESS", "0xc11350Fd29aC48181b0117bd1935dBE781cdd03d"),
		ChainlinkEthUsdFeed:  getEnvOrDefault("CHAINLINK_ETH_USD_FEED", "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"),
		WalletPrivateKey:     strings.TrimPrefix(getEnvOrFatal("WALLET_PRIVATE_KEY"), "0x"),
		AdminAPIKeys:         filteredAPIKeys,
		AdminPasswords:       filteredPasswords,
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
		WorkerPoolConfig: WorkerPoolConfig{
			DepositWorkers:      depositWorkers,
			CalculationWorkers:  calculationWorkers,
			DistributionWorkers: distributionWorkers,
			DBWorkers:           dbWorkers,
		},
		UseQuickNodeWebhook: getEnvAsBoolOrDefault("USE_QUICKNODE_WEBHOOK", false),
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

func getEnvAsIntOrDefault(key string, defaultValue int) int {
	strValue := getEnvOrDefault(key, fmt.Sprintf("%d", defaultValue))
	var value int
	if _, err := fmt.Sscanf(strValue, "%d", &value); err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsBoolOrDefault(key string, defaultValue bool) bool {
	strValue := os.Getenv(key)
	if strValue == "" {
		return defaultValue
	}

	// Convert to lowercase for more reliable parsing
	strValue = strings.ToLower(strValue)

	// Check for true values
	if strValue == "true" || strValue == "1" || strValue == "yes" || strValue == "y" || strValue == "on" {
		return true
	}

	// Check for false values
	if strValue == "false" || strValue == "0" || strValue == "no" || strValue == "n" || strValue == "off" {
		return false
	}

	// If not recognized, return default
	return defaultValue
}

// Validate checks if all required configuration values are present
func (c *Config) Validate() error {
	var missingVars []string

	// Check required environment variables
	if c.WalletPrivateKey == "" {
		missingVars = append(missingVars, "WALLET_PRIVATE_KEY")
	}
	if c.ArbRpcURL == "" {
		missingVars = append(missingVars, "ARB_RPC_URL")
	}
	if c.MonadRpcURL == "" {
		missingVars = append(missingVars, "MONAD_RPC_URL")
	}
	if c.ArbDepositorAddr == "0xYourDepositorAddressHere" || c.ArbDepositorAddr == "" {
		missingVars = append(missingVars, "ARB_DEPOSITOR_ADDRESS")
	}
	if c.MonadDistributorAddr == "0xYourDistributorAddressHere" || c.MonadDistributorAddr == "" {
		missingVars = append(missingVars, "MONAD_DISTRIBUTOR_ADDRESS")
	}
	if c.ChainlinkEthUsdFeed == "0xYourChainlinkContractAddressHere" || c.ChainlinkEthUsdFeed == "" {
		missingVars = append(missingVars, "CHAINLINK_ETH_USD_FEED")
	}

	if len(missingVars) > 0 {
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missingVars, ", "))
	}

	return nil
}
