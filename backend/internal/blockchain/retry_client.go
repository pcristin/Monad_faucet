package blockchain

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// RetryConfig defines the configuration for retry operations
type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	RandomFactor    float64
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:      5,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		RandomFactor:    0.1,
	}
}

// Cache represents a simple time-based cache
type Cache struct {
	data       map[string]cacheItem
	mu         sync.RWMutex
	defaultTTL time.Duration
}

type cacheItem struct {
	value     interface{}
	expiresAt time.Time
}

// NewCache creates a new cache with the specified default TTL
func NewCache(defaultTTL time.Duration) *Cache {
	return &Cache{
		data:       make(map[string]cacheItem),
		defaultTTL: defaultTTL,
	}
}

// Set adds or updates a value in the cache with the default TTL
func (c *Cache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL adds or updates a value in the cache with a specific TTL
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = cacheItem{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

// Get retrieves a value from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.data[key]
	if !exists {
		return nil, false
	}

	// Check if the item has expired
	if time.Now().After(item.expiresAt) {
		return nil, false
	}

	return item.value, true
}

// RetryClient wraps an ethclient.Client with retry logic
type RetryClient struct {
	client *ethclient.Client
	config RetryConfig
	cache  *Cache
}

// NewRetryClient creates a new RetryClient
func NewRetryClient(client *ethclient.Client) *RetryClient {
	return &RetryClient{
		client: client,
		config: DefaultRetryConfig(),
		cache:  NewCache(30 * time.Second), // 30 second cache for common queries
	}
}

// Client returns the underlying ethclient.Client
func (rc *RetryClient) Client() *ethclient.Client {
	return rc.client
}

// IsRateLimitError checks if an error is an Alchemy rate limit error
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "exceeded its compute units") ||
		strings.Contains(errMsg, "429") ||
		strings.Contains(errMsg, "Too Many Requests")
}

// RetryWithBackoff executes the given operation with exponential backoff
func RetryWithBackoff(operation func() error, config RetryConfig) error {
	var err error
	currentInterval := config.InitialInterval

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Execute the operation
		err = operation()

		// If no error or it's not a rate limit error, return immediately
		if err == nil || !IsRateLimitError(err) {
			return err
		}

		// Last attempt, return the error
		if attempt == config.MaxRetries {
			return err
		}

		// Add jitter to prevent thundering herd
		jitter := rand.Float64() * config.RandomFactor * float64(currentInterval)
		sleepTime := time.Duration(float64(currentInterval) + jitter)

		// Sleep before next attempt
		time.Sleep(sleepTime)

		// Increase the interval for next attempt
		currentInterval = time.Duration(float64(currentInterval) * config.Multiplier)
		if currentInterval > config.MaxInterval {
			currentInterval = config.MaxInterval
		}
	}

	return fmt.Errorf("retry failed: max retries exceeded")
}

// CallWithRetry executes a contract call with retry logic
func (rc *RetryClient) CallWithRetry(ctx context.Context, contract *bind.BoundContract, result *[]interface{}, method string, args ...interface{}) error {
	// Create a cache key for this call if it's cacheable
	cacheKey := ""
	if isCacheableMethod(method) {
		cacheKey = fmt.Sprintf("%s-%v", method, args)

		// Check cache first
		if cachedResult, found := rc.cache.Get(cacheKey); found {
			*result = cachedResult.([]interface{})
			return nil
		}
	}

	operation := func() error {
		err := contract.Call(&bind.CallOpts{Context: ctx}, result, method, args...)
		if err != nil {
			logger.Info("Contract call to %s failed (will retry if rate limit): %v", method, err)
			return err
		}
		return nil
	}

	err := RetryWithBackoff(operation, rc.config)

	// If successful and cacheable, store in cache
	if err == nil && cacheKey != "" {
		rc.cache.Set(cacheKey, *result)
	}

	return err
}

// BalanceAtWithRetry gets the balance with retry logic
func (rc *RetryClient) BalanceAtWithRetry(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("balance-%s-%v", account.Hex(), blockNumber)
	if cachedBalance, found := rc.cache.Get(cacheKey); found {
		return cachedBalance.(*big.Int), nil
	}

	var balance *big.Int

	operation := func() error {
		var err error
		balance, err = rc.client.BalanceAt(ctx, account, blockNumber)
		if err != nil {
			logger.Info("BalanceAt failed (will retry if rate limit): %v", err)
			return err
		}
		return nil
	}

	err := RetryWithBackoff(operation, rc.config)

	// If successful, store in cache
	if err == nil {
		rc.cache.Set(cacheKey, balance)
	}

	return balance, err
}

// FilterLogsWithRetry filters logs with retry logic
func (rc *RetryClient) FilterLogsWithRetry(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var logs []types.Log

	operation := func() error {
		var err error
		logs, err = rc.client.FilterLogs(ctx, query)
		if err != nil {
			logger.Info("FilterLogs failed (will retry if rate limit): %v", err)
			return err
		}
		return nil
	}

	err := RetryWithBackoff(operation, rc.config)
	return logs, err
}

// Helper function to determine if a method call should be cached
func isCacheableMethod(method string) bool {
	// List of methods that are safe to cache
	cacheableMethods := map[string]bool{
		"paused":        true,
		"minEthDeposit": true,
		// Add other cacheable methods here
	}

	return cacheableMethods[method]
}
