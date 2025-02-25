package utils

import (
	"errors"
	"math/rand"
	"strings"
	"time"
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

	return errors.New("retry failed: max retries exceeded")
}
