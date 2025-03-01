package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
	"golang.org/x/time/rate"
)

// IPRateLimiter is a simple rate limiter for IP addresses
type IPRateLimiter struct {
	ips map[string]*rate.Limiter
	mu  *sync.RWMutex
	r   rate.Limit
	b   int
}

// NewIPRateLimiter creates a new rate limiter for IP addresses
func NewIPRateLimiter(r rate.Limit, b int) *IPRateLimiter {
	return &IPRateLimiter{
		ips: make(map[string]*rate.Limiter),
		mu:  &sync.RWMutex{},
		r:   r,
		b:   b,
	}
}

// AddIP adds an IP address to the rate limiter
func (i *IPRateLimiter) AddIP(ip string) *rate.Limiter {
	i.mu.Lock()
	defer i.mu.Unlock()

	limiter := rate.NewLimiter(i.r, i.b)
	i.ips[ip] = limiter
	return limiter
}

// GetLimiter gets the rate limiter for an IP address
func (i *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	i.mu.RLock()
	limiter, exists := i.ips[ip]
	i.mu.RUnlock()

	if !exists {
		return i.AddIP(ip)
	}
	return limiter
}

// Allow checks if the IP is allowed to make a request
func (i *IPRateLimiter) Allow(ip string) bool {
	return i.GetLimiter(ip).Allow()
}

// Handler handles API requests
type Handler struct {
	bridgeService  *blockchain.BridgeService
	rateLimiter    *IPRateLimiter
	startTime      time.Time
	adminAPIKey    string
	requestCounter atomic.Int64
	baseGoroutines int
	responseCache  *cache.Cache // New cache field for responses
}

// NewHandler creates a new API handler
func NewHandler(bridgeService *blockchain.BridgeService) *Handler {
	// Create a rate limiter with 10 requests per minute and burst of 20
	rateLimiter := NewIPRateLimiter(rate.Limit(10/60.0), 20)

	// Record the base number of goroutines at startup
	baseGoroutines := runtime.NumGoroutine()

	// Create a cache with 5-second default expiration and 10-second cleanup interval
	responseCache := cache.New(5*time.Second, 10*time.Second)

	return &Handler{
		bridgeService:  bridgeService,
		rateLimiter:    rateLimiter,
		startTime:      time.Now(),
		adminAPIKey:    os.Getenv("ADMIN_API_KEY"),
		baseGoroutines: baseGoroutines,
		responseCache:  responseCache,
	}
}

// GetBridgeState method removed as it's redundant with GetFaucetInfo

// AdminUpdateRatioRequest represents the request to update MON/USD ratio
type AdminUpdateRatioRequest struct {
	MonUsdRatio string `json:"mon_usd_ratio" binding:"required"` // e.g. "0.1" for 1 MON = 0.1 USD
}

// AdminUpdateRatio updates the MON/USD ratio (requires admin authentication)
func (h *Handler) AdminUpdateRatio(c *gin.Context) {
	// Check admin API key
	apiKey := c.GetHeader("X-Admin-Key")
	if !isValidAdminKey(apiKey) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Invalid admin API key",
		})
		return
	}

	var req AdminUpdateRatioRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request parameters",
		})
		return
	}

	// Parse and validate the new ratio
	newRatio, ok := new(big.Float).SetString(req.MonUsdRatio)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid ratio format",
		})
		return
	}

	// Convert to big.Int with 18 decimals
	multiplier := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	newRatioFloat := new(big.Float).Mul(newRatio, multiplier)

	newRatioInt, _ := newRatioFloat.Int(nil)
	if newRatioInt.Sign() <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Ratio must be positive",
		})
		return
	}

	// Update the ratio
	blockchain.UpdateMonUsdRatio(newRatioInt)

	// Log the admin action in the database
	db := h.bridgeService.GetDB()
	if db != nil {
		// Debug the database type
		logger.Info("Database type in handler: %T", db)

		// Use JSON format instead of key=value format to avoid SQL syntax issues
		params := fmt.Sprintf(`{"new_ratio":"%s"}`, req.MonUsdRatio)
		if err := db.LogAdminAction("update_ratio", params, apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   "MON/USD ratio updated successfully",
		"new_ratio": req.MonUsdRatio,
	})
}

// isValidAdminKey checks if the provided API key is valid
func isValidAdminKey(apiKey string) bool {
	adminKey1 := os.Getenv("ADMIN_API_KEY_1")
	adminKey2 := os.Getenv("ADMIN_API_KEY_2")
	return apiKey == adminKey1 || apiKey == adminKey2
}

// PauseDeposits pauses deposit functionality (requires admin authentication)
func (h *Handler) PauseDeposits(c *gin.Context) {
	// Check admin API key
	apiKey := c.GetHeader("X-Admin-Key")
	if !isValidAdminKey(apiKey) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Invalid admin API key",
		})
		return
	}

	if err := h.bridgeService.PauseDeposits(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to pause deposits: %v", err),
		})
		return
	}

	// Log the admin action in the database
	db := h.bridgeService.GetDB()
	if db != nil {
		if err := db.LogAdminAction("pause_deposits", "", apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Deposits paused successfully",
	})
}

// ResumeDeposits resumes deposit functionality (requires admin authentication)
func (h *Handler) ResumeDeposits(c *gin.Context) {
	// Check admin API key
	apiKey := c.GetHeader("X-Admin-Key")
	if !isValidAdminKey(apiKey) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Invalid admin API key",
		})
		return
	}

	if err := h.bridgeService.ResumeDeposits(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to resume deposits: %v", err),
		})
		return
	}

	// Log the admin action in the database
	db := h.bridgeService.GetDB()
	if db != nil {
		if err := db.LogAdminAction("resume_deposits", "", apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Deposits resumed successfully",
	})
}

// GetFaucetInfo returns simplified faucet information
func (h *Handler) GetFaucetInfo(c *gin.Context) {
	// Try to get cached response first
	cacheKey := "faucetInfo" // Using a simple cache key as this data is the same for all users

	if cachedResponse, found := h.responseCache.Get(cacheKey); found {
		logger.Debug("Using cached faucet info response")
		c.JSON(http.StatusOK, cachedResponse)
		return
	}

	logger.Debug("Cache miss for faucet info, fetching fresh data")

	state, err := h.bridgeService.GetState(c.Request.Context())
	if err != nil {
		logger.Error("Failed to get faucet info: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	// Convert MON balance from wei to MON (divide by 10^18)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	monBalance := new(big.Float).SetInt(state.MonBalance)
	monBalance = new(big.Float).Quo(monBalance, divisor)

	// Calculate wallet limit (percentage of total MON balance)
	var walletLimitText string
	var walletLimit *big.Float

	if blockchain.WalletLimitPercentage == 0 {
		walletLimitText = "No limit"
	} else {
		walletLimitBig := new(big.Int).Mul(state.MonBalance, big.NewInt(blockchain.WalletLimitPercentage))
		walletLimitBig = new(big.Int).Div(walletLimitBig, big.NewInt(100))
		walletLimit = new(big.Float).SetInt(walletLimitBig)
		walletLimit = new(big.Float).Quo(walletLimit, divisor)
		walletLimitText = walletLimit.Text('f', 6)
	}

	// Convert swap ratios to exchange rates
	exchangeRates := make(map[string]string)
	for currency, ratio := range state.SwapRatios {
		if ratio.Sign() > 0 {
			// The contract returns ratio as "MON wei per smallest unit of currency"
			// For USDC/USDT with 1 MON = 0.1 USD, the ratio is 10^18 / 0.1 = 10^19
			// This means 1 smallest unit of USDC (0.000001 USDC) gives 10^19/10^6 = 10^13 MON wei
			// To get USDC per 1 MON, we need: 10^6 / (10^19/10^18) = 10^6 / 10 = 0.1 USDC

			var currencyDecimals int64
			switch currency {
			case blockchain.CurrencyETH:
				currencyDecimals = 18 // ETH has 18 decimals
			case blockchain.CurrencyUSDC, blockchain.CurrencyUSDT:
				currencyDecimals = 6 // USDC/USDT have 6 decimals
			default:
				currencyDecimals = 18
			}

			// For USD-based tokens (USDC/USDT), the ratio directly represents MON/USD ratio
			// For ETH, we need to calculate based on ETH/USD price
			var currencyPerMon *big.Float

			if currency == blockchain.CurrencyUSDC || currency == blockchain.CurrencyUSDT {
				// For USDC/USDT: 1 MON = 0.1 USD, so we need 0.1 USDC/USDT per 1 MON
				// The ratio is 10^18 / monUsdRatio, so to get USDC per MON:
				// 10^6 (USDC decimals) / (10^18 / monUsdRatio) * 10^18 = monUsdRatio / 10^12

				// Get the MON/USD ratio (e.g., 0.1 * 10^18 for 0.1 USD per 1 MON)
				monUsdRatio := blockchain.GetMonUsdRatio()

				// Convert to USDC/USDT per MON
				currencyPerMon = new(big.Float).Quo(
					new(big.Float).SetInt(monUsdRatio),
					new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
				)
			} else {
				// For ETH: Calculate based on the ratio from contract
				// First calculate smallest units per MON
				smallestUnitsPerMon := new(big.Float).Quo(
					new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
					new(big.Float).SetInt(ratio),
				)

				// Then convert to human-readable format
				currencyPerMon = new(big.Float).Quo(
					smallestUnitsPerMon,
					new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(currencyDecimals), nil)),
				)
			}

			// For ETH, use 18 decimals of precision; for other currencies, use 6
			decimals := 6
			if currency == blockchain.CurrencyETH {
				decimals = 18
			}
			exchangeRates[blockchain.CurrencyTypeToString(currency)] = currencyPerMon.Text('f', decimals)
		} else {
			// For ETH, use 18 decimals of precision; for other currencies, use 6
			if currency == blockchain.CurrencyETH {
				exchangeRates[blockchain.CurrencyTypeToString(currency)] = "0.000000000000000000"
			} else {
				exchangeRates[blockchain.CurrencyTypeToString(currency)] = "0.000000"
			}
		}
	}

	// Create the response
	response := gin.H{
		"faucetWorking": !state.IsPaused,
		"faucetReserve": monBalance.Text('f', 6),
		"exchangeRate":  exchangeRates,
		"walletLimit":   walletLimitText,
		"limitType":     "per transaction",
	}

	// Store in cache for future requests
	h.responseCache.Set(cacheKey, response, cache.DefaultExpiration)

	c.JSON(http.StatusOK, response)
}

// AdminUpdateWalletLimitRequest represents the request to update wallet limit percentage
type AdminUpdateWalletLimitRequest struct {
	LimitPercentage int64 `json:"limit_percentage" binding:"required"` // e.g. 30 for 30% of total MON balance
}

// AdminUpdateWalletLimit updates the wallet limit percentage (requires admin authentication)
func (h *Handler) AdminUpdateWalletLimit(c *gin.Context) {
	// Check admin API key
	apiKey := c.GetHeader("X-Admin-Key")
	if !isValidAdminKey(apiKey) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Invalid admin API key",
		})
		return
	}

	var req AdminUpdateWalletLimitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request parameters",
		})
		return
	}

	// Update the wallet limit percentage
	if err := blockchain.UpdateWalletLimitPercentage(req.LimitPercentage); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	// Log the admin action in the database
	db := h.bridgeService.GetDB()
	if db != nil {
		// Use JSON format instead of key=value format to avoid SQL syntax issues
		params := fmt.Sprintf(`{"limit_percentage":%d}`, req.LimitPercentage)
		if err := db.LogAdminAction("update_wallet_limit", params, apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message":          "Wallet limit percentage updated successfully",
		"limit_percentage": req.LimitPercentage,
	})
}

// GetTransactionStatus returns the status of a transaction
func (h *Handler) GetTransactionStatus(c *gin.Context) {
	startTime := time.Now()

	// Save the request body so we can restore it later
	var buf bytes.Buffer
	tee := io.TeeReader(c.Request.Body, &buf)
	requestBody, _ := io.ReadAll(tee)
	c.Request.Body = io.NopCloser(&buf)

	logger := slog.With(
		slog.String("handler", "GetTransactionStatus"),
		slog.String("request_id", c.GetString("request_id")),
		slog.String("raw_body", string(requestBody)),
	)

	// Define the request structure
	var request struct {
		TxHash         string `json:"tx_hash"`
		ArbitrumTxHash string `json:"arbitrum_tx_hash"`
		DepositID      string `json:"deposit_id"`
	}

	// Decode JSON
	if err := c.ShouldBindJSON(&request); err != nil {
		logger.Error("Failed to decode JSON", slog.String("error", err.Error()))
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request format",
			"error":   err.Error(),
		})
		return
	}

	logger = logger.With(
		slog.String("client_ip", c.ClientIP()),
		slog.String("user_agent", c.Request.UserAgent()),
	)

	txHash := request.TxHash
	arbitrumTxHash := request.ArbitrumTxHash
	depositID := request.DepositID

	logger.Info("Transaction status request received",
		slog.String("deposit_id", depositID),
		slog.String("tx_hash", txHash),
		slog.String("arbitrum_tx_hash", arbitrumTxHash),
	)

	// Common response structure
	type TransactionResponse struct {
		Status    string            `json:"status"`
		Message   string            `json:"message"`
		Txs       map[string]string `json:"txs"`
		DepositID string            `json:"deposit_id,omitempty"`
	}

	// Helper function to create response
	createResponse := func(tx *database.Transaction) *TransactionResponse {
		response := &TransactionResponse{
			Status:  tx.Status,
			Message: "Transaction status retrieved successfully",
			Txs:     make(map[string]string),
		}

		if tx.DepositID != nil {
			response.DepositID = tx.DepositID.String()
		}

		// Always include Arbitrum hash if available
		if tx.TxHash != "" {
			response.Txs["Arbitrum"] = tx.TxHash
		}

		// Always include Monad hash if available
		if tx.MonadTxHash != "" {
			response.Txs["Monad"] = tx.MonadTxHash

			logger.Info("Including Monad hash in response",
				slog.String("monad_tx_hash", tx.MonadTxHash),
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("response_txs", fmt.Sprintf("%+v", response.Txs)))
		}

		return response
	}

	// Prioritize depositID for lookup
	if depositID != "" {
		// Parse deposit ID
		depositBigInt, ok := new(big.Int).SetString(depositID, 10)
		if !ok {
			logger.Error("Invalid deposit ID format", slog.String("error", "Failed to parse as big int"))
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "error",
				"message": "Invalid deposit ID format",
				"error":   "Failed to parse deposit ID",
			})
			return
		}

		// Look up transaction by deposit ID
		tx, err := h.bridgeService.GetDB().GetTransactionByDepositID(depositBigInt)
		if err != nil {
			logger.Error("Error looking up transaction", slog.String("error", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Error looking up transaction",
				"error":   err.Error(),
			})
			return
		}

		if tx != nil {
			logger.Info("Transaction found",
				slog.String("status", tx.Status),
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("arbitrum_tx_hash", tx.TxHash),
				slog.String("monad_tx_hash", tx.MonadTxHash),
			)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification for all clients, not just mobile
				status, monadTxHash, err := h.bridgeService.FindMonadTransactionByDepositID(c, tx.DepositID)
				if err != nil {
					logger.Warn("Error checking blockchain for transaction",
						slog.String("error", err.Error()),
						slog.String("deposit_id", tx.DepositID.String()),
					)
				} else if monadTxHash != "" && status == "completed" {
					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
					)

					// Update transaction status in database
					err = h.bridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, "completed", monadTxHash)
					if err != nil {
						logger.Error("Error updating transaction status", slog.String("error", err.Error()))
					} else {
						// Update tx object for response
						tx.Status = "completed"
						tx.MonadTxHash = monadTxHash

						// Verify the update was successful
						verifyTx, verifyErr := h.bridgeService.GetDB().GetTransactionByDepositID(tx.DepositID)
						if verifyErr != nil {
							logger.Error("Error verifying transaction update", slog.String("error", verifyErr.Error()))
						} else if verifyTx.Status != "completed" || verifyTx.MonadTxHash != monadTxHash {
							logger.Error("Transaction update verification failed",
								slog.String("expected_status", "completed"),
								slog.String("actual_status", verifyTx.Status),
								slog.String("expected_hash", monadTxHash),
								slog.String("actual_hash", verifyTx.MonadTxHash))
						} else {
							logger.Info("Transaction update verified successfully",
								slog.String("status", verifyTx.Status),
								slog.String("monad_tx_hash", verifyTx.MonadTxHash))
						}

						// Log that we found and are including the Monad hash
						logger.Info("Including completed Monad transaction in response",
							slog.String("deposit_id", tx.DepositID.String()),
							slog.String("monad_tx_hash", monadTxHash))
					}
				}
			}

			response := createResponse(tx)

			// Debug log the full response for completed transactions
			if tx.Status == "completed" {
				responseJSON, _ := json.Marshal(response)
				logger.Info("Sending completed transaction response",
					slog.String("response_body", string(responseJSON)),
					slog.String("deposit_id", tx.DepositID.String()),
					slog.String("monad_tx_hash", tx.MonadTxHash))
			}

			// Always log the final response JSON for any request
			finalJSON, _ := json.Marshal(response)
			logger.Info("Final API response", slog.String("json", string(finalJSON)))

			c.JSON(http.StatusOK, response)
			logger.Info("Response sent",
				slog.String("duration", time.Since(startTime).String()),
				slog.String("status", tx.Status),
			)
			return
		}

		// Transaction not found for this deposit ID
		response := &TransactionResponse{
			Status:  "not_found",
			Message: "No transaction found for this deposit ID",
			Txs:     make(map[string]string),
		}
		response.DepositID = depositID

		c.JSON(http.StatusOK, response)
		logger.Info("No transaction found for deposit ID",
			slog.String("duration", time.Since(startTime).String()),
		)
		return
	}

	// Handle Monad tx hash lookup
	if txHash != "" {
		logger.Info("Looking up transaction by Monad hash", slog.String("tx_hash", txHash))
		tx, err := h.bridgeService.GetDB().GetTransactionByMonadTxHash(txHash)
		if err != nil {
			logger.Error("Error in DB lookup by Monad hash", slog.String("error", err.Error()))
		} else if tx != nil {
			logger.Info("Found transaction via Monad hash",
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("status", tx.Status),
			)

			response := createResponse(tx)

			// Debug log the full response for completed transactions
			if tx.Status == "completed" {
				responseJSON, _ := json.Marshal(response)
				logger.Info("Sending completed transaction response",
					slog.String("response_body", string(responseJSON)),
					slog.String("deposit_id", tx.DepositID.String()),
					slog.String("monad_tx_hash", tx.MonadTxHash))
			}

			// Always log the final response JSON for any request
			finalJSON, _ := json.Marshal(response)
			logger.Info("Final API response", slog.String("json", string(finalJSON)))

			c.JSON(http.StatusOK, response)
			logger.Info("Response sent",
				slog.String("duration", time.Since(startTime).String()),
				slog.String("status", tx.Status),
			)
			return
		}
	}

	// Handle Arbitrum tx hash lookup
	if arbitrumTxHash != "" {
		logger.Info("Looking up transaction by Arbitrum hash", slog.String("arbitrum_tx_hash", arbitrumTxHash))
		tx, err := h.bridgeService.GetDB().GetTransactionByArbitrumTxHash(arbitrumTxHash)
		if err != nil {
			logger.Error("Error in DB lookup by Arbitrum hash", slog.String("error", err.Error()))
		} else if tx != nil {
			logger.Info("Found transaction via Arbitrum hash",
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("status", tx.Status),
				slog.String("monad_tx_hash", tx.MonadTxHash),
			)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification for all clients, not just mobile
				status, monadTxHash, err := h.bridgeService.FindMonadTransactionByDepositID(c, tx.DepositID)
				if err != nil {
					logger.Warn("Error checking blockchain for transaction",
						slog.String("error", err.Error()),
						slog.String("deposit_id", tx.DepositID.String()),
					)
				} else if monadTxHash != "" && status == "completed" {
					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
					)

					// Update transaction status in database
					err = h.bridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, "completed", monadTxHash)
					if err != nil {
						logger.Error("Error updating transaction status", slog.String("error", err.Error()))
					} else {
						// Update tx object for response
						tx.Status = "completed"
						tx.MonadTxHash = monadTxHash

						// Verify the update was successful
						verifyTx, verifyErr := h.bridgeService.GetDB().GetTransactionByDepositID(tx.DepositID)
						if verifyErr != nil {
							logger.Error("Error verifying transaction update", slog.String("error", verifyErr.Error()))
						} else if verifyTx.Status != "completed" || verifyTx.MonadTxHash != monadTxHash {
							logger.Error("Transaction update verification failed",
								slog.String("expected_status", "completed"),
								slog.String("actual_status", verifyTx.Status),
								slog.String("expected_hash", monadTxHash),
								slog.String("actual_hash", verifyTx.MonadTxHash))
						} else {
							logger.Info("Transaction update verified successfully",
								slog.String("status", verifyTx.Status),
								slog.String("monad_tx_hash", verifyTx.MonadTxHash))
						}

						// Log that we found and are including the Monad hash
						logger.Info("Including completed Monad transaction in response",
							slog.String("deposit_id", tx.DepositID.String()),
							slog.String("monad_tx_hash", monadTxHash))
					}
				}
			}

			response := createResponse(tx)

			// Debug log the full response for completed transactions
			if tx.Status == "completed" {
				responseJSON, _ := json.Marshal(response)
				logger.Info("Sending completed transaction response",
					slog.String("response_body", string(responseJSON)),
					slog.String("deposit_id", tx.DepositID.String()),
					slog.String("monad_tx_hash", tx.MonadTxHash))
			}

			// Always log the final response JSON for any request
			finalJSON, _ := json.Marshal(response)
			logger.Info("Final API response", slog.String("json", string(finalJSON)))

			c.JSON(http.StatusOK, response)
			logger.Info("Response sent",
				slog.String("duration", time.Since(startTime).String()),
				slog.String("status", tx.Status),
			)
			return
		}
	}

	// One final attempt - try direct DB lookup of Arbitrum hash
	logger.Info("Attempting direct DB lookup for transaction")
	hashToCheck := txHash
	if arbitrumTxHash != "" {
		hashToCheck = arbitrumTxHash
	}

	if hashToCheck != "" {
		tx, err := h.bridgeService.GetDB().GetTransactionByArbitrumTxHash(hashToCheck)
		if err != nil {
			logger.Error("Error in direct DB lookup for hash %s: %v", hashToCheck, err)
		} else if tx != nil {
			logger.Info("Found transaction via direct DB lookup: deposit_id=%s, status=%s",
				tx.DepositID.String(), tx.Status)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification for all clients, not just mobile
				status, monadTxHash, err := h.bridgeService.FindMonadTransactionByDepositID(c, tx.DepositID)
				if err != nil {
					logger.Warn("Error checking blockchain for transaction",
						slog.String("error", err.Error()),
						slog.String("deposit_id", tx.DepositID.String()),
					)
				} else if monadTxHash != "" && status == "completed" {
					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
					)

					// Update transaction status in database
					err = h.bridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, "completed", monadTxHash)
					if err != nil {
						logger.Error("Error updating transaction status", slog.String("error", err.Error()))
					} else {
						// Update tx object for response
						tx.Status = "completed"
						tx.MonadTxHash = monadTxHash

						// Verify the update was successful
						verifyTx, verifyErr := h.bridgeService.GetDB().GetTransactionByDepositID(tx.DepositID)
						if verifyErr != nil {
							logger.Error("Error verifying transaction update", slog.String("error", verifyErr.Error()))
						} else if verifyTx.Status != "completed" || verifyTx.MonadTxHash != monadTxHash {
							logger.Error("Transaction update verification failed",
								slog.String("expected_status", "completed"),
								slog.String("actual_status", verifyTx.Status),
								slog.String("expected_hash", monadTxHash),
								slog.String("actual_hash", verifyTx.MonadTxHash))
						} else {
							logger.Info("Transaction update verified successfully",
								slog.String("status", verifyTx.Status),
								slog.String("monad_tx_hash", verifyTx.MonadTxHash))
						}

						// Log that we found and are including the Monad hash
						logger.Info("Including completed Monad transaction in response",
							slog.String("deposit_id", tx.DepositID.String()),
							slog.String("monad_tx_hash", monadTxHash))
					}
				}
			}

			response := createResponse(tx)

			// Debug log the full response for completed transactions
			if tx.Status == "completed" {
				responseJSON, _ := json.Marshal(response)
				logger.Info("Sending completed transaction response",
					slog.String("response_body", string(responseJSON)),
					slog.String("deposit_id", tx.DepositID.String()),
					slog.String("monad_tx_hash", tx.MonadTxHash))
			}

			// Always log the final response JSON for any request
			finalJSON, _ := json.Marshal(response)
			logger.Info("Final API response", slog.String("json", string(finalJSON)))

			c.JSON(http.StatusOK, response)
			logger.Info("Response sent",
				slog.String("duration", time.Since(startTime).String()),
				slog.String("status", tx.Status),
			)
			return
		}
	}

	// Check if it's a refund transaction
	// This would need to be implemented if refunds are tracked separately

	// If we got here, the transaction is not found
	logger.Info("Transaction not found after all lookup attempts")
	response := &TransactionResponse{
		Status:  "not_found",
		Message: "Transaction not found in our system",
		Txs:     make(map[string]string),
	}

	if arbitrumTxHash != "" {
		response.Txs["Arbitrum"] = arbitrumTxHash
	} else if txHash != "" {
		response.Txs["Monad"] = txHash
	}

	// Always log the final response JSON for any request
	finalJSON, _ := json.Marshal(response)
	logger.Info("Final API response", slog.String("json", string(finalJSON)))

	c.JSON(http.StatusOK, response)
	logger.Info("Response sent",
		slog.String("duration", time.Since(startTime).String()),
		slog.String("status", "not_found"),
	)
}

// HealthCheck handles health check requests
func (h *Handler) HealthCheck(c *gin.Context) {
	// Check database connection
	if err := h.bridgeService.CheckDatabaseConnection(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": "Database connection failed",
			"error":   err.Error(),
		})
		return
	}

	// Check blockchain connections
	if err := h.bridgeService.CheckBlockchainConnections(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": "Blockchain connection failed",
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"message": "Service is healthy",
		"version": "1.0.0", // Replace with actual version from build
		"uptime":  time.Since(h.startTime).String(),
	})
}

// Metrics represents system metrics
type Metrics struct {
	Uptime                string            `json:"uptime"`
	TotalRequests         int64             `json:"total_requests"`
	ActiveConnections     int               `json:"active_connections"`
	PendingTransactions   int               `json:"pending_transactions"`
	CompletedTransactions int               `json:"completed_transactions"`
	FailedTransactions    int               `json:"failed_transactions"`
	RefundedTransactions  int               `json:"refunded_transactions"`
	CacheSize             int               `json:"cache_size"`
	MemoryUsage           uint64            `json:"memory_usage"`
	GoroutineCount        int               `json:"goroutine_count"`
	BlockchainStatus      map[string]string `json:"blockchain_status"`
}

// GetMetrics returns system metrics
func (h *Handler) GetMetrics(c *gin.Context) {
	// Check if the request has the admin API key
	apiKey := c.GetHeader("X-API-Key")
	if apiKey != h.adminAPIKey {
		c.JSON(http.StatusUnauthorized, gin.H{
			"status":  "error",
			"message": "Unauthorized",
		})
		return
	}

	// Get transaction counts
	pendingCount, completedCount, failedCount, refundedCount, err := h.bridgeService.GetTransactionCounts()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to get transaction counts",
			"error":   err.Error(),
		})
		return
	}

	// Get memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Get blockchain status
	blockchainStatus := map[string]string{
		"arbitrum": "connected",
		"monad":    "connected",
	}

	// Check blockchain connections
	if err := h.bridgeService.CheckBlockchainConnections(); err != nil {
		if strings.Contains(err.Error(), "arbitrum") {
			blockchainStatus["arbitrum"] = "disconnected"
		}
		if strings.Contains(err.Error(), "monad") {
			blockchainStatus["monad"] = "disconnected"
		}
	}

	// Build metrics response
	metrics := Metrics{
		Uptime:                time.Since(h.startTime).String(),
		TotalRequests:         h.requestCounter.Load(),
		ActiveConnections:     runtime.NumGoroutine() - h.baseGoroutines,
		PendingTransactions:   pendingCount,
		CompletedTransactions: completedCount,
		FailedTransactions:    failedCount,
		RefundedTransactions:  refundedCount,
		CacheSize:             h.bridgeService.GetCacheSize(),
		MemoryUsage:           memStats.Alloc,
		GoroutineCount:        runtime.NumGoroutine(),
		BlockchainStatus:      blockchainStatus,
	}

	c.JSON(http.StatusOK, metrics)
}

// RegisterRoutes registers all API routes
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Add request counter middleware
	r.Use(func(c *gin.Context) {
		h.requestCounter.Add(1)
		c.Next()
	})

	bridge := r.Group("/bridge")
	// Add health check endpoint
	bridge.GET("/health", h.HealthCheck)

	// Add metrics endpoint
	bridge.GET("/metrics", h.GetMetrics)
	api := bridge.Group("/api")
	{

		v1 := api.Group("/v1")
		{
			v1.POST("/transaction/status", h.GetTransactionStatus)
			v1.GET("/info", h.GetFaucetInfo)
		}

		// Admin endpoints
		api.POST("/admin/ratio", h.AdminUpdateRatio)
		api.POST("/admin/pause", h.PauseDeposits)
		api.POST("/admin/resume", h.ResumeDeposits)
		api.POST("/admin/wallet-limit", h.AdminUpdateWalletLimit)

		// Deprecated endpoints
		api.GET("/info", h.GetFaucetInfo)
		api.POST("/tx-status", h.GetTransactionStatus) // Keep for backward compatibility
	}
}
