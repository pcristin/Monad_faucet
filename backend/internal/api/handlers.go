package api

import (
	"fmt"
	"math/big"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/internal/blockchain"
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
}

// NewHandler creates a new API handler
func NewHandler(bridgeService *blockchain.BridgeService) *Handler {
	// Create a rate limiter with 10 requests per minute and burst of 20
	rateLimiter := NewIPRateLimiter(rate.Limit(10/60.0), 20)

	// Record the base number of goroutines at startup
	baseGoroutines := runtime.NumGoroutine()

	return &Handler{
		bridgeService:  bridgeService,
		rateLimiter:    rateLimiter,
		startTime:      time.Now(),
		adminAPIKey:    os.Getenv("ADMIN_API_KEY"),
		baseGoroutines: baseGoroutines,
	}
}

// GetBridgeState returns the current state of the bridge
func (h *Handler) GetBridgeState(c *gin.Context) {
	state, err := h.bridgeService.GetState(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	// Convert big.Int values to strings for JSON response
	swapRatios := make(map[string]string)
	for currency, ratio := range state.SwapRatios {
		swapRatios[blockchain.CurrencyTypeToString(currency)] = ratio.String()
	}

	c.JSON(http.StatusOK, gin.H{
		"is_paused":   state.IsPaused,
		"min_amount":  state.MinAmount.String(),
		"mon_balance": state.MonBalance.String(),
		"swap_ratios": swapRatios,
	})
}

type EstimateSwapRequest struct {
	Amount   string `json:"amount" binding:"required"`
	Currency string `json:"currency" binding:"required,oneof=ETH USDC USDT"`
}

// EstimateSwap calculates the amount of MON tokens to be received
func (h *Handler) EstimateSwap(c *gin.Context) {
	var req EstimateSwapRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request parameters",
		})
		return
	}

	// Parse amount
	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid amount format",
		})
		return
	}

	// Get current state
	state, err := h.bridgeService.GetState(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	// Convert currency string to type
	var currencyType blockchain.CurrencyType
	switch req.Currency {
	case "ETH":
		currencyType = blockchain.CurrencyETH
	case "USDC":
		currencyType = blockchain.CurrencyUSDC
	case "USDT":
		currencyType = blockchain.CurrencyUSDT
	}

	// Calculate MON amount
	swapRatio := state.SwapRatios[currencyType]
	if swapRatio == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid currency",
		})
		return
	}

	monAmount := new(big.Int).Mul(amount, swapRatio)

	c.JSON(http.StatusOK, gin.H{
		"input_amount":   req.Amount,
		"input_currency": req.Currency,
		"mon_amount":     monAmount.String(),
		"swap_ratio":     swapRatio.String(),
	})
}

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
	if h.bridgeService != nil && h.bridgeService.GetDB() != nil {
		// Use JSON format instead of key=value format to avoid SQL syntax issues
		params := fmt.Sprintf(`{"new_ratio":"%s"}`, req.MonUsdRatio)
		if err := h.bridgeService.GetDB().LogAdminAction("update_ratio", params, apiKey); err != nil {
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
	if h.bridgeService != nil && h.bridgeService.GetDB() != nil {
		if err := h.bridgeService.GetDB().LogAdminAction("pause_deposits", "", apiKey); err != nil {
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
	if h.bridgeService != nil && h.bridgeService.GetDB() != nil {
		if err := h.bridgeService.GetDB().LogAdminAction("resume_deposits", "", apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Deposits resumed successfully",
	})
}

// GetFaucetInfo returns simplified faucet information
func (h *Handler) GetFaucetInfo(c *gin.Context) {
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

	// For each currency, convert the swap ratio to exchange rates
	for currency, ratio := range state.SwapRatios {
		if ratio.Sign() > 0 {
			// The swap ratio is MON per 1 unit of currency
			// We need to invert it to get how much of the currency equals 1 MON
			// First convert ratio to big.Float for precision
			ratioFloat := new(big.Float).SetInt(ratio)
			ratioFloat = new(big.Float).Quo(ratioFloat, divisor)

			// Calculate currency needed for 1 MON (1/ratio)
			one := new(big.Float).SetInt64(1)
			exchangeRateFloat := new(big.Float).Quo(one, ratioFloat)

			currencyString := blockchain.CurrencyTypeToString(currency)
			// Format to 6 decimal places
			exchangeRates[currencyString] = exchangeRateFloat.Text('f', 6)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"faucetWorking": !state.IsPaused,
		"faucetReserve": monBalance.Text('f', 6),
		"exchangeRate":  exchangeRates,
		"walletLimit":   walletLimitText,
		"limitType":     "per transaction",
	})
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
	if h.bridgeService != nil && h.bridgeService.GetDB() != nil {
		// Use JSON format instead of key=value format to avoid SQL syntax issues
		params := fmt.Sprintf(`{"limit_percentage":%d}`, req.LimitPercentage)
		if err := h.bridgeService.GetDB().LogAdminAction("update_wallet_limit", params, apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message":          "Wallet limit percentage updated successfully",
		"limit_percentage": req.LimitPercentage,
	})
}

// GetTransactionStatusRequest represents the request to get transaction status
type GetTransactionStatusRequest struct {
	ArbitrumTxHash string `json:"arbitrum_tx_hash" binding:"required"`
}

// TransactionResponse represents the response with transaction details
type TransactionResponse struct {
	Status  string            `json:"status"`
	Message string            `json:"message"`
	Txs     map[string]string `json:"txs"`
}

// validateTxHash validates the format of a transaction hash
func validateTxHash(txHash string) error {
	// Check if the hash starts with 0x
	if len(txHash) < 2 || txHash[:2] != "0x" {
		return fmt.Errorf("transaction hash must start with 0x")
	}

	// Check if the hash has the correct length (0x + 64 hex chars)
	if len(txHash) != 66 {
		return fmt.Errorf("transaction hash must be 66 characters long (including 0x prefix)")
	}

	// Check if the hash contains only hex characters
	for _, c := range txHash[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("transaction hash must contain only hexadecimal characters")
		}
	}

	return nil
}

// GetTransactionStatus returns the status of a transaction
func (h *Handler) GetTransactionStatus(c *gin.Context) {
	// Start timing the request for logging
	startTime := time.Now()

	// Get client IP for rate limiting
	clientIP := c.ClientIP()

	// Simple in-memory rate limiting (can be replaced with Redis in production)
	if !h.rateLimiter.Allow(clientIP) {
		c.JSON(http.StatusTooManyRequests, gin.H{
			"status":  "error",
			"message": "Rate limit exceeded. Please try again later.",
		})
		return
	}

	var req GetTransactionStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request parameters",
		})
		return
	}

	// Validate transaction hash format
	if err := validateTxHash(req.ArbitrumTxHash); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Invalid transaction hash: %v", err),
		})
		return
	}

	// Log the request
	logger.Info("Transaction status request: tx_hash=%s, client_ip=%s, user_agent=%s",
		req.ArbitrumTxHash,
		clientIP,
		c.Request.UserAgent())

	// Get transaction from database by deposit ID
	depositID, err := h.bridgeService.GetDepositIDFromTxHash(c.Request.Context(), req.ArbitrumTxHash)
	if err != nil {
		logger.Warn("Transaction not found: tx_hash=%s, error=%s",
			req.ArbitrumTxHash,
			err.Error())

		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Transaction not found: %v", err),
		})
		return
	}

	// Get transaction details
	tx, err := h.bridgeService.GetTransactionByDepositID(c.Request.Context(), depositID)
	if err != nil {
		logger.Error("Failed to get transaction details for deposit ID %s: %v",
			depositID.String(), err)

		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to get transaction details: %v", err),
		})
		return
	}

	// Prepare response
	response := TransactionResponse{
		Txs: make(map[string]string),
	}

	// Add Arbitrum deposit transaction hash
	response.Txs["Arbitrum"] = req.ArbitrumTxHash

	// Set status and message based on transaction status
	switch tx.Status {
	case "completed":
		response.Status = "success"
		response.Message = "MON distribution successful"
		response.Txs["Monad"] = tx.TxHash
	case "failed":
		response.Status = "error"
		response.Message = "Transaction execution reverted"
	case "refunded":
		response.Status = "refunded"
		response.Message = "Deposit was successful, but MON couldn't be distributed"
		response.Txs["Arbitrum refund"] = tx.TxHash
	case "pending":
		response.Status = "pending"
		response.Message = "Transaction is still being processed"
	default:
		response.Status = "unknown"
		response.Message = "Unknown transaction status"
	}

	// Log the response
	logger.Info("Transaction status response: tx_hash=%s, deposit_id=%s, status=%s, duration_ms=%d",
		req.ArbitrumTxHash,
		depositID.String(),
		response.Status,
		time.Since(startTime).Milliseconds())

	c.JSON(http.StatusOK, response)
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

	// Add health check endpoint
	r.GET("/health", h.HealthCheck)

	// Add metrics endpoint
	r.GET("/metrics", h.GetMetrics)

	api := r.Group("/api")
	{
		api.GET("/info", h.GetFaucetInfo)
		api.GET("/state", h.GetBridgeState)
		api.POST("/estimate", h.EstimateSwap)

		// Transaction status endpoints
		api.POST("/tx-status", h.GetTransactionStatus) // Keep for backward compatibility

		v1 := api.Group("/v1")
		{
			v1.POST("/transaction/status", h.GetTransactionStatus)
		}

		// Admin endpoints
		api.POST("/admin/ratio", h.AdminUpdateRatio)
		api.POST("/admin/pause", h.PauseDeposits)
		api.POST("/admin/resume", h.ResumeDeposits)
		api.POST("/admin/wallet-limit", h.AdminUpdateWalletLimit)
	}
}
