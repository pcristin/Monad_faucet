package api

import (
	"fmt"
	"math/big"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

type Handler struct {
	bridgeService *blockchain.BridgeService
}

func NewHandler(bridgeService *blockchain.BridgeService) *Handler {
	return &Handler{
		bridgeService: bridgeService,
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

	// Calculate wallet limit (30% of total MON balance)
	walletLimitBig := new(big.Int).Mul(state.MonBalance, big.NewInt(blockchain.WalletLimitPercentage))
	walletLimitBig = new(big.Int).Div(walletLimitBig, big.NewInt(100))
	walletLimit := new(big.Float).SetInt(walletLimitBig)
	walletLimit = new(big.Float).Quo(walletLimit, divisor)

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
		"faucetWorking":    !state.IsPaused,
		"faucetReserve":    monBalance.Text('f', 6),
		"exchangeRate":     exchangeRates,
		"walletDailyLimit": walletLimit.Text('f', 6),
		"limitDuration":    "24 hours",
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

	c.JSON(http.StatusOK, gin.H{
		"message":          "Wallet limit percentage updated successfully",
		"limit_percentage": req.LimitPercentage,
	})
}

// RegisterRoutes registers all API routes
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api")
	{
		api.GET("/info", h.GetFaucetInfo)
		api.GET("/state", h.GetBridgeState)
		api.POST("/estimate", h.EstimateSwap)

		// Admin endpoints
		api.POST("/admin/ratio", h.AdminUpdateRatio)
		api.POST("/admin/pause", h.PauseDeposits)
		api.POST("/admin/resume", h.ResumeDeposits)
		api.POST("/admin/wallet-limit", h.AdminUpdateWalletLimit)
	}
}
