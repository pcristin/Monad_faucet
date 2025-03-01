package admins

import (
	"fmt"
	"math/big"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// AdminUpdateRatio updates the MON/USD ratio (requires admin authentication)
func (h *AdminHandler) AdminUpdateRatio(c *gin.Context) {
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
	db := h.Handler.GetBridgeService().GetDB()
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
