package admins

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// AdminUpdateWalletLimit updates the wallet limit percentage (requires admin authentication)
func (h *AdminHandler) AdminUpdateWalletLimit(c *gin.Context) {
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
	if err := h.Handler.GetBridgeService().UpdateWalletLimitPercentage(req.LimitPercentage); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	// Log the admin action in the database
	db := h.Handler.GetBridgeService().GetDB()
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
