package admins

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// PauseDeposits pauses deposit functionality (requires admin authentication)
func (h *AdminHandler) PauseDeposits(c *gin.Context) {
	// Check admin API key
	apiKey := c.GetHeader("X-Admin-Key")
	if !isValidAdminKey(apiKey) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Invalid admin API key",
		})
		return
	}

	if err := h.Handler.GetBridgeService().PauseDeposits(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to pause deposits: %v", err),
		})
		return
	}

	// Log the admin action in the database
	db := h.Handler.GetBridgeService().GetDB()
	if db != nil {
		if err := db.LogAdminAction("pause_deposits", "", apiKey); err != nil {
			logger.Error("Failed to log admin action: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Deposits paused successfully",
	})
}
