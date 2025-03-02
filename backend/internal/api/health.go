package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HealthCheck handles health check requests
func (h *Handler) HealthCheck(c *gin.Context) {
	// Check database connection
	if err := h.BridgeService.CheckDatabaseConnection(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": "Database connection failed",
			"error":   err.Error(),
		})
		return
	}

	// Check blockchain connections
	if err := h.BridgeService.CheckBlockchainConnections(); err != nil {
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
		"uptime":  time.Since(h.GetStartTime()).String(),
	})
}
