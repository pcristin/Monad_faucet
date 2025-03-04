package admins

import (
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetMetrics returns system metrics
func (h *AdminHandler) GetMetrics(c *gin.Context) {
	// Check if the request has the admin API key
	apiKey := c.GetHeader("X-Admin-Key")
	if apiKey != h.Handler.GetAdminAPIKey() {
		c.JSON(http.StatusUnauthorized, gin.H{
			"status":  "error",
			"message": "Unauthorized",
		})
		return
	}

	// Get transaction counts
	pendingCount, completedCount, failedCount, refundedCount, err := h.Handler.GetBridgeService().GetTransactionCounts()
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
	if err := h.Handler.GetBridgeService().CheckBlockchainConnections(); err != nil {
		if strings.Contains(err.Error(), "arbitrum") {
			blockchainStatus["arbitrum"] = "disconnected"
		}
		if strings.Contains(err.Error(), "monad") {
			blockchainStatus["monad"] = "disconnected"
		}
	}

	// Build metrics response
	metrics := Metrics{
		Uptime:                time.Since(h.Handler.GetStartTime()).String(),
		TotalRequests:         h.Handler.GetRequestCounter().Load(),
		ActiveConnections:     runtime.NumGoroutine() - h.Handler.GetBaseGoroutines(),
		PendingTransactions:   pendingCount,
		CompletedTransactions: completedCount,
		FailedTransactions:    failedCount,
		RefundedTransactions:  refundedCount,
		CacheSize:             h.Handler.GetResponseCache().ItemCount(),
		MemoryUsage:           memStats.Alloc,
		GoroutineCount:        runtime.NumGoroutine(),
		BlockchainStatus:      blockchainStatus,
	}

	c.JSON(http.StatusOK, metrics)
}
