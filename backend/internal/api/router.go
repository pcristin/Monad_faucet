package api

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes registers all API routes
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Add request counter middleware
	r.Use(func(c *gin.Context) {
		h.GetRequestCounter().Add(1)
		c.Next()
	})

	bridge := r.Group("/bridge")
	// Add health check endpoint
	bridge.GET("/health", h.HealthCheck)

	// Add metrics endpoint (now handled in main.go)
	api := bridge.Group("/api")
	{
		v1 := api.Group("/v1")
		{
			v1.POST("/transaction/status", h.GetTransactionStatus)
			v1.GET("/info", h.GetFaucetInfo)
			v1.GET("/wallet/transactions", h.GetWalletTransactions)

			// Add webhook endpoint for QuickNode
			v1.POST("/webhook/quicknode", h.HandleQuickNodeWebhook)
		}

		// Admin endpoints are registered separately to avoid import cycles

		// Deprecated endpoints
		api.GET("/info", h.GetFaucetInfo)
		api.POST("/tx-status", h.GetTransactionStatus) // Keep for backward compatibility
	}
}
