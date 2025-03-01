package admins

import (
	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/internal/core"
)

// RegisterRoutes registers all admin API routes
func RegisterRoutes(r *gin.Engine, handler core.HandlerInterface) {
	adminHandler := NewAdminHandler(handler)

	// Add metrics endpoint
	r.GET("/bridge/metrics", adminHandler.GetMetrics)

	// Admin endpoints
	adminGroup := r.Group("/bridge/api")
	{
		adminGroup.POST("/admin/ratio", adminHandler.AdminUpdateRatio)
		adminGroup.POST("/admin/pause", adminHandler.PauseDeposits)
		adminGroup.POST("/admin/resume", adminHandler.ResumeDeposits)
		adminGroup.POST("/admin/wallet-limit", adminHandler.AdminUpdateWalletLimit)
	}
}
