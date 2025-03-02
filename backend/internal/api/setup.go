package api

import (
	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/internal/api/admins"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// SetupWorkerPoolRoutes configures all API routes for the application with worker pool support
func SetupWorkerPoolRoutes(router *gin.Engine, handler *Handler, db *database.DB) {
	// Add logging middleware
	router.Use(RequestLoggingMiddleware())

	// Add CORS middleware
	router.Use(StandardCORSMiddleware())

	// Register main API routes
	handler.RegisterRoutes(router)

	// Register admin routes - using handler directly as it implements the required interface
	admins.RegisterRoutes(router, handler)

	logger.Info("API routes with worker pool support configured successfully")
}

// RequestLoggingMiddleware creates a middleware for request logging
func RequestLoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Process request
		c.Next()

		// Log request details after completion
		path := c.Request.URL.Path

		// Skip health check logging to reduce noise
		if path == "/bridge/health" || path == "/bridge/" {
			return
		}

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()

		logger.Info("REQUEST: %s | %d | %s | %s",
			method, statusCode, clientIP, path)
	}
}

// StandardCORSMiddleware creates a middleware for CORS handling
func StandardCORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
