package api

import (
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/pcristin/monad-faucet/internal/api/admins"
	"github.com/pcristin/monad-faucet/internal/bridge"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// Handler represents the API handler
type Handler struct {
	DB             *database.DB
	BridgeService  *bridge.BridgeService
	StartTime      interface{}
	RequestCounter interface{}
	AdminAPIKey    string
	BaseGoroutines interface{}
	ResponseCache  interface{}
}

// NewHandler creates a new API handler
func NewHandler(db *database.DB, bridgeService *bridge.BridgeService) *Handler {
	return &Handler{
		DB:             db,
		BridgeService:  bridgeService,
		StartTime:      time.Now(),
		RequestCounter: &atomic.Int64{},
		AdminAPIKey:    "", // Set this from environment if needed
		BaseGoroutines: 0,
		ResponseCache:  cache.New(5*time.Second, 10*time.Second),
	}
}

// SetupRoutes configures all API routes for the application
func SetupRoutes(router *gin.Engine, handler *Handler, db *database.DB) {
	// Add logging middleware
	router.Use(LoggingMiddleware())

	// Add CORS middleware
	router.Use(CORSMiddleware())

	// Register main API routes
	handler.RegisterRoutes(router)

	// Register admin routes
	admins.RegisterRoutes(router, handler)

	logger.Info("API routes configured successfully")
}

// LoggingMiddleware creates a middleware for request logging
func LoggingMiddleware() gin.HandlerFunc {
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

// CORSMiddleware creates a middleware for CORS handling
func CORSMiddleware() gin.HandlerFunc {
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

// GetBridgeService returns the bridge service
func (h *Handler) GetBridgeService() *bridge.BridgeService {
	return h.BridgeService
}

// GetAdminAPIKey returns the admin API key
func (h *Handler) GetAdminAPIKey() string {
	return h.AdminAPIKey
}

// GetStartTime returns the start time
func (h *Handler) GetStartTime() time.Time {
	return h.StartTime.(time.Time)
}

// GetRequestCounter returns the request counter
func (h *Handler) GetRequestCounter() *atomic.Int64 {
	return h.RequestCounter.(*atomic.Int64)
}

// GetBaseGoroutines returns the base goroutines count
func (h *Handler) GetBaseGoroutines() int {
	return h.BaseGoroutines.(int)
}

// GetResponseCache returns the response cache
func (h *Handler) GetResponseCache() *cache.Cache {
	return h.ResponseCache.(*cache.Cache)
}
