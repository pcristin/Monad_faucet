package core

import (
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/pcristin/monad-faucet/internal/bridge"
)

// HandlerInterface defines the common interface needed by admin handlers
type HandlerInterface interface {
	GetBridgeService() *bridge.BridgeService
	GetAdminAPIKey() string
	GetStartTime() time.Time
	GetRequestCounter() *atomic.Int64
	GetBaseGoroutines() int
	GetResponseCache() *cache.Cache
}

// BaseHandler contains common fields used by various handlers
type BaseHandler struct {
	BridgeService  *bridge.BridgeService
	StartTime      time.Time
	AdminAPIKey    string
	RequestCounter atomic.Int64
	BaseGoroutines int
	ResponseCache  *cache.Cache
}

// GetBridgeService returns the bridge service
func (h *BaseHandler) GetBridgeService() *bridge.BridgeService {
	return h.BridgeService
}

// GetAdminAPIKey returns the admin API key
func (h *BaseHandler) GetAdminAPIKey() string {
	return h.AdminAPIKey
}

// GetStartTime returns the start time
func (h *BaseHandler) GetStartTime() time.Time {
	return h.StartTime
}

// GetRequestCounter returns the request counter
func (h *BaseHandler) GetRequestCounter() *atomic.Int64 {
	return &h.RequestCounter
}

// GetBaseGoroutines returns the base goroutines count
func (h *BaseHandler) GetBaseGoroutines() int {
	return h.BaseGoroutines
}

// GetResponseCache returns the response cache
func (h *BaseHandler) GetResponseCache() *cache.Cache {
	return h.ResponseCache
}
