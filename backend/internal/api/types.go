package api

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/pcristin/monad-faucet/internal/bridge"
	"github.com/pcristin/monad-faucet/internal/core"
	"golang.org/x/time/rate"
)

type IPRateLimiter struct {
	ips map[string]*rate.Limiter
	mu  *sync.RWMutex
	r   rate.Limit
	b   int
}

type Handler struct {
	core.BaseHandler
	RateLimiter *IPRateLimiter
}

// NewHandler creates a new API handler
func NewHandler(bridgeService *bridge.BridgeService) *Handler {
	// Create a rate limiter with 10 requests per minute and burst of 20
	rateLimiter := NewIPRateLimiter(rate.Limit(10/60.0), 20)

	// Record the base number of goroutines at startup
	baseGoroutines := runtime.NumGoroutine()

	// Create a cache with 5-second default expiration and 10-second cleanup interval
	responseCache := cache.New(5*time.Second, 10*time.Second)

	return &Handler{
		BaseHandler: core.BaseHandler{
			BridgeService:  bridgeService,
			StartTime:      time.Now(),
			AdminAPIKey:    os.Getenv("ADMIN_API_KEY"),
			BaseGoroutines: baseGoroutines,
			ResponseCache:  responseCache,
		},
		RateLimiter: rateLimiter,
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
	return h.StartTime
}

// GetRequestCounter returns the request counter
func (h *Handler) GetRequestCounter() *atomic.Int64 {
	return &h.RequestCounter
}

// GetBaseGoroutines returns the base goroutines count
func (h *Handler) GetBaseGoroutines() int {
	return h.BaseGoroutines
}

// GetResponseCache returns the response cache
func (h *Handler) GetResponseCache() *cache.Cache {
	return h.ResponseCache
}

type TransactionResponse struct {
	Status    string            `json:"status"`
	Message   string            `json:"message"`
	Txs       map[string]string `json:"txs"`
	DepositID string            `json:"deposit_id,omitempty"`
}
