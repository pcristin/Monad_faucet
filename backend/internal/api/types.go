package api

import (
	"encoding/json"
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

type RateLimitHandler struct {
	core.BaseHandler
	RateLimiter *IPRateLimiter
}

// NewRateLimitHandler creates a new API handler
func NewRateLimitHandler(bridgeService *bridge.BridgeService) *RateLimitHandler {
	// Create a rate limiter with 10 requests per minute and burst of 20
	rateLimiter := NewIPRateLimiter(rate.Limit(10/60.0), 20)

	// Record the base number of goroutines at startup
	baseGoroutines := runtime.NumGoroutine()

	// Create a cache with 5-second default expiration and 10-second cleanup interval
	responseCache := cache.New(5*time.Second, 10*time.Second)

	return &RateLimitHandler{
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
func (h *RateLimitHandler) GetBridgeService() *bridge.BridgeService {
	return h.BridgeService
}

// GetAdminAPIKey returns the admin API key
func (h *RateLimitHandler) GetAdminAPIKey() string {
	return h.AdminAPIKey
}

// GetStartTime returns the start time
func (h *RateLimitHandler) GetStartTime() time.Time {
	return h.StartTime
}

// GetRequestCounter returns the request counter
func (h *RateLimitHandler) GetRequestCounter() *atomic.Int64 {
	return &h.RequestCounter
}

// GetBaseGoroutines returns the base goroutines count
func (h *RateLimitHandler) GetBaseGoroutines() int {
	return h.BaseGoroutines
}

// GetResponseCache returns the response cache
func (h *RateLimitHandler) GetResponseCache() *cache.Cache {
	return h.ResponseCache
}

type TransactionResponse struct {
	Status             string            `json:"status"`
	Message            string            `json:"message"`
	Txs                map[string]string `json:"txs"`
	DepositID          string            `json:"deposit_id,omitempty"`
	SourceChainId      int               `json:"source_chain_id,omitempty"`
	DestinationChainId int               `json:"destination_chain_id,omitempty"`
}

// AlchemyWebhookPayload represents the webhook payload from Alchemy
type AlchemyWebhookPayload struct {
	WebhookID      string       `json:"webhookId"`
	ID             string       `json:"id"`
	CreatedAt      string       `json:"createdAt"`
	Type           string       `json:"type"`
	Event          AlchemyEvent `json:"event"`
	SequenceNumber string       `json:"sequenceNumber"`
}

// AlchemyEvent represents an event in the Alchemy webhook payload
type AlchemyEvent struct {
	Data    AlchemyData `json:"data"`
	Network string      `json:"network"`
}

// AlchemyData contains the actual event data
type AlchemyData struct {
	Block AlchemyBlock `json:"block,omitempty"`
}

// AlchemyBlock contains block information
type AlchemyBlock struct {
	Hash      string       `json:"hash"`
	Number    json.Number  `json:"number"`
	Timestamp json.Number  `json:"timestamp"`
	Logs      []AlchemyLog `json:"logs,omitempty"`
}

// AlchemyLog contains log information
type AlchemyLog struct {
	Data        string         `json:"data"`
	Topics      []string       `json:"topics"`
	Index       json.Number    `json:"index"`
	Account     AlchemyAccount `json:"account"`
	Transaction AlchemyTx      `json:"transaction,omitempty"`
}

// AlchemyAccount represents an account in the log
type AlchemyAccount struct {
	Address string `json:"address"`
}

// AlchemyTx contains transaction information
type AlchemyTx struct {
	Hash                 string          `json:"hash"`
	Nonce                json.Number     `json:"nonce"`
	Index                json.Number     `json:"index"`
	From                 AlchemyAccount  `json:"from"`
	To                   AlchemyAccount  `json:"to"`
	Value                string          `json:"value"`
	GasPrice             string          `json:"gasPrice"`
	MaxFeePerGas         string          `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string          `json:"maxPriorityFeePerGas,omitempty"`
	Gas                  json.Number     `json:"gas"`
	Status               json.Number     `json:"status"`
	GasUsed              json.Number     `json:"gasUsed"`
	CumulativeGasUsed    json.Number     `json:"cumulativeGasUsed"`
	EffectiveGasPrice    string          `json:"effectiveGasPrice"`
	CreatedContract      *AlchemyAccount `json:"createdContract"`
}

// EventParams represents the decoded parameters from the event
type EventParams struct {
	Amount    string `json:"amount"`
	ID        string `json:"id"`
	Recipient string `json:"recipient"`
}
