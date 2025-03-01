package admins

import "github.com/pcristin/monad-faucet/internal/core"

// AdminHandler wraps the handler interface to provide admin-specific functionality
type AdminHandler struct {
	Handler core.HandlerInterface
}

type AdminUpdateRatioRequest struct {
	MonUsdRatio string `json:"mon_usd_ratio" binding:"required"` // e.g. "0.1" for 1 MON = 0.1 USD
}

// AdminUpdateWalletLimitRequest represents the request to update wallet limit percentage
type AdminUpdateWalletLimitRequest struct {
	LimitPercentage int64 `json:"limit_percentage" binding:"required"` // e.g. 30 for 30% of total MON balance
}

// Metrics represents system metrics
type Metrics struct {
	Uptime                string            `json:"uptime"`
	TotalRequests         int64             `json:"total_requests"`
	ActiveConnections     int               `json:"active_connections"`
	PendingTransactions   int               `json:"pending_transactions"`
	CompletedTransactions int               `json:"completed_transactions"`
	FailedTransactions    int               `json:"failed_transactions"`
	RefundedTransactions  int               `json:"refunded_transactions"`
	CacheSize             int               `json:"cache_size"`
	MemoryUsage           uint64            `json:"memory_usage"`
	GoroutineCount        int               `json:"goroutine_count"`
	BlockchainStatus      map[string]string `json:"blockchain_status"`
}
