package blockchain

import (
	"math/big"
	"time"
)

type CurrencyType uint8

const (
	CurrencyETH CurrencyType = iota
	CurrencyUSDC
	CurrencyUSDT
)

// WalletUsage struct is kept for compatibility but no longer used for tracking limits
// since limits are now per-transaction instead of time-based
type WalletUsage struct {
	TotalAmount *big.Int  // Total MON tokens distributed to this wallet (no longer used)
	LastUpdated time.Time // Time of last distribution (no longer used)
}

// CurrencyTypeToString converts a CurrencyType to its string representation
func CurrencyTypeToString(c CurrencyType) string {
	switch c {
	case CurrencyETH:
		return "ETH"
	case CurrencyUSDC:
		return "USDC"
	case CurrencyUSDT:
		return "USDT"
	default:
		return "UNKNOWN"
	}
}
