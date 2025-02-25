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

// WalletUsage tracks the MON tokens distributed to a wallet
type WalletUsage struct {
	TotalAmount *big.Int  // Total MON tokens distributed to this wallet
	LastUpdated time.Time // Time of last distribution
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
