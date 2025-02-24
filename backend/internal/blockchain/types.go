package blockchain

type CurrencyType uint8

const (
	CurrencyETH CurrencyType = iota
	CurrencyUSDC
	CurrencyUSDT
)

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
