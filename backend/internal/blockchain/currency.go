package blockchain

// GetCurrencyDecimals returns the number of decimals for a given currency
func GetCurrencyDecimals(currency CurrencyType) int {
	switch currency {
	case CurrencyETH:
		return 18 // ETH has 18 decimals
	case CurrencyUSDT, CurrencyUSDC:
		return 6 // USDT and USDC have 6 decimals
	default:
		return 18 // Default to 18 decimals
	}
}
