package bridge

import (
	"fmt"
	"math/big"

	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Utility Functions ---
//

// formatMonAmount formats MON amount for display, converting Wei to MON.
func formatMonAmount(amount *big.Int) string {
	if amount == nil {
		return "0 MON"
	}

	decimals := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	monPart := new(big.Int).Div(amount, decimals)

	weiPart := new(big.Int).Mod(amount, decimals)
	weiStr := weiPart.String()

	// Pad with leading zeros
	for len(weiStr) < 18 {
		weiStr = "0" + weiStr
	}

	// Trim trailing zeros
	for len(weiStr) > 0 && weiStr[len(weiStr)-1] == '0' {
		weiStr = weiStr[:len(weiStr)-1]
	}

	if len(weiStr) > 0 {
		return fmt.Sprintf("%s.%s MON", monPart.String(), weiStr)
	}
	return fmt.Sprintf("%s MON", monPart.String())
}

// calculateMonAmount calculates the amount of MON tokens based on the deposit amount and swap ratio.
func calculateMonAmount(amount *big.Int, swapRatio *big.Int, currency blockchain.CurrencyType) *big.Int {
	if amount == nil || swapRatio == nil {
		logger.Error("Null values in calculateMonAmount: amount=%v, swapRatio=%v", amount, swapRatio)
		return big.NewInt(0)
	}

	// Log input values for debugging
	logger.Info("Calculating MON amount: amount=%s, swapRatio=%s, currency=%s",
		amount.String(), swapRatio.String(), blockchain.CurrencyTypeToString(currency))

	// Make a copy of the input amount to avoid modifying the original
	amountCopy := new(big.Int).Set(amount)

	// Get the standard MON decimals (18)
	monDecimals := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

	// Calculate based on currency type
	var monAmount *big.Int

	if currency == blockchain.CurrencyUSDT || currency == blockchain.CurrencyUSDC {
		// USDT and USDC have 6 decimals
		// 1. Calculate how many full USD tokens we have (divide by 10^6)
		// 2. Apply the swap ratio to get MON tokens

		// First log the raw amount in wei
		logger.Info("Raw USD token amount in wei: %s", amountCopy.String())

		// Get USD token decimals (6)
		usdTokenDecimals := new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)

		// Scale the swap ratio to account for USD token decimals
		// swapRatio is MON tokens per 1 USD with 18 decimals precision
		scaledRatio := new(big.Int).Mul(swapRatio, usdTokenDecimals)
		logger.Info("Scaled swap ratio: %s", scaledRatio.String())

		// Apply scaled ratio directly to get MON amount in wei
		monAmount = new(big.Int).Mul(amountCopy, scaledRatio)
		logger.Info("Intermediate MON calculation: %s", monAmount.String())

		// Normalize to MON 18 decimals by dividing by 10^18
		monAmount = new(big.Int).Div(monAmount, monDecimals)
		logger.Info("Final normalized MON amount: %s", monAmount.String())

		// Double-check the calculation with a different approach
		// This is for validation only
		usdValue := new(big.Float).SetInt(amountCopy)
		usdValue = new(big.Float).Quo(usdValue, new(big.Float).SetInt(usdTokenDecimals))
		swapRatioFloat := new(big.Float).SetInt(swapRatio)
		swapRatioFloat = new(big.Float).Quo(swapRatioFloat, new(big.Float).SetInt(monDecimals))
		expectedMon := new(big.Float).Mul(usdValue, swapRatioFloat)
		expectedMonFloat, _ := expectedMon.Float64()
		logger.Info("Expected MON (float check): %f", expectedMonFloat)

		// Convert to a string with fixed decimal places for better logging
		expectedMonStr := fmt.Sprintf("%.18f", expectedMonFloat)
		logger.Info("Expected MON as string: %s", expectedMonStr)

	} else if currency == blockchain.CurrencyETH {
		// ETH has 18 decimals, same as MON
		// For ETH, the swap ratio already accounts for ETH's USD price and MON's USD price
		monAmount = new(big.Int).Mul(amountCopy, swapRatio)
		logger.Info("After applying ETH swap ratio: %s", monAmount.String())

		// Normalize to MON 18 decimals
		monAmount = new(big.Int).Div(monAmount, monDecimals)
		logger.Info("Final normalized ETH->MON amount: %s", monAmount.String())

	} else {
		logger.Warn("Unknown currency type %d, using default calculation", currency)
		// Default calculation for unknown currencies
		monAmount = new(big.Int).Mul(amountCopy, swapRatio)
		monAmount = new(big.Int).Div(monAmount, monDecimals)
	}

	// Safety check - ensure we're not sending less than minimum amount (0.001 MON = 10^15)
	minAmount := new(big.Int).Exp(big.NewInt(10), big.NewInt(15), nil) // 0.001 MON minimum
	if monAmount.Cmp(minAmount) < 0 {
		logger.Warn("Calculated MON amount is too small: %s, using minimum amount: %s",
			monAmount.String(), minAmount.String())
		return minAmount
	}

	// Final sanity check - make sure amount is reasonable
	// For reference, 1 MON = 10^18 wei
	maxReasonableAmount := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil) // 10^9 MON
	if monAmount.Cmp(maxReasonableAmount) > 0 {
		logger.Error("Calculated MON amount is suspiciously large: %s, capping at reasonable maximum",
			monAmount.String())
		return maxReasonableAmount
	}

	// Log the final amount to be distributed in a human-readable format
	monString := formatMonAmount(monAmount)
	logger.Info("Final MON amount to distribute: %s (%s wei)", monString, monAmount.String())

	return monAmount
}
