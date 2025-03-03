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
		// The swap ratio is defined as MON wei per USD (with full precision)

		// First log the raw amount in smallest units
		logger.Info("Raw USD token amount in smallest units: %s", amountCopy.String())

		// Get USD token decimals (6)
		usdTokenDecimals := new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)

		// Step 1: Calculate the MON amount directly
		// MON amount = USD amount * (MON/USD ratio)
		// Where:
		// - USD amount is in smallest units (e.g., 250000 = 0.25 USDT)
		// - MON/USD ratio is in wei per 1 full USD

		// Calculate MON wei per smallest USD unit
		// monPerSmallestUsd = swap_ratio / 10^6
		monPerSmallestUsd := new(big.Int).Div(swapRatio, usdTokenDecimals)
		logger.Info("MON wei per smallest USD unit: %s", monPerSmallestUsd.String())

		// Apply ratio: amount of smallest USD units * MON wei per smallest USD unit
		monAmount = new(big.Int).Mul(amountCopy, monPerSmallestUsd)
		logger.Info("Final MON amount in wei: %s", monAmount.String())

		// Validate calculation with float-based approach
		// Convert deposit amount to USD value
		depositUsd := new(big.Float).SetInt(amountCopy)
		depositUsd = new(big.Float).Quo(depositUsd, new(big.Float).SetInt(usdTokenDecimals))

		// Convert swap ratio to MON per USD (float)
		swapRatioFloat := new(big.Float).SetInt(swapRatio)
		swapRatioFloat = new(big.Float).Quo(swapRatioFloat, new(big.Float).SetInt(monDecimals))

		// MON = USD value * (MON/USD ratio)
		expectedMon := new(big.Float).Mul(depositUsd, swapRatioFloat)
		expectedMonFloat, _ := expectedMon.Float64()

		// Log validation values
		logger.Info("Deposit amount in USD: %f", depositUsd)
		logger.Info("Swap ratio (MON per USD): %f", swapRatioFloat)
		logger.Info("Expected MON (float check): %f", expectedMonFloat)

		// Convert to a string with fixed decimal places for better logging
		expectedMonIntPart := new(big.Float).Mul(expectedMon, new(big.Float).SetInt(monDecimals))
		expectedMonIntPartBig, _ := expectedMonIntPart.Int(nil)
		logger.Info("Expected MON in wei (float check): %s", expectedMonIntPartBig.String())

	} else if currency == blockchain.CurrencyETH {
		// ETH has 18 decimals, same as MON
		// For ETH, the swap ratio is (ETH/USD * 1/MON/USD) with 18 decimals precision
		// To get MON amount, we need to divide deposit amount by swap ratio:
		// MON = ETH amount / swap ratio

		// Make sure we're working with full precision by multiplying first
		// MON amount = (Deposit amount in wei * 10^18) / swap ratio
		scaledAmount := new(big.Int).Mul(amountCopy, monDecimals)
		monAmount = new(big.Int).Div(scaledAmount, swapRatio)
		logger.Info("After applying ETH swap ratio: %s", monAmount.String())

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
