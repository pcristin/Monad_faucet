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
		// The swap ratio is now defined as MON wei per smallest USD unit
		// So we can directly multiply the amount by the swap ratio

		// First log the raw amount in smallest units
		logger.Info("Raw USD token amount in smallest units: %s", amountCopy.String())

		// Get USD token decimals (6) for float calculations
		usdTokenDecimals := new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)

		// Double-check with a manual calculation based on current MON/USD ratio
		monUsdRatio := blockchain.GetMonUsdRatio()
		logger.Info("Current MON/USD ratio: %s (%s USD per MON)",
			monUsdRatio.String(),
			formatBigIntAsFloat(monUsdRatio, 18))

		// Calculate how many MON you should get from this USD amount
		// Step 1: Convert amount to USD value
		usdValue := new(big.Float).Quo(
			new(big.Float).SetInt(amountCopy),
			new(big.Float).SetInt(usdTokenDecimals),
		)

		// Step 2: Calculate theoretical MON amount (USD amount / USD per MON)
		theoreticalMon := new(big.Float).Quo(
			usdValue,
			new(big.Float).Quo(
				new(big.Float).SetInt(monUsdRatio),
				new(big.Float).SetInt(monDecimals),
			),
		)

		// Step 3: Convert to MON wei
		theoreticalMonWei := new(big.Float).Mul(
			theoreticalMon,
			new(big.Float).SetInt(monDecimals),
		)

		// Convert to big.Int for comparison
		theoreticalMonWeiBig, _ := theoreticalMonWei.Int(nil)
		logger.Info("Theoretical MON amount (calculated from MON/USD ratio): %s wei",
			theoreticalMonWeiBig.String())

		// Direct calculation using swap ratio
		// Where swap ratio is now MON wei per smallest USD unit
		if swapRatio.Sign() <= 0 || swapRatio.Cmp(big.NewInt(1000000)) < 0 {
			// If the swap ratio is too small or zero, use the theoretical calculation
			logger.Warn("Swap ratio for %s is too small: %s. Using theoretical calculation instead.",
				blockchain.CurrencyTypeToString(currency),
				swapRatio.String())
			monAmount = theoreticalMonWeiBig
		} else {
			monAmount = new(big.Int).Mul(amountCopy, swapRatio)
			logger.Info("Calculated MON amount using swap ratio: %s wei", monAmount.String())

			// Validate that our calculation is reasonable
			// Allow for some minor difference due to rounding
			diff := new(big.Int).Sub(monAmount, theoreticalMonWeiBig)
			diffAbs := new(big.Int).Abs(diff)
			threshold := new(big.Int).Div(theoreticalMonWeiBig, big.NewInt(100)) // 1% difference threshold

			if diffAbs.Cmp(threshold) > 0 && theoreticalMonWeiBig.Sign() > 0 {
				logger.Warn("Calculated MON amount differs from theoretical value by more than 1%%!")
				logger.Warn("Calculated: %s wei, Theoretical: %s wei, Diff: %s wei",
					monAmount.String(), theoreticalMonWeiBig.String(), diff.String())

				// If the difference is huge, use the theoretical value instead
				if diffAbs.Cmp(new(big.Int).Div(theoreticalMonWeiBig, big.NewInt(10))) > 0 {
					logger.Warn("Difference is more than 10%%, using theoretical calculation instead")
					monAmount = theoreticalMonWeiBig
				}
			}
		}

		// Validate calculation with float-based approach
		// Convert deposit amount to USD value
		depositUsd := new(big.Float).SetInt(amountCopy)
		depositUsd = new(big.Float).Quo(depositUsd, new(big.Float).SetInt(usdTokenDecimals))

		// Expected MON in USD
		monUsdRatioValue := new(big.Float).SetInt(blockchain.GetMonUsdRatio())
		monUsdRatioValue = new(big.Float).Quo(monUsdRatioValue, new(big.Float).SetInt(monDecimals))
		expectedMon := new(big.Float).Quo(depositUsd, monUsdRatioValue)
		expectedMonFloat, _ := expectedMon.Float64()

		// Log validation values
		logger.Info("Deposit amount in USD: %f", depositUsd)
		logger.Info("MON/USD ratio: %f", monUsdRatioValue)
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
	logger.Info("Calculated MON amount: %s from deposit amount: %s %s",
		monString,
		formatBigIntAsFloat(amountCopy, blockchain.GetCurrencyDecimals(currency)),
		blockchain.CurrencyTypeToString(currency))

	return monAmount
}

// formatBigIntAsFloat formats a big.Int with decimals as a human-readable string
func formatBigIntAsFloat(value *big.Int, decimals int) string {
	if value == nil {
		return "0"
	}

	// Convert to a big.Float for easier decimal handling
	floatValue := new(big.Float).SetInt(value)

	// Divide by 10^decimals
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	result := new(big.Float).Quo(floatValue, divisor)

	// Convert to string with appropriate precision
	str := result.Text('f', 6) // 6 decimal places should be enough for display

	return str
}
