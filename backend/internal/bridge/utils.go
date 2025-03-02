package bridge

import (
	"fmt"
	"math/big"

	"github.com/pcristin/monad-faucet/internal/blockchain"
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
		return big.NewInt(0)
	}

	// Multiply by swap ratio to get MON amount
	monAmount := new(big.Int).Mul(amount, swapRatio)

	// Different currencies might have different decimal places
	if currency == blockchain.CurrencyETH || currency == blockchain.CurrencyUSDT {
		// For ETH and USDT, divide by 10^18 to account for 18 decimals
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
		monAmount = new(big.Int).Div(monAmount, divisor)
	} else if currency == blockchain.CurrencyUSDC {
		// For USDC, divide by 10^6 to account for 6 decimals
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)
		monAmount = new(big.Int).Div(monAmount, divisor)
	}

	return monAmount
}
