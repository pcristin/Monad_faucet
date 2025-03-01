package bridge

import (
	"math/big"

	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Utility Functions ---
//

func formatMonAmount(amount *big.Int) string {
	f := new(big.Float).SetInt(amount)
	f = new(big.Float).Quo(f, new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	return f.Text('f', 6)
}

func logMonCalculation(event blockchain.DepositEvent, monAmount *big.Int) {
	ethUsdPrice := GetEthUsdPrice()
	ethUsdPriceFloat := new(big.Float).Quo(new(big.Float).SetInt(ethUsdPrice), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)))
	var decimals int64
	var currencySymbol string
	switch event.Currency {
	case blockchain.CurrencyETH:
		decimals = 18
		currencySymbol = "ETH"
	case blockchain.CurrencyUSDC, blockchain.CurrencyUSDT:
		decimals = 6
		currencySymbol = blockchain.CurrencyTypeToString(event.Currency)
	default:
		decimals = 18
		currencySymbol = "UNKNOWN"
	}
	depositAmountFloat := new(big.Float).Quo(new(big.Float).SetInt(event.Amount), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil)))
	depositAmountStr := depositAmountFloat.Text('f', 12)
	usdValue := new(big.Float).Mul(depositAmountFloat, ethUsdPriceFloat)
	monUsdRatio := blockchain.GetMonUsdRatio()
	monUsdRatioFloat := new(big.Float).Quo(new(big.Float).SetInt(monUsdRatio), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	if event.Currency == blockchain.CurrencyETH {
		logger.Info("%s -> MON: %s %s ≈ $%s USD (ETH price: $%s) / $%s per MON = %s MON (%s wei)",
			currencySymbol, depositAmountStr, currencySymbol, usdValue.Text('f', 6), ethUsdPriceFloat.Text('f', 2),
			monUsdRatioFloat.Text('f', 6), formatMonAmount(monAmount), monAmount.String())
	} else {
		logger.Info("%s -> MON: %s %s / $%s per MON = %s MON (%s wei)",
			currencySymbol, depositAmountStr, currencySymbol, monUsdRatioFloat.Text('f', 6), formatMonAmount(monAmount), monAmount.String())
	}
}
