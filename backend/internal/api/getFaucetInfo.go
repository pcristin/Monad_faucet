package api

import (
	"math/big"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// GetFaucetInfo returns simplified faucet information
func (h *Handler) GetFaucetInfo(c *gin.Context) {
	// Try to get cached response first
	cacheKey := "faucetInfo" // Using a simple cache key as this data is the same for all users

	if cachedResponse, found := h.GetResponseCache().Get(cacheKey); found {
		logger.Debug("Using cached faucet info response")
		c.JSON(http.StatusOK, cachedResponse)
		return
	}

	logger.Debug("Cache miss for faucet info, fetching fresh data")

	state, err := h.BridgeService.GetState(c.Request.Context())
	if err != nil {
		logger.Error("Failed to get faucet info: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	// Convert MON balance from wei to MON (divide by 10^18)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	monBalance := new(big.Float).SetInt(state.MonBalance)
	monBalance = new(big.Float).Quo(monBalance, divisor)

	// Calculate wallet limit (percentage of total MON balance)
	var walletLimitText string
	var walletLimit *big.Float

	if blockchain.WalletLimitPercentage == 0 {
		walletLimitText = "No limit"
	} else {
		walletLimitBig := new(big.Int).Mul(state.MonBalance, big.NewInt(blockchain.WalletLimitPercentage))
		walletLimitBig = new(big.Int).Div(walletLimitBig, big.NewInt(100))
		walletLimit = new(big.Float).SetInt(walletLimitBig)
		walletLimit = new(big.Float).Quo(walletLimit, divisor)
		walletLimitText = walletLimit.Text('f', 6)
	}

	// Convert swap ratios to exchange rates
	exchangeRates := make(map[string]string)
	for currency, ratio := range state.SwapRatios {
		if ratio.Sign() > 0 {
			// The contract returns ratio as "MON wei per smallest unit of currency"
			// For USDC/USDT with 1 MON = 0.1 USD, the ratio is 10^18 / 0.1 = 10^19
			// This means 1 smallest unit of USDC (0.000001 USDC) gives 10^19/10^6 = 10^13 MON wei
			// To get USDC per 1 MON, we need: 10^6 / (10^19/10^18) = 10^6 / 10 = 0.1 USDC

			// For USD-based tokens (USDC/USDT), the ratio directly represents MON/USD ratio
			// For ETH, we need to calculate based on ETH/USD price
			var currencyPerMon *big.Float

			if currency == blockchain.CurrencyUSDC || currency == blockchain.CurrencyUSDT {
				// For USDC/USDT: 1 MON = 0.1 USD, so we need 0.1 USDC/USDT per 1 MON
				// The ratio is 10^18 / monUsdRatio, so to get USDC per MON:
				// 10^6 (USDC decimals) / (10^18 / monUsdRatio) * 10^18 = monUsdRatio / 10^12

				// Get the MON/USD ratio (e.g., 0.1 * 10^18 for 0.1 USD per 1 MON)
				monUsdRatio := blockchain.GetMonUsdRatio()

				// Convert to USDC/USDT per MON
				currencyPerMon = new(big.Float).Quo(
					new(big.Float).SetInt(monUsdRatio),
					new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
				)
			} else {
				// For ETH: The ETH/MON ratio represents how much ETH equals 1 MON
				// The stored ratio is ETH/MON in wei with 18 decimals precision
				// We display this directly as the exchange rate
				currencyPerMon = new(big.Float).Quo(
					new(big.Float).SetInt(ratio),
					new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
				)

				logger.Debug("ETH exchange rate: ratio=%s, calculated=%s ETH per MON",
					ratio.String(), currencyPerMon.Text('f', 18))
			}

			// For ETH, use 18 decimals of precision; for other currencies, use 6
			decimals := 6
			if currency == blockchain.CurrencyETH {
				decimals = 18
			}
			exchangeRates[blockchain.CurrencyTypeToString(currency)] = currencyPerMon.Text('f', decimals)
		} else {
			// For ETH, use 18 decimals of precision; for other currencies, use 6
			if currency == blockchain.CurrencyETH {
				exchangeRates[blockchain.CurrencyTypeToString(currency)] = "0.000000000000000000"
			} else {
				exchangeRates[blockchain.CurrencyTypeToString(currency)] = "0.000000"
			}
		}
	}

	// Create the response
	response := gin.H{
		"faucetWorking": !state.IsPaused,
		"faucetReserve": monBalance.Text('f', 6),
		"exchangeRate":  exchangeRates,
		"walletLimit":   walletLimitText,
		"limitType":     "per transaction",
	}

	// Store in cache for future requests
	h.GetResponseCache().Set(cacheKey, response, cache.DefaultExpiration)

	c.JSON(http.StatusOK, response)
}
