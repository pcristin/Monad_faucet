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
			// The swap ratio format has changed:
			// For USDC/USDT: The ratio now represents "MON wei per smallest unit of USDT/USDC"
			// For ETH: The ratio directly represents how much ETH 1 MON costs in wei
			var currencyPerMon *big.Float

			if currency == blockchain.CurrencyUSDC || currency == blockchain.CurrencyUSDT {
				// For USDC/USDT: The ratio is "MON wei per smallest USDT/USDC unit"
				// We want to display "USDT/USDC per 1 MON"

				// Get the MON/USD ratio (e.g., 0.17 * 10^18 for 0.17 USD per 1 MON)
				monUsdRatio := blockchain.GetMonUsdRatio()

				// Calculate USDC/USDT per MON = MON/USD ratio / 10^(18-decimals)
				// For USDT/USDC with 6 decimals and MON/USD ratio of 0.17:
				// USD per MON = 0.17 USD
				currencyPerMon = new(big.Float).SetInt(monUsdRatio)
				divisorUsd := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
				currencyPerMon = new(big.Float).Quo(currencyPerMon, divisorUsd)

				logger.Debug("Calculated %s per 1 MON: %s", blockchain.CurrencyTypeToString(currency), currencyPerMon.Text('f', 6))

				// Format with 6 decimal places for USD-based tokens
				exchangeRates[blockchain.CurrencyTypeToString(currency)] = currencyPerMon.Text('f', 6)
			} else if currency == blockchain.CurrencyETH {
				// For ETH, the ratio directly represents how much ETH 1 MON costs
				// The ratio is in wei with 18 decimals precision

				// Create a big.Float from the ratio
				currencyPerMon = new(big.Float).SetInt(ratio)

				// Divide by 10^18 to convert from wei to ETH
				divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
				currencyPerMon = new(big.Float).Quo(currencyPerMon, divisor)

				logger.Debug("ETH/MON ratio from state: %s (1 MON = %s ETH)",
					ratio.String(),
					currencyPerMon.Text('f', 18))

				exchangeRates[blockchain.CurrencyTypeToString(currency)] = currencyPerMon.Text('f', 18)
			}
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
