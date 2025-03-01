package bridge

import (
	"context"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Currency Conversion and Price Feed ---
//

func calculateMonAmount(depositAmount *big.Int, swapRatio *big.Int, currencyType blockchain.CurrencyType) *big.Int {
	if currencyType == blockchain.CurrencyUSDC || currencyType == blockchain.CurrencyUSDT {
		decimalAdjustment := int64(12)
		adjustedDepositAmount := new(big.Int).Mul(depositAmount, new(big.Int).Exp(big.NewInt(10), big.NewInt(decimalAdjustment), nil))
		result := new(big.Int).Mul(adjustedDepositAmount, swapRatio)
		result = new(big.Int).Div(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
		depositValueFloat := new(big.Float).Quo(new(big.Float).SetInt(depositAmount), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)))
		resultFloat := new(big.Float).Quo(new(big.Float).SetInt(result), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
		depositValue, _ := depositValueFloat.Float64()
		resultValue, _ := resultFloat.Float64()
		logger.Info("Stablecoin -> MON: deposit %s units ($%.6f), adjusted=%s, swapRatio=%s, result=%s wei (%.6f MON)",
			depositAmount.String(), depositValue, adjustedDepositAmount.String(), swapRatio.String(), result.String(), resultValue)
		if (result.Sign() <= 0 && depositAmount.Sign() > 0) || resultValue < 0.00001 {
			monUsdRatio := blockchain.GetMonUsdRatio()
			monUsdRatioFloat := new(big.Float).Quo(new(big.Float).SetInt(monUsdRatio), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
			monRatioValue, _ := monUsdRatioFloat.Float64()
			expectedMon := new(big.Float).Quo(depositValueFloat, monUsdRatioFloat)
			expectedMonWei := new(big.Float).Mul(expectedMon, new(big.Float).SetFloat64(1e18))
			expectedMonWeiInt, _ := expectedMonWei.Int(nil)
			minMonWei := big.NewInt(1000000000000000) // 0.001 MON
			if expectedMonWeiInt.Cmp(minMonWei) < 0 {
				expectedMonWeiInt = new(big.Int).Set(minMonWei)
			}
			if depositValue < 0.001 && depositAmount.Sign() > 0 {
				microMon := big.NewInt(10000000000000) // 0.00001 MON
				if expectedMonWeiInt.Cmp(microMon) < 0 {
					expectedMonWeiInt = microMon
				}
			}
			humanReadableMon := new(big.Float).Quo(new(big.Float).SetInt(expectedMonWeiInt), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))).Text('f', 6)
			logger.Info("Small deposit: $%.6f at ratio $%.6f, allocating %s MON (%s wei)", depositValue, monRatioValue, humanReadableMon, expectedMonWeiInt.String())
			return expectedMonWeiInt
		}
		return result

		// ETH deposits conversion.
	} else if currencyType == blockchain.CurrencyETH {
		ethUsdPrice := GetEthUsdPrice()            // *big.Int in 8 decimals (e.g. 220066000000 for $2200.66)
		monUsdRatio := blockchain.GetMonUsdRatio() // *big.Int in 18 decimals (e.g. 170000000000000000 for $0.17/MON)

		// Pre-calculate constants for 10^18 and 10^8.
		oneEth := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
		oneE8 := new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)

		// For logging (convert deposit from wei to ETH, price to float, etc.)
		depositEthFloat := new(big.Float).Quo(new(big.Float).SetInt(depositAmount), new(big.Float).SetInt(oneEth))
		ethUsdPriceFloat := new(big.Float).Quo(new(big.Float).SetInt(ethUsdPrice), new(big.Float).SetInt(oneE8))
		monUsdRatioFloat := new(big.Float).Quo(new(big.Float).SetInt(monUsdRatio), new(big.Float).SetInt(oneEth))

		// Compute the USD value (scaled to 18 decimals):
		depositTimesPrice := new(big.Int).Mul(depositAmount, ethUsdPrice)
		usdValueWith18Decimals := new(big.Int).Div(depositTimesPrice, oneE8) // (D * P) / 1e8

		// Correct calculation:
		// MON (in wei) = (depositAmount * ethUsdPrice * 1e18) / (monUsdRatio * 1e8)
		monWeiInt := new(big.Int).Div(new(big.Int).Mul(usdValueWith18Decimals, oneEth), monUsdRatio)

		// For logging using float approximations.
		ethUsdValue, _ := ethUsdPriceFloat.Float64()
		monUsdValue, _ := monUsdRatioFloat.Float64()
		usdValue, _ := new(big.Float).Quo(new(big.Float).SetInt(usdValueWith18Decimals), new(big.Float).SetInt(oneEth)).Float64() // USD value in human-readable form
		monAmountFloat := usdValue / monUsdValue

		// Enforce a minimum MON amount if needed.
		minMonWei := big.NewInt(1000000000000000) // 0.001 MON in wei
		if monWeiInt.Sign() <= 0 || monWeiInt.Cmp(minMonWei) < 0 {
			if depositAmount.Sign() > 0 {
				logger.Info("Deposit less than minimum MON; adjusting to minimum")
				monWeiInt = new(big.Int).Set(minMonWei)
			}
		}

		humanReadableMon := new(big.Float).Quo(new(big.Float).SetInt(monWeiInt), new(big.Float).SetInt(oneEth)).Text('f', 6)
		logger.Info("ETH -> MON: %s ETH ≈ $%.6f (ETH price: $%.2f) / $%.6f per MON = %s MON (%.6f MON, %s wei)",
			depositEthFloat.Text('f', 18), usdValue, ethUsdValue, monUsdValue, humanReadableMon, monAmountFloat, monWeiInt.String())
		return monWeiInt
	} else {
		logger.Error("Unsupported currency type: %s", blockchain.CurrencyTypeToString(currencyType))
		return nil
	}
}

// GetEthUsdPrice returns the current ETH/USD price from Chainlink oracle.
func GetEthUsdPrice() *big.Int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	priceFeedAbi, err := abi.JSON(strings.NewReader(blockchain.PriceFeedABI))
	if err != nil {
		logger.Error("Failed to parse price feed ABI: %v", err)
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000))
	}
	var client *ethclient.Client
	clients := getAvailableClients()
	if len(clients) > 0 {
		client = clients[0]
	} else {
		logger.Error("No Ethereum clients available for Chainlink price feed")
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000))
	}
	priceFeed := bind.NewBoundContract(common.HexToAddress(blockchain.ChainlinkEthUsdFeed), priceFeedAbi, client, client, client)
	var out []interface{}
	err = priceFeed.Call(&bind.CallOpts{Context: ctx}, &out, "latestRoundData")
	if err != nil {
		logger.Error("Failed to get ETH/USD price: %v", err)
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000))
	}
	ethUsdPrice := out[1].(*big.Int)
	ethUsdPriceFloat := new(big.Float).Quo(new(big.Float).SetInt(ethUsdPrice), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)))
	ethUsdPriceValue, _ := ethUsdPriceFloat.Float64()
	logger.Info("Current ETH/USD price: $%.2f", ethUsdPriceValue)
	return ethUsdPrice
}

func getAvailableClients() []*ethclient.Client {
	var clients []*ethclient.Client
	rpcURL := os.Getenv("ARB_HTTP_RPC_URL")
	client, err := ethclient.Dial(rpcURL)
	if err == nil && client != nil {
		clients = append(clients, client)
	}
	return clients
}
