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
		ethUsdPrice := GetEthUsdPrice()            // 8 decimals
		monUsdRatio := blockchain.GetMonUsdRatio() // 18 decimals

		// Convert depositAmount from wei to ETH as a float for accurate calculation
		depositEthFloat := new(big.Float).Quo(
			new(big.Float).SetInt(depositAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		)

		// Convert ETH/USD price to a float
		ethUsdPriceFloat := new(big.Float).Quo(
			new(big.Float).SetInt(ethUsdPrice),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)),
		)

		// Convert MON/USD ratio to a float
		monUsdRatioFloat := new(big.Float).Quo(
			new(big.Float).SetInt(monUsdRatio),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		)

		// Calculate USD value of the deposit
		usdValueFloat := new(big.Float).Mul(depositEthFloat, ethUsdPriceFloat)

		// Calculate MON tokens based on USD value and MON/USD ratio
		monAmountFloat := new(big.Float).Quo(usdValueFloat, monUsdRatioFloat)

		// Convert MON amount to wei (multiply by 10^18)
		monWeiFloat := new(big.Float).Mul(monAmountFloat, new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))

		// Convert to big.Int
		monWeiInt, _ := monWeiFloat.Int(nil)

		// (Optional) Minimum value check
		minMonWei := big.NewInt(1000000000000000) // 0.001 MON in wei
		if monWeiInt.Sign() <= 0 || monWeiInt.Cmp(minMonWei) < 0 {
			if depositAmount.Sign() > 0 {
				logger.Info("Deposit less than minimum MON; adjusting to minimum")
				monWeiInt = new(big.Int).Set(minMonWei)
			}
		}

		// For logging
		usdValue, _ := usdValueFloat.Float64()
		calculatedMonAmount, _ := monAmountFloat.Float64()

		logger.Info("ETH -> MON: %s ETH ≈ $%.6f (ETH price: $%s) / $%s per MON = %.6f MON (%s wei)",
			depositEthFloat.Text('f', 18), usdValue, ethUsdPriceFloat.Text('f', 2),
			monUsdRatioFloat.Text('f', 6), calculatedMonAmount, monWeiInt.String())

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
