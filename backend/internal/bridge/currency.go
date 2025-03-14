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

	// Get the Chainlink ETH/USD feed address from environment variables
	feedAddress := os.Getenv("CHAINLINK_ETH_USD_FEED")
	if feedAddress == "" {
		// Fallback to the default address
		feedAddress = blockchain.ChainlinkEthUsdFeed
	}

	// Log the feed address being used for better debugging
	logger.Info("Using Chainlink ETH/USD price feed at address: %s", feedAddress)

	// Check if the address is valid
	if len(feedAddress) < 42 || feedAddress == "0x0000000000000000000000000000000000000000" {
		logger.Error("Invalid Chainlink ETH/USD feed address: %s", feedAddress)
		// Return a default ETH price to avoid service disruption
		defaultPrice := new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // $3000 with 8 decimals
		logger.Warn("Using fallback ETH price: $3000")
		return defaultPrice
	}

	priceFeed := bind.NewBoundContract(common.HexToAddress(feedAddress), priceFeedAbi, client, client, client)
	var out []interface{}
	err = priceFeed.Call(&bind.CallOpts{Context: ctx}, &out, "latestRoundData")
	if err != nil {
		logger.Error("Failed to get ETH/USD price: %v", err)
		if strings.Contains(err.Error(), "no contract code at given address") {
			logger.Error("Contract call to latestRoundData failed (will retry if rate limit): %v", err)
			logger.Warn("Using fallback ETH price: $3000")
			// Return a default ETH price to avoid service disruption
			return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000))
		}
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000))
	}
	ethUsdPrice := out[1].(*big.Int)
	ethUsdPriceFloat := new(big.Float).Quo(new(big.Float).SetInt(ethUsdPrice), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)))
	ethUsdPriceValue, _ := ethUsdPriceFloat.Float64()
	logger.Info("Current ETH/USD price: $%.2f from feed %s", ethUsdPriceValue, feedAddress)
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
