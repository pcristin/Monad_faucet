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
