package blockchain

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// CheckNetworkConnections tests the connections to all configured L2 networks
func CheckNetworkConnections(urls map[string]string) map[string]string {
	results := make(map[string]string)
	timeout := 5 * time.Second

	for name, url := range urls {
		if url == "" {
			results[name] = "Not configured"
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		client, err := ethclient.DialContext(ctx, url)
		if err != nil {
			results[name] = fmt.Sprintf("Connection failed: %v", err)
			continue
		}

		blockNum, err := client.BlockNumber(ctx)
		if err != nil {
			results[name] = fmt.Sprintf("Connected but failed to get block number: %v", err)
		} else {
			results[name] = fmt.Sprintf("Connected - current block: %d", blockNum)
		}

		client.Close()
	}

	return results
}

// CheckContractAddresses tests that all configured contract addresses are valid
func CheckContractAddresses(addresses map[string]string) map[string]string {
	results := make(map[string]string)

	for name, address := range addresses {
		if address == "" {
			results[name] = "Not configured"
			continue
		}

		if !common.IsHexAddress(address) {
			results[name] = "Invalid address format"
			continue
		}

		results[name] = "Valid address format"
	}

	return results
}

// LogNetworkStatus logs the status of all configured networks and contracts
func LogNetworkStatus(networkUrls, contractAddresses map[string]string) {
	networkResults := CheckNetworkConnections(networkUrls)
	addressResults := CheckContractAddresses(contractAddresses)

	logger.Info("=== L2 Network Status ===")
	for name, result := range networkResults {
		logger.Info("%s: %s", name, result)
	}

	logger.Info("=== Contract Address Status ===")
	for name, result := range addressResults {
		logger.Info("%s: %s", name, result)
	}
}
