package listener

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// NewClient creates a new Ethereum client
func NewClient(rpcURL string) (*ethclient.Client, error) {
	logger.Info("Connecting to RPC endpoint: %s", rpcURL)
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Web3 client: %v", err)
	}

	return client, nil
}

// GetClient returns the underlying ethclient.Client
func (l *EventListener) GetClient() *ethclient.Client {
	return l.client
}
