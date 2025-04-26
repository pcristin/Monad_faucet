package nonce_manager

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// NonceManager is a struct provides a thread-safe way to manage nonces for multiple Ethereum addresses.
type NonceManager struct {
	client     *ethclient.Client
	nonceLocks map[common.Address]*sync.Mutex
	nonces     map[common.Address]uint64
	mu         sync.Mutex
}

// NewNonceManager creates a new NonceManager instance.
func NewNonceManager(client *ethclient.Client) *NonceManager {
	return &NonceManager{
		client:     client,
		nonceLocks: make(map[common.Address]*sync.Mutex),
		nonces:     make(map[common.Address]uint64),
	}
}
