package nonce_manager

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// GetNonce returns the next nonce for an address in a thread-safe manner
func (nm *NonceManager) GetNonce(ctx context.Context, address common.Address) (uint64, error) {
	// Get or create a mutex for this address
	nm.mu.Lock()
	addrMutex, ok := nm.nonceLocks[address]
	if !ok {
		addrMutex = &sync.Mutex{}
		nm.nonceLocks[address] = addrMutex
	}
	nm.mu.Unlock()

	// Lock the address-specific mutex
	addrMutex.Lock()
	defer addrMutex.Unlock()

	// Check if we already have a nonce for this address
	nm.mu.Lock()
	nonce, exists := nm.nonces[address]
	nm.mu.Unlock()

	if !exists {
		// Get the initial nonce from the blockchain
		var err error
		nonce, err = nm.client.PendingNonceAt(ctx, address)
		if err != nil {
			return 0, fmt.Errorf("failed to get initial nonce: %w", err)
		}
	}

	// Store and increment the nonce for next use
	nm.mu.Lock()
	nm.nonces[address] = nonce + 1
	nm.mu.Unlock()

	return nonce, nil
}

// ResetNonce forces the nonce manager to refresh the nonce from the blockchain on next request
func (nm *NonceManager) ResetNonce(address common.Address) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	delete(nm.nonces, address)
}
