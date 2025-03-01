package bridge

import (
	"math/big"

	"github.com/pcristin/monad-faucet/internal/database"
)

//
// Cache Helpers ---
//

// updateTxCache updates the in-memory transaction cache.
func (s *BridgeService) updateTxCache(depositID *big.Int, status, txHash string) {
	s.txCacheMutex.Lock()
	s.txCache[depositID.String()] = &database.Transaction{
		DepositID:   depositID,
		Status:      status,
		MonadTxHash: txHash,
	}
	s.txCacheMutex.Unlock()
}

// clearTransactionCache removes a transaction from the cache.
func (s *BridgeService) clearTransactionCache(depositID string) {
	s.txCacheMutex.Lock()
	delete(s.txCache, depositID)
	s.txCacheMutex.Unlock()
}

// GetCacheSize returns the size of the transaction cache.
func (s *BridgeService) GetCacheSize() int {
	s.txCacheMutex.RLock()
	defer s.txCacheMutex.RUnlock()
	return len(s.txCache)
}
