package blockchain

import (
	"crypto/ecdsa"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain/nonce_manager"
)

type CurrencyType uint8

const WalletLimitPercentage = 30

const (
	CurrencyETH CurrencyType = iota
	CurrencyUSDC
	CurrencyUSDT
)

// CurrencyTypeToString converts a CurrencyType to its string representation
func CurrencyTypeToString(c CurrencyType) string {
	switch c {
	case CurrencyETH:
		return "ETH"
	case CurrencyUSDC:
		return "USDC"
	case CurrencyUSDT:
		return "USDT"
	default:
		return "UNKNOWN"
	}
}

// ArbitrumDepositor represents the Arbitrum depositor contract
type ArbitrumDepositor struct {
	Client     *ethclient.Client
	Address    common.Address
	ChainID    *big.Int
	PrivateKey *ecdsa.PrivateKey
	*bind.BoundContract
}

// OptimismDepositor represents the Optimism depositor contract
type OptimismDepositor struct {
	Client     *ethclient.Client
	Address    common.Address
	ChainID    *big.Int
	PrivateKey *ecdsa.PrivateKey
	*bind.BoundContract
}

// BaseDepositor represents the Base depositor contract
type BaseDepositor struct {
	Client     *ethclient.Client
	Address    common.Address
	ChainID    *big.Int
	PrivateKey *ecdsa.PrivateKey
	*bind.BoundContract
}

// MonadDistributor represents the Monad distributor contract
type MonadDistributor struct {
	Client     *ethclient.Client
	Address    common.Address
	PrivateKey *ecdsa.PrivateKey
	*bind.BoundContract
	NonceManager *nonce_manager.NonceManager
}

// MonUsdRatio represents the ratio of MON to USD (atomic value)
type MonUsdRatio struct {
	value atomic.Value // stores *big.Int
}

func NewMonUsdRatio(initialValue *big.Int) *MonUsdRatio {
	r := &MonUsdRatio{}
	r.value.Store(initialValue)
	return r
}
