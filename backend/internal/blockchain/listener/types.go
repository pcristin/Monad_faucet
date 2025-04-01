package listener

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// ChainType identifies which L2 chain a deposit came from
type ChainType uint32

const (
	ChainArbitrumMainnet ChainType = 42161
	ChainArbitrumSepolia ChainType = 421614
	ChainBaseMainnet     ChainType = 8453
	ChainBaseSepolia     ChainType = 84532
	ChainOptimismMainnet ChainType = 10
	ChainOptimismSepolia ChainType = 11155420
)

// ChainTypeToString converts a ChainType to its string representation
func ChainTypeToString(c ChainType) string {
	switch c {
	case ChainArbitrumMainnet:
		return "Arbitrum Mainnet"
	case ChainArbitrumSepolia:
		return "Arbitrum Sepolia"
	case ChainBaseMainnet:
		return "Base Mainnet"
	case ChainBaseSepolia:
		return "Base Sepolia"
	case ChainOptimismMainnet:
		return "Optimism Mainnet"
	case ChainOptimismSepolia:
		return "Optimism Sepolia"
	default:
		return "Unknown"
	}
}

type DepositEvent struct {
	Depositor   common.Address
	Amount      *big.Int
	DepositId   *big.Int
	Currency    blockchain.CurrencyType
	BlockNumber uint64
	TxHash      string    // Transaction hash of the deposit
	Metadata    string    // User-provided metadata string for this deposit
	Chain       ChainType // Which chain the deposit came from
}

type EventListener struct {
	client        *ethclient.Client
	contractABI   abi.ABI
	address       common.Address
	rpcURL        string
	reconnectChan chan struct{}
	chain         ChainType // Which chain this listener is for
}

func NewEventListener(rpcURL string, contractAddress common.Address, chain ChainType) (*EventListener, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Web3 RPC client: %v", err)
	}

	// Test connection by getting network ID
	_, err = client.NetworkID(context.Background())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to get network ID, connection might be invalid: %v", err)
	}

	listener := &EventListener{
		client:        client,
		contractABI:   blockchain.DepositorABI,
		address:       contractAddress,
		rpcURL:        rpcURL,
		reconnectChan: make(chan struct{}, 1),
		chain:         chain,
	}

	logger.Info("Event listener created for chain %s with contract address: %s",
		ChainTypeToString(chain), contractAddress.Hex())
	return listener, nil
}

// AlchemyMinedTxResult represents the result structure from alchemy_minedTransactions
type AlchemyMinedTxResult struct {
	Removed     bool                `json:"removed"`
	Transaction *AlchemyTransaction `json:"transaction"`
}

// AlchemyTransaction represents a transaction in the alchemy_minedTransactions result
type AlchemyTransaction struct {
	Hash             string `json:"hash"`
	BlockHash        string `json:"blockHash"`
	BlockNumber      string `json:"blockNumber"`
	From             string `json:"from"`
	To               string `json:"to"`
	Input            string `json:"input"`
	TransactionIndex string `json:"transactionIndex"`
	Gas              string `json:"gas"`
	GasPrice         string `json:"gasPrice"`
	Value            string `json:"value"`
	Nonce            string `json:"nonce"`
	V                string `json:"v"`
	R                string `json:"r"`
	S                string `json:"s"`
	Type             string `json:"type"`
}

// AlchemySubscriptionMsg represents a subscription message from Alchemy WebSocket
type AlchemySubscriptionMsg struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Result       AlchemyMinedTxResult `json:"result"`
		Subscription string               `json:"subscription"`
	} `json:"params"`
}

type rawDepositEvent struct {
	Depositor   common.Address
	Amount      *big.Int
	DepositId   *big.Int
	Currency    uint8
	BlockNumber uint64
	Metadata    string // User-provided metadata string
}

// GetChainInfo returns network name and if it's a testnet for the given chain type
func GetChainInfo(chain ChainType) (name string, isTestnet bool) {
	switch chain {
	case ChainArbitrumMainnet:
		return "Arbitrum", false
	case ChainArbitrumSepolia:
		return "Arbitrum", true
	case ChainBaseMainnet:
		return "Base", false
	case ChainBaseSepolia:
		return "Base", true
	case ChainOptimismMainnet:
		return "Optimism", false
	case ChainOptimismSepolia:
		return "Optimism", true
	default:
		return "Unknown", false
	}
}

// GetChain returns the chain type this listener is configured for
func (l *EventListener) GetChain() ChainType {
	return l.chain
}
