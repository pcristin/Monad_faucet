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

type DepositEvent struct {
	Depositor   common.Address
	Amount      *big.Int
	DepositId   *big.Int
	Currency    blockchain.CurrencyType
	BlockNumber uint64
	TxHash      string // Transaction hash of the deposit
	Metadata    string // User-provided metadata string for this deposit
}

type EventListener struct {
	client        *ethclient.Client
	contractABI   abi.ABI
	address       common.Address
	rpcURL        string
	reconnectChan chan struct{}
}

func NewEventListener(rpcURL string, contractAddress common.Address) (*EventListener, error) {
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
	}

	logger.Info("Event listener created for contract address: %s", contractAddress.Hex())
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
