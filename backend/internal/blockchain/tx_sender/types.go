package tx_sender

import (
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// TransactionRequest contains everything needed to send a transaction
type TransactionRequest struct {
	Method   string
	Params   []interface{}
	Opts     *bind.TransactOpts
	ResultCh chan<- *TransactionResult
}

// TransactionResult represents the result of a transaction attempt
type TransactionResult struct {
	Tx  *types.Transaction
	Err error
}

// TransactionSender handles sending transactions to the blockchain in a sequential manner
type TransactionSender struct {
	client      *ethclient.Client
	contract    *bind.BoundContract
	fromAddress common.Address
	requestCh   chan *TransactionRequest
	wg          sync.WaitGroup
	quitCh      chan struct{}
}

// NewTxSender creates a new TxSender
func NewTxSender(client *ethclient.Client, contract *bind.BoundContract, fromAddress common.Address) *TransactionSender {
	sender := &TransactionSender{
		client:      client,
		contract:    contract,
		fromAddress: fromAddress,
		requestCh:   make(chan *TransactionRequest, 200),
		quitCh:      make(chan struct{}),
	}

	// Add the worker goroutine
	sender.wg.Add(1)
	go sender.processTransactions()

	return sender
}
