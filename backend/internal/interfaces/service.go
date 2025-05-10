package interfaces

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// DepositEvent contains information about a deposit event
type DepositEvent struct {
	DepositID   *big.Int
	UserAddress common.Address
	Amount      *big.Int
	TxHash      string
	BlockNumber uint64
	SourceChain string
	Timestamp   uint64
}

// BridgeServiceInterface defines methods that a bridge service must implement
type BridgeServiceInterface interface {
	Start(ctx context.Context) error
	Stop() error
	ProcessDeposit(depositID *big.Int, userAddress common.Address, amount *big.Int, txHash string) error
	SimulateDeposit(depositID *big.Int, userAddress common.Address, amount *big.Int, txHash string)
	SubscribeToDepositEvents(ch chan<- DepositEvent) error
	UnsubscribeFromDepositEvents(ch chan<- DepositEvent) error
	SimulateTransactionConfirmation(txHash string) error
}

// MockBridgeService implements BridgeServiceInterface for testing
type MockBridgeService struct {
	StartFunc                           func(ctx context.Context) error
	StopFunc                            func() error
	ProcessDepositFunc                  func(depositID *big.Int, userAddress common.Address, amount *big.Int, txHash string) error
	SimulateDepositFunc                 func(depositID *big.Int, userAddress common.Address, amount *big.Int, txHash string)
	SubscribeToDepositEventsFunc        func(ch chan<- DepositEvent) error
	UnsubscribeFromDepositEventsFunc    func(ch chan<- DepositEvent) error
	SimulateTransactionConfirmationFunc func(txHash string) error
}
