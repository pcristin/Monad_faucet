package blockchain

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

type DepositEvent struct {
	Depositor   common.Address
	Amount      *big.Int
	DepositId   *big.Int
	Currency    CurrencyType
	BlockNumber uint64
}

func (e DepositEvent) String() string {
	decimals := uint8(18)
	if e.Currency != CurrencyETH {
		decimals = 6 // USDC and USDT have 6 decimals
	}

	// Convert amount to float with proper decimals
	amount := new(big.Float).SetInt(e.Amount)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	amount.Quo(amount, divisor)

	return fmt.Sprintf("Deposit: %s %.6f %s (ID: %s)",
		e.Depositor.Hex(),
		amount,
		CurrencyTypeToString(e.Currency),
		e.DepositId.String())
}

type EventListener struct {
	client      *ethclient.Client
	contractABI abi.ABI
	address     common.Address
}

func NewEventListener(rpcURL string) (*EventListener, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Web3 client: %v", err)
	}

	// Test connection by getting network ID
	_, err = client.NetworkID(context.Background())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to get network ID, connection might be invalid: %v", err)
	}

	listener := &EventListener{
		client:      client,
		contractABI: DepositorABI,
		address:     common.HexToAddress(BridgeAddress),
	}

	return listener, nil
}

func (l *EventListener) ListenToDeposits(ctx context.Context) (<-chan DepositEvent, <-chan error) {
	events := make(chan DepositEvent)
	errors := make(chan error)

	query := ethereum.FilterQuery{
		Addresses: []common.Address{l.address},
		Topics: [][]common.Hash{
			{l.contractABI.Events["DepositEvent"].ID},
		},
	}

	go func() {
		defer close(events)
		defer close(errors)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				logs := make(chan types.Log)
				sub, err := l.client.SubscribeFilterLogs(ctx, query, logs)
				if err != nil {
					errors <- fmt.Errorf("subscription error: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}

				for {
					select {
					case err := <-sub.Err():
						errors <- fmt.Errorf("subscription error: %v", err)
						sub.Unsubscribe()
						time.Sleep(5 * time.Second)
						goto RECONNECT
					case vLog := <-logs:
						event, err := l.parseDepositEvent(vLog)
						if err != nil {
							errors <- fmt.Errorf("parse error: %v", err)
							continue
						}
						events <- event
					case <-ctx.Done():
						sub.Unsubscribe()
						return
					}
				}
			RECONNECT:
				continue
			}
		}
	}()

	return events, errors
}

type rawDepositEvent struct {
	Depositor   common.Address
	Amount      *big.Int
	DepositId   *big.Int
	Currency    uint8
	BlockNumber uint64
}

func (l *EventListener) parseDepositEvent(vLog types.Log) (DepositEvent, error) {
	var raw rawDepositEvent

	err := l.contractABI.UnpackIntoInterface(&raw, "DepositEvent", vLog.Data)
	if err != nil {
		return DepositEvent{}, fmt.Errorf("failed to unpack event: %v", err)
	}

	raw.Depositor = common.BytesToAddress(vLog.Topics[1].Bytes())
	raw.BlockNumber = vLog.BlockNumber

	// Convert raw event to DepositEvent with proper CurrencyType
	return DepositEvent{
		Depositor:   raw.Depositor,
		Amount:      raw.Amount,
		DepositId:   raw.DepositId,
		Currency:    CurrencyType(raw.Currency),
		BlockNumber: raw.BlockNumber,
	}, nil
}

func (l *EventListener) Close() {
	logger.Info("Closing event listener")
	l.client.Close()
}
