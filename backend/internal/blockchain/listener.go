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
	TxHash      string // Transaction hash of the deposit
	Metadata    string // User-provided metadata string for this deposit
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

	return fmt.Sprintf("Deposit: %s %.6f %s (ID: %s, Metadata: %s)",
		e.Depositor.Hex(),
		amount,
		CurrencyTypeToString(e.Currency),
		e.DepositId.String(),
		e.Metadata)
}

type EventListener struct {
	client      *ethclient.Client
	contractABI abi.ABI
	address     common.Address
}

func NewEventListener(rpcURL string, contractAddress common.Address) (*EventListener, error) {
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
		address:     contractAddress,
	}

	logger.Info("Event listener created for contract address: %s", contractAddress.Hex())
	return listener, nil
}

// min returns the smaller of x or y
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
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

	logger.Info("Setting up deposit event subscription for contract: %s", l.address.Hex())
	logger.Info("Event signature: %s", l.contractABI.Events["DepositEvent"].ID.Hex())

	go func() {
		defer close(events)
		defer close(errors)

		reconnectAttempt := 0
		maxReconnectDelay := 60 * time.Second
		baseReconnectDelay := 5 * time.Second

		for {
			select {
			case <-ctx.Done():
				logger.Info("Context done, stopping deposit event listener")
				return
			default:
				reconnectDelay := time.Duration(min(int64(reconnectAttempt), 10)) * baseReconnectDelay
				if reconnectDelay > maxReconnectDelay {
					reconnectDelay = maxReconnectDelay
				}

				if reconnectAttempt > 0 {
					logger.Info("Reconnect attempt %d, waiting %v before reconnecting",
						reconnectAttempt, reconnectDelay)
					time.Sleep(reconnectDelay)
				}

				logger.Info("Creating new subscription for deposit events (attempt %d)", reconnectAttempt+1)
				logs := make(chan types.Log)
				sub, err := l.client.SubscribeFilterLogs(ctx, query, logs)
				if err != nil {
					reconnectAttempt++
					logger.Error("Failed to subscribe to deposit events (attempt %d): %v",
						reconnectAttempt, err)
					errors <- fmt.Errorf("subscription error: %v", err)
					continue
				}

				// Reset reconnect counter on successful connection
				reconnectAttempt = 0
				logger.Info("Subscription created successfully, waiting for deposit events")

				for {
					select {
					case err := <-sub.Err():
						reconnectAttempt++
						logger.Error("Subscription error received (attempt %d): %v",
							reconnectAttempt, err)
						errors <- fmt.Errorf("subscription error: %v", err)
						sub.Unsubscribe()
						logger.Info("Unsubscribed due to error, will reconnect")
						goto RECONNECT
					case vLog := <-logs:
						logger.Info("Received blockchain log event: txHash=%s blockNumber=%d",
							vLog.TxHash.Hex(), vLog.BlockNumber)
						event, err := l.parseDepositEvent(vLog)
						if err != nil {
							logger.Error("Failed to parse deposit event: %v", err)
							errors <- fmt.Errorf("parse error: %v", err)
							continue
						}
						logger.Info("Successfully parsed deposit event: ID=%s, Amount=%s, Currency=%s",
							event.DepositId.String(), event.Amount.String(),
							CurrencyTypeToString(event.Currency))
						events <- event
					case <-ctx.Done():
						logger.Info("Context done, stopping active subscription")
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
	Metadata    string // User-provided metadata string
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
		TxHash:      vLog.TxHash.Hex(), // Set transaction hash from the log
		Metadata:    raw.Metadata,      // Include the metadata in the parsed event
	}, nil
}

func (l *EventListener) Close() {
	logger.Info("Closing event listener")
	l.client.Close()
}
