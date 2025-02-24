package blockchain

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type CurrencyType = uint8

const (
	CurrencyETH  CurrencyType = 0
	CurrencyUSDC CurrencyType = 1
	CurrencyUSDT CurrencyType = 2
)

func CurrencyTypeToString(c CurrencyType) string {
	switch c {
	case CurrencyETH:
		return "ETH"
	case CurrencyUSDC:
		return "USDC"
	case CurrencyUSDT:
		return "USDT"
	default:
		return fmt.Sprintf("Unknown(%d)", c)
	}
}

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
	lastBlock   uint64
}

func NewEventListener(rpcURL string) (*EventListener, error) {
	log.Printf("Connecting to WebSocket endpoint: %s", rpcURL)
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Web3 client: %v", err)
	}

	// Test connection by getting network ID
	networkID, err := client.NetworkID(context.Background())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to get network ID, connection might be invalid: %v", err)
	}
	log.Printf("Connected to network ID: %s", networkID.String())

	parsedABI, err := abi.JSON(strings.NewReader(BridgeABI))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to parse ABI: %v", err)
	}

	listener := &EventListener{
		client:      client,
		contractABI: parsedABI,
		address:     common.HexToAddress(BridgeAddress),
	}

	log.Printf("Event listener initialized for contract: %s", BridgeAddress)
	return listener, nil
}

func (l *EventListener) ListenToDeposits(ctx context.Context) (<-chan DepositEvent, <-chan error) {
	events := make(chan DepositEvent)
	errors := make(chan error)

	// Create a filter query for the Deposit event
	query := ethereum.FilterQuery{
		Addresses: []common.Address{l.address},
		Topics: [][]common.Hash{
			{l.contractABI.Events["DepositEvent"].ID},
		},
	}

	log.Printf("Starting subscription with filter ID: %s", l.contractABI.Events["DepositEvent"].ID.Hex())

	// Subscribe to logs with retry mechanism
	go func() {
		defer close(events)
		defer close(errors)

		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping event listener")
				return
			default:
				logs := make(chan types.Log)
				sub, err := l.client.SubscribeFilterLogs(ctx, query, logs)
				if err != nil {
					log.Printf("Failed to subscribe to logs: %v, retrying in 5 seconds...", err)
					errors <- fmt.Errorf("subscription error: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}

				log.Println("Successfully subscribed to events")

				// Handle events in a nested loop
				for {
					select {
					case err := <-sub.Err():
						log.Printf("Subscription error: %v, reconnecting...", err)
						errors <- fmt.Errorf("subscription error: %v", err)
						sub.Unsubscribe()
						time.Sleep(5 * time.Second)
						goto RECONNECT
					case vLog := <-logs:
						log.Printf("Received log from block %d, tx: %s", vLog.BlockNumber, vLog.TxHash.Hex())
						event, err := l.parseDepositEvent(vLog)
						if err != nil {
							log.Printf("Failed to parse event: %v", err)
							errors <- fmt.Errorf("parse error: %v", err)
							continue
						}
						events <- event
					case <-ctx.Done():
						log.Println("Context cancelled, stopping subscription")
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

func (l *EventListener) pollEvents(ctx context.Context, events chan<- DepositEvent) error {
	currentBlock, err := l.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %v", err)
	}

	if currentBlock <= l.lastBlock {
		return nil // No new blocks
	}

	fromBlock := l.lastBlock + 1
	toBlock := currentBlock

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{l.address},
		Topics: [][]common.Hash{
			{l.contractABI.Events["DepositEvent"].ID},
		},
	}

	logs, err := l.client.FilterLogs(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs: %v", err)
	}

	if len(logs) > 0 {
		log.Printf("Found %d events between blocks %d and %d", len(logs), fromBlock, toBlock)
	}

	for _, vLog := range logs {
		event, err := l.parseDepositEvent(vLog)
		if err != nil {
			log.Printf("Failed to parse event: %v", err)
			continue
		}
		log.Printf("📥 %s", event.String())
		events <- event
	}

	l.lastBlock = toBlock
	return nil
}

func (l *EventListener) parseDepositEvent(vLog types.Log) (DepositEvent, error) {
	var event DepositEvent

	err := l.contractABI.UnpackIntoInterface(&event, "DepositEvent", vLog.Data)
	if err != nil {
		return event, fmt.Errorf("failed to unpack event: %v", err)
	}

	event.Depositor = common.BytesToAddress(vLog.Topics[1].Bytes())
	event.BlockNumber = vLog.BlockNumber

	return event, nil
}

func (l *EventListener) Close() {
	log.Println("Closing event listener")
	l.client.Close()
}
