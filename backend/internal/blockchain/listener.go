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

type DepositEvent struct {
	Depositor   common.Address
	Amount      *big.Int
	DepositId   *big.Int
	Currency    uint8
	BlockNumber uint64
}

type EventListener struct {
	client      *ethclient.Client
	contractABI abi.ABI
	address     common.Address
	lastBlock   uint64
}

func NewEventListener(rpcURL string) (*EventListener, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Web3 client: %v", err)
	}

	parsedABI, err := abi.JSON(strings.NewReader(BridgeABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ABI: %v", err)
	}

	latestBlock, err := client.BlockNumber(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block number: %v", err)
	}

	listener := &EventListener{
		client:      client,
		contractABI: parsedABI,
		address:     common.HexToAddress(BridgeAddress),
		lastBlock:   latestBlock,
	}

	return listener, nil
}

func (l *EventListener) ListenToDeposits(ctx context.Context) (<-chan DepositEvent, <-chan error) {
	events := make(chan DepositEvent)
	errors := make(chan error)

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		defer close(events)
		defer close(errors)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := l.pollEvents(ctx, events); err != nil {
					errors <- err
				}
			}
		}
	}()

	return events, errors
}

func (l *EventListener) pollEvents(ctx context.Context, events chan<- DepositEvent) error {
	latestBlock, err := l.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %v", err)
	}

	if latestBlock <= l.lastBlock {
		return nil // No new blocks
	}

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(l.lastBlock + 1),
		ToBlock:   new(big.Int).SetUint64(latestBlock),
		Addresses: []common.Address{l.address},
		Topics: [][]common.Hash{
			{l.contractABI.Events["DepositEvent"].ID},
		},
	}

	logs, err := l.client.FilterLogs(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs: %v", err)
	}

	for _, vLog := range logs {
		event, err := l.parseDepositEvent(vLog)
		if err != nil {
			log.Printf("Failed to parse event: %v", err)
			continue
		}
		events <- event
	}

	l.lastBlock = latestBlock
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
	l.client.Close()
}
