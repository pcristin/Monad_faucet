package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
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
	client        *ethclient.Client
	rpcClient     *rpc.Client
	contractABI   abi.ABI
	address       common.Address
	rpcURL        string
	isAlchemy     bool
	reconnectChan chan struct{}
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

func NewEventListener(rpcURL string, contractAddress common.Address) (*EventListener, error) {
	isAlchemy := false
	// Check if this is an Alchemy URL
	if len(rpcURL) > 10 && rpcURL[:10] == "wss://eth-" {
		isAlchemy = true
		logger.Info("Using Alchemy-specific optimizations for WebSocket")
	}

	rpcClient, err := rpc.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Web3 RPC client: %v", err)
	}

	client := ethclient.NewClient(rpcClient)

	// Test connection by getting network ID
	_, err = client.NetworkID(context.Background())
	if err != nil {
		client.Close()
		rpcClient.Close()
		return nil, fmt.Errorf("failed to get network ID, connection might be invalid: %v", err)
	}

	listener := &EventListener{
		client:        client,
		rpcClient:     rpcClient,
		contractABI:   DepositorABI,
		address:       contractAddress,
		rpcURL:        rpcURL,
		isAlchemy:     isAlchemy,
		reconnectChan: make(chan struct{}, 1),
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

	// Initialize event tracking variables
	var (
		eventCount     int64
		firstEventTime time.Time
		lastEventTime  time.Time
		trackingMutex  sync.Mutex
	)

	// Start a goroutine to log stats periodically
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				trackingMutex.Lock()
				if eventCount > 0 && !firstEventTime.IsZero() {
					elapsed := time.Since(firstEventTime)
					rate := float64(eventCount) / elapsed.Seconds()
					logger.Info("DEPOSIT-INFLOW: Total deposits received: %d over %v (%.2f/sec overall)",
						eventCount, elapsed, rate)

					if !lastEventTime.IsZero() {
						timeSinceLast := time.Since(lastEventTime)
						logger.Info("DEPOSIT-INFLOW: Time since last deposit: %v", timeSinceLast)
					}
				}
				trackingMutex.Unlock()
			}
		}
	}()

	// Create a wrapper for the events channel to track metrics
	trackedEvents := make(chan DepositEvent)
	go func() {
		defer close(trackedEvents)

		for evt := range events {
			trackingMutex.Lock()
			now := time.Now()

			eventCount++
			if firstEventTime.IsZero() {
				firstEventTime = now
				logger.Info("DEPOSIT-INFLOW: First deposit event received at %v", now.Format(time.RFC3339))
			}

			if !lastEventTime.IsZero() {
				timeSinceLast := now.Sub(lastEventTime)
				logger.Info("DEPOSIT-INFLOW: Deposit %d received after %v since previous (ID: %s)",
					eventCount, timeSinceLast, evt.DepositId.String())

				// Log warning for significant delays
				if timeSinceLast > 5*time.Second {
					logger.Warn("DEPOSIT-INFLOW: Long gap of %v between deposits %d and %d",
						timeSinceLast, eventCount-1, eventCount)
				}
			} else {
				logger.Info("DEPOSIT-INFLOW: Deposit %d received (first deposit, ID: %s)",
					eventCount, evt.DepositId.String())
			}

			lastEventTime = now

			// Calculate and log current rate
			if eventCount > 1 {
				elapsed := lastEventTime.Sub(firstEventTime)
				rate := float64(eventCount) / elapsed.Seconds()
				logger.Info("DEPOSIT-INFLOW: Current rate: %.2f deposits/sec (%d in %v)",
					rate, eventCount, elapsed.Round(time.Millisecond))
			}

			trackingMutex.Unlock()

			trackedEvents <- evt
		}
	}()

	if l.isAlchemy {
		go l.listenWithAlchemyOptimization(ctx, events, errors)
	} else {
		go l.listenWithStandardMethod(ctx, events, errors)
	}

	return trackedEvents, errors
}

func (l *EventListener) listenWithAlchemyOptimization(ctx context.Context, events chan<- DepositEvent, errors chan<- error) {
	defer close(events)
	defer close(errors)

	reconnectAttempt := 0
	maxReconnectDelay := 60 * time.Second
	baseReconnectDelay := 5 * time.Second
	pingInterval := 30 * time.Second

	var subscriptionID string
	lastProcessedBlock := uint64(0)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context done, stopping deposit event listener")
			if subscriptionID != "" {
				// Properly unsubscribe
				var unsubscribed bool
				err := l.rpcClient.Call(&unsubscribed, "eth_unsubscribe", subscriptionID)
				if err != nil {
					logger.Error("Failed to unsubscribe: %v", err)
				} else if unsubscribed {
					logger.Info("Successfully unsubscribed from %s", subscriptionID)
				}
			}
			return
		default:
			reconnectDelay := time.Duration(min(int64(reconnectAttempt), 12)) * baseReconnectDelay
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}

			if reconnectAttempt > 0 {
				logger.Info("Reconnect attempt %d, waiting %v before reconnecting",
					reconnectAttempt, reconnectDelay)
				select {
				case <-time.After(reconnectDelay):
				case <-ctx.Done():
					return
				}
			}

			// Close the previous connections if reconnecting
			if reconnectAttempt > 0 {
				logger.Info("Closing previous connections before reconnecting")
				l.Close()
				var err error
				l.rpcClient, err = rpc.Dial(l.rpcURL)
				if err != nil {
					reconnectAttempt++
					logger.Error("Failed to reconnect to Web3 RPC client (attempt %d): %v",
						reconnectAttempt, err)
					errors <- fmt.Errorf("reconnection error: %v", err)
					continue
				}
				l.client = ethclient.NewClient(l.rpcClient)
			}

			logger.Info("Creating new Alchemy minedTransactions subscription (attempt %d)", reconnectAttempt+1)

			// Set up subscription parameters for contract address
			params := map[string]interface{}{
				"addresses": []map[string]string{
					{"to": l.address.Hex()}, // Filter for transactions TO our contract
				},
				"includeRemoved": false,
				"hashesOnly":     false, // We need full transaction details
			}

			// Subscribe to alchemy_minedTransactions with our filter
			err := l.rpcClient.Call(&subscriptionID, "eth_subscribe", "alchemy_minedTransactions", params)
			if err != nil {
				reconnectAttempt++
				logger.Error("Failed to subscribe to Alchemy mined transactions (attempt %d): %v",
					reconnectAttempt, err)
				errors <- fmt.Errorf("subscription error: %v", err)
				continue
			}

			// Reset reconnect counter on successful connection
			reconnectAttempt = 0
			logger.Info("Alchemy subscription created successfully with ID: %s", subscriptionID)

			// Set up ping timer for keepalive
			pingTimer := time.NewTicker(pingInterval)
			defer pingTimer.Stop()

			// Use proper channels for subscription
			msgChan := make(chan interface{})
			sub, err := l.rpcClient.EthSubscribe(ctx, msgChan, "alchemy_minedTransactions", params)
			if err != nil {
				reconnectAttempt++
				logger.Error("Failed to subscribe to Alchemy mined transactions (attempt %d): %v",
					reconnectAttempt, err)
				errors <- fmt.Errorf("subscription error: %v", err)
				continue
			}

			// Subscribe to responses
			for {
				select {
				case <-ctx.Done():
					logger.Info("Context done, stopping active Alchemy subscription")
					sub.Unsubscribe()
					return
				case <-pingTimer.C:
					// Send a ping (network ID request) to keep connection alive
					_, err := l.client.NetworkID(ctx)
					if err != nil {
						logger.Error("Ping failed, connection might be dead: %v", err)
						goto RECONNECT
					}
				case <-l.reconnectChan:
					logger.Info("Reconnection requested by error handler")
					goto RECONNECT
				case err := <-sub.Err():
					if err != nil {
						logger.Error("Subscription error detected: %v", err)
						errors <- fmt.Errorf("subscription error: %v", err)
						goto RECONNECT
					}
				case msg := <-msgChan:
					// Parse the message
					var alcMsg AlchemySubscriptionMsg
					jsonData, err := json.Marshal(msg)
					if err != nil {
						logger.Error("Failed to marshal message: %v", err)
						continue
					}

					if err := json.Unmarshal(jsonData, &alcMsg); err != nil {
						logger.Error("Failed to unmarshal Alchemy message: %v", err)
						continue
					}

					// Process the received transaction
					if alcMsg.Method == "eth_subscription" && alcMsg.Params.Subscription == subscriptionID {
						tx := alcMsg.Params.Result.Transaction

						// Only process transactions to our contract address
						if common.HexToAddress(tx.To) == l.address {
							logger.Info("DEPOSIT-SOURCE: Received transaction to contract: %s", tx.Hash)
							startProcessingTime := time.Now()

							// Get the full transaction to decode data
							actualTx, isPending, err := l.client.TransactionByHash(ctx, common.HexToHash(tx.Hash))
							if err != nil {
								logger.Error("Failed to get transaction by hash: %v", err)
								continue
							}

							if isPending {
								logger.Warn("Transaction is still pending, which is unexpected in mined transactions")
								continue
							}

							// Get the block to get the actual block number
							blockNumInt, ok := new(big.Int).SetString(tx.BlockNumber[2:], 16) // Remove "0x" prefix
							if !ok {
								logger.Error("Failed to parse block number: %s", tx.BlockNumber)
								continue
							}
							blockNum := blockNumInt.Uint64()

							// Log block progression
							if lastProcessedBlock > 0 {
								blockDiff := blockNum - lastProcessedBlock
								if blockDiff > 1 {
									logger.Info("DEPOSIT-SOURCE: Processing block %d (skipped %d blocks since last event)",
										blockNum, blockDiff-1)
								} else {
									logger.Info("DEPOSIT-SOURCE: Processing consecutive block %d", blockNum)
								}
							} else {
								logger.Info("DEPOSIT-SOURCE: Processing first block %d", blockNum)
							}
							lastProcessedBlock = blockNum

							// Parse the transaction input data to get the deposit event data
							depositEvent, err := l.parseTransactionInput(ctx, actualTx, blockNum)
							if err != nil {
								logger.Error("Failed to parse transaction input: %v", err)
								continue
							}

							// Add the transaction hash to the deposit event
							depositEvent.TxHash = tx.Hash

							processingTime := time.Since(startProcessingTime)
							logger.Info("DEPOSIT-SOURCE: Processed deposit event ID %s in %v",
								depositEvent.DepositId.String(), processingTime)

							// Send the deposit event to the events channel
							sendStart := time.Now()
							events <- depositEvent
							sendTime := time.Since(sendStart)

							if sendTime > 100*time.Millisecond {
								logger.Warn("DEPOSIT-SOURCE: Slow channel send for ID %s took %v",
									depositEvent.DepositId.String(), sendTime)
							}
						}
					}
				case <-time.After(30 * time.Second):
					// No message received, check connection health
					_, err := l.client.NetworkID(ctx)
					if err != nil {
						logger.Error("No messages received and connection check failed: %v", err)
						goto RECONNECT
					}
				}
			}

		RECONNECT:
			logger.Info("Attempting to reconnect WebSocket")
			continue
		}
	}
}

// parseTransactionInput processes a transaction and tries to extract the deposit event data
func (l *EventListener) parseTransactionInput(ctx context.Context, tx *types.Transaction, blockNum uint64) (DepositEvent, error) {
	// Default empty event
	event := DepositEvent{
		BlockNumber: blockNum,
	}

	// The transaction data contains the method signature and encoded parameters
	data := tx.Data()

	// Find the deposit method and decode the parameters
	method, err := l.contractABI.MethodById(data[:4])
	if err != nil {
		return event, fmt.Errorf("could not find method: %v", err)
	}

	if method.Name != "deposit" {
		return event, fmt.Errorf("not a deposit method call: %s", method.Name)
	}

	// Get receipt to find the emitted event
	receipt, err := l.client.TransactionReceipt(ctx, tx.Hash())
	if err != nil {
		return event, fmt.Errorf("failed to get transaction receipt: %v", err)
	}

	// Look for the deposit event in the logs
	for _, log := range receipt.Logs {
		if log.Address == l.address && len(log.Topics) > 0 && log.Topics[0] == l.contractABI.Events["DepositEvent"].ID {
			// Parse the deposit event from the log
			parsedEvent, err := l.parseDepositEvent(*log)
			if err != nil {
				return event, fmt.Errorf("failed to parse deposit event from log: %v", err)
			}
			return parsedEvent, nil
		}
	}

	return event, fmt.Errorf("deposit event not found in transaction logs")
}

func (l *EventListener) listenWithStandardMethod(ctx context.Context, events chan<- DepositEvent, errors chan<- error) {
	defer close(events)
	defer close(errors)

	query := ethereum.FilterQuery{
		Addresses: []common.Address{l.address},
		Topics: [][]common.Hash{
			{l.contractABI.Events["DepositEvent"].ID},
		},
	}

	logger.Info("Setting up standard deposit event subscription for contract: %s", l.address.Hex())
	logger.Info("Event signature: %s", l.contractABI.Events["DepositEvent"].ID.Hex())

	reconnectAttempt := 0
	maxReconnectDelay := 60 * time.Second
	baseReconnectDelay := 5 * time.Second
	pingInterval := 30 * time.Second
	lastProcessedBlock := uint64(0)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context done, stopping deposit event listener")
			return
		default:
			reconnectDelay := time.Duration(min(int64(reconnectAttempt), 12)) * baseReconnectDelay
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}

			if reconnectAttempt > 0 {
				logger.Info("Reconnect attempt %d, waiting %v before reconnecting",
					reconnectAttempt, reconnectDelay)
				select {
				case <-time.After(reconnectDelay):
				case <-ctx.Done():
					return
				}
			}

			// Close the previous connections if reconnecting
			if reconnectAttempt > 0 {
				logger.Info("Closing previous connections before reconnecting")
				l.Close()
				var err error
				l.rpcClient, err = rpc.Dial(l.rpcURL)
				if err != nil {
					reconnectAttempt++
					logger.Error("Failed to reconnect to Web3 RPC client (attempt %d): %v",
						reconnectAttempt, err)
					errors <- fmt.Errorf("reconnection error: %v", err)
					continue
				}
				l.client = ethclient.NewClient(l.rpcClient)
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

			// Set up ping timer for keepalive
			pingTimer := time.NewTicker(pingInterval)
			defer pingTimer.Stop()

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
				case <-pingTimer.C:
					// Send a ping (network ID request) to keep connection alive
					_, err := l.client.NetworkID(ctx)
					if err != nil {
						logger.Error("Ping failed, connection might be dead: %v", err)
						sub.Unsubscribe()
						goto RECONNECT
					}
				case vLog := <-logs:
					logger.Info("DEPOSIT-SOURCE: Received blockchain log event: txHash=%s blockNumber=%d",
						vLog.TxHash.Hex(), vLog.BlockNumber)

					startProcessingTime := time.Now()

					// Log block progression
					if lastProcessedBlock > 0 {
						blockDiff := vLog.BlockNumber - lastProcessedBlock
						if blockDiff > 1 {
							logger.Info("DEPOSIT-SOURCE: Processing block %d (skipped %d blocks since last event)",
								vLog.BlockNumber, blockDiff-1)
						} else {
							logger.Info("DEPOSIT-SOURCE: Processing consecutive block %d", vLog.BlockNumber)
						}
					} else {
						logger.Info("DEPOSIT-SOURCE: Processing first block %d", vLog.BlockNumber)
					}
					lastProcessedBlock = vLog.BlockNumber

					event, err := l.parseDepositEvent(vLog)
					if err != nil {
						logger.Error("Failed to parse deposit event: %v", err)
						errors <- fmt.Errorf("parse error: %v", err)
						continue
					}

					processingTime := time.Since(startProcessingTime)
					logger.Info("DEPOSIT-SOURCE: Processed deposit event ID %s in %v",
						event.DepositId.String(), processingTime)

					logger.Info("Successfully parsed deposit event: ID=%s, Amount=%s, Currency=%s",
						event.DepositId.String(), event.Amount.String(),
						CurrencyTypeToString(event.Currency))

					sendStart := time.Now()
					events <- event
					sendTime := time.Since(sendStart)

					if sendTime > 100*time.Millisecond {
						logger.Warn("DEPOSIT-SOURCE: Slow channel send for ID %s took %v",
							event.DepositId.String(), sendTime)
					}
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
	l.rpcClient.Close()
}

// Helper function to request reconnection from another goroutine
func (l *EventListener) RequestReconnect() {
	select {
	case l.reconnectChan <- struct{}{}:
		logger.Info("Reconnection requested")
	default:
		// Channel is full, reconnection already requested
	}
}
