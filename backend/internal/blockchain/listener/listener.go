package listener

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

func (e DepositEvent) String() string {
	decimals := uint8(18)
	if e.Currency != blockchain.CurrencyETH {
		decimals = 6 // USDC and USDT have 6 decimals
	}

	// Convert amount to float with proper decimals
	amount := new(big.Float).SetInt(e.Amount)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	amount.Quo(amount, divisor)

	// Get network type info
	networkName, isTestnet := GetChainInfo(e.Chain)
	networkType := "Mainnet"
	if isTestnet {
		networkType = "Testnet"
	}

	return fmt.Sprintf("Deposit: %s %.6f %s (ID: %s, Chain: %s-%s, Metadata: %s)",
		e.Depositor.Hex(),
		amount,
		blockchain.CurrencyTypeToString(e.Currency),
		e.DepositId.String(),
		networkName,
		networkType,
		e.Metadata)
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
				logger.Debug("DEPOSIT-INFLOW: Deposit %d received after %v since previous (ID: %s)",
					eventCount, timeSinceLast, evt.DepositId.String())

				// Log warning for significant delays
				if timeSinceLast > 5*time.Second {
					logger.Warn("DEPOSIT-INFLOW: Long gap of %v between deposits %d and %d",
						timeSinceLast, eventCount-1, eventCount)
				}
			} else {
				logger.Debug("DEPOSIT-INFLOW: Deposit %d received (first deposit, ID: %s)",
					eventCount, evt.DepositId.String())
			}

			lastEventTime = now

			// Calculate and log current rate
			if eventCount > 1 {
				elapsed := lastEventTime.Sub(firstEventTime)
				rate := float64(eventCount) / elapsed.Seconds()
				logger.Debug("DEPOSIT-INFLOW: Current rate: %.2f deposits/sec (%d in %v)",
					rate, eventCount, elapsed.Round(time.Millisecond))
			}

			trackingMutex.Unlock()

			trackedEvents <- evt
		}
	}()

	go l.listenWithAlchemyOptimization(ctx, events, errors)

	return trackedEvents, errors
}

func (l *EventListener) listenWithAlchemyOptimization(ctx context.Context, events chan<- DepositEvent, errors chan<- error) {
	defer close(events)
	defer close(errors)

	reconnectAttempt := 0
	maxReconnectDelay := 60 * time.Second
	baseReconnectDelay := 5 * time.Second
	pingInterval := 30 * time.Second

	// Create buffered channels for better performance
	bufferSize := 1000 // Adjust based on expected event volume
	bufferedEvents := make(chan DepositEvent, bufferSize)

	// Create a worker pool to process events in parallel
	const maxWorkers = 10
	workerPool := make(chan struct{}, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		workerPool <- struct{}{}
	}

	// Start a goroutine to handle the events asynchronously
	go func() {
		for event := range bufferedEvents {
			select {
			case events <- event: // Try to send to the original channel
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context done, stopping deposit event listener")
			return
		default:
			// Handle reconnection delay logic
			if reconnectAttempt > 0 {
				reconnectDelay := time.Duration(min(int64(reconnectAttempt), 12)) * baseReconnectDelay
				if reconnectDelay > maxReconnectDelay {
					reconnectDelay = maxReconnectDelay
				}

				logger.Info("Reconnect attempt %d, waiting %v before reconnecting", reconnectAttempt, reconnectDelay)
				select {
				case <-time.After(reconnectDelay):
				case <-ctx.Done():
					return
				}

				l.Close()
				var err error
				l.client, err = ethclient.Dial(l.rpcURL)
				if err != nil {
					reconnectAttempt++
					errors <- fmt.Errorf("reconnection error: %v", err)
					continue
				}
			}

			// Create filter query
			depositEventID := l.contractABI.Events["DepositEvent"].ID
			filterQuery := ethereum.FilterQuery{
				Addresses: []common.Address{l.address},
				Topics: [][]common.Hash{
					{depositEventID},
				},
			}

			// Only use one subscription method - the native one
			msgChan := make(chan types.Log, 200) // Buffered to prevent blocking
			sub, err := l.client.SubscribeFilterLogs(ctx, filterQuery, msgChan)
			if err != nil {
				reconnectAttempt++
				errors <- fmt.Errorf("subscription error: %v", err)
				continue
			}

			// Reset reconnect counter on successful connection
			reconnectAttempt = 0
			logger.Info("Logs subscription created successfully for event ID: %s", depositEventID.Hex())

			// Set up ping timer
			pingTimer := time.NewTicker(pingInterval)
			defer pingTimer.Stop()

			// Process events with minimal overhead
			for {
				select {
				case <-ctx.Done():
					sub.Unsubscribe()
					return
				case <-pingTimer.C:
					// Simplified ping - only log errors
					if _, err := l.client.NetworkID(ctx); err != nil {
						logger.Error("Ping failed: %v", err)
						goto RECONNECT
					}
				case <-l.reconnectChan:
					logger.Info("Reconnection requested by error handler")
					goto RECONNECT
				case err := <-sub.Err():
					if err != nil {
						errors <- fmt.Errorf("subscription error: %v", err)
						goto RECONNECT
					}
				case vLog := <-msgChan:
					// Direct processing without JSON marshaling
					if vLog.Address == l.address && len(vLog.Topics) > 0 && vLog.Topics[0] == depositEventID {
						// Process event in a worker goroutine
						select {
						case worker := <-workerPool:
							go func(log types.Log, worker struct{}) {
								defer func() { workerPool <- worker }() // Return worker to pool

								// Parse the deposit event directly from the log
								depositEvent, err := l.parseDepositEvent(log)
								if err != nil {
									logger.Error("Failed to parse deposit event: %v", err)
									return
								}

								// Send to buffered channel to prevent blocking
								select {
								case bufferedEvents <- depositEvent:
									if log.BlockNumber > 0 {
										// Only log once in a while to reduce overhead
										if log.BlockNumber%10 == 0 {
											logger.Debug("DEPOSIT-SOURCE: Processing block %d, event ID %s",
												log.BlockNumber, depositEvent.DepositId.String())
										}
									}
								case <-ctx.Done():
									return
								default:
									// If buffer is full, log warning and continue
									logger.Warn("Event buffer full, dropping event: %s", depositEvent.DepositId.String())
								}
							}(vLog, worker)
						default:
							// All workers are busy, process in current goroutine
							depositEvent, err := l.parseDepositEvent(vLog)
							if err != nil {
								logger.Error("Failed to parse deposit event: %v", err)
								continue
							}

							// Try to send event non-blocking
							select {
							case bufferedEvents <- depositEvent:
							default:
								logger.Warn("Event buffer full, dropping event: %s", depositEvent.DepositId.String())
							}
						}
					}
				case <-time.After(30 * time.Second):
					// No message received, check connection health
					if _, err := l.client.NetworkID(ctx); err != nil {
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
		Currency:    blockchain.CurrencyType(raw.Currency),
		BlockNumber: raw.BlockNumber,
		TxHash:      vLog.TxHash.Hex(), // Set transaction hash from the log
		Metadata:    raw.Metadata,      // Include the metadata in the parsed event
		Chain:       l.chain,           // Include the chain information
	}, nil
}

func (l *EventListener) Close() {
	logger.Info("Closing event listener")
	l.client.Close()
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
