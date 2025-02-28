package blockchain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// WalletLimitPercentage is the percentage of total MON balance that can be distributed to a single wallet per transaction
var WalletLimitPercentage int64 = 30 // 30% of total distributor MON balance

// UpdateWalletLimitPercentage updates the wallet limit percentage
func UpdateWalletLimitPercentage(newPercentage int64) error {
	if newPercentage < 0 {
		return fmt.Errorf("wallet limit percentage cannot be negative")
	}
	if newPercentage > 100 {
		return fmt.Errorf("wallet limit percentage cannot exceed 100%%")
	}

	WalletLimitPercentage = newPercentage
	logger.Info("Wallet limit percentage updated to %d%%", newPercentage)

	// Update the database if available
	if dbInstance != nil {
		if err := dbInstance.SetIntSetting("wallet_limit_percentage", int(newPercentage)); err != nil {
			logger.Error("Failed to update wallet limit percentage in database: %v", err)
			return err
		}
	}

	return nil
}

// BridgeService handles the business logic for the bridge operations
type BridgeService struct {
	arbDepositor      *ArbitrumDepositor
	monadDistributor  *MonadDistributor
	depositChan       chan DepositEvent
	refundChan        chan *big.Int
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	walletUsage       map[common.Address]*WalletUsage  // Track wallet usage
	walletMutex       sync.RWMutex                     // Mutex for thread-safe access to walletUsage
	db                *database.DB                     // Database connection
	txCache           map[string]*database.Transaction // Cache for transaction status
	txCacheMutex      sync.RWMutex
	txCacheExpiration time.Duration
}

// NewBridgeService creates a new instance of BridgeService
func NewBridgeService(
	arbDepositor *ArbitrumDepositor,
	monadDistributor *MonadDistributor,
	db *database.DB,
) *BridgeService {
	ctx, cancel := context.WithCancel(context.Background())

	// Set the database instance for the contracts package
	SetDatabase(db)

	return &BridgeService{
		arbDepositor:      arbDepositor,
		monadDistributor:  monadDistributor,
		depositChan:       make(chan DepositEvent),
		refundChan:        make(chan *big.Int),
		wg:                sync.WaitGroup{},
		ctx:               ctx,
		cancel:            cancel,
		walletUsage:       make(map[common.Address]*WalletUsage),
		walletMutex:       sync.RWMutex{},
		db:                db,
		txCache:           make(map[string]*database.Transaction),
		txCacheExpiration: 5 * time.Minute, // Cache transactions for 5 minutes
	}
}

// Start initializes the service and starts processing deposits
func (s *BridgeService) Start() error {
	logger.Info("Starting bridge service...")
	s.wg.Add(1)
	go s.processDeposits()
	s.wg.Add(1)
	go s.processRefunds()
	return nil
}

// Stop gracefully shuts down the service
func (s *BridgeService) Stop() error {
	logger.Info("Stopping bridge service...")
	s.cancel()
	s.wg.Wait()
	logger.Info("Bridge service stopped")
	return nil
}

// HandleDeposit queues a deposit for processing
func (s *BridgeService) HandleDeposit(event DepositEvent) {
	select {
	case s.depositChan <- event:
		logger.Info("Queued deposit: %s", event.String())
	default:
		logger.Warn("Deposit channel full, dropping event: %s", event.String())
	}
}

// QueueRefund queues a deposit ID for refund
func (s *BridgeService) QueueRefund(depositId *big.Int) {
	select {
	case s.refundChan <- depositId:
		logger.Info("Queued refund for deposit ID: %s", depositId.String())
	default:
		logger.Warn("Refund channel full, dropping deposit ID: %s", depositId.String())
	}
}

// GetState returns the current state of both contracts
func (s *BridgeService) GetState(ctx context.Context) (*ContractState, error) {
	return GetContractState(ctx, s.arbDepositor, s.monadDistributor)
}

func (s *BridgeService) processDeposits() {
	defer s.wg.Done()
	logger.Info("Starting deposit processor...")

	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-s.depositChan:
			start := time.Now()
			if err := s.processDeposit(event); err != nil {
				// Check if this is a duplicate mint error, which shouldn't trigger a refund
				if strings.Contains(err.Error(), "duplicate mint attempt") {
					logger.Warn("Skipping refund for duplicate mint attempt: %v", err)
				} else {
					// Only queue a refund for non-duplicate errors
					logger.Error("Error processing deposit: %v", err)
					s.QueueRefund(event.DepositId)
				}
			}
			logger.Info("Processing time: %v", time.Since(start))
		}
	}
}

func (s *BridgeService) processRefunds() {
	defer s.wg.Done()
	logger.Info("Starting refund processor...")

	for {
		select {
		case <-s.ctx.Done():
			return
		case depositId := <-s.refundChan:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
			if err := s.refundDeposit(ctx, depositId); err != nil {
				logger.Error("Error processing refund for deposit ID %s: %v", depositId.String(), err)
			} else {
				logger.Info("Successfully refunded deposit ID: %s", depositId.String())
			}
			cancel()
		}
	}
}

func (s *BridgeService) processDeposit(event DepositEvent) error {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
	defer cancel()

	state, err := s.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %v", err)
	}

	// Calculate MON amount once and reuse it throughout the function
	monAmount := calculateMonAmount(event.Amount, state.SwapRatios[event.Currency], event.Currency)

	// Create a pending transaction record in the database
	txRecord := &database.Transaction{
		DepositID:     event.DepositId,
		WalletAddress: event.Depositor,
		Amount:        event.Amount,
		Currency:      database.CurrencyType(event.Currency),
		MonAmount:     monAmount,
		Status:        database.StatusPending,
		TxHash:        event.TxHash, // Arbitrum transaction hash
	}

	if err := s.db.CreateTransaction(txRecord); err != nil {
		logger.Error("Failed to create transaction record: %v", err)
		// Continue processing even if DB record creation fails
	}

	if err := s.validateDepositWithAmount(state, event, monAmount); err != nil {
		// Update transaction status to failed
		if err2 := s.db.UpdateTransactionStatus(event.DepositId, database.StatusFailed, ""); err2 != nil {
			logger.Error("Failed to update transaction status to failed: %v", err2)
		}
		return fmt.Errorf("deposit validation failed: %v", err)
	}

	if err := s.waitForConfirmations(ctx, event.BlockNumber, 10); err != nil {
		// Update transaction status to failed
		if err2 := s.db.UpdateTransactionStatus(event.DepositId, database.StatusFailed, ""); err2 != nil {
			logger.Error("Failed to update transaction status to failed: %v", err2)
		}
		return fmt.Errorf("failed to wait for confirmations: %v", err)
	}

	// Attempt to mint tokens
	monadTxHash, err := s.mintTokens(ctx, event.Depositor, monAmount, event.DepositId)
	if err != nil {
		// Update transaction status to failed
		if err2 := s.db.UpdateTransactionStatus(event.DepositId, database.StatusFailed, ""); err2 != nil {
			logger.Error("Failed to update transaction status to failed: %v", err2)
		}
		return fmt.Errorf("failed to mint tokens: %v", err)
	}

	// Update transaction status to completed with the Monad transaction hash
	if err := s.db.UpdateTransactionStatus(event.DepositId, database.StatusCompleted, monadTxHash); err != nil {
		logger.Error("Failed to update transaction status to completed: %v", err)
	}

	// Update wallet usage after successful mint
	s.updateWalletUsage(event.Depositor, monAmount)

	logger.Info("Successfully processed deposit %s", event.String())
	return nil
}

// validateDepositWithAmount validates a deposit with a precalculated MON amount
// to avoid redundant calculations
func (s *BridgeService) validateDepositWithAmount(state *ContractState, event DepositEvent, monAmount *big.Int) error {
	if state.IsPaused {
		return fmt.Errorf("bridge is paused")
	}

	if state.MonBalance.Cmp(monAmount) < 0 {
		return fmt.Errorf("insufficient MON balance in distributor (required: %s MON, available: %s MON)",
			new(big.Float).Quo(
				new(big.Float).SetInt(monAmount),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			).Text('f', 6),
			new(big.Float).Quo(
				new(big.Float).SetInt(state.MonBalance),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			).Text('f', 6),
		)
	}

	// Check wallet limit
	if err := s.checkWalletLimit(monAmount, state.MonBalance); err != nil {
		return err
	}

	var out []interface{}
	var method string
	switch event.Currency {
	case CurrencyETH:
		method = "minEthDeposit"
	case CurrencyUSDC:
		method = "minUsdcDeposit"
	case CurrencyUSDT:
		method = "minUsdtDeposit"
	default:
		return fmt.Errorf("unsupported currency type")
	}

	err := s.arbDepositor.BoundContract.Call(&bind.CallOpts{Context: s.ctx}, &out, method)
	if err != nil {
		return fmt.Errorf("failed to get min amount for %s: %v", CurrencyTypeToString(event.Currency), err)
	}

	minAmount := out[0].(*big.Int)
	if event.Amount.Cmp(minAmount) < 0 {
		return fmt.Errorf("deposit amount below minimum for %s", CurrencyTypeToString(event.Currency))
	}

	return nil
}

func (s *BridgeService) waitForConfirmations(ctx context.Context, blockNumber uint64, confirmations uint64) error {
	targetBlock := blockNumber + confirmations
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentBlock, err := s.arbDepositor.client.BlockNumber(ctx)
			if err != nil {
				log.Printf("Error getting current block number: %v", err)
				continue
			}

			if currentBlock >= targetBlock {
				return nil
			}
		}
	}
}

func (s *BridgeService) mintTokens(ctx context.Context, recipient common.Address, amount *big.Int, depositId *big.Int) (string, error) {
	// First check if we already have a successful or pending transaction for this deposit ID
	existingTx, err := s.db.GetTransactionByDepositID(depositId)
	if err == nil && existingTx != nil {
		// If there's already a completed transaction for this deposit ID, return the existing hash
		if existingTx.Status == database.StatusCompleted && existingTx.MonadTxHash != "" {
			logger.Warn("Skipping duplicate mint attempt for deposit ID %s - already completed with tx %s",
				depositId.String(), existingTx.MonadTxHash)
			return existingTx.MonadTxHash, nil
		}

		// If there's already a transaction in progress with a Monad tx hash, provide a specific error
		if existingTx.Status == database.StatusPending && existingTx.MonadTxHash != "" {
			logger.Warn("Rejecting duplicate mint attempt for deposit ID %s - already in progress with tx %s",
				depositId.String(), existingTx.MonadTxHash)
			return "", fmt.Errorf("duplicate mint attempt for deposit ID %s - distribution already in progress", depositId.String())
		}
	}

	transfer := []struct {
		Recipient common.Address `abi:"recipient"`
		Amount    *big.Int       `abi:"amount"`
		ID        *big.Int       `abi:"id"`
	}{
		{
			Recipient: recipient,
			Amount:    amount,
			ID:        depositId,
		},
	}

	opts, err := s.monadDistributor.GetTransactOpts(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction options: %v", err)
	}

	tx, err := s.monadDistributor.BoundContract.Transact(opts, "distributeFunds", transfer)
	if err != nil {
		return "", fmt.Errorf("failed to distribute funds: %v", err)
	}

	receipt, err := bind.WaitMined(ctx, s.monadDistributor.client, tx)
	if err != nil {
		return "", fmt.Errorf("failed to wait for distribution transaction: %v", err)
	}

	if receipt.Status == 0 {
		return "", fmt.Errorf("distribution transaction failed")
	}

	logger.Info("✅ Distributed %s MON to %s (tx: %s)",
		new(big.Float).Quo(
			new(big.Float).SetInt(amount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))).Text('f', 9),
		recipient.Hex(),
		tx.Hash().Hex(),
	)
	return tx.Hash().Hex(), nil
}

func calculateMonAmount(depositAmount *big.Int, swapRatio *big.Int, currencyType CurrencyType) *big.Int {
	// For stablecoins (USDC/USDT), we need to adjust for decimal differences
	if currencyType == CurrencyUSDC || currencyType == CurrencyUSDT {
		// Stablecoins have 6 decimals, need to adjust by 12 (18-6)
		var decimalAdjustment int64 = 12

		// Adjust deposit amount for decimal difference
		adjustedDepositAmount := new(big.Int).Mul(
			depositAmount,
			new(big.Int).Exp(big.NewInt(10), big.NewInt(decimalAdjustment), nil),
		)

		// Calculate MON amount using the swap ratio
		result := new(big.Int).Mul(adjustedDepositAmount, swapRatio)
		result = new(big.Int).Div(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

		// Convert to human-readable values for logging and additional calculations
		depositValueFloat := new(big.Float).Quo(
			new(big.Float).SetInt(depositAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)), // 6 decimals for stablecoins
		)

		resultFloat := new(big.Float).Quo(
			new(big.Float).SetInt(result),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)), // 18 decimals for MON
		)

		depositValue, _ := depositValueFloat.Float64() // USD value
		resultValue, _ := resultFloat.Float64()        // MON value

		// Log the calculation
		logger.Info("Stablecoin to MON calculation: %s units ($%.6f), adjusted=%s, swapRatio=%s, result=%s MON wei (%.6f MON)",
			depositAmount.String(),
			depositValue,
			adjustedDepositAmount.String(),
			swapRatio.String(),
			result.String(),
			resultValue)

		// Handle small deposits (when calculated MON is very low or zero)
		if (result.Sign() <= 0 && depositAmount.Sign() > 0) || resultValue < 0.00001 {
			// For stablecoins, the USD value is directly the deposit amount
			// We need to use the current MON/USD ratio rather than a hardcoded multiplier

			// Get MON/USD ratio from the global setting
			monUsdRatio := GetMonUsdRatio()

			// Convert MON/USD ratio to float for easier math
			monUsdRatioFloat := new(big.Float).Quo(
				new(big.Float).SetInt(monUsdRatio),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			)
			monRatioValue, _ := monUsdRatioFloat.Float64() // e.g., 0.17

			// Calculate expected MON amount from USD value (depositValue / monRatioValue)
			// For example: $0.17 / 0.17 = 1 MON
			expectedMon := new(big.Float).Quo(
				depositValueFloat,
				monUsdRatioFloat,
			)

			// Convert to wei (multiply by 10^18)
			expectedMonWei := new(big.Float).Mul(
				expectedMon,
				new(big.Float).SetFloat64(1e18),
			)

			// Convert to integer with proper rounding
			expectedMonWeiInt, _ := expectedMonWei.Int(nil)

			// Base minimum MON amount (in wei) for any valid deposit
			minMonWei := big.NewInt(1000000000000000) // 0.001 MON (10^15 wei)

			// Ensure we provide at least the minimum MON amount for any valid deposit
			if expectedMonWeiInt.Cmp(minMonWei) < 0 {
				expectedMonWeiInt = new(big.Int).Set(minMonWei)
			}

			// For very small deposits (under $0.001), ensure they still get something if valid
			if depositValue < 0.001 && depositAmount.Sign() > 0 {
				// Ensure minimum amount of MON wei for extremely small deposits
				microMon := big.NewInt(10000000000000) // 0.00001 MON (10^13 wei)
				if expectedMonWeiInt.Cmp(microMon) < 0 {
					expectedMonWeiInt = microMon
				}
			}

			humanReadableMon := new(big.Float).Quo(
				new(big.Float).SetInt(expectedMonWeiInt),
				new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			).Text('f', 6)

			logger.Info("Small stablecoin deposit ($%.6f with MON/USD ratio $%.6f), allocating %s MON (%s wei)",
				depositValue, monRatioValue, humanReadableMon, expectedMonWeiInt.String())

			return expectedMonWeiInt
		}

		return result
	}

	// For ETH deposits, we need to:
	// 1. Get ETH/USD price directly from Chainlink (8 decimal precision)
	// 2. Get MON/USD ratio (18 decimal precision)
	// 3. Calculate: depositAmount * ethUsdPrice / monUsdRatio (with proper decimal adjustments)

	// Get direct ETH/USD price from Chainlink (8 decimal precision)
	ethUsdPrice := GetEthUsdPrice() // This should return the Chainlink price feed value

	// Get MON/USD ratio with 18 decimal precision
	monUsdRatio := GetMonUsdRatio()

	// Convert to human-readable values for logging
	depositEthFloat := new(big.Float).Quo(
		new(big.Float).SetInt(depositAmount),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
	)

	ethUsdPriceFloat := new(big.Float).Quo(
		new(big.Float).SetInt(ethUsdPrice),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)), // 8 decimals for Chainlink
	)

	monUsdRatioFloat := new(big.Float).Quo(
		new(big.Float).SetInt(monUsdRatio),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
	)

	// Calculate USD value: depositAmount * ethUsdPrice / 10^(18+8-18) = depositAmount * ethUsdPrice / 10^8
	// Using big.Rat for precise calculation
	depositRat := new(big.Rat).SetInt(depositAmount)
	ethUsdPriceRat := new(big.Rat).SetInt(ethUsdPrice)
	usdValueRat := new(big.Rat).Mul(depositRat, ethUsdPriceRat)
	// Adjust for Chainlink decimals (8)
	usdValueRat = new(big.Rat).Quo(usdValueRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)))
	// Further adjust for ETH decimals (18)
	usdValueRat = new(big.Rat).Quo(usdValueRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))

	// Calculate MON amount: usdValue * 10^18 / monUsdRatio
	monAmountRat := new(big.Rat).Mul(usdValueRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	monAmountRat = new(big.Rat).Quo(monAmountRat, new(big.Rat).SetInt(monUsdRatio))

	// Convert to floats for logging
	usdValueFloat, _ := usdValueRat.Float64()
	monAmountFloat, _ := new(big.Rat).Quo(monAmountRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))).Float64()

	// Calculate final MON wei amount (rounding properly)
	monWeiRat := new(big.Rat).Add(monAmountRat, new(big.Rat).SetFrac(big.NewInt(1), big.NewInt(2))) // Add 0.5 for rounding
	monWeiInt := new(big.Int)
	monWeiRat.Num().Div(monWeiRat.Num(), monWeiRat.Denom())
	monWeiInt.Set(monWeiRat.Num())

	// Log the calculation
	ethUsdPriceValue, _ := ethUsdPriceFloat.Float64()
	monUsdRatioValue, _ := monUsdRatioFloat.Float64()

	// Handle small deposits (when calculated MON is very low or zero)
	if (monWeiInt.Sign() <= 0 && depositAmount.Sign() > 0) || monAmountFloat < 0.00001 {
		// Base minimum MON amount (in wei) for any valid deposit
		minMonWei := big.NewInt(1000000000000000) // 0.001 MON (10^15 wei)

		// Get MON/USD ratio for direct calculation
		monUsdRatio := GetMonUsdRatio()
		monUsdRatioFloat := new(big.Float).Quo(
			new(big.Float).SetInt(monUsdRatio),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		)
		monRatioValue, _ := monUsdRatioFloat.Float64()

		// Calculate expected MON amount in proper proportion to USD value
		// For an ETH deposit worth $0.02 with MON/USD = $0.17, we should get 0.02/0.17 = 0.1176 MON
		expectedMon := new(big.Float).Quo(
			new(big.Float).SetFloat64(usdValueFloat),
			monUsdRatioFloat,
		)

		// Convert to wei (multiply by 10^18)
		expectedMonWei := new(big.Float).Mul(
			expectedMon,
			new(big.Float).SetFloat64(1e18),
		)

		// Convert to integer with proper rounding
		expectedMonWeiInt, _ := expectedMonWei.Int(nil)

		// Ensure we provide at least the minimum MON amount for any valid deposit
		if expectedMonWeiInt.Cmp(minMonWei) < 0 {
			expectedMonWeiInt = new(big.Int).Set(minMonWei)
		}

		// For very small deposits (under $0.001), ensure they still get something if valid
		if usdValueFloat < 0.001 && depositAmount.Sign() > 0 {
			// Ensure minimum amount of MON wei for extremely small deposits
			microMon := big.NewInt(10000000000000) // 0.00001 MON (10^13 wei)
			if expectedMonWeiInt.Cmp(microMon) < 0 {
				expectedMonWeiInt = microMon
			}
		}

		humanReadableMon := new(big.Float).Quo(
			new(big.Float).SetInt(expectedMonWeiInt),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6)

		logger.Info("ETH to MON calculation: %s ETH ≈ $%.6f USD (ETH price: $%.2f) / $%.6f per MON = %s MON (%s wei)",
			depositEthFloat.Text('f', 18),
			usdValueFloat,
			ethUsdPriceValue,
			monRatioValue,
			humanReadableMon,
			expectedMonWeiInt.String())

		return expectedMonWeiInt
	}

	logger.Info("ETH to MON calculation: %s ETH ≈ $%.6f USD (ETH price: $%.2f) / $%.6f per MON = %.6f MON (%s wei)",
		depositEthFloat.Text('f', 18),
		usdValueFloat,
		ethUsdPriceValue,
		monUsdRatioValue,
		monAmountFloat,
		monWeiInt.String())

	return monWeiInt
}

// GetEthUsdPrice returns the current ETH/USD price from Chainlink oracle
// This price is in 8 decimal precision as per Chainlink standard
func GetEthUsdPrice() *big.Int {
	// Use the Chainlink ETH/USD price feed
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Parse the Chainlink price feed ABI
	priceFeedAbi, err := abi.JSON(strings.NewReader(PriceFeedABI))
	if err != nil {
		logger.Error("Failed to parse price feed ABI: %v", err)
		// Return fallback value
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // ~$3000 with 8 decimals
	}

	// Get a client to use for the price feed
	// We'll use the ArbitrumDepositor's client if available, or create a temporary one if needed
	var client *ethclient.Client

	// Try to get a client from a globally available service
	clients := getAvailableClients()
	if len(clients) > 0 {
		client = clients[0]
	} else {
		// No clients available, log the error and return default
		logger.Error("No Ethereum clients available for Chainlink price feed")
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // ~$3000 with 8 decimals
	}

	// Create a price feed contract binding
	priceFeed := bind.NewBoundContract(common.HexToAddress(ChainlinkEthUsdFeed), priceFeedAbi, client, client, client)

	// Call the latestRoundData function
	var out []interface{}
	err = priceFeed.Call(&bind.CallOpts{Context: ctx}, &out, "latestRoundData")
	if err != nil {
		logger.Error("Failed to get ETH/USD price from Chainlink: %v", err)
		// Return fallback value
		return new(big.Int).Mul(big.NewInt(3000), big.NewInt(100000000)) // ~$3000 with 8 decimals
	}

	// Extract the price value (second return value, index 1)
	ethUsdPrice := out[1].(*big.Int)

	// Log the price for debugging
	ethUsdPriceFloat := new(big.Float).Quo(
		new(big.Float).SetInt(ethUsdPrice),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(8), nil)),
	)
	ethUsdPriceValue, _ := ethUsdPriceFloat.Float64()
	logger.Info("Current ETH/USD price from Chainlink: $%.2f", ethUsdPriceValue)

	return ethUsdPrice
}

// getAvailableClients returns available ethclient.Client instances
// This is a helper function to get any available client for price feed queries
func getAvailableClients() []*ethclient.Client {
	var clients []*ethclient.Client

	// Get global variables or singletons that might have clients
	// This is a placeholder - in a real implementation, you would access
	// your application's client pool or global client instances

	// For now, let's create a temporary client
	rpcURL := "https://arb1.arbitrum.io/rpc" // Arbitrum One RPC URL
	client, err := ethclient.Dial(rpcURL)
	if err == nil && client != nil {
		clients = append(clients, client)
	}

	return clients
}

// PauseDeposits pauses deposit functionality
func (s *BridgeService) PauseDeposits(ctx context.Context) error {
	// Get transaction options
	publicKey := s.arbDepositor.privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Pack the method call
	input, err := DepositorABI.Pack("pauseDeposits")
	if err != nil {
		return fmt.Errorf("failed to pack pauseDeposits data: %v", err)
	}

	// Get current gas price with a small buffer (20% increase)
	gasPrice, err := s.arbDepositor.client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))

	// Estimate gas
	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &s.arbDepositor.address,
		Data: input,
	}
	gasLimit, err := s.arbDepositor.client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add 20% buffer to gas limit
	gasLimit = gasLimit * 12 / 10

	// Create transaction
	nonce, err := s.arbDepositor.client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.arbDepositor.address,
		Value:    big.NewInt(0),
		Data:     input,
	})

	// Sign and send transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.arbDepositor.chainID), s.arbDepositor.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	err = s.arbDepositor.client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send pause transaction: %v", err)
	}

	// Wait for transaction receipt
	receipt, err := bind.WaitMined(ctx, s.arbDepositor.client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for pause transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("pause transaction failed")
	}

	logger.Info("Successfully paused deposits (tx: %s)", signedTx.Hash().Hex())
	return nil
}

// ResumeDeposits resumes deposit functionality
func (s *BridgeService) ResumeDeposits(ctx context.Context) error {
	// Get transaction options
	publicKey := s.arbDepositor.privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Pack the method call
	input, err := DepositorABI.Pack("resumeDeposits")
	if err != nil {
		return fmt.Errorf("failed to pack resumeDeposits data: %v", err)
	}

	// Get current gas price with a small buffer (20% increase)
	gasPrice, err := s.arbDepositor.client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))

	// Estimate gas
	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &s.arbDepositor.address,
		Data: input,
	}
	gasLimit, err := s.arbDepositor.client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add 20% buffer to gas limit
	gasLimit = gasLimit * 12 / 10

	// Create transaction
	nonce, err := s.arbDepositor.client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &s.arbDepositor.address,
		Value:    big.NewInt(0),
		Data:     input,
	})

	// Sign and send transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(s.arbDepositor.chainID), s.arbDepositor.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	err = s.arbDepositor.client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send resume transaction: %v", err)
	}

	// Wait for transaction receipt
	receipt, err := bind.WaitMined(ctx, s.arbDepositor.client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for resume transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("resume transaction failed")
	}

	logger.Info("Successfully resumed deposits (tx: %s)", signedTx.Hash().Hex())
	return nil
}

// checkWalletLimit checks if a transaction exceeds the per-transaction wallet limit
// Returns nil if the wallet is within limits, otherwise returns an error
func (s *BridgeService) checkWalletLimit(requestedAmount *big.Int, totalMonBalance *big.Int) error {
	// If limit is set to 0, there are no limits
	if WalletLimitPercentage == 0 {
		return nil
	}

	// Calculate the maximum allowed amount (percentage of total MON balance)
	maxAllowedAmount := new(big.Int).Mul(totalMonBalance, big.NewInt(WalletLimitPercentage))
	maxAllowedAmount = new(big.Int).Div(maxAllowedAmount, big.NewInt(100))

	// Check if the current request exceeds the limit
	if requestedAmount.Cmp(maxAllowedAmount) > 0 {
		// Format amounts for logging
		maxAllowedFormatted := new(big.Float).Quo(
			new(big.Float).SetInt(maxAllowedAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6)
		requestedFormatted := new(big.Float).Quo(
			new(big.Float).SetInt(requestedAmount),
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		).Text('f', 6)

		return fmt.Errorf("requested amount (%s MON) exceeds wallet limit (%s MON per transaction)",
			requestedFormatted, maxAllowedFormatted)
	}

	return nil
}

// updateWalletUsage updates the usage record for a wallet
// This is kept for compatibility but no longer tracks usage over time
func (s *BridgeService) updateWalletUsage(wallet common.Address, amount *big.Int) {
	// No longer needed for per-transaction limits, but kept for compatibility
	// with existing code structure
}

// GetDepositIDFromTxHash retrieves the deposit ID from a transaction hash
func (s *BridgeService) GetDepositIDFromTxHash(ctx context.Context, txHash string) (*big.Int, error) {
	// Parse the transaction hash
	hash := common.HexToHash(txHash)
	if hash == (common.Hash{}) {
		return nil, fmt.Errorf("invalid transaction hash format")
	}

	// Try to find the transaction in the database
	tx, err := s.db.GetTransactionByArbitrumTxHash(hash.Hex())
	if err == nil && tx != nil {
		logger.Info("Found transaction with deposit ID %s for tx %s in database", tx.DepositID.String(), txHash)
		return tx.DepositID, nil
	}

	// If not in database, return an error
	return nil, fmt.Errorf("transaction not found in database")
}

// GetTransactionByDepositID retrieves transaction details by deposit ID
func (s *BridgeService) GetTransactionByDepositID(ctx context.Context, depositID *big.Int) (*database.Transaction, error) {
	// Check cache first
	cacheKey := depositID.String()
	s.txCacheMutex.RLock()
	cachedTx, exists := s.txCache[cacheKey]
	s.txCacheMutex.RUnlock()

	// If transaction is in cache and not pending, return it
	if exists && cachedTx.Status != database.StatusPending {
		return cachedTx, nil
	}

	// Query the database for the transaction
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction from database: %w", err)
	}

	// Cache the transaction if it's not pending
	if tx.Status != database.StatusPending {
		s.txCacheMutex.Lock()
		s.txCache[cacheKey] = tx
		s.txCacheMutex.Unlock()

		// Start a goroutine to remove the transaction from cache after expiration
		go func(key string, expiration time.Duration) {
			time.Sleep(expiration)
			s.txCacheMutex.Lock()
			delete(s.txCache, key)
			s.txCacheMutex.Unlock()
		}(cacheKey, s.txCacheExpiration)
	}

	return tx, nil
}

// UpdateTransactionStatus updates the status of a transaction
func (s *BridgeService) UpdateTransactionStatus(ctx context.Context, depositID *big.Int, status, txHash string) error {
	// Update the transaction in the database
	err := s.db.UpdateTransactionStatus(depositID, status, txHash)
	if err != nil {
		return fmt.Errorf("failed to update transaction status in database: %w", err)
	}

	// Clear the cache for this transaction
	s.clearTransactionCache(depositID.String())

	return nil
}

// clearTransactionCache removes a transaction from the cache
func (s *BridgeService) clearTransactionCache(depositID string) {
	s.txCacheMutex.Lock()
	delete(s.txCache, depositID)
	s.txCacheMutex.Unlock()
}

// CheckDatabaseConnection verifies the database connection is working
func (s *BridgeService) CheckDatabaseConnection() error {
	// Simple ping to verify database connection
	return s.db.Ping()
}

// CheckBlockchainConnections verifies connections to blockchain nodes
func (s *BridgeService) CheckBlockchainConnections() error {
	// Check Arbitrum connection by attempting to get the latest block number
	arbClient := s.arbDepositor.client
	if _, err := arbClient.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("arbitrum connection failed: %w", err)
	}

	// Check Monad connection by attempting to get the latest block number
	monadClient := s.monadDistributor.client
	if _, err := monadClient.BlockNumber(context.Background()); err != nil {
		return fmt.Errorf("monad connection failed: %w", err)
	}

	return nil
}

// GracefulShutdown waits for in-progress operations to complete
func (s *BridgeService) GracefulShutdown(ctx context.Context) {
	// Signal that we're shutting down
	s.cancel()

	// Create a channel to signal completion
	done := make(chan struct{})

	// Wait for processing to complete in a goroutine
	go func() {
		s.wg.Wait()
		close(done)
	}()

	// Wait for processing to complete or context to timeout
	select {
	case <-done:
		// Processing completed normally
		logger.Info("All in-progress transactions completed")
	case <-ctx.Done():
		// Timeout occurred
		logger.Warn("Shutdown timed out, some transactions may not have completed")
	}
}

// GetTransactionCounts returns counts of transactions by status
func (s *BridgeService) GetTransactionCounts() (pending, completed, failed, refunded int, err error) {
	// Query the database for transaction counts by status
	rows, err := s.db.Query(`
		SELECT status, COUNT(*) 
		FROM transaction_history 
		GROUP BY status
	`)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to query transaction counts: %w", err)
	}
	defer rows.Close()

	// Process the results
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("failed to scan transaction count row: %w", err)
		}

		// Increment the appropriate counter
		switch status {
		case database.StatusPending:
			pending = count
		case database.StatusCompleted:
			completed = count
		case database.StatusFailed:
			failed = count
		case database.StatusRefunded:
			refunded = count
		}
	}

	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("error iterating transaction count rows: %w", err)
	}

	return pending, completed, failed, refunded, nil
}

// GetCacheSize returns the current size of the transaction cache
func (s *BridgeService) GetCacheSize() int {
	s.txCacheMutex.RLock()
	defer s.txCacheMutex.RUnlock()
	return len(s.txCache)
}

// GetDB returns the database connection
func (s *BridgeService) GetDB() *database.DB {
	return s.db
}

// GetArbitrumClient returns the Arbitrum client
func (s *BridgeService) GetArbitrumClient() *ethclient.Client {
	return s.arbDepositor.client
}

// GetArbitrumContractAddress returns the Arbitrum contract address
func (s *BridgeService) GetArbitrumContractAddress() common.Address {
	return s.arbDepositor.address
}

// FindMonadTransactionByDepositID looks for a transaction on the Monad blockchain by deposit ID
// Returns the transaction hash, status, and any error that occurred
func (s *BridgeService) FindMonadTransactionByDepositID(ctx context.Context, depositID *big.Int) (string, string, error) {
	logger.Info("Looking for Monad transaction with deposit ID: %s", depositID.String())

	// Try looking in the database as a first option
	tx, err := s.db.GetTransactionByDepositID(depositID)
	if err == nil && tx != nil {
		logger.Info("Found transaction in database: deposit_id=%s, status=%s, tx_hash=%s",
			depositID.String(), tx.Status, tx.TxHash)

		// If there's a transaction hash in the database, use it
		if tx.TxHash != "" {
			return tx.TxHash, string(tx.Status), nil
		}

		// If status is completed but no hash, it might be special handling
		if tx.Status == database.StatusCompleted {
			logger.Info("Transaction marked as completed in database: %s", depositID.String())
			return "", "success", nil
		}
	}

	// Not found in database
	logger.Info("No Monad transaction found for deposit ID: %s", depositID.String())
	return "", "", nil
}

func (s *BridgeService) refundDeposit(ctx context.Context, depositId *big.Int) error {
	logger.Info("Delegating refund of deposit ID %s to ArbitrumDepositor", depositId.String())
	return s.arbDepositor.RefundDeposit(ctx, depositId)
}
