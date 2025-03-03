package blockchain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"strings"
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

func (r *MonUsdRatio) Get() *big.Int {
	return r.value.Load().(*big.Int)
}

func (r *MonUsdRatio) Set(newValue *big.Int) {
	r.value.Store(newValue)
}

// ContractState represents the current state of the bridge contracts
type ContractState struct {
	IsPaused    bool
	MinAmount   *big.Int
	MonBalance  *big.Int
	SwapRatios  map[CurrencyType]*big.Int
	MonUsdRatio *MonUsdRatio // Thread-safe MON/USD ratio
}

// Initial MON/USD ratio (0.1 USD = 1 MON)
var initialMonUsdRatio = new(big.Int).Div(
	new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil), // 10^18
	big.NewInt(10), // Divide by 10 to get 0.1 * 10^18
) // 0.1 * 10^18

// Create global ratio instance
var globalMonUsdRatio = NewMonUsdRatio(initialMonUsdRatio)
var dbInstance *database.DB

// SetDatabase sets the database instance for persistence
func SetDatabase(db *database.DB) {
	dbInstance = db

	// Load settings from the database
	loadSettingsFromDB()
}

// loadSettingsFromDB loads settings from the database if available
func loadSettingsFromDB() {
	if dbInstance == nil {
		return
	}

	// Load MON/USD ratio
	ratio, err := dbInstance.GetBigIntSetting("mon_usd_ratio")
	if err == nil && ratio != nil {
		// Update the in-memory value without updating the database again
		globalMonUsdRatio.Set(ratio)
		log.Printf("Loaded MON/USD ratio from database: %s", ratio.String())
	} else {
		// If database value is not available, use the default value
		log.Printf("Could not load MON/USD ratio from database (error: %v). Using default value: %s",
			err, initialMonUsdRatio.String())
	}

	// Load wallet limit percentage
	limitPercentage, err := dbInstance.GetIntSetting("wallet_limit_percentage")
	if err == nil {
		// Update the in-memory value without updating the database again
		err = dbInstance.SetIntSetting("wallet_limit_percentage", limitPercentage)
		if err != nil {
			logger.Error("Error updating wallet limit percentage in database: %v", err)
		}
		log.Printf("Loaded wallet limit percentage from database: %d%%", limitPercentage)
	}
}

// UpdateMonUsdRatio updates the global MON/USD ratio
func UpdateMonUsdRatio(newRatio *big.Int) {
	// Update the in-memory value
	globalMonUsdRatio.Set(newRatio)
	log.Printf("MON/USD ratio updated to: %s", newRatio.String())

	// Update the database if available
	if dbInstance != nil {
		if err := dbInstance.SetBigIntSetting("mon_usd_ratio", newRatio); err != nil {
			log.Printf("Error updating MON/USD ratio in database: %v", err)
		}
	}
}

// GetMonUsdRatio returns the current MON/USD ratio
func GetMonUsdRatio() *big.Int {
	return globalMonUsdRatio.Get()
}

// Calculate swap ratios based on current MON/USD ratio
func calculateSwapRatios(ethUsdPrice *big.Int) map[CurrencyType]*big.Int {
	ratios := make(map[CurrencyType]*big.Int)
	monUsdRatio := GetMonUsdRatio()

	// For USDC/USDT, we want to calculate MON wei per smallest USDT/USDC unit
	// monUsdRatio is "USD price per 1 MON" in 18 decimals (e.g., 0.17 * 10^18)

	// Instead of integer division which loses precision, use a completely different approach
	// 1. Calculate how many MON per USD (float): 1/0.17 = 5.88 MON per USD
	// 2. Convert to MON wei per smallest USD unit

	// First, find MON per USD in wei
	// monPerUsd = 10^36 / monUsdRatio (for 0.17, this is 5.88 * 10^18)
	monPerUsd := new(big.Float).Quo(
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		new(big.Float).SetInt(monUsdRatio),
	)

	// Log this value for debugging
	logger.Info("MON per USD (float calculation): %s", monPerUsd.Text('f', 6))

	// Calculate MON wei per smallest USD unit (divide by 10^6 since USDT/USDC have 6 decimals)
	// For 5.88 MON per USD, this would be 5.88 * 10^18 / 10^6 = 5.88 * 10^12 wei
	monPerSmallestUsd := new(big.Float).Quo(
		new(big.Float).Mul(
			monPerUsd,
			new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)),
	)

	// Convert the result to a big.Int for the ratio
	monWeiPerSmallestUsd, _ := monPerSmallestUsd.Int(nil)

	// This gives us the number of MON wei per smallest USDT/USDC unit
	ratios[CurrencyUSDC] = new(big.Int).Set(monWeiPerSmallestUsd)
	ratios[CurrencyUSDT] = new(big.Int).Set(monWeiPerSmallestUsd)

	// For ETH: To calculate how much ETH 1 MON costs:
	// If 1 ETH = $2000 (ethUsdPrice) and 1 MON = $0.17 (monUsdRatio)
	// Then 1 MON = 0.17/2000 ETH = 0.000085 ETH

	// With decimal scaling:
	// ethUsdPrice from Chainlink has 8 decimals (e.g., 2000 * 10^8)
	// monUsdRatio has 18 decimals (e.g., 0.17 * 10^18)

	// First convert ethUsdPrice to 18 decimal places to match MON
	ethUsdPriceScaled := new(big.Int).Mul(ethUsdPrice, new(big.Int).Exp(big.NewInt(10), big.NewInt(10), nil))

	// We want (MON/USD) / (ETH/USD) * 10^18 to get the ETH/MON ratio with 18 decimals
	// This is simplified to: (monUsdRatio * 10^18) / ethUsdPriceScaled
	monUsdScaled := new(big.Int).Mul(monUsdRatio, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	// Calculate ETH/MON ratio
	ratios[CurrencyETH] = new(big.Int).Div(monUsdScaled, ethUsdPriceScaled)

	// Log detailed calculation values for debugging
	logger.Info("MON/USD ratio: %s (%s USD per 1 MON)",
		monUsdRatio.String(),
		formatBigIntAsFloat(monUsdRatio, 18))
	logger.Info("ETH/USD price: %s (%s USD per 1 ETH)",
		ethUsdPrice.String(),
		formatBigIntAsFloat(ethUsdPrice, 8))
	logger.Info("Calculated ETH/MON ratio: %s (1 MON = %s ETH)",
		ratios[CurrencyETH].String(),
		formatBigIntAsFloat(ratios[CurrencyETH], 18))

	// Log USD token ratio values with more details
	logger.Info("USDT/USDC ratio: %s (MON wei per smallest USDT/USDC unit)",
		monWeiPerSmallestUsd.String())

	// Pre-calculate and log what 0.25 USDT (250000 units) would yield for debugging
	expectedMon := new(big.Int).Mul(monWeiPerSmallestUsd, big.NewInt(250000))
	logger.Info("Expect 0.25 USDT to yield approximately %s MON",
		formatBigIntAsFloat(expectedMon, 18))

	// Also log the theoretical calculation for 1 USDT to yield how many MON
	logger.Info("Float calculation: 1 USD should yield %s MON (theoretical)", monPerUsd.Text('f', 6))

	return ratios
}

// formatBigIntAsFloat formats a big.Int with decimals as a human-readable string
func formatBigIntAsFloat(value *big.Int, decimals int) string {
	if value == nil {
		return "0"
	}

	// Convert to a big.Float for easier decimal handling
	floatValue := new(big.Float).SetInt(value)

	// Divide by 10^decimals
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	result := new(big.Float).Quo(floatValue, divisor)

	// Convert to string with appropriate precision
	str := result.Text('f', 8) // 8 decimal places should be enough for display

	return str
}

// Chainlink ETH/USD Price Feed address on Arbitrum
const ChainlinkEthUsdFeed = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

// Price feed ABI for Chainlink
var PriceFeedABI = `[{"inputs":[],"name":"latestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"}]`

// NewArbitrumDepositor creates a new instance of ArbitrumDepositor
func NewArbitrumDepositor(client *ethclient.Client, address common.Address, privateKey *ecdsa.PrivateKey) (*ArbitrumDepositor, error) {
	boundContract := bind.NewBoundContract(address, DepositorABI, client, client, client)

	// Get chain ID
	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %v", err)
	}

	return &ArbitrumDepositor{
		Client:        client,
		Address:       address,
		ChainID:       chainID,
		PrivateKey:    privateKey,
		BoundContract: boundContract,
	}, nil
}

// NewMonadDistributor creates a new instance of MonadDistributor
func NewMonadDistributor(client *ethclient.Client, address common.Address, privateKey *ecdsa.PrivateKey) (*MonadDistributor, error) {
	boundContract := bind.NewBoundContract(address, DistributorABI, client, client, client)
	return &MonadDistributor{
		Client:        client,
		Address:       address,
		PrivateKey:    privateKey,
		BoundContract: boundContract,
	}, nil
}

// GetEthSwapRatio returns the current ETH/MON swap ratio based on ETH/USD price from Chainlink
func (d *ArbitrumDepositor) GetEthSwapRatio(ctx context.Context) (*big.Int, error) {
	// Use the retry wrapper function
	return getEthSwapRatioWithRetry(ctx, d, NewRetryClient(d.Client))
}

// GetContractState fetches the current state of both contracts
func GetContractState(ctx context.Context, arb *ArbitrumDepositor, monad *MonadDistributor) (*ContractState, error) {
	var state ContractState

	// Create retry clients
	arbRetryClient := NewRetryClient(arb.Client)
	monadRetryClient := NewRetryClient(monad.Client)

	// Call contract methods in parallel using goroutines
	errChan := make(chan error, 4) // Updated to 4 for the additional ETH ratio check

	// Check if paused
	go func() {
		var out []interface{}
		err := arbRetryClient.CallWithRetry(ctx, arb.BoundContract, &out, "paused")
		if err != nil {
			errChan <- fmt.Errorf("failed to check pause status: %v", err)
			return
		}
		state.IsPaused = out[0].(bool)
		errChan <- nil
	}()

	// Get min amount for ETH deposits
	go func() {
		var out []interface{}
		err := arbRetryClient.CallWithRetry(ctx, arb.BoundContract, &out, "minEthDeposit")
		if err != nil {
			errChan <- fmt.Errorf("failed to get min amount: %v", err)
			return
		}
		state.MinAmount = out[0].(*big.Int)
		errChan <- nil
	}()

	// Get MON balance (native token) with retries
	go func() {
		balance, err := monadRetryClient.BalanceAtWithRetry(ctx, monad.Address, nil)
		if err != nil {
			errChan <- fmt.Errorf("failed to get MON balance: %v", err)
			return
		}
		state.MonBalance = balance
		errChan <- nil
	}()

	// Get current ETH/MON swap ratio
	go func() {
		ethRatio, err := getEthSwapRatioWithRetry(ctx, arb, arbRetryClient)
		if err != nil {
			errChan <- fmt.Errorf("failed to get ETH swap ratio: %v", err)
			return
		}

		// Initialize swap ratios map
		state.SwapRatios = calculateSwapRatios(ethRatio)

		errChan <- nil
	}()

	// Wait for all goroutines to complete
	for i := 0; i < 4; i++ { // Updated to 4
		if err := <-errChan; err != nil {
			return nil, err
		}
	}

	return &state, nil
}

// getEthSwapRatioWithRetry gets the ETH/USD price with retry logic
func getEthSwapRatioWithRetry(ctx context.Context, d *ArbitrumDepositor, retryClient *RetryClient) (*big.Int, error) {
	priceFeedAbi, err := abi.JSON(strings.NewReader(PriceFeedABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse price feed ABI: %v", err)
	}

	priceFeed := bind.NewBoundContract(common.HexToAddress(ChainlinkEthUsdFeed), priceFeedAbi, d.Client, d.Client, d.Client)

	var out []interface{}
	err = retryClient.CallWithRetry(ctx, priceFeed, &out, "latestRoundData")
	if err != nil {
		return nil, fmt.Errorf("failed to get ETH/USD price: %v", err)
	}

	ethUsdPrice := out[1].(*big.Int)
	return ethUsdPrice, nil
}

// RefundDeposit initiates a refund for a failed deposit
func (d *ArbitrumDepositor) RefundDeposit(ctx context.Context, depositId *big.Int) error {
	// Create retry client
	retryClient := NewRetryClient(d.Client)

	// Get deposit details from events
	logs, err := retryClient.FilterLogsWithRetry(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   nil,
		Addresses: []common.Address{d.Address},
		Topics: [][]common.Hash{
			{DepositorABI.Events["DepositEvent"].ID},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to get deposit events: %v", err)
	}

	// Find the deposit with the matching ID
	var depositEvent struct {
		Depositor common.Address
		Amount    *big.Int
		DepositId *big.Int
		Currency  uint8
	}

	found := false
	for _, log := range logs {
		err = d.BoundContract.UnpackLog(&depositEvent, "DepositEvent", log)
		if err != nil {
			continue
		}

		if depositEvent.DepositId.Cmp(depositId) == 0 {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("deposit ID %s not found", depositId.String())
	}

	// Get current gas price with a small buffer (20% increase)
	gasPrice, err := d.Client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))

	// Pack the refund data - Fix the argument mismatch error by providing all 4 required parameters
	input, err := DepositorABI.Pack(
		"refundDeposit",
		depositId,
		depositEvent.Depositor,
		depositEvent.Amount,
		uint8(depositEvent.Currency),
	)
	if err != nil {
		return fmt.Errorf("failed to pack refund data: %v", err)
	}

	// Get our wallet's address from the private key
	publicKey := d.PrivateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Estimate gas
	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &d.Address,
		Data: input,
	}

	// Use retry for gas estimation
	var gasLimit uint64
	estimateGasOp := func() error {
		var estimateErr error
		gasLimit, estimateErr = d.Client.EstimateGas(ctx, msg)
		return estimateErr
	}

	err = RetryWithBackoff(estimateGasOp, DefaultRetryConfig())
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add 20% buffer to gas limit
	gasLimit = gasLimit * 12 / 10

	// Create transaction with retry for nonce
	var nonce uint64
	nonceOp := func() error {
		var nonceErr error
		nonce, nonceErr = d.Client.PendingNonceAt(ctx, fromAddress)
		return nonceErr
	}

	err = RetryWithBackoff(nonceOp, DefaultRetryConfig())
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &d.Address,
		Value:    big.NewInt(0),
		Data:     input,
	})

	// Sign and send transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(d.ChainID), d.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	// Send transaction with retry
	sendTxOp := func() error {
		return d.Client.SendTransaction(ctx, signedTx)
	}

	err = RetryWithBackoff(sendTxOp, DefaultRetryConfig())
	if err != nil {
		return fmt.Errorf("failed to send refund transaction: %v", err)
	}

	// Wait for transaction receipt
	receipt, err := bind.WaitMined(ctx, d.Client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for refund transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("refund transaction failed")
	}

	logger.Info("Successfully refunded deposit ID %s (tx: %s)", depositId.String(), signedTx.Hash().Hex())
	return nil
}

// TransactWithGasBuffer is a wrapper around BoundContract.Transact that adds a buffer to gas estimation
func (m *MonadDistributor) TransactWithGasBuffer(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	// Set gas limit to 0 to force estimation
	opts.GasLimit = 0

	// Create a temporary context with deadline to use for gas estimation
	// This prevents the gas estimation from hanging indefinitely
	estCtx, cancel := context.WithTimeout(opts.Context, time.Second*10)
	defer cancel()

	// Pack the method and parameters to estimate gas
	// Use the pre-defined ABI instead of parsing it again
	input, err := DistributorABI.Pack(method, params...)
	if err != nil {
		logger.Error("failed to pack data: %v", err)
		return nil, err
	}

	// Create a call message for gas estimation
	msg := ethereum.CallMsg{
		From: crypto.PubkeyToAddress(m.PrivateKey.PublicKey),
		To:   &m.Address,
		Data: input,
	}

	// Define maximum reasonable gas limit for Monad
	// Monad has a lower block gas limit than Ethereum
	const maxGasLimit uint64 = 1000000    // 1 million gas is a safe upper bound for Monad
	const defaultGasLimit uint64 = 300000 // Default gas limit if estimation fails

	// Estimate gas with fallback
	var estimatedGas uint64
	estimatedGas, err = m.Client.EstimateGas(estCtx, msg)
	if err != nil {
		logger.Warn("Gas estimation failed: %v, using default gas limit", err)
		opts.GasLimit = defaultGasLimit
		logger.Info("Using default gas limit: %d", defaultGasLimit)
		// Still continue with the transaction with default gas limit
	} else {
		// Add 20% buffer to gas limit
		bufferedGas := estimatedGas * 12 / 10

		// Ensure gas limit doesn't exceed max
		if bufferedGas > maxGasLimit {
			logger.Warn("Estimated gas with buffer (%d) exceeds max limit, capping at %d",
				bufferedGas, maxGasLimit)
			opts.GasLimit = maxGasLimit
		} else {
			opts.GasLimit = bufferedGas
		}

		logger.Debug("Gas estimation for %s: estimated=%d, with buffer=%d, final=%d",
			method, estimatedGas, bufferedGas, opts.GasLimit)
	}

	// Call the actual Transact method with our calculated gas limit
	return m.BoundContract.Transact(opts, method, params...)
}

func (m *MonadDistributor) GetTransactOpts(ctx context.Context) (*bind.TransactOpts, error) {
	chainID, err := m.Client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %v", err)
	}

	auth, err := bind.NewKeyedTransactorWithChainID(m.PrivateKey, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactor: %v", err)
	}

	// Instead of setting a fixed gas limit, we'll let the transaction be
	// estimated when it's run. The Transact method in the bound contract
	// will estimate gas if the gas limit is 0.
	//
	// We'll modify the BoundContract.Transact method to add a gas buffer
	// in a wrapper function.

	// Note: When gas=0, the bind package will automatically estimate gas
	// when the transaction is submitted
	auth.GasLimit = 0

	auth.Context = ctx
	return auth, nil
}
