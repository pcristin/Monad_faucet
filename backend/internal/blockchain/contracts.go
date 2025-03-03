package blockchain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"strings"

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

	// For USDC/USDT: amount = (1 USD) / (MON/USD ratio)
	usdTokenRatio := new(big.Int).Div(
		new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
		monUsdRatio,
	)
	ratios[CurrencyUSDC] = usdTokenRatio
	ratios[CurrencyUSDT] = usdTokenRatio

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

// GetTransactOpts returns properly configured transaction options for signing
func (m *MonadDistributor) GetTransactOpts(ctx context.Context) (*bind.TransactOpts, error) {
	chainID, err := m.Client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %v", err)
	}

	auth, err := bind.NewKeyedTransactorWithChainID(m.PrivateKey, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactor: %v", err)
	}

	auth.Context = ctx
	return auth, nil
}
