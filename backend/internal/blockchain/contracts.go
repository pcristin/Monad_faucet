package blockchain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ArbitrumDepositor represents the Arbitrum depositor contract
type ArbitrumDepositor struct {
	client     *ethclient.Client
	address    common.Address
	chainID    *big.Int
	privateKey *ecdsa.PrivateKey
	*bind.BoundContract
}

// MonadDistributor represents the Monad distributor contract
type MonadDistributor struct {
	client     *ethclient.Client
	address    common.Address
	privateKey *ecdsa.PrivateKey
	*bind.BoundContract
}

// MonUsdRatio represents the ratio of MON to USD (atomic value)
type MonUsdRatio struct {
	value atomic.Value // stores *big.Int
}

func NewMonUsdRatio(initialValue *big.Int) *MonUsdRatio {
	r := &MonUsdRatio{}
	r.value.Store(initialValue)
	return r
}

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
var initialMonUsdRatio = new(big.Int).Exp(big.NewInt(1), big.NewInt(17), nil) // 0.1 * 10^18

// Create global ratio instance
var globalMonUsdRatio = NewMonUsdRatio(initialMonUsdRatio)

// UpdateMonUsdRatio updates the global MON/USD ratio
func UpdateMonUsdRatio(newRatio *big.Int) {
	globalMonUsdRatio.Set(newRatio)
	log.Printf("MON/USD ratio updated to: %s", newRatio.String())
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

	// For ETH: amount = (ETH/USD price) * (1/MON/USD ratio)
	ethUsdPriceWith18Decimals := new(big.Int).Mul(ethUsdPrice, new(big.Int).Exp(big.NewInt(10), big.NewInt(10), nil))
	ratios[CurrencyETH] = new(big.Int).Mul(ethUsdPriceWith18Decimals, usdTokenRatio)
	ratios[CurrencyETH] = new(big.Int).Div(ratios[CurrencyETH], new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	return ratios
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
		client:        client,
		address:       address,
		chainID:       chainID,
		privateKey:    privateKey,
		BoundContract: boundContract,
	}, nil
}

// NewMonadDistributor creates a new instance of MonadDistributor
func NewMonadDistributor(client *ethclient.Client, address common.Address, privateKey *ecdsa.PrivateKey) (*MonadDistributor, error) {
	boundContract := bind.NewBoundContract(address, DistributorABI, client, client, client)
	return &MonadDistributor{
		client:        client,
		address:       address,
		privateKey:    privateKey,
		BoundContract: boundContract,
	}, nil
}

// GetEthSwapRatio returns the current ETH/MON swap ratio based on ETH/USD price from Chainlink
func (d *ArbitrumDepositor) GetEthSwapRatio(ctx context.Context) (*big.Int, error) {
	priceFeedAbi, err := abi.JSON(strings.NewReader(PriceFeedABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse price feed ABI: %v", err)
	}

	priceFeed := bind.NewBoundContract(common.HexToAddress(ChainlinkEthUsdFeed), priceFeedAbi, d.client, d.client, d.client)

	var out []interface{}
	err = priceFeed.Call(&bind.CallOpts{Context: ctx}, &out, "latestRoundData")
	if err != nil {
		return nil, fmt.Errorf("failed to get ETH/USD price: %v", err)
	}

	ethUsdPrice := out[1].(*big.Int)
	return ethUsdPrice, nil
}

// GetContractState fetches the current state of both contracts
func GetContractState(ctx context.Context, arb *ArbitrumDepositor, monad *MonadDistributor) (*ContractState, error) {
	var state ContractState

	// Call contract methods in parallel using goroutines
	errChan := make(chan error, 4) // Updated to 4 for the additional ETH ratio check

	// Check if paused
	go func() {
		var out []interface{}
		err := arb.BoundContract.Call(&bind.CallOpts{Context: ctx}, &out, "paused")
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
		err := arb.BoundContract.Call(&bind.CallOpts{Context: ctx}, &out, "minEthDeposit")
		if err != nil {
			errChan <- fmt.Errorf("failed to get min amount: %v", err)
			return
		}
		state.MinAmount = out[0].(*big.Int)
		errChan <- nil
	}()

	// Get MON balance (native token) with retries
	go func() {
		var balance *big.Int
		var err error
		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			balance, err = monad.client.BalanceAt(timeoutCtx, monad.address, nil)
			cancel()

			if err == nil {
				state.MonBalance = balance
				errChan <- nil
				return
			}

			if ctx.Err() != nil {
				errChan <- fmt.Errorf("failed to get MON balance: context cancelled")
				return
			}

			if i < maxRetries-1 {
				time.Sleep(time.Duration(1<<uint(i)) * time.Second)
			}
		}
		errChan <- fmt.Errorf("failed to get MON balance after %d retries: %v", maxRetries, err)
	}()

	// Get current ETH/MON swap ratio
	go func() {
		ethRatio, err := arb.GetEthSwapRatio(ctx)
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

// RefundDeposit initiates a refund for a failed deposit
func (d *ArbitrumDepositor) RefundDeposit(ctx context.Context, depositId *big.Int) error {
	// Get deposit details from events
	logs, err := d.client.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   nil,
		Addresses: []common.Address{d.address},
		Topics: [][]common.Hash{
			{DepositorABI.Events["DepositEvent"].ID},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to filter logs: %v", err)
	}

	var depositEvent struct {
		Depositor common.Address
		Amount    *big.Int
		Currency  uint8
	}

	found := false
	for _, log := range logs {
		var event struct {
			Depositor common.Address
			Amount    *big.Int
			DepositId *big.Int
			Currency  uint8
		}
		err = d.BoundContract.UnpackLog(&event, "DepositEvent", log)
		if err != nil {
			continue
		}
		if event.DepositId.Cmp(depositId) == 0 {
			depositEvent.Depositor = event.Depositor
			depositEvent.Amount = event.Amount
			depositEvent.Currency = event.Currency
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("deposit event not found for ID: %s", depositId.String())
	}

	// Get current gas price with a small buffer (20% increase)
	gasPrice, err := d.client.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(12))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(10))

	// Pack the refund data
	input, err := DepositorABI.Pack("refundDeposit", depositId, depositEvent.Depositor, depositEvent.Amount, depositEvent.Currency)
	if err != nil {
		return fmt.Errorf("failed to pack refund data: %v", err)
	}

	// Get our wallet's address from the private key
	publicKey := d.privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("error casting public key to ECDSA")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Estimate gas
	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &d.address,
		Data: input,
	}
	gasLimit, err := d.client.EstimateGas(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add 20% buffer to gas limit
	gasLimit = gasLimit * 12 / 10

	// Create transaction
	nonce, err := d.client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %v", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &d.address,
		Value:    big.NewInt(0),
		Data:     input,
	})

	// Sign and send transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(d.chainID), d.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	err = d.client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send refund transaction: %v", err)
	}

	// Wait for transaction receipt
	receipt, err := bind.WaitMined(ctx, d.client, signedTx)
	if err != nil {
		return fmt.Errorf("failed to wait for refund transaction: %v", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("refund transaction failed")
	}

	log.Printf("Successfully refunded deposit ID %s (tx: %s)", depositId.String(), signedTx.Hash().Hex())
	return nil
}

// GetTransactOpts returns properly configured transaction options for signing
func (m *MonadDistributor) GetTransactOpts(ctx context.Context) (*bind.TransactOpts, error) {
	chainID, err := m.client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %v", err)
	}

	auth, err := bind.NewKeyedTransactorWithChainID(m.privateKey, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactor: %v", err)
	}

	auth.Context = ctx
	return auth, nil
}
