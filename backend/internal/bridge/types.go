package bridge

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pcristin/monad-faucet/internal/blockchain"
	"github.com/pcristin/monad-faucet/internal/blockchain/listener"
	"github.com/pcristin/monad-faucet/internal/blockchain/tx_sender"
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/internal/workers"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// BridgeService handles the business logic for the bridge operations.
type BridgeService struct {
	arbDepositor        *blockchain.ArbitrumDepositor
	monadDistributor    *blockchain.MonadDistributor
	txSender            *tx_sender.TransactionSender
	optimismDepositor   *blockchain.OptimismDepositor
	baseDepositor       *blockchain.BaseDepositor
	depositChan         chan listener.DepositEvent
	refundChan          chan *big.Int
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
	db                  *database.DB
	txCache             map[string]*database.Transaction
	txCacheMutex        sync.RWMutex
	txCacheExpiration   time.Duration
	processingMutex     sync.Mutex
	processingDeposits  map[string]bool
	instanceID          string
	lockDuration        time.Duration
	lockRefreshInterval time.Duration
	lockRefreshers      map[string]context.CancelFunc
	lockRefreshersMutex sync.Mutex
	workerManager       *workers.Manager
	workerPools         *BridgeWorkerPools // Reference to worker pools for batch processing
	UseWebhook          bool               // Flag to use webhooks for distribution events instead of polling
	WebhookProvider     string             // The webhook provider (quicknode or alchemy)
}

// NewBridgeService creates a new instance of BridgeService.
func NewBridgeService(
	arbDepositor *blockchain.ArbitrumDepositor,
	optimismDepositor *blockchain.OptimismDepositor,
	baseDepositor *blockchain.BaseDepositor,
	monadDistributor *blockchain.MonadDistributor,
	db *database.DB,
) *BridgeService {
	// Create transaction sender for Monad Distributor
	fromAddress := crypto.PubkeyToAddress(monadDistributor.PrivateKey.PublicKey)
	txSender := tx_sender.NewTxSender(monadDistributor.Client, monadDistributor.BoundContract, fromAddress)

	ctx, cancel := context.WithCancel(context.Background())
	instanceID := fmt.Sprintf("instance-%d", time.Now().UnixNano())
	return &BridgeService{
		arbDepositor:        arbDepositor,
		optimismDepositor:   optimismDepositor,
		baseDepositor:       baseDepositor,
		monadDistributor:    monadDistributor,
		txSender:            txSender,
		depositChan:         make(chan listener.DepositEvent, 1000),
		refundChan:          make(chan *big.Int, 1000),
		wg:                  sync.WaitGroup{},
		ctx:                 ctx,
		cancel:              cancel,
		db:                  db,
		txCache:             make(map[string]*database.Transaction),
		txCacheExpiration:   1 * time.Hour,
		processingDeposits:  make(map[string]bool),
		instanceID:          instanceID,
		lockDuration:        5 * time.Minute,
		lockRefreshInterval: 1 * time.Minute,
		lockRefreshers:      make(map[string]context.CancelFunc),
	}
}

// SetWebhookConfig sets the webhook configuration
func (s *BridgeService) SetWebhookConfig(useWebhook bool, provider string) {
	s.UseWebhook = useWebhook
	s.WebhookProvider = provider
	logger.Info("Webhook configuration: enabled=%v, provider=%s", useWebhook, provider)
}
