package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

// QuickNodeWebhookPayload represents the webhook payload from QuickNode filtered streams
type QuickNodeWebhookPayload struct {
	EventName string                     `json:"eventName"`
	StreamID  string                     `json:"streamId"`
	Event     QuickNodeDistributionEvent `json:"event"`
	Network   string                     `json:"network"`
}

// QuickNodeDistributionEvent represents the Distribution event data
type QuickNodeDistributionEvent struct {
	BlockHash        string           `json:"blockHash"`
	BlockNumber      string           `json:"blockNumber"`
	BlockTimestamp   string           `json:"blockTimestamp"`
	TransactionHash  string           `json:"transactionHash"`
	TransactionIndex string           `json:"transactionIndex"`
	LogIndex         string           `json:"logIndex"`
	Address          string           `json:"address"`
	Topics           []string         `json:"topics"`
	Data             string           `json:"data"`
	Removed          bool             `json:"removed"`
	DecodedData      DistributionData `json:"decodedData,omitempty"`
}

// DistributionData represents the decoded distribution event data
type DistributionData struct {
	Recipient common.Address `json:"recipient"`
	Amount    string         `json:"amount"`
	DepositID *big.Int       `json:"depositId"`
}

// HandleQuickNodeWebhook handles webhook callbacks from QuickNode
func (h *Handler) HandleQuickNodeWebhook(c *gin.Context) {
	// Set timeout for webhook processing
	webhookCtx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Read request body
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		logger.Error("Failed to read webhook body: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	// Parse webhook payload
	var payload QuickNodeWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		logger.Error("Failed to unmarshal webhook payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload"})
		return
	}

	// Log the webhook event
	logger.Info("Received QuickNode webhook event: %s for stream: %s",
		payload.EventName, payload.StreamID)

	// Handle distribution event
	// We need to decode the event data from the payload
	if err := decodeDistributionEvent(&payload.Event); err != nil {
		logger.Error("Failed to decode distribution event: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to decode event data"})
		return
	}

	// Process the distribution event
	// This will be specific to your application logic
	if err := h.processDistributionEvent(webhookCtx, payload.Event); err != nil {
		logger.Error("Failed to process distribution event: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process distribution event"})
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// decodeDistributionEvent decodes the event data from the payload
func decodeDistributionEvent(event *QuickNodeDistributionEvent) error {
	// Expected topics:
	// [0] - event signature (keccak256 hash of the event signature)
	// [1] - recipient address (indexed parameter)
	// [2] - deposit ID (indexed parameter)

	// Extract recipient from topics[1]
	if len(event.Topics) < 3 {
		return fmt.Errorf("invalid topics length: expected at least 3, got %d", len(event.Topics))
	}

	// Parse recipient address (remove 0x prefix if present)
	recipientHex := event.Topics[1]
	recipient := common.HexToAddress(recipientHex)

	// Parse deposit ID from topics[2]
	depositIDHex := event.Topics[2]
	// Convert hex string to big.Int
	depositID, success := new(big.Int).SetString(depositIDHex[2:], 16) // Remove 0x prefix
	if !success {
		return fmt.Errorf("failed to convert deposit ID hex to big.Int: %s", depositIDHex)
	}

	// Parse amount from data field
	// The data field contains the non-indexed parameters
	amountBytes, err := hexutil.Decode(event.Data)
	if err != nil {
		return fmt.Errorf("failed to decode data field: %v", err)
	}

	// Assuming amount is the only non-indexed parameter
	// and it's a uint256 (32 bytes)
	amount := hexutil.Encode(amountBytes)

	// Set decoded data
	event.DecodedData = DistributionData{
		Recipient: recipient,
		Amount:    amount,
		DepositID: depositID,
	}

	return nil
}

// processDistributionEvent processes the distribution event
func (h *Handler) processDistributionEvent(ctx context.Context, event QuickNodeDistributionEvent) error {
	// Extract data from the event
	recipient := event.DecodedData.Recipient
	depositID := event.DecodedData.DepositID
	txHash := event.TransactionHash

	logger.Info("Processing distribution event: Recipient=%s, DepositID=%s, TxHash=%s",
		recipient.Hex(), depositID.String(), txHash)

	// Update transaction status in the database using the bridge service method
	// The bridge service already has the appropriate database transaction handling
	if err := h.BridgeService.UpdateTransactionStatus(ctx, depositID, "completed", txHash); err != nil {
		return fmt.Errorf("failed to update transaction status: %v", err)
	}

	// Log successful update
	logger.Info("Successfully updated transaction status for deposit ID %s to %s with txHash %s",
		depositID.String(), "completed", txHash)

	return nil
}
