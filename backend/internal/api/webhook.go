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
	// Expected structure:
	// topics[0] - event signature (keccak256 hash of the event signature)
	// topics[1] - may contain recipient address (last 40 hex characters)
	// data - contains both amount and deposit ID as consecutive 32-byte values

	// Check if we have at least the event signature
	if len(event.Topics) < 1 {
		return fmt.Errorf("invalid topics length: expected at least 1, got %d", len(event.Topics))
	}

	// Parse recipient address from topics[1] if available
	// Extract last 40 characters and prefix with 0x
	var recipient common.Address
	if len(event.Topics) > 1 && len(event.Topics[1]) >= 40 {
		recipientHex := "0x" + event.Topics[1][len(event.Topics[1])-40:]
		recipient = common.HexToAddress(recipientHex)
	} else {
		return fmt.Errorf("invalid recipient address in topics")
	}

	// Parse amount and deposit ID from data field
	// Remove '0x' prefix if present
	dataStr := event.Data
	if len(dataStr) < 2 {
		return fmt.Errorf("invalid data field: too short")
	}

	// Remove 0x prefix if present
	if dataStr[:2] == "0x" {
		dataStr = dataStr[2:]
	}

	// Data should contain at least 128 hex chars (64 for amount + 64 for deposit ID)
	if len(dataStr) < 128 {
		return fmt.Errorf("invalid data field: expected at least 128 hex chars, got %d", len(dataStr))
	}

	// First 32 bytes (64 hex chars): amount
	amountHex := "0x" + dataStr[:64]
	amount := amountHex

	// Second 32 bytes (64 hex chars): deposit ID
	depositIDHex := "0x" + dataStr[64:128]
	depositID, success := new(big.Int).SetString(depositIDHex[2:], 16) // Remove 0x prefix
	if !success {
		return fmt.Errorf("failed to convert deposit ID hex to big.Int: %s", depositIDHex)
	}

	// Set decoded data
	event.DecodedData = DistributionData{
		Recipient: recipient,
		Amount:    amount,
		DepositID: depositID,
	}

	logger.Info("Decoded Distribution event: Recipient=%s, Amount=%s, DepositID=%s",
		recipient.Hex(), amount, depositID.String())

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
