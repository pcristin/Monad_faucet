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
	Events []QuickNodeDistributionEvent `json:"events"`
}

// QuickNodeDistributionEvent represents the Distribution event data
type QuickNodeDistributionEvent struct {
	Address         string      `json:"address"`
	BlockHash       string      `json:"blockHash"`
	BlockNumber     string      `json:"blockNumber"`
	EventName       string      `json:"eventName"`
	EventSignature  string      `json:"eventSignature"`
	LogIndex        string      `json:"logIndex"`
	Parameters      EventParams `json:"parameters"`
	TransactionHash string      `json:"transactionHash"`
}

// EventParams represents the already decoded parameters from the event
type EventParams struct {
	Amount    string `json:"amount"`
	ID        string `json:"id"`
	Recipient string `json:"recipient"`
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

	// Log the raw payload for debugging
	logger.Info("Received QuickNode webhook raw payload: %s", string(body))

	// Parse webhook payload
	var payload QuickNodeWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		logger.Error("Failed to unmarshal webhook payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload"})
		return
	}

	// Check if we have any events
	if len(payload.Events) == 0 {
		logger.Warn("No events found in webhook payload")
		c.JSON(http.StatusOK, gin.H{"status": "success", "message": "No events to process"})
		return
	}

	// Process each event
	for i, event := range payload.Events {
		logger.Info("Processing event %d: %s, tx: %s", i+1, event.EventName, event.TransactionHash)

		// Process the distribution event
		if err := h.processDistributionEvent(webhookCtx, event); err != nil {
			logger.Error("Failed to process distribution event: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to process event %d: %v", i+1, err)})
			return
		}
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{"status": "success", "processed": len(payload.Events)})
}

// processDistributionEvent processes the distribution event
func (h *Handler) processDistributionEvent(ctx context.Context, event QuickNodeDistributionEvent) error {
	// Convert recipient string to address
	recipient := common.HexToAddress(event.Parameters.Recipient)

	// Convert ID string to big.Int
	depositID, ok := new(big.Int).SetString(event.Parameters.ID, 10)
	if !ok {
		return fmt.Errorf("failed to parse deposit ID: %s", event.Parameters.ID)
	}

	txHash := event.TransactionHash

	logger.Info("Processing distribution event: Recipient=%s, DepositID=%s, Amount=%s, TxHash=%s",
		recipient.Hex(), depositID.String(), event.Parameters.Amount, txHash)

	// Update transaction status in the database using the bridge service method
	if err := h.BridgeService.UpdateTransactionStatus(ctx, depositID, "completed", txHash); err != nil {
		return fmt.Errorf("failed to update transaction status: %v", err)
	}

	// Log successful update
	logger.Info("Successfully updated transaction status for deposit ID %s to %s with txHash %s",
		depositID.String(), "completed", txHash)

	return nil
}
