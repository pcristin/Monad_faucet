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
// Has both structures - direct event and events array
type QuickNodeWebhookPayload struct {
	// New format - array of events
	Events []QuickNodeDistributionEvent `json:"events"`

	// Legacy fields - kept for compatibility
	EventName string                     `json:"eventName"`
	StreamID  string                     `json:"streamId"`
	Event     QuickNodeDistributionEvent `json:"event"`
	Network   string                     `json:"network"`
}

// QuickNodeDistributionEvent represents the Distribution event data
type QuickNodeDistributionEvent struct {
	// Common fields from actual payload
	Address         string      `json:"address"`
	BlockHash       string      `json:"blockHash"`
	BlockNumber     string      `json:"blockNumber"`
	EventName       string      `json:"eventName"`
	EventSignature  string      `json:"eventSignature"`
	LogIndex        string      `json:"logIndex"`
	Parameters      EventParams `json:"parameters"`
	TransactionHash string      `json:"transactionHash"`

	// Legacy fields - kept for compatibility
	BlockTimestamp   string   `json:"blockTimestamp"`
	TransactionIndex string   `json:"transactionIndex"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	Removed          bool     `json:"removed"`
}

// EventParams represents the decoded parameters from the event
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

	// Read full request body
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		logger.Error("Failed to read webhook body: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	// Log the raw body for debugging
	logger.Info("Raw webhook payload: %s", string(body))

	// Log request headers for debugging
	headers := c.Request.Header
	headerJSON, _ := json.Marshal(headers)
	logger.Info("Request headers: %s", string(headerJSON))

	// First, try to parse as generic JSON to see what we received
	var rawPayload map[string]interface{}
	if err := json.Unmarshal(body, &rawPayload); err != nil {
		logger.Error("Failed to unmarshal raw payload as JSON: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
		return
	}

	// Log the raw payload structure
	keys := make([]string, 0, len(rawPayload))
	for k := range rawPayload {
		keys = append(keys, k)
	}
	logger.Info("Payload keys: %v", keys)

	// Now try to parse the expected payload structure
	var payload QuickNodeWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		logger.Error("Failed to unmarshal webhook payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload structure"})
		return
	}

	// Prepare a slice of events to process
	var eventsToProcess []QuickNodeDistributionEvent

	// Check for events array
	if len(payload.Events) > 0 {
		logger.Info("Found %d events in 'events' array", len(payload.Events))
		eventsToProcess = payload.Events
	} else if payload.Event.TransactionHash != "" && payload.Event.Parameters.ID != "" {
		// Check for single event in legacy format
		logger.Info("Found single event in 'event' field with txHash: %s", payload.Event.TransactionHash)
		eventsToProcess = []QuickNodeDistributionEvent{payload.Event}
	} else {
		// Check event inside top-level structure
		if rawEvent, ok := rawPayload["event"].(map[string]interface{}); ok {
			logger.Info("Found event structure at top level, attempting to reparse")

			// Create a fake events wrapper
			wrapper := map[string]interface{}{
				"events": []interface{}{rawEvent},
			}

			// Reserialize and parse
			wrapperJSON, _ := json.Marshal(wrapper)
			var newPayload QuickNodeWebhookPayload
			if err := json.Unmarshal(wrapperJSON, &newPayload); err == nil && len(newPayload.Events) > 0 {
				eventsToProcess = newPayload.Events
				logger.Info("Successfully reparsed event from top level")
			}
		}
	}

	// If no events found, return a friendly response
	if len(eventsToProcess) == 0 {
		logger.Warn("No valid events found in payload")
		c.JSON(http.StatusOK, gin.H{"status": "success", "message": "No events to process"})
		return
	}

	// Process each event
	for i, event := range eventsToProcess {
		logger.Info("Processing event %d: tx=%s", i+1, event.TransactionHash)

		// Safety check - we need parameters to process
		if event.Parameters.ID == "" {
			logger.Warn("Event %d has no ID parameter, checking for legacy format", i+1)

			// If this is a legacy format with topics/data, we'll handle it differently
			if len(event.Topics) > 0 && event.Data != "" {
				logger.Info("Event %d appears to be in legacy format, not supported", i+1)
				continue
			}

			logger.Error("Event %d has no valid parameters to process", i+1)
			continue
		}

		// Log the parameters we're about to process
		logger.Info("Event %d parameters: ID=%s, Recipient=%s, Amount=%s",
			i+1, event.Parameters.ID, event.Parameters.Recipient, event.Parameters.Amount)

		// Process the distribution event
		if err := h.processDistributionEvent(webhookCtx, event); err != nil {
			logger.Error("Failed to process event %d: %v", i+1, err)
			// Continue processing other events instead of returning
			continue
		}

		logger.Info("Successfully processed event %d", i+1)
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"processed": len(eventsToProcess),
	})
}

// processDistributionEvent processes the distribution event
func (h *Handler) processDistributionEvent(ctx context.Context, event QuickNodeDistributionEvent) error {
	// Convert recipient string to address
	recipient := common.HexToAddress(event.Parameters.Recipient)

	// Convert ID string to big.Int - try base 10 first
	depositID, ok := new(big.Int).SetString(event.Parameters.ID, 10)
	if !ok {
		// If base 10 fails, try base 16 (hex) without 0x prefix
		depositIDStr := event.Parameters.ID
		if len(depositIDStr) > 2 && depositIDStr[:2] == "0x" {
			depositIDStr = depositIDStr[2:]
		}
		depositID, ok = new(big.Int).SetString(depositIDStr, 16)
		if !ok {
			return fmt.Errorf("failed to parse deposit ID as decimal or hex: %s", event.Parameters.ID)
		}
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
