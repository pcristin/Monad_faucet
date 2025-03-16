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

// AlchemyWebhookPayload represents the webhook payload from Alchemy
type AlchemyWebhookPayload struct {
	WebhookID      string       `json:"webhookId"`
	ID             string       `json:"id"`
	CreatedAt      string       `json:"createdAt"`
	Type           string       `json:"type"`
	Event          AlchemyEvent `json:"event"`
	SequenceNumber int64        `json:"sequenceNumber"`
}

// AlchemyEvent represents an event in the Alchemy webhook payload
type AlchemyEvent struct {
	Network string      `json:"network"`
	Data    AlchemyData `json:"data"`
}

// AlchemyData contains the actual event data
type AlchemyData struct {
	Block AlchemyBlock `json:"block,omitempty"`
	Log   AlchemyLog   `json:"log,omitempty"`
	Tx    AlchemyTx    `json:"transaction,omitempty"`
}

// AlchemyBlock contains block information
type AlchemyBlock struct {
	Hash       string `json:"hash"`
	Number     string `json:"number"`
	Timestamp  string `json:"timestamp"`
	ParentHash string `json:"parentHash"`
}

// AlchemyLog contains log information
type AlchemyLog struct {
	Address          string   `json:"address"`
	Data             string   `json:"data"`
	Topics           []string `json:"topics"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// AlchemyTx contains transaction information
type AlchemyTx struct {
	Hash      string `json:"hash"`
	From      string `json:"from"`
	To        string `json:"to"`
	Value     string `json:"value"`
	Gas       string `json:"gas"`
	GasPrice  string `json:"gasPrice"`
	Input     string `json:"input"`
	BlockHash string `json:"blockHash"`
}

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

// HandleAlchemyWebhook handles webhook callbacks from Alchemy
func (h *Handler) HandleAlchemyWebhook(c *gin.Context) {
	// Set timeout for webhook processing
	webhookCtx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// Check if we're using webhook as primary distribution tracking method
	usingWebhookAsPrimary := h.BridgeService != nil && h.BridgeService.UseWebhook &&
		h.BridgeService.WebhookProvider == "alchemy"
	if usingWebhookAsPrimary {
		logger.Info("Alchemy webhook is configured as primary distribution event tracker (polling disabled)")
	}

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

	// Now try to parse as Alchemy webhook payload
	var payload AlchemyWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		logger.Error("Failed to unmarshal Alchemy webhook payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload structure"})
		return
	}

	// Verify we have log data (this is where our event data will be)
	if payload.Type != "EVENT" || payload.Event.Data.Log.Address == "" {
		logger.Info("Received non-log event or empty log data, ignoring")
		c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Acknowledged but not processed"})
		return
	}

	// Process the log event
	logEvent := payload.Event.Data.Log

	// Try to decode Distribution event from the log
	if err := h.processAlchemyDistributionEvent(webhookCtx, logEvent); err != nil {
		logger.Error("Failed to process Alchemy event: %v", err)
		// Return 200 to acknowledge receipt even if processing failed
		c.JSON(http.StatusOK, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to process event: %v", err),
		})
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Event processed successfully",
	})
}

// processAlchemyDistributionEvent processes a distribution event from Alchemy webhook
func (h *Handler) processAlchemyDistributionEvent(ctx context.Context, log AlchemyLog) error {
	// Check if this is our Distribution event - should have 4 topics (event signature + 3 indexed params)
	// Distribution(address indexed recipient, uint256 indexed id, uint256 amount)
	if len(log.Topics) != 4 {
		return fmt.Errorf("not a Distribution event, has %d topics", len(log.Topics))
	}

	// First topic should be event signature
	// keccak256("Distribution(address,uint256,uint256)")
	distributionEventSignature := "0x91d1e7819d2a17603e5dbecbe46b351b4a49c578db0cafaeb9f5b5e18f148969"
	if log.Topics[0] != distributionEventSignature {
		return fmt.Errorf("not a Distribution event, signature mismatch: %s", log.Topics[0])
	}

	// Topic 1: recipient (address, indexed)
	// Topic 2: id (uint256, indexed)
	// Topic 3: amount (uint256, indexed)

	// Parse recipient from topic 1 (remove leading zeros)
	recipientHex := log.Topics[1]
	if len(recipientHex) > 2 && recipientHex[:2] == "0x" {
		// Keep 0x prefix but remove padding
		recipientHex = "0x" + recipientHex[26:]
	}
	recipient := common.HexToAddress(recipientHex)

	// Parse deposit ID from topic 2
	depositIDHex := log.Topics[2]
	if len(depositIDHex) > 2 && depositIDHex[:2] == "0x" {
		depositIDHex = depositIDHex[2:] // Remove 0x prefix for parsing
	}
	depositID, ok := new(big.Int).SetString(depositIDHex, 16)
	if !ok {
		return fmt.Errorf("failed to parse deposit ID from topic: %s", log.Topics[2])
	}

	// Parse amount from topic 3
	amountHex := log.Topics[3]
	if len(amountHex) > 2 && amountHex[:2] == "0x" {
		amountHex = amountHex[2:] // Remove 0x prefix for parsing
	}
	amount, ok := new(big.Int).SetString(amountHex, 16)
	if !ok {
		return fmt.Errorf("failed to parse amount from topic: %s", log.Topics[3])
	}

	txHash := log.TransactionHash

	logger.Info("Processing Alchemy distribution event: Recipient=%s, DepositID=%s, Amount=%s, TxHash=%s",
		recipient.Hex(), depositID.String(), amount.String(), txHash)

	// Before updating, check if transaction already completed
	tx, err := h.BridgeService.GetTransactionByDepositID(ctx, depositID)
	if err == nil && tx != nil && tx.Status == "completed" && tx.MonadTxHash != "" {
		logger.Info("Transaction already completed for deposit ID %s with hash %s (skipping update)",
			depositID.String(), tx.MonadTxHash)
		return nil
	}

	// Update transaction status in the database using the bridge service method
	if err := h.BridgeService.UpdateTransactionStatus(ctx, depositID, "completed", txHash); err != nil {
		return fmt.Errorf("failed to update transaction status: %v", err)
	}

	// Log successful update
	logger.Info("Successfully updated transaction status for deposit ID %s to %s with txHash %s via Alchemy webhook",
		depositID.String(), "completed", txHash)

	return nil
}

// HandleQuickNodeWebhook handles webhook callbacks from QuickNode
func (h *Handler) HandleQuickNodeWebhook(c *gin.Context) {
	// Set timeout for webhook processing
	webhookCtx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// Check if we're using webhook as primary distribution tracking method
	usingWebhookAsPrimary := h.BridgeService != nil && h.BridgeService.UseWebhook &&
		h.BridgeService.WebhookProvider == "quicknode"
	if usingWebhookAsPrimary {
		logger.Info("QuickNode webhook is configured as primary distribution event tracker (polling disabled)")
	}

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

	// Before updating, check if transaction already completed
	tx, err := h.BridgeService.GetTransactionByDepositID(ctx, depositID)
	if err == nil && tx != nil && tx.Status == "completed" && tx.MonadTxHash != "" {
		logger.Info("Transaction already completed for deposit ID %s with hash %s (skipping update)",
			depositID.String(), tx.MonadTxHash)
		return nil
	}

	// Update transaction status in the database using the bridge service method
	if err := h.BridgeService.UpdateTransactionStatus(ctx, depositID, "completed", txHash); err != nil {
		return fmt.Errorf("failed to update transaction status: %v", err)
	}

	// Log successful update
	logger.Info("Successfully updated transaction status for deposit ID %s to %s with txHash %s via webhook",
		depositID.String(), "completed", txHash)

	return nil
}
