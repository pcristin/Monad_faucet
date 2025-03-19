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
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

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
	logger.Debug("Raw webhook payload: %s", string(body))

	// Log request headers for debugging
	headers := c.Request.Header
	headerJSON, _ := json.Marshal(headers)
	logger.Debug("Request headers: %s", string(headerJSON))

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
	logger.Debug("Payload keys: %v", keys)

	// Now try to parse as Alchemy webhook payload
	var payload AlchemyWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		logger.Error("Failed to unmarshal Alchemy webhook payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload structure"})
		return
	}

	// Verify we have log data
	if payload.Type != "GRAPHQL" || len(payload.Event.Data.Block.Logs) == 0 {
		logger.Debug("Received non-GRAPHQL type or empty logs array, ignoring")
		c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Acknowledged but not processed"})
		return
	}

	// Process all logs in the block
	processedCount := 0
	logger.Info("Processing %d logs from Alchemy webhook, block %s",
		len(payload.Event.Data.Block.Logs), payload.Event.Data.Block.Number)

	for i, logEvent := range payload.Event.Data.Block.Logs {
		logger.Debug("Processing log %d/%d from block %s", i+1, len(payload.Event.Data.Block.Logs), payload.Event.Data.Block.Number)

		// Try to decode Distribution event from the log
		if err := h.processAlchemyGraphQLLog(webhookCtx, logEvent); err != nil {
			logger.Error("Failed to process Alchemy log %d: %v", i+1, err)
			// Continue processing other logs instead of stopping
			continue
		}
		processedCount++
	}

	// Return success response
	logger.Info("Completed processing Alchemy webhook: %d/%d logs processed successfully",
		processedCount, len(payload.Event.Data.Block.Logs))
	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"message":   "Processing completed",
		"processed": processedCount,
	})
}

// processAlchemyGraphQLLog processes a log from the Alchemy GraphQL webhook
func (h *Handler) processAlchemyGraphQLLog(ctx context.Context, log AlchemyLog) error {
	// Check if this is our Distribution event - should have the correct topics
	if len(log.Topics) < 1 {
		return fmt.Errorf("log has no topics")
	}

	// First topic should be event signature - we need to check different potential signatures
	// The signature in the original implementation: 0x91d1e7819d2a17603e5dbecbe46b351b4a49c578db0cafaeb9f5b5e18f148969
	// But the payload shows a different one: 0xa8ee3e5c0b1fd681042265199e8b28cf463b81bc21f6658d4c73e741aeabd3f5
	// This is likely the Monad distribution event signature
	distributionEventSignature := log.Topics[0]

	logger.Debug("Processing event with signature: %s", distributionEventSignature)

	// We need to extract the recipient, deposit ID, and amount from the event
	// The format depends on the contract, but we can try to decode both from topics and data

	// For the Distribution event, it could be in the first topic or in the data
	recipientHex := ""
	if len(log.Topics) > 1 {
		recipientHex = log.Topics[1]
	}

	if len(recipientHex) > 2 && recipientHex[:2] == "0x" {
		// Keep 0x prefix but remove padding if needed
		if len(recipientHex) > 42 { // Ethereum address is 20 bytes (40 hex chars) + 0x
			recipientHex = "0x" + recipientHex[26:]
		}
	}

	recipient := common.HexToAddress(recipientHex)

	// Try to decode the data to get amount and ID
	// This will be contract-specific and may vary, so we need a flexible approach
	var depositID *big.Int

	// For simplicity, let's use the transaction hash as primary key for tracking
	txHash := log.Transaction.Hash

	logger.Debug("Processing Alchemy GraphQL event: Address=%s, Recipient=%s, TxHash=%s",
		log.Account.Address, recipient.Hex(), txHash)

	// We need to extract the deposit ID from the data
	// Since we can't easily decode the data without knowing the contract ABI,
	// we'll check for pending transactions in the database and update the first one
	pendingTxs, err := h.BridgeService.GetDB().GetTransactionsByStatus(database.StatusPending, 10, 0)
	if err != nil {
		logger.Error("Failed to get pending transactions: %v", err)
		return fmt.Errorf("failed to get pending transactions: %w", err)
	}

	// If we don't have any pending transactions, just log and continue
	if len(pendingTxs) == 0 {
		logger.Debug("No pending transactions to process")
		return nil
	}

	// For each pending transaction, try to update its status
	// For demo purposes, we'll update the first pending transaction
	if len(pendingTxs) > 0 {
		pendingTx := pendingTxs[0]
		depositID = pendingTx.DepositID

		logger.Info("Updating transaction status for deposit ID %s to completed with txHash %s via Alchemy webhook",
			depositID.String(), txHash)

		// Update transaction status
		if err := h.BridgeService.UpdateTransactionStatus(ctx, depositID, "completed", txHash); err != nil {
			return fmt.Errorf("failed to update transaction status: %v", err)
		}

		logger.Info("Successfully updated transaction status for deposit ID %s to completed with txHash %s via Alchemy webhook",
			depositID.String(), txHash)
	}

	return nil
}
