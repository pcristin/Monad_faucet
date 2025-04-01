package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pcristin/monad-faucet/internal/database"
)

// GetTransactionStatus returns the status of a transaction
func (h *Handler) GetTransactionStatus(c *gin.Context) {
	startTime := time.Now()

	// Save the request body so we can restore it later
	var buf bytes.Buffer
	tee := io.TeeReader(c.Request.Body, &buf)
	requestBody, _ := io.ReadAll(tee)
	c.Request.Body = io.NopCloser(&buf)

	logger := slog.With(
		slog.String("handler", "GetTransactionStatus"),
		slog.String("request_id", c.GetString("request_id")),
		slog.String("raw_body", string(requestBody)),
	)

	// Define the request structure
	var request struct {
		SourceTx string `json:"source_tx"`
	}

	// Decode JSON
	if err := c.ShouldBindJSON(&request); err != nil {
		logger.Error("Failed to decode JSON", slog.String("error", err.Error()))
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request format",
			"error":   err.Error(),
		})
		return
	}

	logger = logger.With(
		slog.String("client_ip", c.ClientIP()),
		slog.String("user_agent", c.Request.UserAgent()),
	)

	sourceTx := request.SourceTx

	logger.Info("Transaction status request received",
		slog.String("source_tx", sourceTx),
	)

	// Map chain names to chainIDs
	chainIDMap := map[string]int{
		"Arbitrum":        42161,  // Arbitrum chainID
		"ArbitrumSepolia": 421611, // Arbitrum Sepolia chainID
		"Base":            8453,   // Base chainID
		"BaseSepolia":     84532,  // Base Sepolia chainID
		"Optimism":        10,     // Optimism chainID
		"OptimismSepolia": 101,    // Optimism Sepolia chainID
		"Monad":           20482,  // Monad chainID (used as destination)
	}

	// Helper function to create response
	createResponse := func(tx *database.TransactionView) *TransactionResponse {
		response := &TransactionResponse{
			Status:             tx.Status,
			Message:            "Transaction status retrieved successfully",
			Txs:                make(map[string]string),
			SourceChainId:      chainIDMap["Arbitrum"], // Default to Arbitrum
			DestinationChainId: chainIDMap["Monad"],    // Always Monad as destination
		}

		if tx.DepositID != nil {
			response.DepositID = tx.DepositID.String()

			// Try to determine source chain from deposit ID format or other attributes
			// In a multi-chain system, we might be able to determine this from the deposit ID format
			depositIDStr := tx.DepositID.String()

			// Check if the deposit ID starts with a known chain ID
			for chainName, chainID := range chainIDMap {
				chainIDStr := fmt.Sprintf("%d", chainID)
				if strings.HasPrefix(depositIDStr, chainIDStr) && chainName != "Monad" {
					response.SourceChainId = chainID
					break
				}
			}
		}

		// Use the new format with source_tx and destination_tx keys
		if tx.TxHash != "" {
			response.Txs["source_tx"] = tx.TxHash
		}

		if tx.MonadTxHash != "" {
			response.Txs["destination_tx"] = tx.MonadTxHash

			logger.Info("Including Monad hash in response",
				slog.String("monad_tx_hash", tx.MonadTxHash),
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("response_txs", fmt.Sprintf("%+v", response.Txs)))
		}

		return response
	}

	// If sourceTx is provided, try to find the transaction directly
	if sourceTx != "" {
		// Look up transaction by source transaction hash
		tx, err := h.BridgeService.GetDB().GetTransactionViewByArbitrumTxHash(sourceTx)
		if err != nil {
			logger.Error("Error in transaction view lookup for source hash", slog.String("error", err.Error()))
		} else if tx != nil {
			logger.Info("Found transaction via source hash: deposit_id=%s, status=%s",
				tx.DepositID.String(), tx.Status)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification
				status, monadTxHash, err := h.BridgeService.FindMonadTransactionByDepositID(c, tx.DepositID)
				if err != nil {
					logger.Warn("Error checking blockchain for transaction",
						slog.String("error", err.Error()),
						slog.String("deposit_id", tx.DepositID.String()),
					)
				} else if monadTxHash != "" {
					logger.Info("Found monad transaction hash from blockchain",
						slog.String("monad_tx_hash", monadTxHash))

					// Also check for Distribution event on Monad blockchain
					distTxHash, err := h.BridgeService.FindMonadDistributionByDepositID(c, tx.DepositID)
					if err == nil && distTxHash != "" {
						logger.Info("Found distribution event",
							slog.String("tx_hash", distTxHash),
							slog.String("deposit_id", tx.DepositID.String()))

						// Create distribution record if needed
						_, err := h.BridgeService.CheckOrCreateDistributionTransaction(c, tx.DepositID)
						if err != nil {
							logger.Error("Failed to create distribution record",
								slog.String("error", err.Error()))
						}

						// Update response with distribution transaction
						monadTxHash = distTxHash
						status = "completed"
					} else if err != nil {
						// Log the error but continue - this is non-fatal
						logger.Warn("Error checking for distribution events, will continue with mint tx",
							slog.String("error", err.Error()),
							slog.String("deposit_id", tx.DepositID.String()),
							slog.String("mint_tx_hash", monadTxHash))
					}

					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
						slog.String("status", status),
					)

					// Update deposit status in database
					err = h.BridgeService.GetDB().UpdateDepositStatus(tx.DepositID, status)
					if err != nil {
						logger.Error("Error updating deposit status", slog.String("error", err.Error()))
					}

					// Update distribution status in database if monad tx hash is available
					if monadTxHash != "" {
						err = h.BridgeService.GetDB().UpdateDistributionStatus(tx.DepositID, status, monadTxHash)
						if err != nil {
							logger.Error("Error updating distribution status", slog.String("error", err.Error()))
						}
					}

					// For backward compatibility, also update transaction_history if it exists
					legacyTx, _ := h.BridgeService.GetDB().GetTransactionByDepositID(tx.DepositID)
					if legacyTx != nil {
						err = h.BridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, status, monadTxHash)
						if err != nil {
							logger.Error("Error updating legacy transaction status", slog.String("error", err.Error()))
						}
					}

					// Update tx object for response
					tx.Status = status
					tx.MonadTxHash = monadTxHash

					// Log that we found and are including the Monad hash
					logger.Info("Including completed Monad transaction in response",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
						slog.String("status", status))
				}
			}

			response := createResponse(tx)

			// Debug log the full response for completed transactions
			if tx.Status == "completed" {
				responseJSON, _ := json.Marshal(response)
				logger.Info("Sending completed transaction response",
					slog.String("response_body", string(responseJSON)),
					slog.String("deposit_id", tx.DepositID.String()),
					slog.String("monad_tx_hash", tx.MonadTxHash))
			}

			// Always log the final response JSON for any request
			finalJSON, _ := json.Marshal(response)
			logger.Info("Final API response", slog.String("json", string(finalJSON)))

			c.JSON(http.StatusOK, response)
			logger.Info("Response sent",
				slog.String("duration", time.Since(startTime).String()),
				slog.String("status", tx.Status),
			)
			return
		}

		// Also try Monad tx hash lookup as fallback
		logger.Info("Source Tx not found, trying as Monad transaction hash")
		tx, err = h.BridgeService.GetDB().GetTransactionViewByMonadTxHash(sourceTx)
		if err != nil {
			logger.Error("Error in DB lookup by Monad hash", slog.String("error", err.Error()))
		} else if tx != nil {
			logger.Info("Found transaction via Monad hash",
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("status", tx.Status),
			)

			response := createResponse(tx)

			// Debug log the full response for completed transactions
			if tx.Status == "completed" {
				responseJSON, _ := json.Marshal(response)
				logger.Info("Sending completed transaction response",
					slog.String("response_body", string(responseJSON)),
					slog.String("deposit_id", tx.DepositID.String()),
					slog.String("monad_tx_hash", tx.MonadTxHash))
			}

			// Always log the final response JSON for any request
			finalJSON, _ := json.Marshal(response)
			logger.Info("Final API response", slog.String("json", string(finalJSON)))

			c.JSON(http.StatusOK, response)
			logger.Info("Response sent",
				slog.String("duration", time.Since(startTime).String()),
				slog.String("status", tx.Status),
			)
			return
		}
	}

	// If we got here, the transaction is not found
	logger.Info("Transaction not found after all lookup attempts")
	response := &TransactionResponse{
		Status:             "not_found",
		Message:            "No transaction found",
		Txs:                make(map[string]string),
		SourceChainId:      chainIDMap["Arbitrum"], // Default to Arbitrum
		DestinationChainId: chainIDMap["Monad"],
	}

	c.JSON(http.StatusOK, response)
	logger.Info("Response sent with not_found status",
		slog.String("duration", time.Since(startTime).String()),
	)
}
