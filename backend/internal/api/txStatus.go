package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
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
		TxHash         string `json:"tx_hash"`
		ArbitrumTxHash string `json:"arbitrum_tx_hash"`
		DepositID      string `json:"deposit_id"`
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

	txHash := request.TxHash
	arbitrumTxHash := request.ArbitrumTxHash
	depositID := request.DepositID

	logger.Info("Transaction status request received",
		slog.String("deposit_id", depositID),
		slog.String("tx_hash", txHash),
		slog.String("arbitrum_tx_hash", arbitrumTxHash),
	)

	// Common response structure

	// Helper function to create response
	createResponse := func(tx *database.Transaction) *TransactionResponse {
		response := &TransactionResponse{
			Status:  tx.Status,
			Message: "Transaction status retrieved successfully",
			Txs:     make(map[string]string),
		}

		if tx.DepositID != nil {
			response.DepositID = tx.DepositID.String()
		}

		// Always include Arbitrum hash if available
		if tx.TxHash != "" {
			response.Txs["Arbitrum"] = tx.TxHash
		}

		// Always include Monad hash if available
		if tx.MonadTxHash != "" {
			response.Txs["Monad"] = tx.MonadTxHash

			logger.Info("Including Monad hash in response",
				slog.String("monad_tx_hash", tx.MonadTxHash),
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("response_txs", fmt.Sprintf("%+v", response.Txs)))
		}

		return response
	}

	// Prioritize depositID for lookup
	if depositID != "" {
		// Parse deposit ID
		depositBigInt, ok := new(big.Int).SetString(depositID, 10)
		if !ok {
			logger.Error("Invalid deposit ID format", slog.String("error", "Failed to parse as big int"))
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "error",
				"message": "Invalid deposit ID format",
				"error":   "Failed to parse deposit ID",
			})
			return
		}

		// Look up transaction by deposit ID
		tx, err := h.BridgeService.GetDB().GetTransactionByDepositID(depositBigInt)
		if err != nil {
			logger.Error("Error looking up transaction", slog.String("error", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Error looking up transaction",
				"error":   err.Error(),
			})
			return
		}

		if tx != nil {
			logger.Info("Transaction found",
				slog.String("status", tx.Status),
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("arbitrum_tx_hash", tx.TxHash),
				slog.String("monad_tx_hash", tx.MonadTxHash),
			)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification for all clients, not just mobile
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
					}

					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
						slog.String("status", status),
					)

					// Update transaction status in database
					err = h.BridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, status, monadTxHash)
					if err != nil {
						logger.Error("Error updating transaction status", slog.String("error", err.Error()))
					} else {
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

		// Transaction not found for this deposit ID
		response := &TransactionResponse{
			Status:  "not_found",
			Message: "No transaction found for this deposit ID",
			Txs:     make(map[string]string),
		}
		response.DepositID = depositID

		c.JSON(http.StatusOK, response)
		logger.Info("No transaction found for deposit ID",
			slog.String("duration", time.Since(startTime).String()),
		)
		return
	}

	// Handle Monad tx hash lookup
	if txHash != "" {
		logger.Info("Looking up transaction by Monad hash", slog.String("tx_hash", txHash))
		tx, err := h.BridgeService.GetDB().GetTransactionByMonadTxHash(txHash)
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

	// Handle Arbitrum tx hash lookup
	if arbitrumTxHash != "" {
		logger.Info("Looking up transaction by Arbitrum hash", slog.String("arbitrum_tx_hash", arbitrumTxHash))
		tx, err := h.BridgeService.GetDB().GetTransactionByArbitrumTxHash(arbitrumTxHash)
		if err != nil {
			logger.Error("Error in DB lookup by Arbitrum hash", slog.String("error", err.Error()))
		} else if tx != nil {
			logger.Info("Found transaction via Arbitrum hash",
				slog.String("deposit_id", tx.DepositID.String()),
				slog.String("status", tx.Status),
				slog.String("monad_tx_hash", tx.MonadTxHash),
			)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification for all clients, not just mobile
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
					}

					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
						slog.String("status", status),
					)

					// Update transaction status in database
					err = h.BridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, status, monadTxHash)
					if err != nil {
						logger.Error("Error updating transaction status", slog.String("error", err.Error()))
					} else {
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
	}

	// One final attempt - try direct DB lookup of Arbitrum hash
	logger.Info("Attempting direct DB lookup for transaction")
	hashToCheck := txHash
	if arbitrumTxHash != "" {
		hashToCheck = arbitrumTxHash
	}

	if hashToCheck != "" {
		tx, err := h.BridgeService.GetDB().GetTransactionByArbitrumTxHash(hashToCheck)
		if err != nil {
			logger.Error("Error in direct DB lookup for hash %s: %v", hashToCheck, err)
		} else if tx != nil {
			logger.Info("Found transaction via direct DB lookup: deposit_id=%s, status=%s",
				tx.DepositID.String(), tx.Status)

			// If transaction is pending, attempt a blockchain verification
			if tx.Status == "pending" {
				logger.Info("Transaction is pending, checking blockchain for confirmation")

				// Perform blockchain verification for all clients, not just mobile
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
					}

					// Transaction found on blockchain, update status
					logger.Info("Transaction found on blockchain during status check",
						slog.String("deposit_id", tx.DepositID.String()),
						slog.String("monad_tx_hash", monadTxHash),
						slog.String("status", status),
					)

					// Update transaction status in database
					err = h.BridgeService.GetDB().UpdateTransactionStatus(tx.DepositID, status, monadTxHash)
					if err != nil {
						logger.Error("Error updating transaction status", slog.String("error", err.Error()))
					} else {
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
	}

	// Check if it's a refund transaction
	// This would need to be implemented if refunds are tracked separately

	// If we got here, the transaction is not found
	logger.Info("Transaction not found after all lookup attempts")
	response := &TransactionResponse{
		Status:  "not_found",
		Message: "Transaction not found in our system",
		Txs:     make(map[string]string),
	}

	if arbitrumTxHash != "" {
		response.Txs["Arbitrum"] = arbitrumTxHash
	} else if txHash != "" {
		response.Txs["Monad"] = txHash
	}

	// Always log the final response JSON for any request
	finalJSON, _ := json.Marshal(response)
	logger.Info("Final API response", slog.String("json", string(finalJSON)))

	c.JSON(http.StatusOK, response)
	logger.Info("Response sent",
		slog.String("duration", time.Since(startTime).String()),
		slog.String("status", "not_found"),
	)
}
