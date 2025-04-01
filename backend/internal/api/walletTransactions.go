package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"
)

// WalletTransactionsResponse represents the response for wallet transactions
type WalletTransactionsResponse struct {
	Status       string                `json:"status"`
	Message      string                `json:"message"`
	Transactions []TransactionResponse `json:"transactions"`
	Count        int                   `json:"count"`
}

// GetWalletTransactions returns all transactions for a specific wallet
func (h *Handler) GetWalletTransactions(c *gin.Context) {
	startTime := time.Now()

	// Get wallet address from query parameter
	walletAddress := c.Query("wallet")
	if walletAddress == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Wallet address is required",
		})
		return
	}

	// Validate wallet address
	if !common.IsHexAddress(walletAddress) {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid wallet address format",
		})
		return
	}

	// Parse pagination parameters with defaults
	limit := 10
	offset := 0

	limitParam := c.Query("limit")
	if limitParam != "" {
		parsedLimit, err := strconv.Atoi(limitParam)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
		// Cap limit at 100 to prevent excessive queries
		if limit > 100 {
			limit = 100
		}
	}

	offsetParam := c.Query("offset")
	if offsetParam != "" {
		parsedOffset, err := strconv.Atoi(offsetParam)
		if err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	logger := slog.With(
		slog.String("handler", "GetWalletTransactions"),
		slog.String("request_id", c.GetString("request_id")),
		slog.String("wallet", walletAddress),
		slog.Int("limit", limit),
		slog.Int("offset", offset),
	)

	logger.Info("Wallet transactions request received")

	// Convert wallet address to the appropriate format
	wallet := common.HexToAddress(walletAddress)

	// Get transactions for the wallet using the transaction view
	transactions, err := h.BridgeService.GetDB().GetTransactionViewsByWallet(wallet, limit, offset)
	if err != nil {
		logger.Error("Error getting wallet transactions", slog.String("error", err.Error()))
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Error retrieving wallet transactions",
			"error":   err.Error(),
		})
		return
	}

	// Create response
	response := WalletTransactionsResponse{
		Status:       "success",
		Message:      "Wallet transactions retrieved successfully",
		Transactions: make([]TransactionResponse, 0, len(transactions)),
		Count:        len(transactions),
	}

	// Format each transaction for the response
	for _, tx := range transactions {
		txResponse := TransactionResponse{
			Status:  tx.Status,
			Message: "Transaction retrieved successfully",
			Txs:     make(map[string]string),
		}

		if tx.DepositID != nil {
			txResponse.DepositID = tx.DepositID.String()
		}

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

		// Set default chain IDs
		txResponse.SourceChainId = chainIDMap["Arbitrum"]   // Default to Arbitrum
		txResponse.DestinationChainId = chainIDMap["Monad"] // Always Monad as destination

		// Try to determine source chain from deposit ID format
		if tx.DepositID != nil {
			depositIDStr := tx.DepositID.String()
			for chainName, chainID := range chainIDMap {
				chainIDStr := fmt.Sprintf("%d", chainID)
				if strings.HasPrefix(depositIDStr, chainIDStr) && chainName != "Monad" {
					txResponse.SourceChainId = chainID
					break
				}
			}
		}

		// Include transaction hashes with the new format
		if tx.TxHash != "" {
			txResponse.Txs["source_tx"] = tx.TxHash
		}

		if tx.MonadTxHash != "" {
			txResponse.Txs["destination_tx"] = tx.MonadTxHash
		}

		response.Transactions = append(response.Transactions, txResponse)
	}

	c.JSON(http.StatusOK, response)
	logger.Info("Wallet transactions response sent",
		slog.String("duration", time.Since(startTime).String()),
		slog.Int("count", len(transactions)),
	)
}
