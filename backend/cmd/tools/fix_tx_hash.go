package main

import (
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/pcristin/monad-faucet/internal/database"
)

func main() {
	// Parse command line arguments
	depositIDStr := flag.String("deposit-id", "", "Deposit ID to update")
	txHash := flag.String("tx-hash", "", "New transaction hash")
	_ = flag.String("db-url", os.Getenv("DATABASE_URL"), "Database connection URL (not used, set DATABASE_URL env var instead)")
	flag.Parse()

	// Validate arguments
	if *depositIDStr == "" || *txHash == "" {
		log.Fatal("Both deposit-id and tx-hash are required")
	}

	// Parse deposit ID
	depositID, ok := new(big.Int).SetString(*depositIDStr, 10)
	if !ok {
		log.Fatalf("Invalid deposit ID: %s", *depositIDStr)
	}

	// Connect to database
	db, err := database.New("")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Get current transaction
	tx, err := db.GetTransactionByDepositID(depositID)
	if err != nil {
		log.Fatalf("Failed to get transaction: %v", err)
	}

	fmt.Printf("Current transaction: ID=%d, DepositID=%s, TxHash=%s\n",
		tx.ID, tx.DepositID.String(), tx.TxHash)

	// Update transaction hash
	err = db.UpdateTransactionHash(depositID, *txHash)
	if err != nil {
		log.Fatalf("Failed to update transaction hash: %v", err)
	}

	fmt.Printf("Successfully updated transaction hash for deposit ID %s to %s\n",
		depositID.String(), *txHash)
}
