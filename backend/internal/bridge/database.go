package bridge

import (
	"github.com/pcristin/monad-faucet/internal/database"
	"github.com/pcristin/monad-faucet/pkg/logger"
)

//
// --- Database Helpers ---
//

func (s *BridgeService) CheckDatabaseConnection() error {
	return s.db.Ping()
}

// GetTransactionCounts returns the number of transactions in each status.
func (s *BridgeService) GetTransactionCounts() (pending, completed, failed, refunded int, err error) {
	rows, err := s.db.Query(`
		SELECT status, COUNT(*) 
		FROM transaction_history 
		GROUP BY status
	`)
	if err != nil {
		logger.Error("failed to query transaction counts: %v", err)
		return 0, 0, 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			logger.Error("failed to scan row: %v", err)
			return 0, 0, 0, 0, err
		}
		switch status {
		case database.StatusPending:
			pending = count
		case database.StatusCompleted:
			completed = count
		case database.StatusFailed:
			failed = count
		case database.StatusRefunded:
			refunded = count
		}
	}
	if err := rows.Err(); err != nil {
		logger.Error("error iterating rows: %v", err)
		return 0, 0, 0, 0, err
	}
	return pending, completed, failed, refunded, nil
}

// GetDB returns the database instance.
func (s *BridgeService) GetDB() *database.DB {
	return s.db
}
