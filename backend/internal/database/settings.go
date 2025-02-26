package database

import (
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"time"
)

// Setting keys
const (
	SettingFaucetEnabled    = "faucet_enabled"
	SettingDailyWalletLimit = "daily_wallet_limit"
	SettingMaxMonAmount     = "max_mon_amount"
)

// GetSetting retrieves a setting value from the database
func (db *DB) GetSetting(key string) (string, error) {
	var value string
	err := db.QueryRow("SELECT value FROM settings WHERE key = $1", key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("setting not found: %s", key)
		}
		return "", fmt.Errorf("failed to get setting: %w", err)
	}
	return value, nil
}

// SetSetting sets a setting value in the database
func (db *DB) SetSetting(key, value string) error {
	_, err := db.Exec(
		`INSERT INTO settings (key, value, updated_at) 
		VALUES ($1, $2, $3) 
		ON CONFLICT(key) DO UPDATE SET 
		value = $4, updated_at = $5`,
		key, value, time.Now(),
		value, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("failed to set setting: %w", err)
	}
	return nil
}

// GetBoolSetting retrieves a boolean setting value
func (db *DB) GetBoolSetting(key string) (bool, error) {
	value, err := db.GetSetting(key)
	if err != nil {
		return false, err
	}
	return value == "true", nil
}

// SetBoolSetting sets a boolean setting value
func (db *DB) SetBoolSetting(key string, value bool) error {
	strValue := "false"
	if value {
		strValue = "true"
	}
	return db.SetSetting(key, strValue)
}

// GetIntSetting retrieves an integer setting value
func (db *DB) GetIntSetting(key string) (int, error) {
	value, err := db.GetSetting(key)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(value)
}

// SetIntSetting sets an integer setting value
func (db *DB) SetIntSetting(key string, value int) error {
	return db.SetSetting(key, strconv.Itoa(value))
}

// GetBigIntSetting retrieves a big.Int setting value
func (db *DB) GetBigIntSetting(key string) (*big.Int, error) {
	value, err := db.GetSetting(key)
	if err != nil {
		return nil, err
	}

	bigInt, success := new(big.Int).SetString(value, 10)
	if !success {
		return nil, fmt.Errorf("invalid big.Int format: %s", value)
	}

	return bigInt, nil
}

// SetBigIntSetting sets a big.Int setting value
func (db *DB) SetBigIntSetting(key string, value *big.Int) error {
	return db.SetSetting(key, value.String())
}

// InitDefaultSettings initializes default settings if they don't exist
func (db *DB) InitDefaultSettings() error {
	// Check if faucet_enabled exists
	_, err := db.GetSetting(SettingFaucetEnabled)
	if err != nil {
		// Set default to true
		if err := db.SetBoolSetting(SettingFaucetEnabled, true); err != nil {
			return err
		}
	}

	// Check if daily_wallet_limit exists
	_, err = db.GetSetting(SettingDailyWalletLimit)
	if err != nil {
		// Set default to 5 MON (5 * 10^18)
		defaultLimit := new(big.Int).Mul(
			big.NewInt(5),
			new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
		)
		if err := db.SetBigIntSetting(SettingDailyWalletLimit, defaultLimit); err != nil {
			return err
		}
	}

	// Check if max_mon_amount exists
	_, err = db.GetSetting(SettingMaxMonAmount)
	if err != nil {
		// Set default to 2 MON (2 * 10^18)
		defaultMax := new(big.Int).Mul(
			big.NewInt(2),
			new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
		)
		if err := db.SetBigIntSetting(SettingMaxMonAmount, defaultMax); err != nil {
			return err
		}
	}

	return nil
}

// LogAdminAction logs an admin action to the database
func (db *DB) LogAdminAction(action, params, adminKey string) error {
	// Mask part of the admin key for security
	maskedKey := maskAdminKey(adminKey)

	_, err := db.Exec(
		"INSERT INTO admin_actions (action, params, admin_key) VALUES ($1, $2, $3)",
		action, params, maskedKey,
	)
	if err != nil {
		return fmt.Errorf("failed to log admin action: %w", err)
	}
	return nil
}

// maskAdminKey masks part of the admin key for security
func maskAdminKey(key string) string {
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "..." + key[len(key)-4:]
}
