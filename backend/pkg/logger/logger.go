package logger

import (
	"log"
	"os"
	"strings"
)

var (
	InfoLogger   *log.Logger
	WarnLogger   *log.Logger
	ErrorLogger  *log.Logger
	DebugLogger  *log.Logger
	isProduction bool
)

func init() {
	flags := log.Ldate | log.Ltime | log.Lshortfile

	InfoLogger = log.New(os.Stdout, "INFO: ", flags)
	WarnLogger = log.New(os.Stdout, "WARN: ", flags)
	ErrorLogger = log.New(os.Stderr, "ERROR: ", flags)
	DebugLogger = log.New(os.Stdout, "DEBUG: ", flags)

	// Check if we're running in production (Render)
	isProduction = os.Getenv("PRODUCTION") == "true"
}

// SetProduction explicitly sets production mode
func SetProduction(prod bool) {
	isProduction = prod
}

func Info(format string, v ...interface{}) {
	// In production, filter out verbose logging
	if isProduction {
		// Skip routine informational messages
		if strings.Contains(format, "Starting") ||
			strings.Contains(format, "initialized") ||
			strings.Contains(format, "processor...") {
			return
		}

		// Filter out detailed calculation logs
		if strings.Contains(format, "Calculating") ||
			strings.Contains(format, "ratio") ||
			strings.Contains(format, "per smallest") ||
			strings.Contains(format, "Using cached") ||
			strings.Contains(format, "Theoretical") ||
			strings.Contains(format, "wei") {
			// Send to Debug instead
			Debug(format, v...)
			return
		}

		// Keep important INFO logs related to transactions
		if strings.Contains(format, "Processing") ||
			strings.Contains(format, "Successfully") ||
			strings.Contains(format, "Minting") ||
			strings.Contains(format, "Calculated MON amount:") ||
			strings.Contains(format, "processed") ||
			strings.Contains(format, "completed") {
			// These are important transaction events - keep at INFO level
			InfoLogger.Printf(format, v...)
			return
		}

		// Filter out routine worker logs
		if strings.Contains(format, "Worker") ||
			strings.Contains(format, "worker pool") ||
			strings.Contains(format, "queue") ||
			strings.Contains(format, "stopping") {
			return
		}
	}

	// If we're not in production or the message passed the filters
	InfoLogger.Printf(format, v...)
}

func Warn(format string, v ...interface{}) {
	WarnLogger.Printf(format, v...)
}

func Error(format string, v ...interface{}) {
	ErrorLogger.Printf(format, v...)
}

func Fatal(format string, v ...interface{}) {
	ErrorLogger.Fatalf(format, v...)
}

// Debug logs debug messages
// In production mode, debug messages are suppressed
func Debug(format string, v ...interface{}) {
	if !isProduction {
		DebugLogger.Printf(format, v...)
	}
}
