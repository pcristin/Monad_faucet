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
	isProduction bool
)

func init() {
	flags := log.Ldate | log.Ltime | log.Lshortfile

	InfoLogger = log.New(os.Stdout, "INFO: ", flags)
	WarnLogger = log.New(os.Stdout, "WARN: ", flags)
	ErrorLogger = log.New(os.Stderr, "ERROR: ", flags)

	// Check if we're running in production (Render)
	isProduction = os.Getenv("RENDER") == "true"
}

// SetProduction explicitly sets production mode
func SetProduction(prod bool) {
	isProduction = prod
}

func Info(format string, v ...interface{}) {
	// In production, skip startup and routine info logs
	if isProduction && (strings.Contains(format, "Starting") ||
		strings.Contains(format, "initialized") ||
		strings.Contains(format, "processor...")) {
		return
	}
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
