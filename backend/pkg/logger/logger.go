package logger

import (
	"log"
	"os"
)

var (
	InfoLogger  *log.Logger
	WarnLogger  *log.Logger
	ErrorLogger *log.Logger
)

func init() {
	flags := log.Ldate | log.Ltime | log.Lshortfile

	InfoLogger = log.New(os.Stdout, "INFO: ", flags)
	WarnLogger = log.New(os.Stdout, "WARN: ", flags)
	ErrorLogger = log.New(os.Stderr, "ERROR: ", flags)
}

func Info(format string, v ...interface{}) {
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
