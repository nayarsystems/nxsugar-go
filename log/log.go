// Package log
package log

import (
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

// Singleton logrus logger object with custom format.
// Verbosity can be changed through SetLogLevel.
var log *logrus.Logger

const (
	PanicLevel = "panic"
	FatalLevel = "fatal"
	ErrorLevel = "error"
	WarnLevel  = "warn"
	InfoLevel  = "info"
	DebugLevel = "debug"
)

type customFormatter struct {
}

func (f *customFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	path := entry.Data["path"]
	return []byte(fmt.Sprintf("[%s] [%s] [%s] %s\n", entry.Time.Format(time.RFC3339), strings.ToUpper(entry.Level.String()[:4]), path, entry.Message)), nil
}

func init() {
	SetJSONOutput(false)
}

// SetProductionMode sets the log level to JSON format
func SetJSONOutput(enabled bool) {
	if enabled {
		log = logrus.New()
		jsonFmt := new(logrus.JSONFormatter)
		log.Formatter = jsonFmt
		log.Level = logrus.DebugLevel
	} else {
		log = logrus.New()
		log.Formatter = new(customFormatter)
		log.Level = logrus.DebugLevel
	}
}

// SetLogLevel sets the log level to one of (debug, info, warn, error, fatal, panic)
func SetLogLevel(level string) {
	switch strings.ToLower(level) {
	case PanicLevel:
		log.Level = logrus.PanicLevel
	case FatalLevel:
		log.Level = logrus.FatalLevel
	case ErrorLevel:
		log.Level = logrus.ErrorLevel
	case WarnLevel:
		log.Level = logrus.WarnLevel
	case InfoLevel:
		log.Level = logrus.InfoLevel
	default:
		log.Level = logrus.DebugLevel
	}
}

// GetLogLevel returns the current log level
func GetLogLevel() string {
	switch log.Level {
	case logrus.PanicLevel:
		return PanicLevel
	case logrus.FatalLevel:
		return FatalLevel
	case logrus.ErrorLevel:
		return ErrorLevel
	case logrus.WarnLevel:
		return WarnLevel
	case logrus.InfoLevel:
		return InfoLevel
	case logrus.DebugLevel:
		return DebugLevel
	}
	return DebugLevel
}

// Log
func Log(level string, path string, message string, args ...interface{}) {
	LogWithFields(level, path, map[string]interface{}{}, message, args...)
}

// LogWithFields
func LogWithFields(level string, path string, fields map[string]interface{}, message string, args ...interface{}) {
	le := log.WithField("path", path).WithField("data", fields)
	switch strings.ToLower(level) {
	case PanicLevel:
		le.Panicf(message, args...)
	case FatalLevel:
		le.Fatalf(message, args...)
	case ErrorLevel:
		le.Errorf(message, args...)
	case WarnLevel:
		le.Warnf(message, args...)
	case InfoLevel:
		le.Infof(message, args...)
	default:
		le.Debugf(message, args...)
	}
}
