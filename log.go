package nxsugar

import (
	"fmt"
	"strings"
	"time"

	"sync"

	"github.com/sirupsen/logrus"
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

var logLock *sync.Mutex

type customFormatter struct {
}

func init() {
	logLock = &sync.Mutex{}
}

func (f *customFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	path := entry.Data["path"]
	return []byte(fmt.Sprintf("[%s] [%s] [%s] %s\n", entry.Time.Format(time.RFC3339), strings.ToUpper(entry.Level.String()[:4]), path, entry.Message)), nil
}

// SetProductionMode sets the log level to JSON format
func SetJSONOutput(enabled bool) {
	logLock.Lock()
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
	logLock.Unlock()
}

// SetLogLevel sets the log level to one of (debug, info, warn, error, fatal, panic)
func SetLogLevel(level string) {
	logLock.Lock()
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
	logLock.Unlock()
}

// GetLogLevel returns the current log level
func GetLogLevel() string {
	logLock.Lock()
	defer logLock.Unlock()
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
	logLock.Lock()
	defer logLock.Unlock()
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
