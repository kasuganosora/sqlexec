package api

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultLogger_Debug(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogDebug)
	logger.output = &buf

	logger.Debug("debug message: %s", "test")

	output := buf.String()
	assert.Contains(t, output, "debug message: test")
	assert.Contains(t, strings.ToLower(output), "debug")
}

func TestDefaultLogger_Info(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogInfo)
	logger.output = &buf

	logger.Info("info message: %s", "test")

	output := buf.String()
	assert.Contains(t, output, "info message: test")
	assert.Contains(t, strings.ToLower(output), "info")
}

func TestDefaultLogger_Warn(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogInfo)
	logger.output = &buf

	logger.Warn("warn message: %s", "test")

	output := buf.String()
	assert.Contains(t, output, "warn message: test")
	assert.Contains(t, strings.ToLower(output), "warn")
}

func TestDefaultLogger_Error(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogInfo)
	logger.output = &buf

	logger.Error("error message: %s", "test")

	output := buf.String()
	assert.Contains(t, output, "error message: test")
	assert.Contains(t, strings.ToLower(output), "error")
}

func TestDefaultLogger_SetLevel(t *testing.T) {
	logger := NewDefaultLogger(LogInfo)

	logger.SetLevel(LogDebug)
	assert.Equal(t, LogDebug, logger.GetLevel())

	logger.SetLevel(LogError)
	assert.Equal(t, LogError, logger.GetLevel())
}

func TestDefaultLogger_GetLevel(t *testing.T) {
	logger := NewDefaultLogger(LogDebug)
	assert.Equal(t, LogDebug, logger.GetLevel())

	logger = NewDefaultLogger(LogInfo)
	assert.Equal(t, LogInfo, logger.GetLevel())

	logger = NewDefaultLogger(LogWarn)
	assert.Equal(t, LogWarn, logger.GetLevel())

	logger = NewDefaultLogger(LogError)
	assert.Equal(t, LogError, logger.GetLevel())
}

func TestDefaultLogger_LevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogError)
	logger.output = &buf

	// These should not be logged (below ERROR level)
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")

	// This should be logged
	logger.Error("error")

	output := buf.String()
	assert.Contains(t, output, "error")
	assert.NotContains(t, output, "debug")
	assert.NotContains(t, output, "info")
	assert.NotContains(t, output, "warn")
}

func TestDefaultLogger_NoArgs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogInfo)
	logger.output = &buf

	logger.Info("simple message")

	output := buf.String()
	assert.Contains(t, output, "simple message")
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	// These should not panic
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	logger.SetLevel(LogDebug)
	// NoOpLogger ignores SetLevel and always returns LogInfo
	assert.Equal(t, LogInfo, logger.GetLevel())

	// Test NoOpLogger methods directly
	t.Run("NoOpLogger methods", func(t *testing.T) {
		noOp := &NoOpLogger{}
		noOp.Debug("debug")
		noOp.Info("info")
		noOp.Warn("warn")
		noOp.Error("error")
		noOp.SetLevel(LogDebug)
		assert.Equal(t, LogInfo, noOp.GetLevel())
	})
}

func TestLogLevels_String(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{LogError, "ERROR"},
		{LogWarn, "WARN"},
		{LogInfo, "INFO"},
		{LogDebug, "DEBUG"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.level.String())
		})
	}
}

func TestDefaultLogger_Concurrency(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger(LogInfo)
	logger.output = &buf

	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(n int) {
			logger.Info("message %d", n)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	output := buf.String()
	// Should have all messages
	assert.Contains(t, output, "message 0")
	assert.Contains(t, output, "message 99")
}

func TestNewDefaultLogger(t *testing.T) {
	logger := NewDefaultLogger(LogInfo)

	assert.NotNil(t, logger)
	assert.Equal(t, LogInfo, logger.GetLevel())
	assert.NotNil(t, logger.output)
}

func TestNewDefaultLoggerWithOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLoggerWithOutput(LogInfo, &buf)

	assert.NotNil(t, logger)
	assert.Equal(t, LogInfo, logger.GetLevel())
	assert.Equal(t, &buf, logger.output)

	logger.Info("test message")
	assert.Contains(t, buf.String(), "test message")
}

func ExampleDefaultLogger() {
	logger := NewDefaultLogger(LogInfo)
	logger.Info("Application started")
	logger.Debug("This won't be shown due to log level")
	logger.Error("An error occurred")
}
