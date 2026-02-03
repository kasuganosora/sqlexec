package mock

import (
	"fmt"
	"strings"
	"sync"
)

// MockLogger implements handler.Logger interface for testing
type MockLogger struct {
	mu      sync.Mutex
	logs    []string
	enabled bool
}

// NewMockLogger creates a new mock logger
func NewMockLogger() *MockLogger {
	return &MockLogger{
		logs:    make([]string, 0),
		enabled: true,
	}
}

// Printf implements Logger interface
func (m *MockLogger) Printf(format string, v ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.enabled {
		return
	}

	msg := fmt.Sprintf(format, v...)
	m.logs = append(m.logs, msg)
}

// GetLogs returns all log records
func (m *MockLogger) GetLogs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.logs))
	copy(result, m.logs)
	return result
}

// ClearLogs clears all log records
func (m *MockLogger) ClearLogs() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = make([]string, 0)
}

// ContainsLog checks if contains specific log
func (m *MockLogger) ContainsLog(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, log := range m.logs {
		if strings.Contains(log, substr) {
			return true
		}
	}
	return false
}

// GetLogCount returns log count
func (m *MockLogger) GetLogCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.logs)
}

// GetLastLog returns last log
func (m *MockLogger) GetLastLog() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.logs) == 0 {
		return ""
	}
	return m.logs[len(m.logs)-1]
}

// Disable disables logging
func (m *MockLogger) Disable() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = false
}

// Enable enables logging
func (m *MockLogger) Enable() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = true
}
