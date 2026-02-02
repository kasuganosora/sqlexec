package testing

import (
	"fmt"
	"strings"
	"sync"
)

// MockLogger 实现handler.Logger接口用于测试
type MockLogger struct {
	mu      sync.Mutex
	logs    []string
	enabled bool
}

// NewMockLogger 创建一个新的Mock日志记录器
func NewMockLogger() *MockLogger {
	return &MockLogger{
		logs:    make([]string, 0),
		enabled: true,
	}
}

// Printf 实现Logger接口
func (m *MockLogger) Printf(format string, v ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.enabled {
		return
	}

	msg := fmt.Sprintf(format, v...)
	m.logs = append(m.logs, msg)
}

// GetLogs 获取所有日志记录
func (m *MockLogger) GetLogs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.logs))
	copy(result, m.logs)
	return result
}

// ClearLogs 清除所有日志记录
func (m *MockLogger) ClearLogs() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = make([]string, 0)
}

// ContainsLog 检查是否包含特定日志
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

// GetLogCount 获取日志数量
func (m *MockLogger) GetLogCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.logs)
}

// GetLastLog 获取最后一条日志
func (m *MockLogger) GetLastLog() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.logs) == 0 {
		return ""
	}
	return m.logs[len(m.logs)-1]
}

// Disable 禁用日志记录
func (m *MockLogger) Disable() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = false
}

// Enable 启用日志记录
func (m *MockLogger) Enable() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = true
}
