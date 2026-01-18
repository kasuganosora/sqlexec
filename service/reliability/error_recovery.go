package reliability

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrorType 错误类型
type ErrorType int

const (
	ErrorTypeConnection ErrorType = iota
	ErrorTypeTimeout
	ErrorTypeQuery
	ErrorTypeTransaction
	ErrorTypeDataCorruption
	ErrorTypeResourceExhausted
)

// Severity 严重程度
type Severity int

const (
	SeverityLow Severity = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

// RecoveryAction 恢复动作
type RecoveryAction int

const (
	ActionRetry RecoveryAction = iota
	ActionFallback
	ActionAbort
	ActionIgnore
)

// ErrorInfo 错误信息
type ErrorInfo struct {
	Type      ErrorType
	Severity  Severity
	Message   string
	Err       error
	Timestamp time.Time
	Context   map[string]interface{}
}

// RecoveryStrategy 恢复策略
type RecoveryStrategy struct {
	MaxRetries      int
	RetryInterval   time.Duration
	BackoffFactor   float64
	Action          RecoveryAction
	OnError         func(*ErrorInfo)
	OnSuccess       func()
}

// ErrorRecoveryManager 错误恢复管理�?
type ErrorRecoveryManager struct {
	strategies map[ErrorType]*RecoveryStrategy
	errorLog   []*ErrorInfo
	logLock    sync.RWMutex
}

// NewErrorRecoveryManager 创建错误恢复管理�?
func NewErrorRecoveryManager() *ErrorRecoveryManager {
	return &ErrorRecoveryManager{
		strategies: make(map[ErrorType]*RecoveryStrategy),
		errorLog:   make([]*ErrorInfo, 0),
	}
}

// RegisterStrategy 注册恢复策略
func (m *ErrorRecoveryManager) RegisterStrategy(errorType ErrorType, strategy *RecoveryStrategy) {
	m.strategies[errorType] = strategy
}

// ExecuteWithRetry 使用重试执行操作
func (m *ErrorRecoveryManager) ExecuteWithRetry(errorType ErrorType, fn func() error) error {
	strategy, ok := m.strategies[errorType]
	if !ok {
		// 默认策略：重�?次，间隔1�?
		strategy = &RecoveryStrategy{
			MaxRetries:    3,
			RetryInterval: 1 * time.Second,
			BackoffFactor: 1.0,
			Action:        ActionRetry,
		}
	}

	var lastErr error
	interval := strategy.RetryInterval

	for attempt := 0; attempt <= strategy.MaxRetries; attempt++ {
		err := fn()
		if err == nil {
			if strategy.OnSuccess != nil {
				strategy.OnSuccess()
			}
			return nil
		}

		lastErr = err

		// 记录错误
		errorInfo := &ErrorInfo{
			Type:      errorType,
			Severity:  SeverityMedium,
			Message:   fmt.Sprintf("Attempt %d failed", attempt+1),
			Err:       err,
			Timestamp: time.Now(),
			Context: map[string]interface{}{
				"attempt": attempt + 1,
			},
		}

		m.logError(errorInfo)

		if strategy.OnError != nil {
			strategy.OnError(errorInfo)
		}

		// 如果还有重试机会，等�?
		if attempt < strategy.MaxRetries {
			time.Sleep(interval)
			interval = time.Duration(float64(interval) * strategy.BackoffFactor)
		}
	}

	return fmt.Errorf("max retries (%d) exceeded, last error: %w", strategy.MaxRetries, lastErr)
}

// ExecuteWithFallback 使用备用方案执行
func (m *ErrorRecoveryManager) ExecuteWithFallback(errorType ErrorType, primary, fallback func() error) error {
	strategy, ok := m.strategies[errorType]
	if ok && strategy.Action == ActionFallback {
		// 执行备用方案
		err := fallback()
		if err == nil {
			m.logError(&ErrorInfo{
				Type:      errorType,
				Severity:  SeverityLow,
				Message:   "Fallback successful",
				Timestamp: time.Now(),
			})
			return nil
		}
	}

	// 执行主方�?
	err := primary()
	if err != nil {
		// 如果备用方案存在，尝试执�?
		if fallback != nil {
			fallbackErr := fallback()
			if fallbackErr == nil {
				m.logError(&ErrorInfo{
					Type:      errorType,
					Severity:  SeverityLow,
					Message:   "Fallback successful",
					Timestamp: time.Now(),
				})
				return nil
			}
		}

		return err
	}

	return nil
}

// logError 记录错误
func (m *ErrorRecoveryManager) logError(errorInfo *ErrorInfo) {
	m.logLock.Lock()
	defer m.logLock.Unlock()

	m.errorLog = append(m.errorLog, errorInfo)

	// 保持日志大小在合理范�?
	if len(m.errorLog) > 1000 {
		m.errorLog = m.errorLog[len(m.errorLog)-1000:]
	}
}

// GetErrorLog 获取错误日志
func (m *ErrorRecoveryManager) GetErrorLog(offset, limit int) []*ErrorInfo {
	m.logLock.RLock()
	defer m.logLock.RUnlock()

	start := offset
	if start < 0 {
		start = 0
	}

	end := start + limit
	if end > len(m.errorLog) {
		end = len(m.errorLog)
	}

	if start >= end {
		return []*ErrorInfo{}
	}

	return m.errorLog[start:end]
}

// GetErrorStats 获取错误统计
func (m *ErrorRecoveryManager) GetErrorStats() map[ErrorType]int {
	m.logLock.RLock()
	defer m.logLock.RUnlock()

	stats := make(map[ErrorType]int)
	for _, err := range m.errorLog {
		stats[err.Type]++
	}

	return stats
}

// IsRetryable 判断错误是否可重�?
func (m *ErrorRecoveryManager) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	strategy, ok := m.strategies[ErrorTypeConnection]
	if ok && strategy.Action == ActionRetry {
		return true
	}

	return false
}

// CircuitBreaker 断路�?
type CircuitBreaker struct {
	failureThreshold int
	failureCount     int
	successThreshold int
	successCount     int
	state            CircuitState
	lastFailureTime  time.Time
	timeout          time.Duration
}

// CircuitState 断路器状�?
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// NewCircuitBreaker 创建断路�?
func NewCircuitBreaker(failureThreshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: 3,
		state:            StateClosed,
		timeout:          timeout,
	}
}

// Execute 执行操作（带断路器保护）
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if cb.state == StateOpen {
		if time.Since(cb.lastFailureTime) > cb.timeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
		} else {
			return errors.New("circuit breaker is open")
		}
	}

	err := fn()

	if err != nil {
		cb.onFailure()
		return err
	}

	cb.onSuccess()
	return nil
}

// onSuccess 成功回调
func (cb *CircuitBreaker) onSuccess() {
	cb.failureCount = 0

	if cb.state == StateHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = StateClosed
		}
	}
}

// onFailure 失败回调
func (cb *CircuitBreaker) onFailure() {
	cb.failureCount++
	cb.lastFailureTime = time.Now()

	if cb.failureCount >= cb.failureThreshold {
		cb.state = StateOpen
	}
}

// GetState 获取断路器状�?
func (cb *CircuitBreaker) GetState() CircuitState {
	return cb.state
}

// Reset 重置断路�?
func (cb *CircuitBreaker) Reset() {
	cb.failureCount = 0
	cb.successCount = 0
	cb.state = StateClosed
}
