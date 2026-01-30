package reliability

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewErrorRecoveryManager(t *testing.T) {
	manager := NewErrorRecoveryManager()

	assert.NotNil(t, manager)
	assert.NotNil(t, manager.strategies)
	assert.NotNil(t, manager.errorLog)
}

func TestExecuteWithRetry_Success(t *testing.T) {
	manager := NewErrorRecoveryManager()

	attempts := 0
	err := manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
		attempts++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, attempts)
}

func TestExecuteWithRetry_RetryOnFailure(t *testing.T) {
	manager := NewErrorRecoveryManager()

	attempts := 0
	err := manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary error")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func TestExecuteWithRetry_MaxRetriesExceeded(t *testing.T) {
	manager := NewErrorRecoveryManager()

	attempts := 0
	err := manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
		attempts++
		return errors.New("persistent error")
	})

	assert.Error(t, err)
	assert.Equal(t, 4, attempts) // 1 initial + 3 retries
	assert.Contains(t, err.Error(), "max retries (3) exceeded")
}

func TestRegisterStrategy(t *testing.T) {
	manager := NewErrorRecoveryManager()

	strategy := &RecoveryStrategy{
		MaxRetries:    5,
		RetryInterval: 2 * time.Second,
		BackoffFactor: 2.0,
		Action:        ActionRetry,
	}

	manager.RegisterStrategy(ErrorTypeTimeout, strategy)

	// Verify strategy was registered
	registered, ok := manager.strategies[ErrorTypeTimeout]
	assert.True(t, ok)
	assert.Equal(t, 5, registered.MaxRetries)
	assert.Equal(t, 2*time.Second, registered.RetryInterval)
}

func TestExecuteWithRetry_CustomStrategy(t *testing.T) {
	manager := NewErrorRecoveryManager()

	// Register custom strategy
	strategy := &RecoveryStrategy{
		MaxRetries:    2,
		RetryInterval: 100 * time.Millisecond,
		BackoffFactor: 1.0,
		Action:        ActionRetry,
	}
	manager.RegisterStrategy(ErrorTypeQuery, strategy)

	attempts := 0
	err := manager.ExecuteWithRetry(ErrorTypeQuery, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("query error")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func TestExecuteWithRetry_Callbacks(t *testing.T) {
	manager := NewErrorRecoveryManager()

	onErrorCalled := false
	onSuccessCalled := false

	strategy := &RecoveryStrategy{
		MaxRetries:    1,
		RetryInterval: 100 * time.Millisecond,
		BackoffFactor: 1.0,
		Action:        ActionRetry,
		OnError: func(info *ErrorInfo) {
			onErrorCalled = true
		},
		OnSuccess: func() {
			onSuccessCalled = true
		},
	}
	manager.RegisterStrategy(ErrorTypeTransaction, strategy)

	attempts := 0
	_ = manager.ExecuteWithRetry(ErrorTypeTransaction, func() error {
		attempts++
		if attempts == 1 {
			return errors.New("transaction error")
		}
		return nil
	})

	assert.True(t, onErrorCalled)
	assert.True(t, onSuccessCalled)
}

func TestExecuteWithFallback_PrimarySuccess(t *testing.T) {
	manager := NewErrorRecoveryManager()

	primaryCalled := false
	fallbackCalled := false

	err := manager.ExecuteWithFallback(ErrorTypeConnection,
		func() error {
			primaryCalled = true
			return nil
		},
		func() error {
			fallbackCalled = true
			return nil
		},
	)

	assert.NoError(t, err)
	assert.True(t, primaryCalled)
	assert.False(t, fallbackCalled)
}

func TestExecuteWithFallback_FallbackSuccess(t *testing.T) {
	manager := NewErrorRecoveryManager()

	primaryCalled := false
	fallbackCalled := false

	err := manager.ExecuteWithFallback(ErrorTypeConnection,
		func() error {
			primaryCalled = true
			return errors.New("primary failed")
		},
		func() error {
			fallbackCalled = true
			return nil
		},
	)

	assert.NoError(t, err)
	assert.True(t, primaryCalled)
	assert.True(t, fallbackCalled)
}

func TestExecuteWithFallback_BothFail(t *testing.T) {
	manager := NewErrorRecoveryManager()

	err := manager.ExecuteWithFallback(ErrorTypeConnection,
		func() error {
			return errors.New("primary failed")
		},
		func() error {
			return errors.New("fallback failed")
		},
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "primary failed")
}

func TestGetErrorLog(t *testing.T) {
	manager := NewErrorRecoveryManager()

	// Use a short retry interval for this test
	strategy := &RecoveryStrategy{
		MaxRetries:    0,
		RetryInterval: 1 * time.Millisecond,
		BackoffFactor: 1.0,
		Action:        ActionRetry,
	}
	manager.RegisterStrategy(ErrorTypeConnection, strategy)

	// Generate some errors
	for i := 0; i < 5; i++ {
		_ = manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
			return errors.New("test error")
		})
	}

	// Get error log
	logs := manager.GetErrorLog(0, 10)
	assert.Len(t, logs, 5)

	// Get with offset
	logs = manager.GetErrorLog(2, 2)
	assert.Len(t, logs, 2)
}

func TestGetErrorStats(t *testing.T) {
	manager := NewErrorRecoveryManager()

	// Use short retry intervals for this test
	strategy := &RecoveryStrategy{
		MaxRetries:    0,
		RetryInterval: 1 * time.Millisecond,
		BackoffFactor: 1.0,
		Action:        ActionRetry,
	}
	manager.RegisterStrategy(ErrorTypeConnection, strategy)
	manager.RegisterStrategy(ErrorTypeTimeout, strategy)

	// Generate errors of different types
	_ = manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
		return errors.New("connection error")
	})
	_ = manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
		return errors.New("connection error")
	})
	_ = manager.ExecuteWithRetry(ErrorTypeTimeout, func() error {
		return errors.New("timeout error")
	})

	stats := manager.GetErrorStats()
	assert.Equal(t, 2, stats[ErrorTypeConnection])
	assert.Equal(t, 1, stats[ErrorTypeTimeout])
}

func TestIsRetryable(t *testing.T) {
	manager := NewErrorRecoveryManager()

	// No strategy registered
	assert.False(t, manager.IsRetryable(errors.New("test error")))

	// Register retry strategy
	strategy := &RecoveryStrategy{
		Action: ActionRetry,
	}
	manager.RegisterStrategy(ErrorTypeConnection, strategy)

	assert.True(t, manager.IsRetryable(errors.New("connection error")))
	assert.False(t, manager.IsRetryable(nil))
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	// Initially closed
	assert.Equal(t, StateClosed, cb.GetState())

	// Execute successfully
	err := cb.Execute(func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, StateClosed, cb.GetState())

	// Fail three times to open the circuit
	for i := 0; i < 3; i++ {
		err = cb.Execute(func() error {
			return errors.New("failure")
		})
		assert.Error(t, err)
	}

	// Circuit should be open
	assert.Equal(t, StateOpen, cb.GetState())

	// Execute should fail immediately
	err = cb.Execute(func() error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker is open")

	// Wait for timeout (slightly more than 100ms)
	time.Sleep(120 * time.Millisecond)

	// Circuit should be half-open
	// We need to call Execute to trigger state transition
	err = cb.Execute(func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, StateHalfOpen, cb.GetState())

	// Need 3 successful calls to close the circuit
	for i := 0; i < 3; i++ {
		err = cb.Execute(func() error {
			return nil
		})
		assert.NoError(t, err)
	}
	assert.Equal(t, StateClosed, cb.GetState())
}

func TestCircuitBreakerReset(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	// Fail to open circuit
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error {
			return errors.New("failure")
		})
	}

	assert.Equal(t, StateOpen, cb.GetState())

	// Reset
	cb.Reset()
	assert.Equal(t, StateClosed, cb.GetState())
	assert.Equal(t, 0, cb.failureCount)
	assert.Equal(t, 0, cb.successCount)
}

func TestErrorLogLimit(t *testing.T) {
	manager := NewErrorRecoveryManager()

	// Use a short retry interval for this test
	strategy := &RecoveryStrategy{
		MaxRetries:    0,
		RetryInterval: 1 * time.Millisecond,
		BackoffFactor: 1.0,
		Action:        ActionRetry,
	}
	manager.RegisterStrategy(ErrorTypeConnection, strategy)

	// Generate many errors to test log limiting
	for i := 0; i < 1500; i++ {
		_ = manager.ExecuteWithRetry(ErrorTypeConnection, func() error {
			return errors.New("test error")
		})
	}

	// Log should be limited to 1000 entries
	logs := manager.GetErrorLog(0, 2000)
	assert.LessOrEqual(t, len(logs), 1000)
}

