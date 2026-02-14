package utils

import "errors"

// Common pool errors
var (
	// ErrPoolClosed indicates that the pool has been closed
	ErrPoolClosed = errors.New("pool is closed")

	// ErrPoolEmpty indicates that the pool has no available resources
	ErrPoolEmpty = errors.New("pool is empty")

	// ErrPoolFull indicates that the pool has reached its maximum capacity
	ErrPoolFull = errors.New("pool is full")

	// ErrInvalidConfig indicates that the pool configuration is invalid
	ErrInvalidConfig = errors.New("invalid pool configuration")

	// ErrResourceClosed indicates that the resource has been closed
	ErrResourceClosed = errors.New("resource is closed")

	// ErrAcquireTimeout indicates that acquiring a resource timed out
	ErrAcquireTimeout = errors.New("acquire timeout")

	// ErrMaxRetries indicates that max retries have been exceeded
	ErrMaxRetries = errors.New("max retries exceeded")
)

// PoolError represents a pool-related error with additional context
type PoolError struct {
	Op  string // The operation that failed
	Err error  // The underlying error
}

// Error implements the error interface
func (e *PoolError) Error() string {
	if e.Err != nil {
		return e.Op + ": " + e.Err.Error()
	}
	return e.Op
}

// Unwrap returns the underlying error
func (e *PoolError) Unwrap() error {
	return e.Err
}

// NewPoolError creates a new PoolError
func NewPoolError(op string, err error) *PoolError {
	return &PoolError{
		Op:  op,
		Err: err,
	}
}

// IsPoolClosed checks if the error is ErrPoolClosed
func IsPoolClosed(err error) bool {
	return errors.Is(err, ErrPoolClosed)
}

// IsPoolEmpty checks if the error is ErrPoolEmpty
func IsPoolEmpty(err error) bool {
	return errors.Is(err, ErrPoolEmpty)
}

// IsAcquireTimeout checks if the error is ErrAcquireTimeout
func IsAcquireTimeout(err error) bool {
	return errors.Is(err, ErrAcquireTimeout)
}
