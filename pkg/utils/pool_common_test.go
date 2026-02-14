package utils

import (
	"errors"
	"testing"
)

func TestPoolError(t *testing.T) {
	err := NewPoolError("acquire", ErrPoolClosed)
	if err.Error() != "acquire: pool is closed" {
		t.Errorf("PoolError.Error() = %q, want %q", err.Error(), "acquire: pool is closed")
	}

	if !errors.Is(err, ErrPoolClosed) {
		t.Error("errors.Is should return true for ErrPoolClosed")
	}
}

func TestPoolErrorNil(t *testing.T) {
	err := NewPoolError("test", nil)
	if err.Error() != "test" {
		t.Errorf("PoolError.Error() with nil = %q, want %q", err.Error(), "test")
	}
}

func TestIsPoolClosed(t *testing.T) {
	if !IsPoolClosed(ErrPoolClosed) {
		t.Error("IsPoolClosed(ErrPoolClosed) should be true")
	}
	if IsPoolClosed(ErrPoolEmpty) {
		t.Error("IsPoolClosed(ErrPoolEmpty) should be false")
	}
}

func TestIsPoolEmpty(t *testing.T) {
	if !IsPoolEmpty(ErrPoolEmpty) {
		t.Error("IsPoolEmpty(ErrPoolEmpty) should be true")
	}
	if IsPoolEmpty(ErrPoolClosed) {
		t.Error("IsPoolEmpty(ErrPoolClosed) should be false")
	}
}

func TestIsAcquireTimeout(t *testing.T) {
	if !IsAcquireTimeout(ErrAcquireTimeout) {
		t.Error("IsAcquireTimeout(ErrAcquireTimeout) should be true")
	}
	if IsAcquireTimeout(ErrPoolClosed) {
		t.Error("IsAcquireTimeout(ErrPoolClosed) should be false")
	}
}

func TestPoolErrorsExist(t *testing.T) {
	// Just verify all errors are defined
	errors := []error{
		ErrPoolClosed,
		ErrPoolEmpty,
		ErrPoolFull,
		ErrInvalidConfig,
		ErrResourceClosed,
		ErrAcquireTimeout,
		ErrMaxRetries,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("Pool error should not be nil")
		}
	}
}
