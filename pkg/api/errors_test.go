package api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewError(t *testing.T) {
	err := NewError(ErrCodeInvalidParam, "test error", nil)

	assert.NotNil(t, err)
	assert.Equal(t, ErrCodeInvalidParam, err.Code)
	assert.Equal(t, "test error", err.Message)
	assert.Nil(t, err.Cause)
	assert.NotEmpty(t, err.Stack)
}

func TestWrapError(t *testing.T) {
	originalErr := errors.New("original error")
	wrappedErr := WrapError(originalErr, ErrCodeInternal, "wrapped message")

	assert.NotNil(t, wrappedErr)
	assert.Equal(t, ErrCodeInternal, wrappedErr.Code)
	assert.Equal(t, "wrapped message", wrappedErr.Message)
	assert.Equal(t, originalErr, wrappedErr.Cause)
	assert.NotEmpty(t, wrappedErr.Stack)
}

func TestError_Error(t *testing.T) {
	err := NewError(ErrCodeInvalidParam, "test error", nil)
	errorMsg := err.Error()

	assert.Contains(t, errorMsg, string(ErrCodeInvalidParam))
	assert.Contains(t, errorMsg, "test error")
}

func TestError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	wrappedErr := WrapError(originalErr, ErrCodeInternal, "wrapped message")

	unwrapped := errors.Unwrap(wrappedErr)
	assert.Equal(t, originalErr, unwrapped)
}

func TestError_StackTrace(t *testing.T) {
	err := NewError(ErrCodeInvalidParam, "test error", nil)
	stack := err.StackTrace()

	assert.NotEmpty(t, stack)
	assert.Greater(t, len(stack), 0)
	
	// Each stack line should contain a colon (file:line format)
	for _, line := range stack {
		assert.Contains(t, line, ":")
	}
}

func TestCaptureStackTrace(t *testing.T) {
	stack := captureStackTrace()
	
	assert.NotEmpty(t, stack)
	assert.Greater(t, len(stack), 0)
	
	// Check that we have some stack frames (don't check specific function names as they may vary)
	for _, line := range stack {
		assert.Contains(t, line, ":", "Stack line should contain file:line format")
	}
}

func TestIsErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     ErrorCode
		expected bool
	}{
		{
			name:     "exact match",
			err:      NewError(ErrCodeInvalidParam, "test", nil),
			code:     ErrCodeInvalidParam,
			expected: true,
		},
		{
			name:     "no match",
			err:      NewError(ErrCodeInvalidParam, "test", nil),
			code:     ErrCodeDSNotFound,
			expected: false,
		},
		{
			name:     "non-api error",
			err:      errors.New("plain error"),
			code:     ErrCodeInvalidParam,
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			code:     ErrCodeInvalidParam,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsErrorCode(tt.err, tt.code)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected ErrorCode
	}{
		{
			name:     "api error",
			err:      NewError(ErrCodeInvalidParam, "test", nil),
			expected: ErrCodeInvalidParam,
		},
		{
			name:     "non-api error",
			err:      errors.New("plain error"),
			expected: "",
		},
		{
			name:     "nil error",
			err:      nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetErrorCode(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "api error",
			err:      NewError(ErrCodeInvalidParam, "test message", nil),
			expected: "[INVALID_PARAM] test message",
		},
		{
			name:     "non-api error",
			err:      errors.New("plain error message"),
			expected: "plain error message",
		},
		{
			name:     "nil error",
			err:      nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetErrorMessage(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestErrorCodeValues(t *testing.T) {
	// Ensure all error codes have non-empty string values
	codes := []ErrorCode{
		ErrCodeDSNotFound,
		ErrCodeDSAlreadyExists,
		ErrCodeTableNotFound,
		ErrCodeColumnNotFound,
		ErrCodeSyntax,
		ErrCodeConstraint,
		ErrCodeTransaction,
		ErrCodeTimeout,
		ErrCodeInvalidParam,
		ErrCodeNotSupported,
		ErrCodeClosed,
		ErrCodeInternal,
	}

	for _, code := range codes {
		assert.NotEmpty(t, string(code), "Error code should not be empty")
	}
}

func TestErrorWithCause(t *testing.T) {
	causeErr := errors.New("underlying error")
	apiErr := NewError(ErrCodeInternal, "wrapped error", causeErr)

	// Check that original error is accessible
	assert.Equal(t, causeErr, apiErr.Cause)
	
	// Check that errors.Is works
	assert.True(t, errors.Is(apiErr, causeErr))
}

func ExampleError() {
	err := NewError(ErrCodeInvalidParam, "invalid parameter", nil)
	fmt.Println(err.Error())
	// Output: [INVALID_PARAM] invalid parameter
}

func ExampleWrapError() {
	originalErr := errors.New("connection failed")
	wrappedErr := WrapError(originalErr, ErrCodeInternal, "database error")
	fmt.Println(wrappedErr.Error())
	// Output: [INTERNAL] database error: connection failed
}
