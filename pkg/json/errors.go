package json

import "fmt"

// JSONErrorCode represents JSON error codes
type JSONErrorCode int

const (
	ErrInvalidJSON JSONErrorCode = iota + 1001
	ErrInvalidType
	ErrTypeMismatch
	ErrInvalidPath
	ErrPathNotFound
	ErrPathExists
	ErrInvalidIndex
	ErrInvalidKey
	ErrOverflow
	ErrDivisionByZero
	ErrValueNotFound
	ErrInvalidParam
)

// JSONError represents a JSON operation error
type JSONError struct {
	Code    JSONErrorCode
	Message string
	Path    string // Optional: path where error occurred
}

// Error implements the error interface
func (e *JSONError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("JSON Error (code %d): %s (path: %s)", e.Code, e.Message, e.Path)
	}
	return fmt.Sprintf("JSON Error (code %d): %s", e.Code, e.Message)
}

// NewInvalidJSONError creates a new invalid JSON error
func NewInvalidJSONError(msg string) *JSONError {
	return &JSONError{Code: ErrInvalidJSON, Message: msg}
}

// NewTypeError creates a new type mismatch error
func NewTypeError(msg string) *JSONError {
	return &JSONError{Code: ErrTypeMismatch, Message: msg}
}

// NewPathError creates a new invalid path error
func NewPathError(path string, msg string) *JSONError {
	return &JSONError{Code: ErrInvalidPath, Path: path, Message: msg}
}

// NewNotFoundError creates a new path not found error
func NewNotFoundError(path string) *JSONError {
	return &JSONError{Code: ErrPathNotFound, Path: path, Message: "path not found"}
}

// NewIndexError creates a new invalid index error
func NewIndexError(index int) *JSONError {
	return &JSONError{Code: ErrInvalidIndex, Message: fmt.Sprintf("invalid array index: %d", index)}
}

// NewKeyError creates a new invalid key error
func NewKeyError(key string) *JSONError {
	return &JSONError{Code: ErrInvalidKey, Message: fmt.Sprintf("invalid object key: %s", key)}
}

// WrapError wraps an error with JSON context
func WrapError(err error, path string, message string) *JSONError {
	if err == nil {
		return nil
	}
	return &JSONError{
		Code:    ErrInvalidParam,
		Path:    path,
		Message: fmt.Sprintf("%s: %v", message, err),
	}
}
