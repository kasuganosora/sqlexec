package utils

import (
	"context"
	"errors"
	"strings"
)

// MySQL错误码常量定义
const (
	// Table errors
	ErrNoSuchTable   = 1146 // ER_NO_SUCH_TABLE
	ErrBadFieldError = 1054 // ER_BAD_FIELD_ERROR

	// Query errors
	ErrParseError  = 1064 // ER_PARSE_ERROR
	ErrEmptyQuery  = 1065 // ER_EMPTY_QUERY
	ErrInterrupted = 1317 // ER_QUERY_INTERRUPTED

	// SQL状态码
	SqlStateNoSuchTable   = "42S02" // Table does not exist
	SqlStateBadFieldError = "42S22" // Column does not exist
	SqlStateSyntaxError   = "42000" // Syntax error or access violation
	SqlStateUnknownError  = "HY000" // General error
)

// MapErrorCode 将错误映射到MySQL错误码和SQL状态码
// 返回 (errorCode, sqlState)
func MapErrorCode(err error) (uint16, string) {
	if err == nil {
		return ErrParseError, SqlStateSyntaxError
	}

	// Check error message content (case-insensitive)
	errMsg := strings.ToLower(err.Error())

	// Check for column first (more specific)
	if strings.Contains(errMsg, "column") && strings.Contains(errMsg, "not found") {
		return ErrBadFieldError, SqlStateBadFieldError
	}

	// Then check for table
	if strings.Contains(errMsg, "table") && strings.Contains(errMsg, "not found") {
		return ErrNoSuchTable, SqlStateNoSuchTable
	}

	// Syntax error
	if strings.Contains(errMsg, "syntax") || strings.Contains(errMsg, "parse") {
		return ErrParseError, SqlStateSyntaxError
	}

	// Empty query
	if strings.Contains(errMsg, "no statements found") || strings.Contains(errMsg, "empty query") {
		return ErrEmptyQuery, SqlStateSyntaxError
	}

	// Timeout or cancellation
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrInterrupted, SqlStateUnknownError
	}

	// Default to syntax error
	return ErrParseError, SqlStateSyntaxError
}
