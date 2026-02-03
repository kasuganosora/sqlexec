package utils

import (
	"context"
	"errors"
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

	// 检查错误消息内容
	errMsg := err.Error()

	// 表不存在
	if ContainsSubstring(errMsg, "table") && ContainsSubstring(errMsg, "not found") {
		return ErrNoSuchTable, SqlStateNoSuchTable
	}

	// 列不存在
	if ContainsSubstring(errMsg, "column") && ContainsSubstring(errMsg, "not found") {
		return ErrBadFieldError, SqlStateBadFieldError
	}

	// 语法错误
	if ContainsSubstring(errMsg, "syntax") || ContainsSubstring(errMsg, "SYNTAX_ERROR") || ContainsSubstring(errMsg, "parse") {
		return ErrParseError, SqlStateSyntaxError
	}

	// 空查询
	if ContainsSubstring(errMsg, "no statements found") || ContainsSubstring(errMsg, "empty query") {
		return ErrEmptyQuery, SqlStateSyntaxError
	}

	// 超时或取消
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrInterrupted, SqlStateUnknownError
	}

	// 默认语法错误
	return ErrParseError, SqlStateSyntaxError
}
