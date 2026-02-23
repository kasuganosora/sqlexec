package api

import (
	"fmt"
	"runtime"
	"strings"
)

// Error 错误类型（带堆栈）
type Error struct {
	Code    ErrorCode
	Message string
	Stack   []string // 调用堆栈
	Cause   error    // 原始错误
}

// ErrorCode 错误码
type ErrorCode string

const (
	ErrCodeDSNotFound      ErrorCode = "DS_NOT_FOUND"
	ErrCodeDSAlreadyExists ErrorCode = "DS_ALREADY_EXISTS"
	ErrCodeTableNotFound   ErrorCode = "TABLE_NOT_FOUND"
	ErrCodeColumnNotFound  ErrorCode = "COLUMN_NOT_FOUND"
	ErrCodeSyntax          ErrorCode = "SYNTAX_ERROR"
	ErrCodeConstraint      ErrorCode = "CONSTRAINT"
	ErrCodeTransaction     ErrorCode = "TRANSACTION"
	ErrCodeTimeout         ErrorCode = "TIMEOUT"
	ErrCodeQueryKilled     ErrorCode = "QUERY_KILLED"
	ErrCodeInvalidParam    ErrorCode = "INVALID_PARAM"
	ErrCodeNotSupported    ErrorCode = "NOT_SUPPORTED"
	ErrCodeClosed          ErrorCode = "CLOSED"
	ErrCodeInternal        ErrorCode = "INTERNAL"
)

// Error 接口实现
func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap 返回原始错误
func (e *Error) Unwrap() error {
	return e.Cause
}

// StackTrace 返回调用堆栈
func (e *Error) StackTrace() []string {
	return e.Stack
}

// NewError 创建错误
func NewError(code ErrorCode, message string, cause error) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Stack:   captureStackTrace(),
		Cause:   cause,
	}
}

// WrapError 包装错误
func WrapError(err error, code ErrorCode, message string) *Error {
	if err == nil {
		return nil
	}

	// 如果已经是我们的错误类型，保留原有堆栈
	if apiErr, ok := err.(*Error); ok {
		return &Error{
			Code:    code,
			Message: message,
			Stack:   apiErr.Stack,
			Cause:   apiErr,
		}
	}

	return &Error{
		Code:    code,
		Message: message,
		Stack:   captureStackTrace(),
		Cause:   err,
	}
}

// captureStackTrace 捕获调用堆栈
func captureStackTrace() []string {
	pc := make([]uintptr, 32)
	n := runtime.Callers(3, pc) // 跳过前3层

	if n == 0 {
		return []string{}
	}

	frames := runtime.CallersFrames(pc[:n])
	stack := make([]string, 0, n)

	for {
		frame, more := frames.Next()
		if !more {
			break
		}

		// 格式化堆栈信息
		fn := frame.Function
		file := frame.File
		line := frame.Line

		// 简化文件路径
		if idx := strings.LastIndex(file, "/"); idx != -1 {
			file = file[idx+1:]
		}

		// 提取函数名（去掉包路径）
		if idx := strings.LastIndex(fn, "/"); idx != -1 {
			fn = fn[idx+1:]
		}

		stack = append(stack, fmt.Sprintf("  at %s (%s:%d)", fn, file, line))
	}

	return stack
}

// IsErrorCode 检查错误码
func IsErrorCode(err error, code ErrorCode) bool {
	if err == nil {
		return false
	}

	if apiErr, ok := err.(*Error); ok {
		return apiErr.Code == code
	}

	return false
}

// GetErrorCode 获取错误码
func GetErrorCode(err error) ErrorCode {
	if err == nil {
		return ""
	}

	if apiErr, ok := err.(*Error); ok {
		return apiErr.Code
	}

	return ""
}

// GetErrorMessage 获取错误消息
func GetErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}
