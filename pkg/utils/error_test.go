package utils

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMapErrorCode(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		expectedCode  uint16
		expectedState string
	}{
		// nil 错误
		{
			name:          "nil错误",
			err:           nil,
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		// 表不存在错误
		{
			name:          "表不存在",
			err:           errors.New("table 'test' not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "Table not found大写",
			err:           errors.New("Table 'users' not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "table关键字后跟not found",
			err:           errors.New("Unknown table 'test' not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "not found后跟table",
			err:           errors.New("not found: table test"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		// 列不存在错误
		{
			name:          "列不存在",
			err:           errors.New("column 'id' not found"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "Column not found大写",
			err:           errors.New("Column 'name' not found"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "column关键字后跟not found",
			err:           errors.New("Unknown column 'age' not found"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "not found后跟column",
			err:           errors.New("not found: column test"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		// 语法错误
		{
			name:          "语法错误小写",
			err:           errors.New("syntax error near 'from'"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "语法错误大写",
			err:           errors.New("SYNTAX_ERROR in query"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "parse错误",
			err:           errors.New("failed to parse query"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "Parse大写",
			err:           errors.New("Parse error in SQL statement"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		// 空查询错误
		{
			name:          "空查询",
			err:           errors.New("no statements found"),
			expectedCode:  ErrEmptyQuery,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "empty query",
			err:           errors.New("empty query"),
			expectedCode:  ErrEmptyQuery,
			expectedState: SqlStateSyntaxError,
		},
		// 超时和取消错误
		{
			name:          "超时错误",
			err:           context.DeadlineExceeded,
			expectedCode:  ErrInterrupted,
			expectedState: SqlStateUnknownError,
		},
		{
			name:          "取消错误",
			err:           context.Canceled,
			expectedCode:  ErrInterrupted,
			expectedState: SqlStateUnknownError,
		},
		// 默认语法错误
		{
			name:          "其他错误",
			err:           errors.New("some other error"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "网络错误",
			err:           errors.New("connection refused"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "权限错误",
			err:           errors.New("access denied"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, state := MapErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("MapErrorCode(%v) code = %d, want %d", tt.err, code, tt.expectedCode)
			}
			if state != tt.expectedState {
				t.Errorf("MapErrorCode(%v) state = %q, want %q", tt.err, state, tt.expectedState)
			}
		})
	}
}

func TestMapErrorCodeWithWrappedErrors(t *testing.T) {
	// 测试包装的错误
	tests := []struct {
		name          string
		err           error
		expectedCode  uint16
		expectedState string
	}{
		{
			name:          "包装的表不存在",
			err:           errors.New("db error: table test not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "包装的列不存在",
			err:           errors.New("query failed: column id not found"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "包装的语法错误",
			err:           errors.New("parse error: syntax error near from"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "多层包装",
			err:           errors.New("error: db error: table users not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, state := MapErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("MapErrorCode(%v) code = %d, want %d", tt.err, code, tt.expectedCode)
			}
			if state != tt.expectedState {
				t.Errorf("MapErrorCode(%v) state = %q, want %q", tt.err, state, tt.expectedState)
			}
		})
	}
}

func TestMapErrorCodeContextErrors(t *testing.T) {
	// 测试上下文相关错误
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	tests := []struct {
		name          string
		err           error
		expectedCode  uint16
		expectedState string
	}{
		{
			name:          "DeadlineExceeded",
			err:           context.DeadlineExceeded,
			expectedCode:  ErrInterrupted,
			expectedState: SqlStateUnknownError,
		},
		{
			name:          "Canceled",
			err:           context.Canceled,
			expectedCode:  ErrInterrupted,
			expectedState: SqlStateUnknownError,
		},
		{
			name:          "包装的DeadlineExceeded",
			err:           fmt.Errorf("query timed out: %w", context.DeadlineExceeded),
			expectedCode:  ErrInterrupted,
			expectedState: SqlStateUnknownError,
		},
		{
			name:          "包装的Canceled",
			err:           fmt.Errorf("operation canceled: %w", context.Canceled),
			expectedCode:  ErrInterrupted,
			expectedState: SqlStateUnknownError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, state := MapErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("MapErrorCode(%v) code = %d, want %d", tt.err, code, tt.expectedCode)
			}
			if state != tt.expectedState {
				t.Errorf("MapErrorCode(%v) state = %q, want %q", tt.err, state, tt.expectedState)
			}
		})
	}
}

func TestMapErrorCodeCaseInsensitive(t *testing.T) {
	// 测试大小写不敏感的匹配
	tests := []struct {
		name          string
		err           error
		expectedCode  uint16
		expectedState string
	}{
		{
			name:          "TABLE大写",
			err:           errors.New("TABLE test NOT FOUND"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "table混合大小写",
			err:           errors.New("Table Test Not Found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "COLUMN大写",
			err:           errors.New("COLUMN test NOT FOUND"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "SYNTAX_ERROR混合",
			err:           errors.New("Syntax_Error in query"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "parse混合",
			err:           errors.New("Parse Error in statement"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, state := MapErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("MapErrorCode(%v) code = %d, want %d", tt.err, code, tt.expectedCode)
			}
			if state != tt.expectedState {
				t.Errorf("MapErrorCode(%v) state = %q, want %q", tt.err, state, tt.expectedState)
			}
		})
	}
}

func TestMapErrorCodeEdgeCases(t *testing.T) {
	// 测试边界情况
	tests := []struct {
		name          string
		err           error
		expectedCode  uint16
		expectedState string
	}{
		{
			name:          "空错误消息",
			err:           errors.New(""),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "只有空格",
			err:           errors.New("   "),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "只有table",
			err:           errors.New("table"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "只有column",
			err:           errors.New("column"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "只有not found",
			err:           errors.New("not found"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
		{
			name:          "特殊字符",
			err:           errors.New("table !@#$%^&*() not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "Unicode字符",
			err:           errors.New("table 测试 not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "非常长的错误消息",
			err:           errors.New(string(make([]byte, 10000)) + "table test not found"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, state := MapErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("MapErrorCode(%v) code = %d, want %d", tt.err, code, tt.expectedCode)
			}
			if state != tt.expectedState {
				t.Errorf("MapErrorCode(%v) state = %q, want %q", tt.err, state, tt.expectedState)
			}
		})
	}
}

func TestErrorConstants(t *testing.T) {
	// 测试错误常量的值
	tests := []struct {
		name     string
		actual   uint16
		expected uint16
	}{
		{"ErrNoSuchTable", ErrNoSuchTable, 1146},
		{"ErrBadFieldError", ErrBadFieldError, 1054},
		{"ErrParseError", ErrParseError, 1064},
		{"ErrEmptyQuery", ErrEmptyQuery, 1065},
		{"ErrInterrupted", ErrInterrupted, 1317},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("%s = %d, want %d", tt.name, tt.actual, tt.expected)
			}
		})
	}

	// 测试SQL状态码
	sqlStateTests := []struct {
		name     string
		actual   string
		expected string
	}{
		{"SqlStateNoSuchTable", SqlStateNoSuchTable, "42S02"},
		{"SqlStateBadFieldError", SqlStateBadFieldError, "42S22"},
		{"SqlStateSyntaxError", SqlStateSyntaxError, "42000"},
		{"SqlStateUnknownError", SqlStateUnknownError, "HY000"},
	}

	for _, tt := range sqlStateTests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.actual, tt.expected)
			}
		})
	}
}

func TestMapErrorCodeMultipleKeywords(t *testing.T) {
	// 测试包含多个关键词的情况
	tests := []struct {
		name          string
		err           error
		expectedCode  uint16
		expectedState string
	}{
		{
			name:          "table and column both present",
			err:           errors.New("table users and column id not found"),
			expectedCode:  ErrBadFieldError, // column check takes priority
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "column和table都存在",
			err:           errors.New("column id and table users not found"),
			expectedCode:  ErrBadFieldError,
			expectedState: SqlStateBadFieldError,
		},
		{
			name:          "table和syntax都存在",
			err:           errors.New("table users not found with syntax error"),
			expectedCode:  ErrNoSuchTable,
			expectedState: SqlStateNoSuchTable,
		},
		{
			name:          "syntax和table都存在",
			err:           errors.New("syntax error in table users"),
			expectedCode:  ErrParseError,
			expectedState: SqlStateSyntaxError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, state := MapErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("MapErrorCode(%v) code = %d, want %d", tt.err, code, tt.expectedCode)
			}
			if state != tt.expectedState {
				t.Errorf("MapErrorCode(%v) state = %q, want %q", tt.err, state, tt.expectedState)
			}
		})
	}
}

func BenchmarkMapErrorCode(b *testing.B) {
	err := errors.New("table test not found")
	for i := 0; i < b.N; i++ {
		MapErrorCode(err)
	}
}

func BenchmarkMapErrorCodeColumn(b *testing.B) {
	err := errors.New("column test not found")
	for i := 0; i < b.N; i++ {
		MapErrorCode(err)
	}
}

func BenchmarkMapErrorCodeSyntax(b *testing.B) {
	err := errors.New("syntax error near 'from'")
	for i := 0; i < b.N; i++ {
		MapErrorCode(err)
	}
}

func ExampleMapErrorCode() {
	// 表不存在错误
	err := errors.New("table 'users' not found")
	code, state := MapErrorCode(err)
	fmt.Printf("Code: %d, State: %s\n", code, state)

	// 列不存在错误
	err2 := errors.New("column 'name' not found")
	code2, state2 := MapErrorCode(err2)
	fmt.Printf("Code: %d, State: %s\n", code2, state2)

	// 语法错误
	err3 := errors.New("syntax error near 'from'")
	code3, state3 := MapErrorCode(err3)
	fmt.Printf("Code: %d, State: %s\n", code3, state3)

	// 超时错误
	code4, state4 := MapErrorCode(context.DeadlineExceeded)
	fmt.Printf("Code: %d, State: %s\n", code4, state4)

	// Output:
	// Code: 1146, State: 42S02
	// Code: 1054, State: 42S22
	// Code: 1064, State: 42000
	// Code: 1317, State: HY000
}
