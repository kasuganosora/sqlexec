package domain

import (
	"strings"
	"testing"
)

// TestErrNotConnected_Error 测试ErrNotConnected的Error方法
func TestErrNotConnected_Error(t *testing.T) {
	err := &ErrNotConnected{
		DataSourceType: "mysql",
	}
	errMsg := err.Error()

	expected := "data source mysql is not connected"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "mysql") {
		t.Errorf("Expected error message to contain 'mysql'")
	}
	if !strings.Contains(errMsg, "not connected") {
		t.Errorf("Expected error message to contain 'not connected'")
	}
}

// TestErrReadOnly_Error 测试ErrReadOnly的Error方法
func TestErrReadOnly_Error(t *testing.T) {
	err := &ErrReadOnly{
		DataSourceType: "csv",
		Operation:      "insert",
	}
	errMsg := err.Error()

	expected := "data source csv is read-only, cannot insert"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "csv") {
		t.Errorf("Expected error message to contain 'csv'")
	}
	if !strings.Contains(errMsg, "read-only") {
		t.Errorf("Expected error message to contain 'read-only'")
	}
	if !strings.Contains(errMsg, "insert") {
		t.Errorf("Expected error message to contain 'insert'")
	}
}

// TestErrTableNotFound_Error 测试ErrTableNotFound的Error方法
func TestErrTableNotFound_Error(t *testing.T) {
	err := &ErrTableNotFound{
		TableName: "nonexistent_table",
	}
	errMsg := err.Error()

	expected := "table nonexistent_table not found"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "nonexistent_table") {
		t.Errorf("Expected error message to contain table name")
	}
	if !strings.Contains(errMsg, "not found") {
		t.Errorf("Expected error message to contain 'not found'")
	}
}

// TestErrColumnNotFound_Error 测试ErrColumnNotFound的Error方法
func TestErrColumnNotFound_Error(t *testing.T) {
	err := &ErrColumnNotFound{
		ColumnName: "invalid_column",
		TableName:  "users",
	}
	errMsg := err.Error()

	expected := "column invalid_column not found in table users"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "invalid_column") {
		t.Errorf("Expected error message to contain column name")
	}
	if !strings.Contains(errMsg, "users") {
		t.Errorf("Expected error message to contain table name")
	}
	if !strings.Contains(errMsg, "not found") {
		t.Errorf("Expected error message to contain 'not found'")
	}
}

// TestErrUnsupportedOperation_Error 测试ErrUnsupportedOperation的Error方法
func TestErrUnsupportedOperation_Error(t *testing.T) {
	err := &ErrUnsupportedOperation{
		DataSourceType: "json",
		Operation:      "transaction",
	}
	errMsg := err.Error()

	expected := "operation transaction is not supported by json data source"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "json") {
		t.Errorf("Expected error message to contain data source type")
	}
	if !strings.Contains(errMsg, "transaction") {
		t.Errorf("Expected error message to contain operation")
	}
	if !strings.Contains(errMsg, "not supported") {
		t.Errorf("Expected error message to contain 'not supported'")
	}
}

// TestErrConstraintViolation_Error 测试ErrConstraintViolation的Error方法
func TestErrConstraintViolation_Error(t *testing.T) {
	err := &ErrConstraintViolation{
		Constraint: "unique_email",
		Message:    "email 'test@example.com' already exists",
	}
	errMsg := err.Error()

	expected := "constraint violation: unique_email - email 'test@example.com' already exists"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "unique_email") {
		t.Errorf("Expected error message to contain constraint name")
	}
	if !strings.Contains(errMsg, "constraint violation") {
		t.Errorf("Expected error message to contain 'constraint violation'")
	}
}

// TestErrInvalidConfig_Error 测试ErrInvalidConfig的Error方法
func TestErrInvalidConfig_Error(t *testing.T) {
	err := &ErrInvalidConfig{
		ConfigKey: "host",
		Message:   "host cannot be empty",
	}
	errMsg := err.Error()

	expected := "invalid config for host: host cannot be empty"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "host") {
		t.Errorf("Expected error message to contain config key")
	}
	if !strings.Contains(errMsg, "invalid config") {
		t.Errorf("Expected error message to contain 'invalid config'")
	}
}

// TestErrConnectionFailed_Error 测试ErrConnectionFailed的Error方法
func TestErrConnectionFailed_Error(t *testing.T) {
	err := &ErrConnectionFailed{
		DataSourceType: "mysql",
		Reason:         "connection timeout",
	}
	errMsg := err.Error()

	expected := "failed to connect to mysql data source: connection timeout"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "mysql") {
		t.Errorf("Expected error message to contain data source type")
	}
	if !strings.Contains(errMsg, "failed to connect") {
		t.Errorf("Expected error message to contain 'failed to connect'")
	}
	if !strings.Contains(errMsg, "connection timeout") {
		t.Errorf("Expected error message to contain reason")
	}
}

// TestErrQueryFailed_Error 测试ErrQueryFailed的Error方法
func TestErrQueryFailed_Error(t *testing.T) {
	err := &ErrQueryFailed{
		Query:  "SELECT * FROM users WHERE id = 'abc'",
		Reason: "type mismatch: id is int64",
	}
	errMsg := err.Error()

	expected := "query failed: SELECT * FROM users WHERE id = 'abc' - type mismatch: id is int64"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "SELECT * FROM users WHERE id = 'abc'") {
		t.Errorf("Expected error message to contain query")
	}
	if !strings.Contains(errMsg, "type mismatch") {
		t.Errorf("Expected error message to contain reason")
	}
	if !strings.Contains(errMsg, "query failed") {
		t.Errorf("Expected error message to contain 'query failed'")
	}
}

// TestErrTypeConversion_Error 测试ErrTypeConversion的Error方法
func TestErrTypeConversion_Error(t *testing.T) {
	err := &ErrTypeConversion{
		FieldName: "age",
		FromType:  "string",
		ToType:    "int64",
		Value:     "not a number",
	}
	errMsg := err.Error()

	expected := "type conversion failed for field age: cannot convert not a number from string to int64"
	if errMsg != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, errMsg)
	}

	if !strings.Contains(errMsg, "age") {
		t.Errorf("Expected error message to contain field name")
	}
	if !strings.Contains(errMsg, "string") {
		t.Errorf("Expected error message to contain from type")
	}
	if !strings.Contains(errMsg, "int64") {
		t.Errorf("Expected error message to contain to type")
	}
	if !strings.Contains(errMsg, "type conversion failed") {
		t.Errorf("Expected error message to contain 'type conversion failed'")
	}
}

// TestNewErrNotConnected 测试NewErrNotConnected辅助函数
func TestNewErrNotConnected(t *testing.T) {
	err := error(NewErrNotConnected("postgresql"))

	if err.Error() != "data source postgresql is not connected" {
		t.Errorf("Unexpected error message: %v", err.Error())
	}

	// 验证error类型
	if _, ok := err.(*ErrNotConnected); !ok {
		t.Errorf("Expected error type *ErrNotConnected")
	}
}

// TestNewErrReadOnly 测试NewErrReadOnly辅助函数
func TestNewErrReadOnly(t *testing.T) {
	err := error(NewErrReadOnly("csv", "update"))

	if err.Error() != "data source csv is read-only, cannot update" {
		t.Errorf("Unexpected error message: %v", err.Error())
	}

	// 验证error类型
	if _, ok := err.(*ErrReadOnly); !ok {
		t.Errorf("Expected error type *ErrReadOnly")
	}
}

// TestNewErrTableNotFound 测试NewErrTableNotFound辅助函数
func TestNewErrTableNotFound(t *testing.T) {
	err := error(NewErrTableNotFound("products"))

	if err.Error() != "table products not found" {
		t.Errorf("Unexpected error message: %v", err.Error())
	}

	// 验证error类型
	if _, ok := err.(*ErrTableNotFound); !ok {
		t.Errorf("Expected error type *ErrTableNotFound")
	}
}

// TestNewErrUnsupportedOperation 测试NewErrUnsupportedOperation辅助函数
func TestNewErrUnsupportedOperation(t *testing.T) {
	err := error(NewErrUnsupportedOperation("parquet", "insert"))

	if err.Error() != "operation insert is not supported by parquet data source" {
		t.Errorf("Unexpected error message: %v", err.Error())
	}

	// 验证error类型
	if _, ok := err.(*ErrUnsupportedOperation); !ok {
		t.Errorf("Expected error type *ErrUnsupportedOperation")
	}
}

// TestNewErrConstraintViolation 测试NewErrConstraintViolation辅助函数
func TestNewErrConstraintViolation(t *testing.T) {
	err := error(NewErrConstraintViolation("primary_key", "primary key must be unique"))

	if err.Error() != "constraint violation: primary_key - primary key must be unique" {
		t.Errorf("Unexpected error message: %v", err.Error())
	}

	// 验证error类型
	if _, ok := err.(*ErrConstraintViolation); !ok {
		t.Errorf("Expected error type *ErrConstraintViolation")
	}
}

// TestErrorInterfaces 测试错误类型实现error接口
func TestErrorInterfaces(t *testing.T) {
	errors := []error{
		&ErrNotConnected{DataSourceType: "test"},
		&ErrReadOnly{DataSourceType: "test", Operation: "op"},
		&ErrTableNotFound{TableName: "table"},
		&ErrColumnNotFound{ColumnName: "col", TableName: "table"},
		&ErrUnsupportedOperation{DataSourceType: "test", Operation: "op"},
		&ErrConstraintViolation{Constraint: "constraint", Message: "message"},
		&ErrInvalidConfig{ConfigKey: "key", Message: "message"},
		&ErrConnectionFailed{DataSourceType: "test", Reason: "reason"},
		&ErrQueryFailed{Query: "SELECT", Reason: "reason"},
		&ErrTypeConversion{FieldName: "field", FromType: "string", ToType: "int", Value: "val"},
	}

	for i, err := range errors {
		if err.Error() == "" {
			t.Errorf("Error %d returned empty message", i)
		}

	// 测试error不为nil
	if err == nil {
		t.Errorf("Error %d should not be nil", i)
	}
	}
}

// TestErrorMessageFormats 测试错误消息格式的一致性
func TestErrorMessageFormats(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		contains []string
	}{
		{
			name: "ErrNotConnected",
			err: &ErrNotConnected{DataSourceType: "mysql"},
			contains: []string{"data source", "mysql", "not connected"},
		},
		{
			name: "ErrReadOnly",
			err: &ErrReadOnly{DataSourceType: "csv", Operation: "insert"},
			contains: []string{"data source", "csv", "read-only", "insert"},
		},
		{
			name: "ErrTableNotFound",
			err: &ErrTableNotFound{TableName: "users"},
			contains: []string{"table", "users", "not found"},
		},
		{
			name: "ErrColumnNotFound",
			err: &ErrColumnNotFound{ColumnName: "id", TableName: "products"},
			contains: []string{"column", "id", "table", "products", "not found"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errMsg := tt.err.Error()
			for _, substr := range tt.contains {
				if !strings.Contains(errMsg, substr) {
					t.Errorf("Error message '%s' should contain '%s'", errMsg, substr)
				}
			}
		})
	}
}
