package domain

import "fmt"

// 数据源领域错误

// ErrNotConnected 未连接错误
type ErrNotConnected struct {
	DataSourceType string
}

func (e *ErrNotConnected) Error() string {
	return fmt.Sprintf("data source %s is not connected", e.DataSourceType)
}

// ErrReadOnly 只读错误
type ErrReadOnly struct {
	DataSourceType string
	Operation      string
}

func (e *ErrReadOnly) Error() string {
	return fmt.Sprintf("data source %s is read-only, cannot %s", e.DataSourceType, e.Operation)
}

// ErrTableNotFound 表不存在错误
type ErrTableNotFound struct {
	TableName string
}

func (e *ErrTableNotFound) Error() string {
	return fmt.Sprintf("table %s not found", e.TableName)
}

// ErrColumnNotFound 列不存在错误
type ErrColumnNotFound struct {
	ColumnName string
	TableName  string
}

func (e *ErrColumnNotFound) Error() string {
	return fmt.Sprintf("column %s not found in table %s", e.ColumnName, e.TableName)
}

// ErrUnsupportedOperation 不支持的操作错误
type ErrUnsupportedOperation struct {
	DataSourceType string
	Operation      string
}

func (e *ErrUnsupportedOperation) Error() string {
	return fmt.Sprintf("operation %s is not supported by %s data source", e.Operation, e.DataSourceType)
}

// ErrConstraintViolation 约束违反错误
type ErrConstraintViolation struct {
	Constraint string
	Message    string
}

func (e *ErrConstraintViolation) Error() string {
	return fmt.Sprintf("constraint violation: %s - %s", e.Constraint, e.Message)
}

// ErrInvalidConfig 配置无效错误
type ErrInvalidConfig struct {
	ConfigKey string
	Message   string
}

func (e *ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid config for %s: %s", e.ConfigKey, e.Message)
}

// ErrConnectionFailed 连接失败错误
type ErrConnectionFailed struct {
	DataSourceType string
	Reason         string
}

func (e *ErrConnectionFailed) Error() string {
	return fmt.Sprintf("failed to connect to %s data source: %s", e.DataSourceType, e.Reason)
}

// ErrQueryFailed 查询失败错误
type ErrQueryFailed struct {
	Query   string
	Reason  string
}

func (e *ErrQueryFailed) Error() string {
	return fmt.Sprintf("query failed: %s - %s", e.Query, e.Reason)
}

// ErrTypeConversion 类型转换错误
type ErrTypeConversion struct {
	FieldName  string
	FromType   string
	ToType     string
	Value      interface{}
}

func (e *ErrTypeConversion) Error() string {
	return fmt.Sprintf("type conversion failed for field %s: cannot convert %v from %s to %s",
		e.FieldName, e.Value, e.FromType, e.ToType)
}

// 辅助函数

// NewErrNotConnected 创建未连接错误
func NewErrNotConnected(dataSourceType string) *ErrNotConnected {
	return &ErrNotConnected{DataSourceType: dataSourceType}
}

// NewErrReadOnly 创建只读错误
func NewErrReadOnly(dataSourceType, operation string) *ErrReadOnly {
	return &ErrReadOnly{DataSourceType: dataSourceType, Operation: operation}
}

// NewErrTableNotFound 创建表不存在错误
func NewErrTableNotFound(tableName string) *ErrTableNotFound {
	return &ErrTableNotFound{TableName: tableName}
}

// NewErrUnsupportedOperation 创建不支持操作错误
func NewErrUnsupportedOperation(dataSourceType, operation string) *ErrUnsupportedOperation {
	return &ErrUnsupportedOperation{DataSourceType: dataSourceType, Operation: operation}
}

// NewErrConstraintViolation 创建约束违反错误
func NewErrConstraintViolation(constraint, message string) *ErrConstraintViolation {
	return &ErrConstraintViolation{Constraint: constraint, Message: message}
}
