package resource

import "fmt"

// ==================== 数据源错误消息 ====================

// ErrNotConnected 数据源未连接错误
func ErrNotConnected() error {
	return fmt.Errorf("not connected")
}

// ErrReadOnly 数据源只读错误
func ErrReadOnly(dataSourceType string) error {
	return fmt.Errorf("%s data source is read-only", dataSourceType)
}

// ErrTableNotFound 表不存在错误
func ErrTableNotFound(tableName string) error {
	return fmt.Errorf("table %s not found", tableName)
}

// ErrDataSourceNotFound 数据源不存在错误
func ErrDataSourceNotFound(dataSourceName string) error {
	return fmt.Errorf("data source %s not found", dataSourceName)
}

// ErrFileNotFound 文件不存在错误
func ErrFileNotFound(filePath, fileType string) error {
	return fmt.Errorf("%s file not found: %s", fileType, filePath)
}

// ErrSQLNotSupported SQL执行不支持错误
func ErrSQLNotSupported(dataSourceType string) error {
	return fmt.Errorf("%s data source does not support SQL execution", dataSourceType)
}

// ErrOperationNotSupported 操作不支持错误
func ErrOperationNotSupported(dataSourceType, operation string) error {
	return fmt.Errorf("%s not supported for %s data source", operation, dataSourceType)
}



