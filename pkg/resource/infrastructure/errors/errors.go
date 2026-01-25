package infrastructure

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"

// ==================== 基础设施错误 ====================

// ErrFileNotFound 文件不存在错误
type ErrFileNotFound struct {
	FilePath string
	FileType string
}

func (e *ErrFileNotFound) Error() string {
	return e.FileType + " file not found: " + e.FilePath
}

// ErrSQLNotSupported SQL执行不支持错误
type ErrSQLNotSupported struct {
	DataSourceType string
}

func (e *ErrSQLNotSupported) Error() string {
	return e.DataSourceType + " data source does not support SQL execution"
}

// ErrOperationNotSupported 操作不支持错误
type ErrOperationNotSupported struct {
	DataSourceType string
	Operation      string
}

func (e *ErrOperationNotSupported) Error() string {
	return e.Operation + " not supported for " + e.DataSourceType + " data source"
}

// ErrPoolExhausted 连接池耗尽错误
type ErrPoolExhausted struct {
	Message string
}

func (e *ErrPoolExhausted) Error() string {
	return "connection pool exhausted: " + e.Message
}

// ErrCacheMiss 缓存未命中错误
type ErrCacheMiss struct {
	Key string
}

func (e *ErrCacheMiss) Error() string {
	return "cache miss for key: " + e.Key
}

// ErrTypeConversion 类型转换错误
type ErrTypeConversion struct {
	FieldName string
	FromType  string
	ToType    string
	Value     interface{}
}

func (e *ErrTypeConversion) Error() string {
	return "type conversion failed for field " + e.FieldName +
		": cannot convert " + e.FromType + " to " + e.ToType
}

// 辅助函数

// NewErrFileNotFound 创建文件不存在错误
func NewErrFileNotFound(filePath, fileType string) *ErrFileNotFound {
	return &ErrFileNotFound{FilePath: filePath, FileType: fileType}
}

// NewErrSQLNotSupported 创建SQL不支持错误
func NewErrSQLNotSupported(dataSourceType string) *ErrSQLNotSupported {
	return &ErrSQLNotSupported{DataSourceType: dataSourceType}
}

// NewErrOperationNotSupported 创建操作不支持错误
func NewErrOperationNotSupported(dataSourceType, operation string) *ErrOperationNotSupported {
	return &ErrOperationNotSupported{
		DataSourceType: dataSourceType,
		Operation:      operation,
	}
}

// WrapDomainError 包装领域错误为基础设施错误
func WrapDomainError(err error) error {
	switch e := err.(type) {
	case *domain.ErrNotConnected:
		return e
	case *domain.ErrReadOnly:
		return e
	case *domain.ErrTableNotFound:
		return e
	case *domain.ErrUnsupportedOperation:
		return e
	case *domain.ErrConstraintViolation:
		return e
	case *domain.ErrInvalidConfig:
		return e
	default:
		return err
	}
}
