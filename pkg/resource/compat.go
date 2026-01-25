package resource

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/infrastructure/cache"
	"github.com/kasuganosora/sqlexec/pkg/resource/infrastructure/errors"
	"github.com/kasuganosora/sqlexec/pkg/resource/infrastructure/pool"
	resourcememory "github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== 向后兼容层 ====================
// 这个文件提供向后兼容性，允许现有代码继续使用旧的 API

// ==================== 重新导出类型和常量 ====================

// 重新导出 DataSource
type DataSource = domain.DataSource

// 重新导出 DataSourceConfig
type DataSourceConfig = domain.DataSourceConfig

// 重新导出 DataSourceFactory
type DataSourceFactory = domain.DataSourceFactory

// 重新导出 DataSourceType
type DataSourceType = domain.DataSourceType

// 重新导出数据源类型常量
const (
	DataSourceTypeMemory       = domain.DataSourceTypeMemory
	DataSourceTypeMySQL        = domain.DataSourceTypeMySQL
	DataSourceTypePostgreSQL  = domain.DataSourceTypePostgreSQL
	DataSourceTypeSQLite       = domain.DataSourceTypeSQLite
	DataSourceTypeCSV          = domain.DataSourceTypeCSV
	DataSourceTypeExcel        = domain.DataSourceTypeExcel
	DataSourceTypeJSON         = domain.DataSourceTypeJSON
	DataSourceTypeParquet      = domain.DataSourceTypeParquet
)

// 重新导出 TableInfo
type TableInfo = domain.TableInfo

// 重新导出 ColumnInfo
type ColumnInfo = domain.ColumnInfo

// 重新导出 ForeignKeyInfo
type ForeignKeyInfo = domain.ForeignKeyInfo

// 重新导出 Row
type Row = domain.Row

// 重新导出 QueryResult
type QueryResult = domain.QueryResult

// 重新导出 Filter
type Filter = domain.Filter

// 重新导出 QueryOptions
type QueryOptions = domain.QueryOptions

// 重新导出 InsertOptions
type InsertOptions = domain.InsertOptions

// 重新导出 UpdateOptions
type UpdateOptions = domain.UpdateOptions

// 重新导出 DeleteOptions
type DeleteOptions = domain.DeleteOptions

// 重新导出 TransactionOptions
type TransactionOptions = domain.TransactionOptions

// ==================== 重新导出错误函数 ====================

// 重新导出错误函数
func ErrNotConnected() error {
	return domain.NewErrNotConnected("")
}

func ErrReadOnly(dataSourceType string) error {
	return domain.NewErrReadOnly(dataSourceType, "operation")
}

func ErrTableNotFound(tableName string) error {
	return domain.NewErrTableNotFound(tableName)
}

func ErrDataSourceNotFound(dataSourceName string) error {
	return &ErrDataSourceNotFound{Name: dataSourceName}
}

func ErrFileNotFound(filePath, fileType string) error {
	return errors.NewErrFileNotFound(filePath, fileType)
}

func ErrSQLNotSupported(dataSourceType string) error {
	return errors.NewErrSQLNotSupported(dataSourceType)
}

func ErrOperationNotSupported(dataSourceType, operation string) error {
	return errors.NewErrOperationNotSupported(dataSourceType, operation)
}

// ErrDataSourceNotFound 数据源不存在错误（兼容）
type ErrDataSourceNotFound struct {
	Name string
}

func (e *ErrDataSourceNotFound) Error() string {
	return "data source " + e.Name + " not found"
}

// ==================== 重新导出工具函数 ====================

// 重新导出工具函数
var (
	StartsWith       = util.StartsWith
	EndsWith         = util.EndsWith
	ContainsSimple   = util.ContainsSimple
	Contains         = util.Contains
	ReplaceAll       = util.ReplaceAll
	ContainsTable    = util.ContainsTable
	ContainsWord     = util.ContainsWord
	SplitLines       = util.SplitLines
	JoinWith         = util.JoinWith
	CompareEqual     = util.CompareEqual
	CompareNumeric   = util.CompareNumeric
	CompareGreater   = util.CompareGreater
	CompareLike      = util.CompareLike
	CompareIn        = util.CompareIn
	CompareBetween   = util.CompareBetween
	CompareValues    = util.CompareValues
	ConvertToFloat64 = util.ConvertToFloat64
	ApplyFilters     = util.ApplyFilters
	MatchesFilters   = util.MatchesFilters
	MatchFilter      = util.MatchFilter
	ApplyOrder       = util.ApplyOrder
	ApplyPagination  = util.ApplyPagination
	JoinConditions   = util.JoinConditions
)

// ==================== 重新导出工厂注册函数 ====================

// RegisterFactory 注册数据源工厂
func RegisterFactory(factory DataSourceFactory) {
	application.RegisterFactory(factory)
}

// CreateDataSource 创建数据源
func CreateDataSource(config *DataSourceConfig) (DataSource, error) {
	return application.CreateDataSource(config)
}

// GetSupportedTypes 获取支持的数据源类型
func GetSupportedTypes() []DataSourceType {
	return application.GetSupportedTypes()
}

// ==================== 重新导出数据源管理器 ====================

// DataSourceManager 数据源管理器（兼容）
type DataSourceManager = application.DataSourceManager

// NewDataSourceManager 创建数据源管理器
func NewDataSourceManager() *DataSourceManager {
	return application.NewDataSourceManager()
}

// GetDefaultManager 获取默认数据源管理器
func GetDefaultManager() *DataSourceManager {
	return application.GetDefaultManager()
}

// ==================== 重新导出缓存和连接池 ====================

// StatementCache 语句缓存（兼容）
type StatementCache = sql.StatementCache

// NewStatementCache 创建语句缓存
func NewStatementCache() *StatementCache {
	return sql.NewStatementCache()
}

// QueryCache 查询缓存（兼容）
type QueryCache = cache.QueryCache

// NewQueryCache 创建查询缓存
func NewQueryCache() *QueryCache {
	return cache.NewQueryCache()
}

// ConnectionPool 连接池（兼容）
type ConnectionPool = pool.ConnectionPool

// NewConnectionPool 创建连接池
func NewConnectionPool() *ConnectionPool {
	return pool.NewConnectionPool()
}

// SlowQueryLogger 慢查询日志器（兼容）
type SlowQueryLogger = sql.SlowQueryLogger

// NewSlowQueryLogger 创建慢查询日志器
func NewSlowQueryLogger() *SlowQueryLogger {
	return sql.NewSlowQueryLogger()
}

// ==================== 重新导出索引相关 ====================

// IndexType 索引类型（兼容）
type IndexType = resourcememory.IndexType

const (
	IndexTypeHash  = resourcememory.IndexTypeHash
	IndexTypeBTree = resourcememory.IndexTypeBTree
	IndexTypeSkip  = resourcememory.IndexTypeSkip
)

// IndexInfo 索引信息（兼容）
type IndexInfo = resourcememory.IndexInfo

// ==================== 初始化函数 ====================

// init 初始化兼容层
func init() {
	// 确保全局注册表已初始化
	_ = application.GetRegistry()
	_ = application.GetDefaultManager()
}
