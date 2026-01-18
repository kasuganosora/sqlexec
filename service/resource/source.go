package resource

import (
	"context"
	"fmt"
)

// DataSourceType 数据源类�?
type DataSourceType string

const (
	// DataSourceTypeMemory 内存数据�?
	DataSourceTypeMemory DataSourceType = "memory"
	// DataSourceTypeMySQL MySQL数据�?
	DataSourceTypeMySQL DataSourceType = "mysql"
	// DataSourceTypePostgreSQL PostgreSQL数据�?
	DataSourceTypePostgreSQL DataSourceType = "postgresql"
	// DataSourceTypeSQLite SQLite数据�?
	DataSourceTypeSQLite DataSourceType = "sqlite"
	// DataSourceTypeCSV CSV文件数据�?
	DataSourceTypeCSV DataSourceType = "csv"
	// DataSourceTypeJSON JSON文件数据�?
	DataSourceTypeJSON DataSourceType = "json"
	// DataSourceTypeParquet Parquet文件数据�?
	DataSourceTypeParquet DataSourceType = "parquet"
)

// DataSourceConfig 数据源配�?
type DataSourceConfig struct {
	Type     DataSourceType            `json:"type"`
	Name     string                    `json:"name"`
	Host     string                    `json:"host,omitempty"`
	Port     int                       `json:"port,omitempty"`
	Username string                    `json:"username,omitempty"`
	Password string                    `json:"password,omitempty"`
	Database string                    `json:"database,omitempty"`
	Writable  bool                      `json:"writable,omitempty"` // 是否可写，默认true
	Options  map[string]interface{}    `json:"options,omitempty"`
}

// TableInfo 表信�?
type TableInfo struct {
	Name    string         `json:"name"`
	Schema  string         `json:"schema,omitempty"`
	Columns []ColumnInfo   `json:"columns"`
}

// ColumnInfo 列信�?
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Primary  bool   `json:"primary"`
	Default  string `json:"default,omitempty"`
	// 约束支持
	Unique   bool     `json:"unique,omitempty"`     // 唯一约束
	AutoIncrement bool   `json:"auto_increment,omitempty"` // 自动递增
	ForeignKey *ForeignKeyInfo `json:"foreign_key,omitempty"` // 外键约束
}

// ForeignKeyInfo 外键信息
type ForeignKeyInfo struct {
	Table      string `json:"table"`      // 引用的表
	Column     string `json:"column"`     // 引用的列
	OnDelete   string `json:"on_delete,omitempty"`   // 删除策略：CASCADE, SET NULL, NO ACTION
	OnUpdate   string `json:"on_update,omitempty"`   // 更新策略
}

// Row 行数�?
type Row map[string]interface{}

// QueryResult 查询结果
type QueryResult struct {
	Columns []ColumnInfo `json:"columns"`
	Rows    []Row        `json:"rows"`
	Total   int64        `json:"total"`
}

// Filter 查询过滤�?
type Filter struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // =, !=, >, <, >=, <=, LIKE, IN, BETWEEN
	Value    interface{} `json:"value"`
	// 扩展字段
	LogicOp  string      `json:"logic_op,omitempty"` // AND, OR (用于多条件组�?
	SubFilters []Filter    `json:"sub_filters,omitempty"` // 子过滤器（用于逻辑组合�?
}

// QueryOptions 查询选项
type QueryOptions struct {
	Filters     []Filter `json:"filters,omitempty"`
	OrderBy     string   `json:"order_by,omitempty"`
	Order       string   `json:"order,omitempty"` // ASC, DESC
	Limit       int      `json:"limit,omitempty"`
	Offset      int      `json:"offset,omitempty"`
	SelectAll   bool     `json:"select_all,omitempty"`   // 是否�?select *
	SelectColumns []string `json:"select_columns,omitempty"` // 指定要查询的列（列裁剪）
}

// InsertOptions 插入选项
type InsertOptions struct {
	Replace bool `json:"replace,omitempty"` // 如果存在则替�?
}

// UpdateOptions 更新选项
type UpdateOptions struct {
	Upsert bool `json:"upsert,omitempty"` // 如果不存在则插入
}

// DeleteOptions 删除选项
type DeleteOptions struct {
	Force bool `json:"force,omitempty"` // 强制删除
}

// DataSource 数据源接�?
type DataSource interface {
	// Connect 连接数据�?
	Connect(ctx context.Context) error
	
	// Close 关闭连接
	Close(ctx context.Context) error
	
	// IsConnected 检查是否已连接
	IsConnected() bool
	
	// IsWritable 检查是否可�?
	IsWritable() bool
	
	// GetConfig 获取数据源配�?
	GetConfig() *DataSourceConfig
	
	// GetTables 获取所有表
	GetTables(ctx context.Context) ([]string, error)
	
	// GetTableInfo 获取表信�?
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)
	
	// Query 查询数据
	Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)
	
	// Insert 插入数据
	Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)
	
	// Update 更新数据
	Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)
	
	// Delete 删除数据
	Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)
	
	// CreateTable 创建�?
	CreateTable(ctx context.Context, tableInfo *TableInfo) error
	
	// DropTable 删除�?
	DropTable(ctx context.Context, tableName string) error
	
	// TruncateTable 清空�?
	TruncateTable(ctx context.Context, tableName string) error
	
	// Execute 执行自定义SQL语句
	Execute(ctx context.Context, sql string) (*QueryResult, error)
}

// MVCCDataSource MVCC数据源接口（可选）
type MVCCDataSource interface {
	DataSource
	
	// SupportMVCC 是否支持MVCC
	SupportMVCC() bool
	
	// BeginTransaction 开始事�?
	BeginTransaction(ctx context.Context) (interface{}, error)
	
	// CommitTransaction 提交事务
	CommitTransaction(ctx context.Context, txn interface{}) error
	
	// RollbackTransaction 回滚事务
	RollbackTransaction(ctx context.Context, txn interface{}) error
	
	// QueryWithTransaction 使用事务查询
	QueryWithTransaction(ctx context.Context, txn interface{}, tableName string, options *QueryOptions) (*QueryResult, error)
	
	// InsertWithTransaction 使用事务插入
	InsertWithTransaction(ctx context.Context, txn interface{}, tableName string, rows []Row, options *InsertOptions) (int64, error)
	
	// UpdateWithTransaction 使用事务更新
	UpdateWithTransaction(ctx context.Context, txn interface{}, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)
	
	// DeleteWithTransaction 使用事务删除
	DeleteWithTransaction(ctx context.Context, txn interface{}, tableName string, filters []Filter, options *DeleteOptions) (int64, error)
}

// TransactionOptions 事务选项
type TransactionOptions struct {
	IsolationLevel string `json:"isolation_level,omitempty"` // READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
	ReadOnly       bool   `json:"read_only,omitempty"`        // 只读事务
}

// DataSourceFactory 数据源工厂接�?
type DataSourceFactory interface {
	// Create 创建数据�?
	Create(config *DataSourceConfig) (DataSource, error)
	
	// GetType 支持的数据源类型
	GetType() DataSourceType
}

var (
	factories = make(map[DataSourceType]DataSourceFactory)
)

// RegisterFactory 注册数据源工�?
func RegisterFactory(factory DataSourceFactory) {
	factories[factory.GetType()] = factory
}

// CreateDataSource 创建数据�?
func CreateDataSource(config *DataSourceConfig) (DataSource, error) {
	factory, ok := factories[config.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported data source type: %s", config.Type)
	}
	return factory.Create(config)
}

// GetSupportedTypes 获取支持的数据源类型
func GetSupportedTypes() []DataSourceType {
	types := make([]DataSourceType, 0, len(factories))
	for t := range factories {
		types = append(types, t)
	}
	return types
}
