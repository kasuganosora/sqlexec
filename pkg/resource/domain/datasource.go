package domain

import "context"

// DataSource 数据源接口
type DataSource interface {
	// Connect 连接数据源
	Connect(ctx context.Context) error

	// Close 关闭连接
	Close(ctx context.Context) error

	// IsConnected 检查是否已连接
	IsConnected() bool

	// IsWritable 检查是否可写
	IsWritable() bool

	// GetConfig 获取数据源配置
	GetConfig() *DataSourceConfig

	// GetTables 获取所有表
	GetTables(ctx context.Context) ([]string, error)

	// GetTableInfo 获取表信息
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)

	// Query 查询数据
	Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)

	// Insert 插入数据
	Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)

	// Update 更新数据
	Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)

	// Delete 删除数据
	Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)

	// CreateTable 创建表
	CreateTable(ctx context.Context, tableInfo *TableInfo) error

	// DropTable 删除表
	DropTable(ctx context.Context, tableName string) error

	// TruncateTable 清空表
	TruncateTable(ctx context.Context, tableName string) error

	// Execute 执行自定义SQL语句
	Execute(ctx context.Context, sql string) (*QueryResult, error)
}

// TransactionalDataSource 支持事务的数据源接口
type TransactionalDataSource interface {
	DataSource

	// BeginTransaction 开始事务
	BeginTransaction(ctx context.Context, options *TransactionOptions) (Transaction, error)
}

// Transaction 事务接口
type Transaction interface {
	// Commit 提交事务
	Commit(ctx context.Context) error

	// Rollback 回滚事务
	Rollback(ctx context.Context) error

	// Execute 执行SQL语句
	Execute(ctx context.Context, sql string) (*QueryResult, error)

	// Query 查询数据
	Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)

	// Insert 插入数据
	Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)

	// Update 更新数据
	Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)

	// Delete 删除数据
	Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)
}
