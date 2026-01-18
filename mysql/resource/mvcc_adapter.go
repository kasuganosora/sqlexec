package resource

import (
	"context"
	"fmt"
	"mysql-proxy/mysql/mvcc"
	"sync"
)

// MVCCAdapter MVCC数据源适配器
type MVCCAdapter struct {
	inner      mvcc.MVCCDataSource
	config     *DataSourceConfig
	manager     *mvcc.Manager
	registry    *mvcc.DataSourceRegistry
	downgrader  *mvcc.DowngradeHandler
	mu          sync.RWMutex
	connected   bool
}

// NewMVCCAdapter 创建MVCC数据源适配器
func NewMVCCAdapter(inner mvcc.MVCCDataSource, config *DataSourceConfig) (*MVCCAdapter, error) {
	// 创建MVCC管理器
	mgr := mvcc.GetGlobalManager()
	
	// 创建数据源注册表
	registry := mvcc.NewDataSourceRegistry()
	
	// 注册内部数据源
	registry.Register(inner.GetFeatures().Name, inner)
	
	// 创建降级处理器
	downgrader := mvcc.NewDowngradeHandler(mgr, registry)
	
	return &MVCCAdapter{
		inner:      inner,
		config:     config,
		manager:     mgr,
		registry:    registry,
		downgrader:  downgrader,
		connected:   false,
	}, nil
}

// Connect 连接数据源
func (a *MVCCAdapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	a.connected = true
	return nil
}

// Close 关闭连接
func (a *MVCCAdapter) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	a.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (a *MVCCAdapter) IsConnected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.connected
}

// GetConfig 获取数据源配置
func (a *MVCCAdapter) GetConfig() *DataSourceConfig {
	return a.config
}

// GetTables 获取所有表
func (a *MVCCAdapter) GetTables(ctx context.Context) ([]string, error) {
	// 检查MVCC能力
	supportsMVCC, _ := a.downgrader.CheckBeforeQuery([]string{a.config.Name}, false)
	
	if !supportsMVCC {
		// 降级处理
		return []string{}, nil
	}
	
	// TODO: 从内部数据源获取表列表
	return []string{}, nil
}

// GetTableInfo 获取表信息
func (a *MVCCAdapter) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	// 检查MVCC能力
	supportsMVCC, _ := a.downgrader.CheckBeforeQuery([]string{a.config.Name}, false)
	
	if !supportsMVCC {
		return nil, fmt.Errorf("data source does not support MVCC")
	}
	
	// TODO: 从内部数据源获取表信息
	return nil, nil
}

// Query 查询数据
func (a *MVCCAdapter) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	// 检查MVCC能力（只读查询可降级）
	supportsMVCC, _ := a.downgrader.CheckBeforeQuery([]string{a.config.Name}, true)
	
	if !supportsMVCC {
		// 降级处理
		return &QueryResult{}, nil
	}
	
	// 获取事务上下文
	txn := getTransactionFromContext(ctx)
	if txn == nil {
		// 没有事务，使用普通查询
		return a.queryWithoutTransaction(ctx, tableName, options)
	}
	
	// 使用事务查询
	return a.queryWithTransaction(ctx, txn, tableName, options)
}

// queryWithoutTransaction 不带事务的查询
func (a *MVCCAdapter) queryWithoutTransaction(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	// TODO: 实现普通查询
	return &QueryResult{}, nil
}

// queryWithTransaction 带事务的查询
func (a *MVCCAdapter) queryWithTransaction(ctx context.Context, txn *mvcc.Transaction, tableName string, options *QueryOptions) (*QueryResult, error) {
	// TODO: 实现事务查询
	return &QueryResult{}, nil
}

// Insert 插入数据
func (a *MVCCAdapter) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	// 检查MVCC能力（写入操作要求MVCC支持）
	supportsMVCC, err := a.downgrader.CheckBeforeWrite([]string{a.config.Name})
	if err != nil {
		return 0, err
	}
	
	if !supportsMVCC {
		return 0, fmt.Errorf("insert requires MVCC support")
	}
	
	// 获取事务上下文
	txn := getTransactionFromContext(ctx)
	if txn == nil {
		return 0, fmt.Errorf("insert requires transaction")
	}
	
	// 使用事务插入
	return a.insertWithTransaction(ctx, txn, tableName, rows, options)
}

// insertWithTransaction 带事务的插入
func (a *MVCCAdapter) insertWithTransaction(ctx context.Context, txn *mvcc.Transaction, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	// TODO: 实现事务插入
	return int64(len(rows)), nil
}

// Update 更新数据
func (a *MVCCAdapter) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	// 检查MVCC能力（写入操作要求MVCC支持）
	supportsMVCC, err := a.downgrader.CheckBeforeWrite([]string{a.config.Name})
	if err != nil {
		return 0, err
	}
	
	if !supportsMVCC {
		return 0, fmt.Errorf("update requires MVCC support")
	}
	
	// 获取事务上下文
	txn := getTransactionFromContext(ctx)
	if txn == nil {
		return 0, fmt.Errorf("update requires transaction")
	}
	
	// 使用事务更新
	return a.updateWithTransaction(ctx, txn, tableName, filters, updates, options)
}

// updateWithTransaction 带事务的更新
func (a *MVCCAdapter) updateWithTransaction(ctx context.Context, txn *mvcc.Transaction, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	// TODO: 实现事务更新
	return 0, nil
}

// Delete 删除数据
func (a *MVCCAdapter) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	// 检查MVCC能力（写入操作要求MVCC支持）
	supportsMVCC, err := a.downgrader.CheckBeforeWrite([]string{a.config.Name})
	if err != nil {
		return 0, err
	}
	
	if !supportsMVCC {
		return 0, fmt.Errorf("delete requires MVCC support")
	}
	
	// 获取事务上下文
	txn := getTransactionFromContext(ctx)
	if txn == nil {
		return 0, fmt.Errorf("delete requires transaction")
	}
	
	// 使用事务删除
	return a.deleteWithTransaction(ctx, txn, tableName, filters, options)
}

// deleteWithTransaction 带事务的删除
func (a *MVCCAdapter) deleteWithTransaction(ctx context.Context, txn *mvcc.Transaction, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	// TODO: 实现事务删除
	return 0, nil
}

// CreateTable 创建表
func (a *MVCCAdapter) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	// 检查MVCC能力（写入操作要求MVCC支持）
	supportsMVCC, err := a.downgrader.CheckBeforeWrite([]string{a.config.Name})
	if err != nil {
		return err
	}
	
	if !supportsMVCC {
		return fmt.Errorf("create table requires MVCC support")
	}
	
	// TODO: 实现创建表
	return nil
}

// DropTable 删除表
func (a *MVCCAdapter) DropTable(ctx context.Context, tableName string) error {
	// 检查MVCC能力（写入操作要求MVCC支持）
	supportsMVCC, err := a.downgrader.CheckBeforeWrite([]string{a.config.Name})
	if err != nil {
		return err
	}
	
	if !supportsMVCC {
		return fmt.Errorf("drop table requires MVCC support")
	}
	
	// TODO: 实现删除表
	return nil
}

// TruncateTable 清空表
func (a *MVCCAdapter) TruncateTable(ctx context.Context, tableName string) error {
	// 检查MVCC能力（写入操作要求MVCC支持）
	supportsMVCC, err := a.downgrader.CheckBeforeWrite([]string{a.config.Name})
	if err != nil {
		return err
	}
	
	if !supportsMVCC {
		return fmt.Errorf("truncate table requires MVCC support")
	}
	
	// TODO: 实现清空表
	return nil
}

// Execute 执行自定义SQL语句
func (a *MVCCAdapter) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	// TODO: 实现执行SQL
	return &QueryResult{}, nil
}

// SupportMVCC 是否支持MVCC（实现MVCCDataSource接口）
func (a *MVCCAdapter) SupportMVCC() bool {
	return a.inner.GetFeatures().HasMVCC()
}

// BeginTransaction 开始事务（实现MVCCDataSource接口）
func (a *MVCCAdapter) BeginTransaction(ctx context.Context) (interface{}, error) {
	return a.manager.Begin(mvcc.RepeatableRead, a.inner.GetFeatures())
}

// CommitTransaction 提交事务（实现MVCCDataSource接口）
func (a *MVCCAdapter) CommitTransaction(ctx context.Context, txn interface{}) error {
	if mvccTxn, ok := txn.(*mvcc.Transaction); ok {
		return a.manager.Commit(mvccTxn)
	}
	return fmt.Errorf("invalid transaction type")
}

// RollbackTransaction 回滚事务（实现MVCCDataSource接口）
func (a *MVCCAdapter) RollbackTransaction(ctx context.Context, txn interface{}) error {
	if mvccTxn, ok := txn.(*mvcc.Transaction); ok {
		return a.manager.Rollback(mvccTxn)
	}
	return fmt.Errorf("invalid transaction type")
}

// QueryWithTransaction 使用事务查询（实现MVCCDataSource接口）
func (a *MVCCAdapter) QueryWithTransaction(ctx context.Context, txn interface{}, tableName string, options *QueryOptions) (*QueryResult, error) {
	if mvccTxn, ok := txn.(*mvcc.Transaction); ok {
		return a.queryWithTransaction(ctx, mvccTxn, tableName, options)
	}
	return nil, fmt.Errorf("invalid transaction type")
}

// InsertWithTransaction 使用事务插入（实现MVCCDataSource接口）
func (a *MVCCAdapter) InsertWithTransaction(ctx context.Context, txn interface{}, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	if mvccTxn, ok := txn.(*mvcc.Transaction); ok {
		return a.insertWithTransaction(ctx, mvccTxn, tableName, rows, options)
	}
	return 0, fmt.Errorf("invalid transaction type")
}

// UpdateWithTransaction 使用事务更新（实现MVCCDataSource接口）
func (a *MVCCAdapter) UpdateWithTransaction(ctx context.Context, txn interface{}, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	if mvccTxn, ok := txn.(*mvcc.Transaction); ok {
		return a.updateWithTransaction(ctx, mvccTxn, tableName, filters, updates, options)
	}
	return 0, fmt.Errorf("invalid transaction type")
}

// DeleteWithTransaction 使用事务删除（实现MVCCDataSource接口）
func (a *MVCCAdapter) DeleteWithTransaction(ctx context.Context, txn interface{}, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	if mvccTxn, ok := txn.(*mvcc.Transaction); ok {
		return a.deleteWithTransaction(ctx, mvccTxn, tableName, filters, options)
	}
	return 0, fmt.Errorf("invalid transaction type")
}

// ==================== 事务上下文管理 ====================

// contextKey 事务上下文Key
type contextKey int

const (
	// keyTransaction 事务上下文key
	keyTransaction contextKey = iota
)

// withTransaction 将事务添加到context
func withTransaction(ctx context.Context, txn *mvcc.Transaction) context.Context {
	return context.WithValue(ctx, keyTransaction, txn)
}

// getTransactionFromContext 从context获取事务
func getTransactionFromContext(ctx context.Context) *mvcc.Transaction {
	txn, _ := ctx.Value(keyTransaction).(*mvcc.Transaction)
	return txn
}
