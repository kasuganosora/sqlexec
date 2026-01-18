package resource

import (
	"context"
	"fmt"
	"sync"
)

// ==================== 简化的MVCC实现 ====================

// TransactionID 事务ID
type TransactionID uint32

// Transaction 事务
type Transaction struct {
	ID        TransactionID
	Level     string // READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
	StartTime int64
	Reads     map[string]bool   // 读集合
	Writes    map[string]Row   // 写集合
	mu        sync.RWMutex
}

// TransactionManager 事务管理器
type TransactionManager struct {
	transactions map[TransactionID]*Transaction
	currentID    TransactionID
	mu           sync.RWMutex
	enabled      bool // 是否启用MVCC
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(enabled bool) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[TransactionID]*Transaction),
		currentID:    1,
		enabled:      enabled,
	}
}

// Begin 开始事务
func (tm *TransactionManager) Begin(level string) *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.enabled {
		return nil
	}

	id := tm.currentID
	tm.currentID++

	txn := &Transaction{
		ID:        id,
		Level:     level,
		StartTime: getCurrentTimestamp(),
		Reads:     make(map[string]bool),
		Writes:    make(map[string]Row),
	}

	tm.transactions[id] = txn
	return txn
}

// Commit 提交事务
func (tm *TransactionManager) Commit(txn *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.enabled {
		return nil
	}

	if _, exists := tm.transactions[txn.ID]; !exists {
		return fmt.Errorf("transaction not found: %d", txn.ID)
	}

	delete(tm.transactions, txn.ID)
	return nil
}

// Rollback 回滚事务
func (tm *TransactionManager) Rollback(txn *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.enabled {
		return nil
	}

	if _, exists := tm.transactions[txn.ID]; !exists {
		return fmt.Errorf("transaction not found: %d", txn.ID)
	}

	delete(tm.transactions, txn.ID)
	return nil
}

// IsActive 检查事务是否活跃
func (tm *TransactionManager) IsActive(txn *Transaction) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	_, exists := tm.transactions[txn.ID]
	return exists
}

// GetTransaction 获取事务
func (tm *TransactionManager) GetTransaction(id TransactionID) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txn, exists := tm.transactions[id]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %d", id)
	}
	return txn, nil
}

// GetCurrentTransaction 从context获取当前事务
func (tm *TransactionManager) GetCurrentTransaction(ctx context.Context) *Transaction {
	if txn := ctx.Value("transaction"); txn != nil {
		if t, ok := txn.(*Transaction); ok {
			return t
		}
	}
	return nil
}

// SetCurrentTransaction 设置当前事务到context
func (tm *TransactionManager) SetCurrentTransaction(ctx context.Context, txn *Transaction) context.Context {
	return context.WithValue(ctx, "transaction", txn)
}

// Enable 启用MVCC
func (tm *TransactionManager) Enable() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.enabled = true
}

// Disable 禁用MVCC
func (tm *TransactionManager) Disable() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.enabled = false
}

// IsEnabled 检查是否启用MVCC
func (tm *TransactionManager) IsEnabled() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.enabled
}

// GetCurrentTimestamp 获取当前时间戳
func getCurrentTimestamp() int64 {
	return 0 // 简化版本
}

// ==================== MVCCDataSource适配器 ====================

// MVCCDataSourceAdapter MVCC数据源适配器
type MVCCDataSourceAdapter struct {
	inner      DataSource
	txnMgr     *TransactionManager
	connected   bool
	mu          sync.RWMutex
}

// NewMVCCDataSourceAdapter 创建MVCC数据源适配器
func NewMVCCDataSourceAdapter(inner DataSource, enabled bool) *MVCCDataSourceAdapter {
	return &MVCCDataSourceAdapter{
		inner:      inner,
		txnMgr:     NewTransactionManager(enabled),
		connected:   false,
	}
}

// Connect 连接数据源
func (a *MVCCDataSourceAdapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.inner.Connect(ctx); err != nil {
		return err
	}

	a.connected = true
	return nil
}

// Close 关闭连接
func (a *MVCCDataSourceAdapter) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.connected = false
	return a.inner.Close(ctx)
}

// IsConnected 检查是否已连接
func (a *MVCCDataSourceAdapter) IsConnected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.connected
}

// GetConfig 获取数据源配置
func (a *MVCCDataSourceAdapter) GetConfig() *DataSourceConfig {
	return a.inner.GetConfig()
}

// GetTables 获取所有表
func (a *MVCCDataSourceAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.inner.GetTables(ctx)
}

// GetTableInfo 获取表信息
func (a *MVCCDataSourceAdapter) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	return a.inner.GetTableInfo(ctx, tableName)
}

// Query 查询数据（支持事务）
func (a *MVCCDataSourceAdapter) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// 检查是否启用MVCC
	if !a.txnMgr.IsEnabled() {
		return a.inner.Query(ctx, tableName, options)
	}

	// 获取当前事务
	txn := a.txnMgr.GetCurrentTransaction(ctx)
	if txn == nil {
		// 没有事务，使用普通查询
		result, err := a.inner.Query(ctx, tableName, options)
		if err != nil {
			return nil, err
		}

		// 记录读集合
		for _, row := range result.Rows {
			txn.mu.Lock()
			for key := range row {
				txn.Reads[tableName+":"+key] = true
			}
			txn.mu.Unlock()
		}

		return result, nil
	}

	// 有事务，检查读写冲突
	// TODO: 实现版本可见性检查
	return a.inner.Query(ctx, tableName, options)
}

// Insert 插入数据（支持事务）
func (a *MVCCDataSourceAdapter) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// 检查是否启用MVCC
	if !a.txnMgr.IsEnabled() {
		return a.inner.Insert(ctx, tableName, rows, options)
	}

	// 获取当前事务
	txn := a.txnMgr.GetCurrentTransaction(ctx)
	if txn == nil {
		return 0, fmt.Errorf("insert requires transaction when MVCC is enabled")
	}

	// 记录写集合
	for _, row := range rows {
		txn.mu.Lock()
		for key := range row {
			txn.Writes[tableName+":"+key] = row
		}
		txn.mu.Unlock()
	}

	// 执行插入
	return a.inner.Insert(ctx, tableName, rows, options)
}

// Update 更新数据（支持事务）
func (a *MVCCDataSourceAdapter) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// 检查是否启用MVCC
	if !a.txnMgr.IsEnabled() {
		return a.inner.Update(ctx, tableName, filters, updates, options)
	}

	// 获取当前事务
	txn := a.txnMgr.GetCurrentTransaction(ctx)
	if txn == nil {
		return 0, fmt.Errorf("update requires transaction when MVCC is enabled")
	}

	// 记录写集合
	txn.mu.Lock()
	for key, val := range updates {
		if row, ok := val.(Row); ok {
			txn.Writes[tableName+":"+key] = row
		}
	}
	txn.mu.Unlock()

	// 执行更新
	return a.inner.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据（支持事务）
func (a *MVCCDataSourceAdapter) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// 检查是否启用MVCC
	if !a.txnMgr.IsEnabled() {
		return a.inner.Delete(ctx, tableName, filters, options)
	}

	// 获取当前事务
	txn := a.txnMgr.GetCurrentTransaction(ctx)
	if txn == nil {
		return 0, fmt.Errorf("delete requires transaction when MVCC is enabled")
	}

	// 记录写集合
	for _, filter := range filters {
		txn.mu.Lock()
		txn.Writes[tableName+":"+filter.Field] = nil // 标记为删除
		txn.mu.Unlock()
	}

	// 执行删除
	return a.inner.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表
func (a *MVCCDataSourceAdapter) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	return a.inner.CreateTable(ctx, tableInfo)
}

// DropTable 删除表
func (a *MVCCDataSourceAdapter) DropTable(ctx context.Context, tableName string) error {
	return a.inner.DropTable(ctx, tableName)
}

// TruncateTable 清空表
func (a *MVCCDataSourceAdapter) TruncateTable(ctx context.Context, tableName string) error {
	return a.inner.TruncateTable(ctx, tableName)
}

// Execute 执行自定义SQL语句
func (a *MVCCDataSourceAdapter) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	return a.inner.Execute(ctx, sql)
}

// ==================== 事务操作接口 ====================

// BeginTransaction 开始事务
func (a *MVCCDataSourceAdapter) BeginTransaction(ctx context.Context, level string) (interface{}, error) {
	return a.txnMgr.Begin(level), nil
}

// CommitTransaction 提交事务
func (a *MVCCDataSourceAdapter) CommitTransaction(ctx context.Context, txn interface{}) error {
	if t, ok := txn.(*Transaction); ok {
		return a.txnMgr.Commit(t)
	}
	return fmt.Errorf("invalid transaction type")
}

// RollbackTransaction 回滚事务
func (a *MVCCDataSourceAdapter) RollbackTransaction(ctx context.Context, txn interface{}) error {
	if t, ok := txn.(*Transaction); ok {
		return a.txnMgr.Rollback(t)
	}
	return fmt.Errorf("invalid transaction type")
}

// SupportMVCC 是否支持MVCC
func (a *MVCCDataSourceAdapter) SupportMVCC() bool {
	return a.txnMgr.IsEnabled()
}

// QueryWithTransaction 使用事务查询
func (a *MVCCDataSourceAdapter) QueryWithTransaction(ctx context.Context, txn interface{}, tableName string, options *QueryOptions) (*QueryResult, error) {
	return a.Query(ctx, tableName, options)
}

// InsertWithTransaction 使用事务插入
func (a *MVCCDataSourceAdapter) InsertWithTransaction(ctx context.Context, txn interface{}, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	return a.Insert(ctx, tableName, rows, options)
}

// UpdateWithTransaction 使用事务更新
func (a *MVCCDataSourceAdapter) UpdateWithTransaction(ctx context.Context, txn interface{}, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	return a.Update(ctx, tableName, filters, updates, options)
}

// DeleteWithTransaction 使用事务删除
func (a *MVCCDataSourceAdapter) DeleteWithTransaction(ctx context.Context, txn interface{}, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	return a.Delete(ctx, tableName, filters, options)
}

// GetTransactionManager 获取事务管理器
func (a *MVCCDataSourceAdapter) GetTransactionManager() *TransactionManager {
	return a.txnMgr
}
