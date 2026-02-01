package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// CoreSession 核心会话实现（用于用户 API）
// 这个 Session 不是协议层的 Session，而是数据库层面的会话（相当于 MySQL 的一个连接）
type CoreSession struct {
	dataSource     domain.DataSource
	dsManager      *application.DataSourceManager
	executor       *optimizer.OptimizedExecutor
	adapter        *parser.SQLAdapter
	currentDB      string // 当前使用的数据库名（USE 语句）
	mu             sync.RWMutex
	txn            domain.Transaction
	txnMu         sync.Mutex       // 事务锁（防止嵌套）
	tempTables     []string          // 会话级临时表列表
	closed         bool
}

// NewCoreSession 创建核心会话
func NewCoreSession(dataSource domain.DataSource) *CoreSession {
	return &CoreSession{
		dataSource: dataSource,
		executor:   optimizer.NewOptimizedExecutor(dataSource, true),
		adapter:    parser.NewSQLAdapter(),
		tempTables: []string{},
		closed:     false,
	}
}

// NewCoreSessionWithDSManager 创建带有数据源管理器的核心会话
func NewCoreSessionWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager) *CoreSession {
	return &CoreSession{
		dataSource: dataSource,
		dsManager:  dsManager,
		executor:   optimizer.NewOptimizedExecutorWithDSManager(dataSource, dsManager, true),
		adapter:    parser.NewSQLAdapter(),
		currentDB:  "", // Default to no database selected
		tempTables: []string{},
		closed:     false,
	}
}

// ExecuteQuery 执行查询（底层实现）
// 返回 *domain.QueryResult，供 api 层封装成 Query 对象
func (s *CoreSession) ExecuteQuery(ctx context.Context, sql string) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	// 处理 USE 语句
	if parseResult.Statement.Use != nil {
		return s.executeUseStatement(parseResult.Statement.Use)
	}

	// 执行查询
	if parseResult.Statement.Select != nil {
		return s.executor.ExecuteSelect(ctx, parseResult.Statement.Select)
	}

	// 处理 SHOW 语句 - 转换为 information_schema 查询
	if parseResult.Statement.Show != nil {
		return s.executor.ExecuteShow(ctx, parseResult.Statement.Show)
	}

	return nil, fmt.Errorf("statement type not supported yet")
}

// ExecuteInsert 执行 INSERT（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteInsert(ctx context.Context, sql string, rows []domain.Row) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.Insert == nil {
		return nil, fmt.Errorf("not an INSERT statement")
	}

	// 使用 executor 执行 INSERT (has information_schema support)
	result, err := s.executor.ExecuteInsert(ctx, parseResult.Statement.Insert)
	if err != nil {
		return nil, fmt.Errorf("INSERT failed: %w", err)
	}

	// 返回结果，其中 Total 是影响的行数
	return result, nil
}

// ExecuteUpdate 执行 UPDATE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteUpdate(ctx context.Context, sql string, _ []domain.Filter, _ domain.Row) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.Update == nil {
		return nil, fmt.Errorf("not an UPDATE statement")
	}

	// 使用 executor 执行 UPDATE (has information_schema support)
	result, err := s.executor.ExecuteUpdate(ctx, parseResult.Statement.Update)
	if err != nil {
		return nil, fmt.Errorf("UPDATE failed: %w", err)
	}

	// 返回结果，其中 Total 是影响的行数
	return result, nil
}

// ExecuteDelete 执行 DELETE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteDelete(ctx context.Context, sql string, _ []domain.Filter) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.Delete == nil {
		return nil, fmt.Errorf("not a DELETE statement")
	}

	// 使用 executor 执行 DELETE (has information_schema support)
	result, err := s.executor.ExecuteDelete(ctx, parseResult.Statement.Delete)
	if err != nil {
		return nil, fmt.Errorf("DELETE failed: %w", err)
	}

	// 返回结果，其中 Total 是影响的行数
	return result, nil
}

// ExecuteCreate 执行 CREATE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数（对于 DDL 通常为 0）
func (s *CoreSession) ExecuteCreate(ctx context.Context, sql string) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.Create == nil {
		return nil, fmt.Errorf("not a CREATE statement")
	}

	// 使用 executor 执行 CREATE
	result, err := s.executor.ExecuteCreate(ctx, parseResult.Statement.Create)
	if err != nil {
		return nil, fmt.Errorf("CREATE failed: %w", err)
	}

	return result, nil
}

// ExecuteDrop 执行 DROP（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数（对于 DDL 通常为 0）
func (s *CoreSession) ExecuteDrop(ctx context.Context, sql string) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.Drop == nil {
		return nil, fmt.Errorf("not a DROP statement")
	}

	// 使用 executor 执行 DROP
	result, err := s.executor.ExecuteDrop(ctx, parseResult.Statement.Drop)
	if err != nil {
		return nil, fmt.Errorf("DROP failed: %w", err)
	}

	return result, nil
}

// ExecuteAlter 执行 ALTER（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数（对于 DDL 通常为 0）
func (s *CoreSession) ExecuteAlter(ctx context.Context, sql string) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.Alter == nil {
		return nil, fmt.Errorf("not an ALTER statement")
	}

	// 使用 executor 执行 ALTER
	result, err := s.executor.ExecuteAlter(ctx, parseResult.Statement.Alter)
	if err != nil {
		return nil, fmt.Errorf("ALTER failed: %w", err)
	}

	return result, nil
}

// ExecuteCreateIndex 执行 CREATE INDEX（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数（对于 DDL 通常为 0）
func (s *CoreSession) ExecuteCreateIndex(ctx context.Context, sql string) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.CreateIndex == nil {
		return nil, fmt.Errorf("not a CREATE INDEX statement")
	}

	// 使用 executor 执行 CREATE INDEX
	result, err := s.executor.ExecuteCreateIndex(ctx, parseResult.Statement.CreateIndex)
	if err != nil {
		return nil, fmt.Errorf("CREATE INDEX failed: %w", err)
	}

	return result, nil
}

// ExecuteDropIndex 执行 DROP INDEX（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数（对于 DDL 通常为 0）
func (s *CoreSession) ExecuteDropIndex(ctx context.Context, sql string) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	if parseResult.Statement.DropIndex == nil {
		return nil, fmt.Errorf("not a DROP INDEX statement")
	}

	// 使用 executor 执行 DROP INDEX
	result, err := s.executor.ExecuteDropIndex(ctx, parseResult.Statement.DropIndex)
	if err != nil {
		return nil, fmt.Errorf("DROP INDEX failed: %w", err)
	}

	return result, nil
}

// BeginTx 开始事务（底层实现）
func (s *CoreSession) BeginTx(ctx context.Context) (domain.Transaction, error) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 检查是否已经在事务中
	if s.txn != nil {
		return nil, fmt.Errorf("transaction already active")
	}

	// 检查数据源是否支持事务
	txDS, ok := s.dataSource.(domain.TransactionalDataSource)
	if !ok {
		return nil, fmt.Errorf("data source does not support transactions")
	}

	// 开始事务
	tx, err := txDS.BeginTransaction(ctx, &domain.TransactionOptions{
		IsolationLevel: "REPEATABLE READ",
		ReadOnly:       false,
	})
	if err != nil {
		return nil, fmt.Errorf("begin transaction failed: %w", err)
	}

	s.txn = tx
	return tx, nil
}

// CommitTx 提交事务（底层实现）
func (s *CoreSession) CommitTx(ctx context.Context) error {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.txn == nil {
		return fmt.Errorf("no active transaction")
	}

	err := s.txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	s.txn = nil
	return nil
}

// RollbackTx 回滚事务（底层实现）
func (s *CoreSession) RollbackTx(ctx context.Context) error {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.txn == nil {
		return fmt.Errorf("no active transaction")
	}

	err := s.txn.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	s.txn = nil
	return nil
}

// InTx 检查是否在事务中
func (s *CoreSession) InTx() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.txn != nil
}

// AddTempTable 添加临时表
func (s *CoreSession) AddTempTable(tableName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tempTables = append(s.tempTables, tableName)
}

// RemoveTempTable 移除临时表
func (s *CoreSession) RemoveTempTable(tableName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, name := range s.tempTables {
		if name == tableName {
			s.tempTables = append(s.tempTables[:i], s.tempTables[i+1:]...)
			break
		}
	}
}

// GetTempTables 获取临时表列表
func (s *CoreSession) GetTempTables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]string, len(s.tempTables))
	copy(list, s.tempTables)
	return list
}

// GetDataSource 获取数据源
func (s *CoreSession) GetDataSource() domain.DataSource {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dataSource
}

// GetExecutor 获取执行器
func (s *CoreSession) GetExecutor() *optimizer.OptimizedExecutor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.executor
}

// GetAdapter 获取适配器
func (s *CoreSession) GetAdapter() *parser.SQLAdapter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adapter
}

// executeUseStatement 执行 USE 语句
func (s *CoreSession) executeUseStatement(useStmt *parser.UseStatement) (*domain.QueryResult, error) {
	// Note: This function is called from ExecuteQuery which already holds the lock,
	// so we don't acquire the lock here to avoid deadlock

	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	dbName := useStmt.Database

	// 验证数据库是否存在
	// 允许使用 information_schema（特殊数据库）
	if dbName != "information_schema" {
		if s.dsManager != nil {
			dsNames := s.dsManager.List()
			found := false
			for _, name := range dsNames {
				if name == dbName {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("unknown database '%s'", dbName)
			}
		}
	}

	// 设置当前数据库
	s.SetCurrentDB(dbName)

	// 返回成功结果
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// GetCurrentDB 获取当前使用的数据库名
func (s *CoreSession) GetCurrentDB() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentDB
}

// SetCurrentDB 设置当前使用的数据库名
func (s *CoreSession) SetCurrentDB(dbName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentDB = dbName

	// 同步更新 OptimizedExecutor 的当前数据库
	if s.executor != nil {
		s.executor.SetCurrentDB(dbName)
	}
}

// Close 关闭会话
func (s *CoreSession) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	// 回滚事务
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.txn = nil
	}

	// 删除临时表
	for _, tableName := range s.tempTables {
		_ = s.dataSource.DropTable(ctx, tableName)
	}
	s.tempTables = []string{}

	s.closed = true
	return nil
}

// IsClosed 检查是否已关闭
func (s *CoreSession) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}
