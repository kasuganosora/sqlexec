package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// CoreSession 核心会话实现（用于用户 API）
// 这个 Session 不是协议层的 Session，而是数据库层面的会话（相当于 MySQL 的一个连接）
type CoreSession struct {
	dataSource     domain.DataSource
	dsManager      *application.DataSourceManager
	executor       *optimizer.OptimizedExecutor
	adapter        *parser.SQLAdapter
	currentDB      string // 当前使用的数据库名（USE 语句）
	user           string // 当前登录用户名
	host           string // 当前客户端主机
	mu             sync.RWMutex
	txn            domain.Transaction
	txnMu         sync.Mutex       // 事务锁（防止嵌套）
	tempTables     []string          // 会话级临时表列表
	closed         bool
	queryTimeout   time.Duration    // 查询超时时间
	threadID       uint32           // 关联的线程ID (用于KILL)
	queryMu        sync.Mutex       // 查询锁
}

// NewCoreSession 创建核心会话（默认使用增强优化器）
func NewCoreSession(dataSource domain.DataSource) *CoreSession {
	return &CoreSession{
		dataSource: dataSource,
		executor:   optimizer.NewOptimizedExecutor(dataSource, true), // 默认启用增强优化器
		adapter:    parser.NewSQLAdapter(),
		tempTables: []string{},
		closed:     false,
	}
}

// NewCoreSessionWithDSManager 创建带有数据源管理器的核心会话（默认使用增强优化器）
func NewCoreSessionWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager) *CoreSession {
	return NewCoreSessionWithDSManagerAndEnhanced(dataSource, dsManager, true, true)
}

// NewCoreSessionWithDSManagerAndEnhanced 创建带有数据源管理器的核心会话（支持增强优化器选项）
func NewCoreSessionWithDSManagerAndEnhanced(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer, useEnhanced bool) *CoreSession {
	return &CoreSession{
		dataSource:   dataSource,
		dsManager:    dsManager,
		executor:     optimizer.NewOptimizedExecutorWithDSManager(dataSource, dsManager, useOptimizer),
		adapter:      parser.NewSQLAdapter(),
		currentDB:    "", // Default to no database selected
		user:         "",
		host:         "",
		tempTables:   []string{},
		closed:       false,
		queryTimeout: 0, // 默认不限制
		threadID:     0, // 后续设置
	}
}

// SetQueryTimeout 设置查询超时时间
func (s *CoreSession) SetQueryTimeout(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryTimeout = timeout
}

// GetQueryTimeout 获取查询超时时间
func (s *CoreSession) GetQueryTimeout() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queryTimeout
}

// SetThreadID 设置线程ID
func (s *CoreSession) SetThreadID(threadID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threadID = threadID
}

// GetThreadID 获取线程ID
func (s *CoreSession) GetThreadID() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.threadID
}

// createQueryContext 创建带超时的查询上下文
func (s *CoreSession) createQueryContext(parentCtx context.Context, sql string) (context.Context, context.CancelFunc, *QueryContext) {
	s.mu.RLock()
	timeout := s.queryTimeout
	threadID := s.threadID
	user := s.user
	host := s.host
	currentDB := s.currentDB
	s.mu.RUnlock()

	// 先创建可取消的上下文
	baseCtx, cancel := context.WithCancel(parentCtx)
	queryID := GenerateQueryID(threadID)

	queryCtx := &QueryContext{
		QueryID:    queryID,
		ThreadID:   threadID,
		SQL:        sql,
		StartTime:  time.Now(),
		CancelFunc: cancel,
		User:       user,
		Host:       host,
		DB:         currentDB,
	}

	// 如果设置了超时,包装超时上下文
	var ctx context.Context
	if timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(baseCtx, timeout)
		queryCtx.CancelFunc = timeoutCancel
		// Wrap cancel to release both the timeout timer and the base context
		combinedCancel := func() {
			timeoutCancel()
			cancel()
		}
		return ctx, combinedCancel, queryCtx
	}

	ctx = baseCtx
	return ctx, cancel, queryCtx
}

// ExecuteQuery 执行查询（底层实现）
// 返回 *domain.QueryResult，供 api 层封装成 Query 对象
func (s *CoreSession) ExecuteQuery(ctx context.Context, sql string) (*domain.QueryResult, error) {
	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	// 设置当前用户到 executor（用于权限检查）
	s.mu.RLock()
	currentUser := s.user
	s.mu.RUnlock()

	if s.executor != nil && currentUser != "" {
		s.executor.SetCurrentUser(currentUser)
	}

	// 注册查询到全局注册表
	registry := GetGlobalQueryRegistry()
	if err := registry.RegisterQuery(qc); err != nil {
		// 注册失败,记录日志但继续执行
		fmt.Printf("Failed to register query: %v\n", err)
	}
	defer registry.UnregisterQuery(qc.QueryID)

	// 解析 SQL
	parseResult, err := s.adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse failed: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("SQL parse error: %s", parseResult.Error)
	}

	// 将用户信息传递到上下文（用于权限检查）
	queryCtx = context.WithValue(queryCtx, "user", currentUser)

	// 处理 USE 语句
	if parseResult.Statement.Use != nil {
		return s.executeUseStatement(parseResult.Statement.Use)
	}

	// 执行查询(使用带取消的 context)
	var result *domain.QueryResult
	if parseResult.Statement.Select != nil {
		result, err = s.executor.ExecuteSelect(queryCtx, parseResult.Statement.Select)
	} else if parseResult.Statement.Show != nil {
		// 处理 SHOW 语句 - 转换为 information_schema 查询
		result, err = s.executor.ExecuteShow(queryCtx, parseResult.Statement.Show)
	} else {
		return nil, fmt.Errorf("statement type not supported yet")
	}

	// 检查是否是超时或取消导致的错误
	if errors.Is(err, context.DeadlineExceeded) {
		qc.SetTimeout()
		return nil, fmt.Errorf("query execution timed out after %v", qc.GetDuration())
	}
	if errors.Is(err, context.Canceled) {
		if qc.IsCanceled() {
			return nil, fmt.Errorf("query was killed")
		}
		return nil, fmt.Errorf("query execution cancelled")
	}

	return result, err
}

// ExecuteInsert 执行 INSERT（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteInsert(ctx context.Context, sql string, rows []domain.Row) (*domain.QueryResult, error) {
	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	// 注册查询
	registry := GetGlobalQueryRegistry()
	registry.RegisterQuery(qc)
	defer registry.UnregisterQuery(qc.QueryID)

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
	result, err := s.executor.ExecuteInsert(queryCtx, parseResult.Statement.Insert)

	// 检查是否是超时或取消导致的错误
	if errors.Is(err, context.DeadlineExceeded) {
		qc.SetTimeout()
		return nil, fmt.Errorf("query execution timed out after %v", qc.GetDuration())
	}
	if errors.Is(err, context.Canceled) {
		if qc.IsCanceled() {
			return nil, fmt.Errorf("query was killed")
		}
		return nil, fmt.Errorf("query execution cancelled")
	}

	// 返回结果，其中 Total 是影响的行数
	return result, err
}

// ExecuteUpdate 执行 UPDATE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteUpdate(ctx context.Context, sql string, _ []domain.Filter, _ domain.Row) (*domain.QueryResult, error) {
	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	// 注册查询
	registry := GetGlobalQueryRegistry()
	registry.RegisterQuery(qc)
	defer registry.UnregisterQuery(qc.QueryID)

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
	result, err := s.executor.ExecuteUpdate(queryCtx, parseResult.Statement.Update)

	// 检查是否是超时或取消导致的错误
	if errors.Is(err, context.DeadlineExceeded) {
		qc.SetTimeout()
		return nil, fmt.Errorf("query execution timed out after %v", qc.GetDuration())
	}
	if errors.Is(err, context.Canceled) {
		if qc.IsCanceled() {
			return nil, fmt.Errorf("query was killed")
		}
		return nil, fmt.Errorf("query execution cancelled")
	}

	// 返回结果，其中 Total 是影响的行数
	return result, err
}

// ExecuteDelete 执行 DELETE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteDelete(ctx context.Context, sql string, _ []domain.Filter) (*domain.QueryResult, error) {
	if s.closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	// 注册查询
	registry := GetGlobalQueryRegistry()
	registry.RegisterQuery(qc)
	defer registry.UnregisterQuery(qc.QueryID)

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
	result, err := s.executor.ExecuteDelete(queryCtx, parseResult.Statement.Delete)

	// 检查是否是超时或取消导致的错误
	if errors.Is(err, context.DeadlineExceeded) {
		qc.SetTimeout()
		return nil, fmt.Errorf("query execution timed out after %v", qc.GetDuration())
	}
	if errors.Is(err, context.Canceled) {
		if qc.IsCanceled() {
			return nil, fmt.Errorf("query was killed")
		}
		return nil, fmt.Errorf("query execution cancelled")
	}

	// 返回结果，其中 Total 是影响的行数
	return result, err
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

	// 验证数据库是否存在，如果不存在则自动创建
	// 允许使用 information_schema 和 config（特殊虚拟数据库）
	if dbName != "information_schema" && dbName != "config" {
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
				// 数据库不存在，自动创建
				// 创建 MVCC 数据源
				memoryDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
					Type:     domain.DataSourceTypeMemory,
					Name:     dbName,
					Writable: true,
				})
				if err := memoryDS.Connect(context.Background()); err != nil {
					return nil, fmt.Errorf("failed to create database '%s': %w", dbName, err)
				}
				if err := s.dsManager.Register(dbName, memoryDS); err != nil {
					return nil, fmt.Errorf("failed to register database '%s': %w", dbName, err)
				}
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

// SetConfigDir sets the config directory for the config virtual database
func (s *CoreSession) SetConfigDir(dir string) {
	if s.executor != nil {
		s.executor.SetConfigDir(dir)
	}
}

// CurrentUser returns current logged-in user
func (s *CoreSession) CurrentUser() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.user
}

// SetUser sets current logged-in user
func (s *CoreSession) SetUser(user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.user = user
}

// CurrentHost returns current client host
func (s *CoreSession) CurrentHost() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.host
}

// SetHost sets current client host
func (s *CoreSession) SetHost(host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.host = host
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
