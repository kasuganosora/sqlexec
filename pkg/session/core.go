package session

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	xmlpersist "github.com/kasuganosora/sqlexec/pkg/resource/xml"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
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
	traceID        string           // 追踪ID (来自协议层 Session)
	queryMu        sync.Mutex       // 查询锁
	vdbRegistry    *virtual.VirtualDatabaseRegistry // 虚拟数据库注册表
	sessionVars    map[string]string // 会话级系统变量覆盖 (SET NAMES, SET @@var, etc.)
	databaseDir    string                                                  // 持久化存储根目录
	tablePersistence map[string]map[string]*xmlpersist.TablePersistConfig  // dbName -> tableName -> config
}

// NewCoreSession 创建核心会话（默认使用增强优化器）
func NewCoreSession(dataSource domain.DataSource) *CoreSession {
	return &CoreSession{
		dataSource:       dataSource,
		executor:         optimizer.NewOptimizedExecutor(dataSource, true), // 默认启用增强优化器
		adapter:          parser.NewSQLAdapter(),
		tempTables:       []string{},
		closed:           false,
		sessionVars:      make(map[string]string),
		databaseDir:      "./database",
		tablePersistence: make(map[string]map[string]*xmlpersist.TablePersistConfig),
	}
}

// NewCoreSessionWithDSManager 创建带有数据源管理器的核心会话（默认使用增强优化器）
func NewCoreSessionWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager) *CoreSession {
	return NewCoreSessionWithDSManagerAndEnhanced(dataSource, dsManager, true, true)
}

// NewCoreSessionWithDSManagerAndEnhanced 创建带有数据源管理器的核心会话（支持增强优化器选项）
func NewCoreSessionWithDSManagerAndEnhanced(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer, useEnhanced bool) *CoreSession {
	return &CoreSession{
		dataSource:       dataSource,
		dsManager:        dsManager,
		executor:         optimizer.NewOptimizedExecutorWithDSManager(dataSource, dsManager, useOptimizer),
		adapter:          parser.NewSQLAdapter(),
		currentDB:        "", // Default to no database selected
		user:             "",
		host:             "",
		tempTables:       []string{},
		closed:           false,
		queryTimeout:     0, // 默认不限制
		threadID:         0, // 后续设置
		sessionVars:      make(map[string]string),
		databaseDir:      "./database",
		tablePersistence: make(map[string]map[string]*xmlpersist.TablePersistConfig),
	}
}

// SetDatabaseDir sets the persistence base directory
func (s *CoreSession) SetDatabaseDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databaseDir = dir
}

// registerTablePersistence registers a table for persistence tracking
func (s *CoreSession) registerTablePersistence(dbName, tableName string, cfg *xmlpersist.TablePersistConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tablePersistence[dbName] == nil {
		s.tablePersistence[dbName] = make(map[string]*xmlpersist.TablePersistConfig)
	}
	s.tablePersistence[dbName][tableName] = cfg
}

// getTablePersistence returns the persistence config for a table, or nil
func (s *CoreSession) getTablePersistence(dbName, tableName string) *xmlpersist.TablePersistConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if dbTables, ok := s.tablePersistence[dbName]; ok {
		return dbTables[tableName]
	}
	return nil
}

// removeTablePersistence removes persistence tracking for a table
func (s *CoreSession) removeTablePersistence(dbName, tableName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dbTables, ok := s.tablePersistence[dbName]; ok {
		delete(dbTables, tableName)
	}
}

// persistTableData writes current table data to XML files
func (s *CoreSession) persistTableData(ctx context.Context, dbName string, cfg *xmlpersist.TablePersistConfig) error {
	var ds domain.DataSource
	if s.dsManager != nil {
		var err error
		ds, err = s.dsManager.Get(dbName)
		if err != nil {
			return fmt.Errorf("database '%s' not found: %w", dbName, err)
		}
	} else {
		ds = s.dataSource
	}

	// Get table schema
	tableInfo, err := ds.GetTableInfo(ctx, cfg.TableName)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// Query all rows
	result, err := ds.Query(ctx, cfg.TableName, nil)
	if err != nil {
		return fmt.Errorf("failed to query table data: %w", err)
	}

	return xmlpersist.PersistTableData(cfg, tableInfo, result.Rows)
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

// SetTraceID 设置追踪ID
func (s *CoreSession) SetTraceID(traceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traceID = traceID
}

// GetTraceID 获取追踪ID
func (s *CoreSession) GetTraceID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.traceID
}

// createQueryContext 创建带超时的查询上下文
func (s *CoreSession) createQueryContext(parentCtx context.Context, sql string) (context.Context, context.CancelFunc, *QueryContext) {
	s.mu.RLock()
	timeout := s.queryTimeout
	threadID := s.threadID
	traceID := s.traceID
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
		TraceID:    traceID,
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
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("session is closed")
	}

	// 提取 SQL 注释中的 trace_id（优先级最高）
	commentTraceID, sql := ExtractTraceID(sql)

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	// SQL 注释中的 trace_id 覆盖 session 级别的 trace_id
	if commentTraceID != "" {
		qc.TraceID = commentTraceID
	}

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
	} else if parseResult.Statement.Describe != nil {
		// DESC/DESCRIBE 转换为 SHOW COLUMNS FROM table
		descStmt := parseResult.Statement.Describe
		showStmt := &parser.ShowStatement{
			Type:  "COLUMNS",
			Table: descStmt.Table,
		}
		if descStmt.Column != "" {
			showStmt.Like = descStmt.Column
		}
		result, err = s.executor.ExecuteShow(queryCtx, showStmt)
	} else if parseResult.Statement.Create != nil {
		// 处理 CREATE 语句
		result, err = s.executeCreateStatement(queryCtx, parseResult.Statement.Create)
	} else if parseResult.Statement.Set != nil {
		// 处理 SET 语句 (SET NAMES, SET CHARACTER SET, SET SESSION var, etc.)
		result, err = s.executeSetStatement(queryCtx, parseResult.Statement.Set)
	} else if parseResult.Statement.Drop != nil {
		// 处理 DROP 语句 (DROP TABLE t1, t2, etc.)
		result, err = s.executor.ExecuteDrop(queryCtx, parseResult.Statement.Drop)
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
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("session is closed")
	}

	commentTraceID, sql := ExtractTraceID(sql)

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	if commentTraceID != "" {
		qc.TraceID = commentTraceID
	}

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

	// Persistence write-back for ENGINE=xml tables
	if err == nil && parseResult.Statement.Insert != nil {
		if cfg := s.getTablePersistence(s.currentDB, parseResult.Statement.Insert.Table); cfg != nil {
			if pErr := s.persistTableData(queryCtx, s.currentDB, cfg); pErr != nil {
				log.Printf("warning: persistence write-back failed for %s: %v", parseResult.Statement.Insert.Table, pErr)
			}
		}
	}

	// 返回结果，其中 Total 是影响的行数
	return result, err
}

// ExecuteUpdate 执行 UPDATE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteUpdate(ctx context.Context, sql string, _ []domain.Filter, _ domain.Row) (*domain.QueryResult, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("session is closed")
	}

	commentTraceID, sql := ExtractTraceID(sql)

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	if commentTraceID != "" {
		qc.TraceID = commentTraceID
	}

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

	// Persistence write-back for ENGINE=xml tables
	if err == nil && parseResult.Statement.Update != nil {
		if cfg := s.getTablePersistence(s.currentDB, parseResult.Statement.Update.Table); cfg != nil {
			if pErr := s.persistTableData(queryCtx, s.currentDB, cfg); pErr != nil {
				log.Printf("warning: persistence write-back failed for %s: %v", parseResult.Statement.Update.Table, pErr)
			}
		}
	}

	// 返回结果，其中 Total 是影响的行数
	return result, err
}

// ExecuteDelete 执行 DELETE（底层实现）
// 返回 *domain.QueryResult，其中 Total 字段是影响的行数
func (s *CoreSession) ExecuteDelete(ctx context.Context, sql string, _ []domain.Filter) (*domain.QueryResult, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("session is closed")
	}

	commentTraceID, sql := ExtractTraceID(sql)

	// 创建查询上下文(带超时和取消支持)
	queryCtx, cancel, qc := s.createQueryContext(ctx, sql)
	defer cancel()

	if commentTraceID != "" {
		qc.TraceID = commentTraceID
	}

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

	// Persistence write-back for ENGINE=xml tables
	if err == nil && parseResult.Statement.Delete != nil {
		if cfg := s.getTablePersistence(s.currentDB, parseResult.Statement.Delete.Table); cfg != nil {
			if pErr := s.persistTableData(queryCtx, s.currentDB, cfg); pErr != nil {
				log.Printf("warning: persistence write-back failed for %s: %v", parseResult.Statement.Delete.Table, pErr)
			}
		}
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

	// Persist index metadata for ENGINE=xml tables
	tableName := parseResult.Statement.CreateIndex.TableName
	s.persistIndexMetaIfNeeded(ctx, tableName)

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

	// Persist index metadata for ENGINE=xml tables
	tableName := parseResult.Statement.DropIndex.TableName
	s.persistIndexMetaIfNeeded(ctx, tableName)

	return result, nil
}

// tableIndexLister is a local interface for datasources that can list table indexes.
// Both *memory.MVCCDataSource and file adapters (CSV/JSON/JSONL) satisfy this via embedding.
type tableIndexLister interface {
	GetTableIndexes(tableName string) ([]*memory.IndexInfo, error)
}

// persistIndexMetaIfNeeded saves index metadata to disk for tables that support persistence.
// This handles both XML persistence tables and file-based datasources (CSV/JSON/JSONL).
func (s *CoreSession) persistIndexMetaIfNeeded(ctx context.Context, tableName string) {
	var ds domain.DataSource
	if s.dsManager != nil {
		var err error
		ds, err = s.dsManager.Get(s.currentDB)
		if err != nil {
			return
		}
	} else {
		ds = s.dataSource
	}

	// Path 1: XML persistence tables
	if cfg := s.getTablePersistence(s.currentDB, tableName); cfg != nil {
		mvccDS, ok := ds.(*memory.MVCCDataSource)
		if !ok {
			return
		}

		indexInfos, err := mvccDS.GetTableIndexes(tableName)
		if err != nil {
			return
		}

		indexes := make([]*xmlpersist.IndexMeta, 0, len(indexInfos))
		for _, info := range indexInfos {
			indexes = append(indexes, &xmlpersist.IndexMeta{
				Name:    info.Name,
				Table:   info.TableName,
				Type:    string(info.Type),
				Unique:  info.Unique,
				Columns: info.Columns,
			})
		}

		if err := xmlpersist.PersistIndexMeta(cfg, indexes); err != nil {
			log.Printf("warning: failed to persist index metadata for %s: %v", tableName, err)
		}
		return
	}

	// Path 2: File-based datasources implementing IndexPersister
	persister, ok := ds.(domain.IndexPersister)
	if !ok {
		return
	}

	lister, ok := ds.(tableIndexLister)
	if !ok {
		return
	}

	indexInfos, err := lister.GetTableIndexes(tableName)
	if err != nil {
		return
	}

	indexes := make([]domain.IndexMetaInfo, 0, len(indexInfos))
	for _, info := range indexInfos {
		indexes = append(indexes, domain.IndexMetaInfo{
			Name:    info.Name,
			Table:   info.TableName,
			Type:    string(info.Type),
			Unique:  info.Unique,
			Columns: info.Columns,
		})
	}

	if err := persister.PersistIndexMeta(indexes); err != nil {
		log.Printf("warning: failed to persist index metadata for %s: %v", tableName, err)
	}
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

// executeCreateStatement executes CREATE TABLE statement
func (s *CoreSession) executeCreateStatement(ctx context.Context, createStmt *parser.CreateStatement) (*domain.QueryResult, error) {
	// Read session state under lock to avoid data races.
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("session is closed")
	}

	// Only support CREATE TABLE
	if createStmt.Type != "TABLE" {
		s.mu.Unlock()
		return nil, fmt.Errorf("CREATE %s is not supported yet", createStmt.Type)
	}

	tableName := createStmt.Name
	if tableName == "" {
		s.mu.Unlock()
		return nil, fmt.Errorf("table name is required")
	}

	// Determine target database:
	// 1. If createStmt.Database is set (db.table format), use it
	// 2. Otherwise use currentDB
	// 3. If currentDB is empty, try "test" as default (MariaDB test compatibility)
	var targetDB string
	if createStmt.Database != "" {
		targetDB = createStmt.Database
	} else {
		targetDB = s.currentDB
		if targetDB == "" {
			// Auto-create "test" database if not exists (MariaDB/MySQL compatibility)
			if s.dsManager != nil {
				if _, err := s.dsManager.Get("test"); err == nil {
					targetDB = "test"
					s.currentDB = "test"
					if s.executor != nil {
						s.executor.SetCurrentDB("test")
					}
				} else {
					// Auto-create test database using memory factory
					registry := s.dsManager.GetRegistry()
					if registry != nil {
						if factory, err := registry.Get(domain.DataSourceTypeMemory); err == nil {
							testDS, createErr := factory.Create(&domain.DataSourceConfig{
								Type:     domain.DataSourceTypeMemory,
								Name:     "test",
								Writable: true,
							})
							if createErr == nil {
								if connectErr := testDS.Connect(ctx); connectErr == nil {
									if regErr := s.dsManager.Register("test", testDS); regErr == nil {
										targetDB = "test"
										s.currentDB = "test"
										if s.executor != nil {
											s.executor.SetCurrentDB("test")
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if targetDB == "" {
		s.mu.Unlock()
		return nil, fmt.Errorf("no database selected")
	}

	// Check if this is a virtual database
	if s.vdbRegistry != nil && s.vdbRegistry.IsVirtualDB(targetDB) {
		s.mu.Unlock()
		return nil, fmt.Errorf("%s is a virtual database: CREATE TABLE operation not supported", targetDB)
	}

	// Get data source for target database
	var ds domain.DataSource
	if s.dsManager != nil {
		var err error
		ds, err = s.dsManager.Get(targetDB)
		if err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("database '%s' not found: %w", targetDB, err)
		}
	} else {
		ds = s.dataSource
	}
	databaseDir := s.databaseDir
	s.mu.Unlock()

	// Build table info from columns
	tableInfo := &domain.TableInfo{
		Name:    tableName,
		Columns: make([]domain.ColumnInfo, 0, len(createStmt.Columns)),
	}

	for _, col := range createStmt.Columns {
		// Convert Default to string if present
		var defaultVal string
		if col.Default != nil {
			switch v := col.Default.(type) {
			case string:
				defaultVal = v
			default:
				defaultVal = fmt.Sprintf("%v", v)
			}
		}

		colInfo := domain.ColumnInfo{
			Name:          col.Name,
			Type:          col.Type,
			Nullable:      col.Nullable,
			Default:       defaultVal,
			AutoIncrement: col.AutoInc,
			Primary:       col.Primary,
			Unique:        col.Unique,
		}
		tableInfo.Columns = append(tableInfo.Columns, colInfo)
	}

	// Create table
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		return nil, fmt.Errorf("failed to create table '%s': %w", tableName, err)
	}

	// ENGINE=xml persistence setup
	if engine, ok := createStmt.Options["engine"].(string); ok && engine == "xml" {
		comment, _ := createStmt.Options["comment"].(string)
		mode := xmlpersist.ParseStorageMode(comment)
		cfg := &xmlpersist.TablePersistConfig{
			BasePath:    filepath.Join(databaseDir, targetDB),
			TableName:   tableName,
			RootTag:     "Row",
			StorageMode: mode,
		}
		if err := xmlpersist.PersistTableSchema(cfg, tableInfo); err != nil {
			log.Printf("warning: failed to persist schema for %s: %v", tableName, err)
		} else {
			s.registerTablePersistence(targetDB, tableName, cfg)
		}
	}

	// Return success result
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// executeUseStatement 执行 USE 语句
func (s *CoreSession) executeUseStatement(useStmt *parser.UseStatement) (*domain.QueryResult, error) {
	s.mu.RLock()
	closed := s.closed
	vdbReg := s.vdbRegistry
	dsMgr := s.dsManager
	s.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("session is closed")
	}

	dbName := useStmt.Database

	// 验证数据库是否存在，如果不存在则自动创建
	// 允许使用 information_schema 和所有已注册的虚拟数据库
	isVirtual := dbName == "information_schema" || (vdbReg != nil && vdbReg.IsVirtualDB(dbName))
	if !isVirtual {
		if dsMgr != nil {
			dsNames := dsMgr.List()
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
				if err := dsMgr.Register(dbName, memoryDS); err != nil {
					return nil, fmt.Errorf("failed to register database '%s': %w", dbName, err)
				}
			}
		}
	}

	// 设置当前数据库
	s.SetCurrentDB(dbName)

	// Load persisted tables from disk (ENGINE=xml)
	if !isVirtual && dsMgr != nil {
		s.loadPersistedTables(dbName)
	}

	// 返回成功结果
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// loadPersistedTables loads XML-persisted tables from disk into the memory datasource
func (s *CoreSession) loadPersistedTables(dbName string) {
	basePath := filepath.Join(s.databaseDir, dbName)
	configs, err := xmlpersist.LoadPersistedTables(basePath)
	if err != nil || len(configs) == 0 {
		return
	}

	ds, err := s.dsManager.Get(dbName)
	if err != nil {
		return
	}

	ctx := context.Background()
	for _, cfg := range configs {
		// Skip if already loaded (table already exists)
		if _, err := ds.GetTableInfo(ctx, cfg.TableName); err == nil {
			// Table exists, just register persistence config
			s.registerTablePersistence(dbName, cfg.TableName, cfg)
			continue
		}

		// Try batched loading through buffer pool for MVCC datasources.
		// This loads rows in page-sized batches, allowing the buffer pool to evict
		// cold pages and keep peak memory bounded regardless of table size.
		if mvccDS, ok := ds.(*memory.MVCCDataSource); ok {
			// Load schema and index metadata only (no data parsing)
			tableInfo, indexes, err := xmlpersist.LoadTableSchemaAndIndexes(cfg)
			if err != nil {
				log.Printf("warning: failed to load persisted table %s: %v", cfg.TableName, err)
				continue
			}

			// Create table in memory
			if err := ds.CreateTable(ctx, tableInfo); err != nil {
				log.Printf("warning: failed to create table %s from disk: %v", cfg.TableName, err)
				continue
			}

			// BulkLoad data through buffer pool — single pass, pages are registered incrementally
			pageSize := mvccDS.GetBufferPool().PageSize()
			var rowCount int
			if err := mvccDS.BulkLoad(cfg.TableName, func(addPage func([]domain.Row)) error {
				_, _, err := xmlpersist.LoadTableFromDiskBatched(cfg, pageSize, func(batch []domain.Row) {
					rowCount += len(batch)
					addPage(batch)
				})
				return err
			}); err != nil {
				log.Printf("warning: failed to bulk load data for table %s: %v", cfg.TableName, err)
			}

			// Rebuild indexes
			for _, idx := range indexes {
				if err := mvccDS.CreateIndexWithColumns(cfg.TableName, idx.Columns, idx.Type, idx.Unique); err != nil {
					log.Printf("warning: failed to create index %s on %s: %v", idx.Name, cfg.TableName, err)
				}
			}

			s.registerTablePersistence(dbName, cfg.TableName, cfg)
			log.Printf("loaded persisted table: %s.%s (%d rows, %d indexes)", dbName, cfg.TableName, rowCount, len(indexes))
			continue
		}

		// Fallback for non-MVCC datasources: use original full-load path
		tableInfo, rows, indexes, err := xmlpersist.LoadTableFromDisk(cfg)
		if err != nil {
			log.Printf("warning: failed to load persisted table %s: %v", cfg.TableName, err)
			continue
		}

		if err := ds.CreateTable(ctx, tableInfo); err != nil {
			log.Printf("warning: failed to create table %s from disk: %v", cfg.TableName, err)
			continue
		}

		if len(rows) > 0 {
			if _, err := ds.Insert(ctx, cfg.TableName, rows, nil); err != nil {
				log.Printf("warning: failed to insert data for table %s: %v", cfg.TableName, err)
			}
		}

		s.registerTablePersistence(dbName, cfg.TableName, cfg)
		log.Printf("loaded persisted table: %s.%s (%d rows, %d indexes)", dbName, cfg.TableName, len(rows), len(indexes))
	}
}

// executeSetStatement executes SET statement (SET NAMES, SET CHARACTER SET, etc.)
// Stores session variables so they can be queried via SELECT @@variable.
func (s *CoreSession) executeSetStatement(ctx context.Context, setStmt *parser.SetStatement) (*domain.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch setStmt.Type {
	case "NAMES":
		// SET NAMES 'charset' [COLLATE 'collation']
		// Sets character_set_client, character_set_connection, character_set_results
		charset := setStmt.Value
		if charset == "" {
			charset = "utf8mb4"
		}
		s.sessionVars["character_set_client"] = charset
		s.sessionVars["character_set_connection"] = charset
		s.sessionVars["character_set_results"] = charset

	case "CHARACTER SET":
		// SET CHARACTER SET charset
		charset := setStmt.Value
		if charset == "" {
			charset = "utf8mb4"
		}
		s.sessionVars["character_set_client"] = charset
		s.sessionVars["character_set_results"] = charset

	case "VARIABLE":
		// SET [SESSION|GLOBAL] var = value
		for varName, varValue := range setStmt.Variables {
			// Normalize: remove scope prefix and lowercase
			name := strings.ToLower(varName)
			name = strings.TrimPrefix(name, "global ")
			name = strings.TrimPrefix(name, "session ")
			s.sessionVars[name] = varValue
		}
	}

	// Sync session vars to executor for SELECT @@variable queries
	if s.executor != nil {
		s.executor.SetSessionVars(s.sessionVars)
	}

	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// GetSessionVar returns a session variable value, or empty string if not set
func (s *CoreSession) GetSessionVar(name string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.sessionVars[strings.ToLower(name)]
	return val, ok
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

// SetVirtualDBRegistry 设置虚拟数据库注册表
func (s *CoreSession) SetVirtualDBRegistry(registry *virtual.VirtualDatabaseRegistry) {
	s.vdbRegistry = registry
	if s.executor != nil {
		s.executor.SetVirtualDBRegistry(registry)
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
