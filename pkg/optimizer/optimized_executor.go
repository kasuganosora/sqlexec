package optimizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/executor"
	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// ProcessListProvider 进程列表提供者函数类型（用于避免循环依赖）
type ProcessListProvider func() []interface{}

var processListProvider ProcessListProvider

// RegisterProcessListProvider 注册进程列表提供者
func RegisterProcessListProvider(provider ProcessListProvider) {
	processListProvider = provider
}

// OptimizedExecutor 优化的执行器
// 集成 Optimizer 和 QueryBuilder，提供优化后的查询执行
type OptimizedExecutor struct {
	dataSource    domain.DataSource
	dsManager     *application.DataSourceManager
	optimizer     *EnhancedOptimizer // 统一使用增强优化器
	planExecutor  executor.Executor // 新的执行器
	useOptimizer  bool
	currentDB     string
	currentUser   string // 当前用户（用于权限检查）
	functionAPI   *builtin.FunctionAPI // 函数API
	exprEvaluator *ExpressionEvaluator // 表达式求值器
}

// contextKey 是context中的key类型
type contextKey int

const (
	aclManagerKey contextKey = iota
)

// NewOptimizedExecutor 创建优化的执行器
func NewOptimizedExecutor(dataSource domain.DataSource, useOptimizer bool) *OptimizedExecutor {
	return newOptimizedExecutor(dataSource, nil, useOptimizer, "")
}

// NewOptimizedExecutorWithDSManager 创建带有数据源管理器的优化执行器
func NewOptimizedExecutorWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer bool) *OptimizedExecutor {
	return newOptimizedExecutor(dataSource, dsManager, useOptimizer, "default")
}

// newOptimizedExecutor 内部构造函数，合并公共逻辑
func newOptimizedExecutor(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer bool, defaultDB string) *OptimizedExecutor {
	functionAPI := builtin.NewFunctionAPI()
	// 使用包装器将旧的 FunctionRegistry 适配到新的 FunctionAPI
	registry := builtin.GetGlobalRegistry()
	// 注册所有旧的全局函数到新的API
	for _, info := range registry.List() {
		functionAPI.RegisterScalarFunction(
			info.Name,
			info.Name,
			info.Description,
			info.Handler,
		)
	}

	// 统一使用增强优化器
	opt := NewEnhancedOptimizer(dataSource, 0) // parallelism=0 表示自动选择最优并行度

	// 创建数据访问服务
	dataAccessService := dataaccess.NewDataService(dataSource)
	// 创建执行器
	planExecutor := executor.NewExecutor(dataAccessService)

	return &OptimizedExecutor{
		dataSource:    dataSource,
		dsManager:     dsManager,
		optimizer:     opt,
		planExecutor:  planExecutor,
		useOptimizer:  useOptimizer,
		currentDB:     defaultDB,
		functionAPI:   functionAPI,
		exprEvaluator: NewExpressionEvaluator(functionAPI),
	}
}

// SetUseOptimizer 设置是否使用优化器
func (e *OptimizedExecutor) SetUseOptimizer(use bool) {
	e.useOptimizer = use
}

// GetQueryBuilder 获取底层的 QueryBuilder（如果存在）
// 用于设置当前数据库上下文
func (e *OptimizedExecutor) GetQueryBuilder() interface{} {
	return nil
}

// GetOptimizer 获取优化器
func (e *OptimizedExecutor) GetOptimizer() *EnhancedOptimizer {
	return e.optimizer
}

// SetCurrentDB 设置当前数据库
func (e *OptimizedExecutor) SetCurrentDB(dbName string) {
	e.currentDB = dbName
	fmt.Printf("  [DEBUG] OptimizedExecutor.SetCurrentDB: currentDB 设置为 %q\n", dbName)
}

// GetCurrentDB 获取当前数据库
func (e *OptimizedExecutor) GetCurrentDB() string {
	return e.currentDB
}

// SetCurrentUser 设置当前用户
func (e *OptimizedExecutor) SetCurrentUser(user string) {
	e.currentUser = user
	fmt.Printf("  [DEBUG] OptimizedExecutor.SetCurrentUser: 当前用户设置为 %q\n", user)
}

// GetCurrentUser 获取当前用户
func (e *OptimizedExecutor) GetCurrentUser() string {
	return e.currentUser
}

// ExecuteSelect 执行 SELECT 查询（支持优化）
func (e *OptimizedExecutor) ExecuteSelect(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	// 将用户信息传递到 context（用于权限检查）
	if e.currentUser != "" {
		ctx = context.WithValue(ctx, "user", e.currentUser)
	}

	// Check if this is an information_schema query
	// information_schema queries should use QueryBuilder path to access virtual tables
	if isInformationSchemaQuery(stmt.From, e.currentDB, e.dsManager) {
		fmt.Println("  [DEBUG] Detected information_schema query, using QueryBuilder path")
		return e.executeWithBuilder(ctx, stmt)
	}

	// 如果启用了优化器，使用优化路径
	if e.useOptimizer {
		return e.executeWithOptimizer(ctx, stmt)
	}

	// 否则使用传统的 QueryBuilder 路径
	return e.executeWithBuilder(ctx, stmt)
}

// ExecuteShow 执行 SHOW 语句 - 转换为 information_schema 查询
func (e *OptimizedExecutor) ExecuteShow(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	// 将用户信息传递到 context（用于权限检查）
	if e.currentUser != "" {
		ctx = context.WithValue(ctx, "user", e.currentUser)
	}

	showExecutor := NewShowExecutor(e.currentDB, e.dsManager, e.executeWithBuilder)
	return showExecutor.ExecuteShow(ctx, showStmt)
}

// executeWithOptimizer 使用优化器执行查询
func (e *OptimizedExecutor) executeWithOptimizer(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	fmt.Println("  [DEBUG] 开始优化查询...")

	// 处理没有 FROM 子句的查询（如 SELECT DATABASE()）
	if stmt.From == "" {
		fmt.Println("  [DEBUG] 检测到无 FROM 子句的查询")
		exprExecutor := NewExpressionExecutor(e.currentDB, e.functionAPI, e.exprEvaluator)
		result, err := exprExecutor.HandleNoFromQuery(stmt)
		if err != nil {
			return nil, err
		}
		return &domain.QueryResult{
			Columns: result.Columns,
			Rows:    convertToDomainRows(result.Rows),
			Total:   result.Total,
		}, nil
	}

	// 再次检查是否是 information_schema 查询
	// 因为 optimizer 路径不支持 information_schema 虚拟表
	if isInformationSchemaQuery(stmt.From, e.currentDB, e.dsManager) {
		fmt.Println("  [DEBUG] 在 optimizer 路径中检测到 information_schema 查询，切换到 builder 路径")
		return e.executeWithBuilder(ctx, stmt)
	}

	// 1. 构建 SQLStatement
	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}
	fmt.Println("  [DEBUG] SQLStatement构建完成")

	// 2. 优化查询计划
	fmt.Println("  [DEBUG] 调用 Optimize...")
	executionPlan, err := e.optimizer.Optimize(ctx, sqlStmt)

	if err != nil {
		return nil, fmt.Errorf("optimizer failed: %w", err)
	}
	fmt.Println("  [DEBUG] Optimize完成")

	// 3. 执行计划（使用新的 executor）
	fmt.Println("  [DEBUG] 开始执行计划...")
	result, err := e.executePlan(ctx, executionPlan)
	if err != nil {
		return nil, fmt.Errorf("execute plan failed: %w", err)
	}
	fmt.Println("  [DEBUG] 计划执行完成")

	// 4. 设置列信息
	tableInfo, err := e.dataSource.GetTableInfo(ctx, stmt.From)
	if err == nil {
		// 根据选择的列过滤
		if !isWildcard(stmt.Columns) {
			result.Columns = filterColumns(tableInfo.Columns, stmt.Columns)
		} else {
			result.Columns = tableInfo.Columns
		}
	}

	return result, nil
}

// getVirtualDataSource 获取 information_schema 虚拟数据源
func (e *OptimizedExecutor) getVirtualDataSource() domain.DataSource {
	if e.dsManager == nil {
		return nil
	}

	// 尝试获取全局ACL Manager adapter
	aclAdapter := information_schema.GetACLManagerAdapter()

	// 使用ACL Manager adapter创建provider，以便权限表可以正常工作
	if aclAdapter != nil {
		provider := information_schema.NewProviderWithACL(e.dsManager, aclAdapter)
		return virtual.NewVirtualDataSource(provider)
	}

	// 如果没有ACL Manager，使用不带ACL的provider
	provider := information_schema.NewProvider(e.dsManager)
	return virtual.NewVirtualDataSource(provider)
}

// executePlan 使用新的执行器执行计划
func (e *OptimizedExecutor) executePlan(ctx context.Context, executionPlan *plan.Plan) (*domain.QueryResult, error) {
	if e.planExecutor == nil {
		return nil, fmt.Errorf("plan executor not initialized")
	}
	
	return e.planExecutor.Execute(ctx, executionPlan)
}

// executeWithBuilder 使用 QueryBuilder 执行查询（传统路径）
func (e *OptimizedExecutor) executeWithBuilder(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	// If this is an information_schema query, use virtual data source
	if isInformationSchemaQuery(stmt.From, e.currentDB, e.dsManager) {
		vds := e.getVirtualDataSource()
		if vds != nil {
			// Strip the "information_schema." prefix from the table name
			tableName := stmt.From
			if strings.HasPrefix(strings.ToLower(tableName), "information_schema.") {
				tableName = strings.TrimPrefix(tableName, "information_schema.")
				// Also handle case where prefix is "INFORMATION_SCHEMA."
				tableName = strings.TrimPrefix(tableName, "INFORMATION_SCHEMA.")
			}

			// Create a new SelectStatement with the stripped table name
			newStmt := *stmt
			newStmt.From = tableName

			builder := parser.NewQueryBuilder(vds)
			return builder.ExecuteStatement(ctx, &parser.SQLStatement{
				Type:   parser.SQLTypeSelect,
				Select: &newStmt,
			})
		}
	}

	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	})
}

// ExecuteInsert 执行 INSERT
func (e *OptimizedExecutor) ExecuteInsert(ctx context.Context, stmt *parser.InsertStatement) (*domain.QueryResult, error) {
	// Check if trying to INSERT into information_schema
	if isInformationSchemaTable(stmt.Table) {
		return nil, fmt.Errorf("information_schema is read-only: INSERT operation not supported")
	}

	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeInsert,
		Insert: stmt,
	})
}

// ExecuteUpdate 执行 UPDATE
func (e *OptimizedExecutor) ExecuteUpdate(ctx context.Context, stmt *parser.UpdateStatement) (*domain.QueryResult, error) {
	// Check if trying to UPDATE information_schema
	if isInformationSchemaTable(stmt.Table) {
		return nil, fmt.Errorf("information_schema is read-only: UPDATE operation not supported")
	}

	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeUpdate,
		Update: stmt,
	})
}

// ExecuteDelete 执行 DELETE
func (e *OptimizedExecutor) ExecuteDelete(ctx context.Context, stmt *parser.DeleteStatement) (*domain.QueryResult, error) {
	// Check if trying to DELETE from information_schema
	if isInformationSchemaTable(stmt.Table) {
		return nil, fmt.Errorf("information_schema is read-only: DELETE operation not supported")
	}

	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeDelete,
		Delete: stmt,
	})
}

// ExecuteCreate 执行 CREATE
func (e *OptimizedExecutor) ExecuteCreate(ctx context.Context, stmt *parser.CreateStatement) (*domain.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeCreate,
		Create: stmt,
	})
}

// ExecuteDrop 执行 DROP
func (e *OptimizedExecutor) ExecuteDrop(ctx context.Context, stmt *parser.DropStatement) (*domain.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:  parser.SQLTypeDrop,
		Drop:  stmt,
	})
}

// ExecuteAlter 执行 ALTER
func (e *OptimizedExecutor) ExecuteAlter(ctx context.Context, stmt *parser.AlterStatement) (*domain.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:  parser.SQLTypeAlter,
		Alter: stmt,
	})
}

// ExecuteCreateIndex 执行 CREATE INDEX
func (e *OptimizedExecutor) ExecuteCreateIndex(ctx context.Context, stmt *parser.CreateIndexStatement) (*domain.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:       parser.SQLTypeCreate,
		CreateIndex: stmt,
	})
}

// ExecuteDropIndex 执行 DROP INDEX
func (e *OptimizedExecutor) ExecuteDropIndex(ctx context.Context, stmt *parser.DropIndexStatement) (*domain.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:     parser.SQLTypeDrop,
		DropIndex: stmt,
	})
}

// convertToDomainRows 将 map 行转换为 domain.Row
func convertToDomainRows(rows []map[string]interface{}) []domain.Row {
	result := make([]domain.Row, len(rows))
	for i, row := range rows {
		result[i] = domain.Row(row)
	}
	return result
}

