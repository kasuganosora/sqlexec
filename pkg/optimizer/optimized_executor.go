package optimizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// OptimizedExecutor 优化的执行器
// 集成 Optimizer 和 QueryBuilder，提供优化后的查询执行
type OptimizedExecutor struct {
	dataSource  domain.DataSource
	dsManager   *application.DataSourceManager
	optimizer   *Optimizer
	useOptimizer bool
}

// NewOptimizedExecutor 创建优化的执行器
func NewOptimizedExecutor(dataSource domain.DataSource, useOptimizer bool) *OptimizedExecutor {
	return &OptimizedExecutor{
		dataSource:  dataSource,
		optimizer:   NewOptimizer(dataSource),
		useOptimizer: useOptimizer,
	}
}

// NewOptimizedExecutorWithDSManager 创建带有数据源管理器的优化执行器
func NewOptimizedExecutorWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer bool) *OptimizedExecutor {
	return &OptimizedExecutor{
		dataSource: dataSource,
		dsManager:  dsManager,
		optimizer:   NewOptimizer(dataSource),
		useOptimizer: useOptimizer,
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
func (e *OptimizedExecutor) GetOptimizer() interface{} {
	return e.optimizer
}

// ExecuteSelect 执行 SELECT 查询（支持优化）
func (e *OptimizedExecutor) ExecuteSelect(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	// Check if this is an information_schema query
	// information_schema queries should use QueryBuilder path to access virtual tables
	if e.isInformationSchemaQuery(stmt.From) {
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

// isInformationSchemaQuery 检查是否是 information_schema 查询
func (e *OptimizedExecutor) isInformationSchemaQuery(tableName string) bool {
	if e.dsManager == nil {
		return false
	}

	// Check for information_schema. prefix (case-insensitive)
	return strings.HasPrefix(strings.ToLower(tableName), "information_schema.")
}

// executeWithOptimizer 使用优化器执行查询
func (e *OptimizedExecutor) executeWithOptimizer(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	fmt.Println("  [DEBUG] 开始优化查询...")

	// 1. 构建 SQLStatement
	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}
	fmt.Println("  [DEBUG] SQLStatement构建完成")

	// 2. 优化查询计划
	fmt.Println("  [DEBUG] 调用 Optimize...")
	physicalPlan, err := e.optimizer.Optimize(ctx, sqlStmt)
	if err != nil {
		return nil, fmt.Errorf("optimizer failed: %w", err)
	}
	fmt.Println("  [DEBUG] Optimize完成")

	// 3. 执行物理计划
	fmt.Println("  [DEBUG] 开始执行物理计划...")
	result, err := physicalPlan.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute physical plan failed: %w", err)
	}
	fmt.Println("  [DEBUG] 物理计划执行完成")

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

	// Create information_schema virtual data source
	provider := information_schema.NewProvider(e.dsManager)
	return virtual.NewVirtualDataSource(provider)
}

// executeWithBuilder 使用 QueryBuilder 执行查询（传统路径）
func (e *OptimizedExecutor) executeWithBuilder(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	// If this is an information_schema query, use virtual data source
	if e.isInformationSchemaQuery(stmt.From) {
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
	if e.isInformationSchemaTable(stmt.Table) {
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
	if e.isInformationSchemaTable(stmt.Table) {
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
	if e.isInformationSchemaTable(stmt.Table) {
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

// filterColumns 过滤列信息
func filterColumns(columns []domain.ColumnInfo, selectCols []parser.SelectColumn) []domain.ColumnInfo {
	result := make([]domain.ColumnInfo, 0, len(selectCols))

	// 构建选择的列名映射
	selectMap := make(map[string]bool)
	for _, col := range selectCols {
		if !col.IsWildcard && col.Name != "" {
			selectMap[col.Name] = true
		}
	}

	// 过滤列
	for _, col := range columns {
		if selectMap[col.Name] {
			result = append(result, col)
		}
	}

	return result
}

// isInformationSchemaTable 检查表是否属于 information_schema
func (e *OptimizedExecutor) isInformationSchemaTable(tableName string) bool {
	if e.dsManager == nil {
		return false
	}

	// Check for information_schema. prefix (case-insensitive)
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "information_schema" {
			return true
		}
	}

	return false
}

