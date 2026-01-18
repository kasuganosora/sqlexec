package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/mysql/parser"
	"github.com/kasuganosora/sqlexec/mysql/resource"
)

// OptimizedExecutor 优化的执行器
// 集成 Optimizer 和 QueryBuilder，提供优化后的查询执行
type OptimizedExecutor struct {
	dataSource  resource.DataSource
	optimizer   *Optimizer
	useOptimizer bool
}

// NewOptimizedExecutor 创建优化的执行器
func NewOptimizedExecutor(dataSource resource.DataSource, useOptimizer bool) *OptimizedExecutor {
	return &OptimizedExecutor{
		dataSource:  dataSource,
		optimizer:   NewOptimizer(dataSource),
		useOptimizer: useOptimizer,
	}
}

// SetUseOptimizer 设置是否使用优化器
func (e *OptimizedExecutor) SetUseOptimizer(use bool) {
	e.useOptimizer = use
}

// ExecuteSelect 执行 SELECT 查询（支持优化）
func (e *OptimizedExecutor) ExecuteSelect(ctx context.Context, stmt *parser.SelectStatement) (*resource.QueryResult, error) {
	// 如果启用了优化器，使用优化路径
	if e.useOptimizer {
		return e.executeWithOptimizer(ctx, stmt)
	}

	// 否则使用传统的 QueryBuilder 路径
	return e.executeWithBuilder(ctx, stmt)
}

// executeWithOptimizer 使用优化器执行查询
func (e *OptimizedExecutor) executeWithOptimizer(ctx context.Context, stmt *parser.SelectStatement) (*resource.QueryResult, error) {
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

// executeWithBuilder 使用 QueryBuilder 执行查询（传统路径）
func (e *OptimizedExecutor) executeWithBuilder(ctx context.Context, stmt *parser.SelectStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	})
}

// ExecuteInsert 执行 INSERT
func (e *OptimizedExecutor) ExecuteInsert(ctx context.Context, stmt *parser.InsertStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeInsert,
		Insert: stmt,
	})
}

// ExecuteUpdate 执行 UPDATE
func (e *OptimizedExecutor) ExecuteUpdate(ctx context.Context, stmt *parser.UpdateStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeUpdate,
		Update: stmt,
	})
}

// ExecuteDelete 执行 DELETE
func (e *OptimizedExecutor) ExecuteDelete(ctx context.Context, stmt *parser.DeleteStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeDelete,
		Delete: stmt,
	})
}

// ExecuteCreate 执行 CREATE
func (e *OptimizedExecutor) ExecuteCreate(ctx context.Context, stmt *parser.CreateStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeCreate,
		Create: stmt,
	})
}

// ExecuteDrop 执行 DROP
func (e *OptimizedExecutor) ExecuteDrop(ctx context.Context, stmt *parser.DropStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:  parser.SQLTypeDrop,
		Drop:  stmt,
	})
}

// ExecuteAlter 执行 ALTER
func (e *OptimizedExecutor) ExecuteAlter(ctx context.Context, stmt *parser.AlterStatement) (*resource.QueryResult, error) {
	builder := parser.NewQueryBuilder(e.dataSource)
	return builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:  parser.SQLTypeAlter,
		Alter: stmt,
	})
}

// filterColumns 过滤列信息
func filterColumns(columns []resource.ColumnInfo, selectCols []parser.SelectColumn) []resource.ColumnInfo {
	result := make([]resource.ColumnInfo, 0, len(selectCols))

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
