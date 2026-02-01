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
	currentDB   string // 当前数据库名（用于 SELECT DATABASE()）
}

// NewOptimizedExecutor 创建优化的执行器
func NewOptimizedExecutor(dataSource domain.DataSource, useOptimizer bool) *OptimizedExecutor {
	return &OptimizedExecutor{
		dataSource:  dataSource,
		optimizer:   NewOptimizer(dataSource),
		useOptimizer: useOptimizer,
		currentDB:   "", // 默认为空字符串
	}
}

// NewOptimizedExecutorWithDSManager 创建带有数据源管理器的优化执行器
func NewOptimizedExecutorWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer bool) *OptimizedExecutor {
	return &OptimizedExecutor{
		dataSource: dataSource,
		dsManager:  dsManager,
		optimizer:   NewOptimizer(dataSource),
		useOptimizer: useOptimizer,
		currentDB:   "default", // 默认数据库
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

// SetCurrentDB 设置当前数据库
func (e *OptimizedExecutor) SetCurrentDB(dbName string) {
	e.currentDB = dbName
	fmt.Printf("  [DEBUG] OptimizedExecutor.SetCurrentDB: currentDB 设置为 %q\n", dbName)
}

// GetCurrentDB 获取当前数据库
func (e *OptimizedExecutor) GetCurrentDB() string {
	return e.currentDB
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

// ExecuteShow 执行 SHOW 语句 - 转换为 information_schema 查询
func (e *OptimizedExecutor) ExecuteShow(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	fmt.Printf("  [DEBUG] Executing SHOW statement: Type=%s, Table=%s, Like=%s, Where=%s\n",
		showStmt.Type, showStmt.Table, showStmt.Like, showStmt.Where)

	// 根据 SHOW 类型转换为相应的 information_schema 查询
	switch showStmt.Type {
	case "TABLES":
		// SHOW TABLES -> SELECT table_name FROM information_schema.tables WHERE table_schema = ?
		var whereClause string
		if showStmt.Like != "" {
			whereClause = fmt.Sprintf(" AND table_name LIKE '%s'", showStmt.Like)
		}
		if showStmt.Where != "" {
			whereClause = fmt.Sprintf(" AND (%s)", showStmt.Where)
		}

		// 获取当前数据库（从 session 上下文）
		currentDB := e.currentDB
		if showStmt.Table != "" {
			// 如果指定了数据库，使用指定的
			currentDB = showStmt.Table
		}

		// 构建 SQL 语句
		sql := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'%s",
			currentDB, whereClause)
		fmt.Printf("  [DEBUG] SHOW TABLES converted to: %s, currentDB=%s\n", sql, currentDB)

		// 解析 SQL
		adapter := parser.NewSQLAdapter()
		parseResult, err := adapter.Parse(sql)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SHOW TABLES query: %w", err)
		}

		if parseResult.Statement.Select == nil {
			return nil, fmt.Errorf("SHOW TABLES conversion failed: not a SELECT statement")
		}

		return e.executeWithBuilder(ctx, parseResult.Statement.Select)

	case "DATABASES":
		// SHOW DATABASES -> SELECT schema_name FROM information_schema.schemata
		var whereClause string
		if showStmt.Like != "" {
			whereClause = fmt.Sprintf(" WHERE schema_name LIKE '%s'", showStmt.Like)
		}
		if showStmt.Where != "" {
			if whereClause == "" {
				whereClause = fmt.Sprintf(" WHERE (%s)", showStmt.Where)
			} else {
				whereClause = fmt.Sprintf("%s AND (%s)", whereClause, showStmt.Where)
			}
		}

		sql := fmt.Sprintf("SELECT schema_name FROM information_schema.schemata%s", whereClause)
		fmt.Printf("  [DEBUG] SHOW DATABASES converted to: %s\n", sql)

		// 解析 SQL
		adapter := parser.NewSQLAdapter()
		parseResult, err := adapter.Parse(sql)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SHOW DATABASES query: %w", err)
		}

		if parseResult.Statement.Select == nil {
			return nil, fmt.Errorf("SHOW DATABASES conversion failed: not a SELECT statement")
		}

		return e.executeWithBuilder(ctx, parseResult.Statement.Select)

	case "COLUMNS":
		// SHOW COLUMNS FROM table -> SELECT * FROM information_schema.columns WHERE table_name = ?
		if showStmt.Table == "" {
			return nil, fmt.Errorf("SHOW COLUMNS requires a table name")
		}

		var whereClause string
		if showStmt.Like != "" {
			whereClause = fmt.Sprintf(" AND column_name LIKE '%s'", showStmt.Like)
		}
		if showStmt.Where != "" {
			whereClause = fmt.Sprintf(" AND (%s)", showStmt.Where)
		}

		sql := fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_name = '%s'%s",
			showStmt.Table, whereClause)
		fmt.Printf("  [DEBUG] SHOW COLUMNS converted to: %s\n", sql)

		// 解析 SQL
		adapter := parser.NewSQLAdapter()
		parseResult, err := adapter.Parse(sql)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SHOW COLUMNS query: %w", err)
		}

		if parseResult.Statement.Select == nil {
			return nil, fmt.Errorf("SHOW COLUMNS conversion failed: not a SELECT statement")
		}

		return e.executeWithBuilder(ctx, parseResult.Statement.Select)

	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", showStmt.Type)
	}
}

// isInformationSchemaQuery 检查是否是 information_schema 查询
func (e *OptimizedExecutor) isInformationSchemaQuery(tableName string) bool {
	if e.dsManager == nil {
		return false
	}

	// 空表名不是 information_schema 查询（如 SELECT DATABASE()）
	if tableName == "" {
		return false
	}

	// Check for information_schema. prefix (case-insensitive)
	if strings.HasPrefix(strings.ToLower(tableName), "information_schema.") {
		return true
	}

	// 检查当前数据库是否为 information_schema
	if strings.EqualFold(e.currentDB, "information_schema") {
		return true
	}

	return false
}

// handleNoFromQuery 处理没有 FROM 子句的查询（如 SELECT DATABASE()）
func (e *OptimizedExecutor) handleNoFromQuery(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	fmt.Println("  [DEBUG] handleNoFromQuery: 开始处理")
	fmt.Printf("  [DEBUG] handleNoFromQuery: e.currentDB = %q\n", e.currentDB)

	// 检查是否是 SELECT DATABASE()
	if len(stmt.Columns) == 1 {
		col := stmt.Columns[0]
		if col.Expr != nil && col.Expr.Type == parser.ExprTypeFunction {
			funcName := strings.ToUpper(col.Expr.Function)
			if funcName == "DATABASE" {
				fmt.Println("  [DEBUG] handleNoFromQuery: 识别为 SELECT DATABASE(), 当前数据库:", e.currentDB)

				// 使用列别名（如果有）或默认名称
				colName := "DATABASE()"
				if col.Alias != "" {
					colName = col.Alias
				}

				result := &domain.QueryResult{
					Columns: []domain.ColumnInfo{
						{Name: colName, Type: "string"},
					},
					Rows: []domain.Row{
						{colName: e.currentDB},
					},
					Total: 1,
				}
				return result, nil
			}
		}

		// 处理其他系统变量（如 @@version_comment）
		if col.Expr != nil && col.Expr.Type == parser.ExprTypeColumn {
			colName := strings.ToUpper(col.Expr.Column)
			if colName == "@@VERSION_COMMENT" {
				fmt.Println("  [DEBUG] handleNoFromQuery: 识别为 SELECT @@version_comment")
				result := &domain.QueryResult{
					Columns: []domain.ColumnInfo{
						{Name: "@@version_comment", Type: "string"},
					},
					Rows: []domain.Row{
						{"@@version_comment": "sqlexec MySQL-compatible database"},
					},
					Total: 1,
				}
				return result, nil
			}
		}
	}

	// 其他无 FROM 子句的查询暂时不支持
	return nil, fmt.Errorf("unsupported query without FROM clause")
}

// executeWithOptimizer 使用优化器执行查询
func (e *OptimizedExecutor) executeWithOptimizer(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	fmt.Println("  [DEBUG] 开始优化查询...")

	// 处理没有 FROM 子句的查询（如 SELECT DATABASE()）
	if stmt.From == "" {
		fmt.Println("  [DEBUG] 检测到无 FROM 子句的查询")
		return e.handleNoFromQuery(ctx, stmt)
	}

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

