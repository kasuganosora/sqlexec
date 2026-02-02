package optimizer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
	"github.com/kasuganosora/sqlexec/pkg/information_schema"
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
	optimizer     *Optimizer
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

	return &OptimizedExecutor{
		dataSource:    dataSource,
		optimizer:     NewOptimizer(dataSource),
		useOptimizer:  useOptimizer,
		currentDB:     "", // 默认为空字符串
		functionAPI:   functionAPI,
		exprEvaluator: NewExpressionEvaluator(functionAPI),
	}
}

// NewOptimizedExecutorWithDSManager 创建带有数据源管理器的优化执行器
func NewOptimizedExecutorWithDSManager(dataSource domain.DataSource, dsManager *application.DataSourceManager, useOptimizer bool) *OptimizedExecutor {
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

	return &OptimizedExecutor{
		dataSource:    dataSource,
		dsManager:     dsManager,
		optimizer:     NewOptimizer(dataSource),
		useOptimizer:  useOptimizer,
		currentDB:     "default", // 默认数据库
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

	// 将用户信息传递到 context（用于权限检查）
	if e.currentUser != "" {
		ctx = context.WithValue(ctx, "user", e.currentUser)
		fmt.Printf("  [DEBUG] ExecuteShow: 设置用户到context: %s\n", e.currentUser)
	}

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

	case "PROCESSLIST":
		// SHOW PROCESSLIST - 从查询注册表获取所有查询
		return e.executeShowProcessList(ctx, showStmt.Full)

	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", showStmt.Type)
	}
}

// executeShowProcessList 执行 SHOW PROCESSLIST
func (e *OptimizedExecutor) executeShowProcessList(ctx context.Context, full bool) (*domain.QueryResult, error) {
	// 使用进程列表提供者获取查询列表
	var processList []interface{}
	if processListProvider != nil {
		processList = processListProvider()
	}

	// 定义 PROCESSLIST 字段
	columns := []domain.ColumnInfo{
		{Name: "Id", Type: "BIGINT UNSIGNED"},
		{Name: "User", Type: "VARCHAR"},
		{Name: "Host", Type: "VARCHAR"},
		{Name: "db", Type: "VARCHAR"},
		{Name: "Command", Type: "VARCHAR"},
		{Name: "Time", Type: "BIGINT UNSIGNED"},
		{Name: "State", Type: "VARCHAR"},
		{Name: "Info", Type: "TEXT"},
	}

	// 构建结果行
	rows := make([]domain.Row, 0, len(processList))
	for _, item := range processList {
		// 使用类型断言和反射来访问字段
		// 由于避免循环依赖，我们假设 item 是一个结构体，包含 QueryID, ThreadID, SQL, StartTime, Duration, Status, User, Host, DB 字段
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			// 如果不是 map，跳过
			continue
		}

		threadID, _ := itemMap["ThreadID"].(uint32)
		sql, _ := itemMap["SQL"].(string)
		duration, _ := itemMap["Duration"].(time.Duration)
		status, _ := itemMap["Status"].(string)
		user, _ := itemMap["User"].(string)
		host, _ := itemMap["Host"].(string)
		db, _ := itemMap["DB"].(string)

		timeSeconds := uint64(duration.Seconds())

		// 获取 Info 字段
		info := sql
		if !full && len(info) > 100 {
			info = info[:100]
		}

		// 构建 State
		state := "executing"
		if status == "canceled" {
			state = "killed"
		} else if status == "timeout" {
			state = "timeout"
		}

		// User 和 Host 的默认值
		if user == "" {
			user = "user"
		}
		if host == "" {
			host = "localhost:3306"
		}

		row := domain.Row{
			"Id":     int64(threadID),
			"User":   user,
			"Host":   host,
			"db":     db,
			"Command": "Query",
			"Time":   timeSeconds,
			"State":  state,
			"Info":   info,
		}
		rows = append(rows, row)
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
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

// handleNoFromQuery 处理没有 FROM 子句的查询（如 SELECT DATABASE(), SELECT NOW()）
func (e *OptimizedExecutor) handleNoFromQuery(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	fmt.Println("  [DEBUG] handleNoFromQuery: 开始处理")
	fmt.Printf("  [DEBUG] handleNoFromQuery: e.currentDB = %q\n", e.currentDB)

	// 构建空 row（用于表达式求值）
	row := make(parser.Row)

	// 处理多个列
	columns := make([]domain.ColumnInfo, 0, len(stmt.Columns))
	rowData := make(domain.Row)
	colIdx := 0

	for _, col := range stmt.Columns {
		if col.Expr == nil {
			return nil, fmt.Errorf("column expression is nil")
		}

		// 确定列名
		colName := col.Alias
		if colName == "" {
			colName = col.Name
		}
		if colName == "" {
			// 如果没有别名和名称，根据表达式生成列名
			colName = e.generateColumnName(col.Expr)
		}

		// 特殊处理函数调用：如果有名称但没有括号，添加括号
		if col.Expr.Type == parser.ExprTypeFunction && colName != "" && !strings.HasSuffix(colName, "()") {
			colName = colName + "()"
		}

		// 特殊处理：如果生成的列名是 NULL，尝试从表达式中提取更多信息
		if colName == "NULL" {
			// 对于无法确定列名的情况，使用默认名称
			colName = fmt.Sprintf("expr_%d", colIdx)
		}

		fmt.Printf("  [DEBUG] handleNoFromQuery: 处理列 %s, 表达式类型=%s\n", colName, col.Expr.Type)

		// 计算表达式值
		value, err := e.evaluateNoFromExpression(col.Expr, row)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate expression for column %s: %w", colName, err)
		}

		// 确定列类型
		colType := e.inferType(value)

		columns = append(columns, domain.ColumnInfo{Name: colName, Type: colType})
		rowData[colName] = value
		colIdx++
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    []domain.Row{rowData},
		Total:   1,
	}, nil
}

// generateColumnName 根据表达式生成列名
func (e *OptimizedExecutor) generateColumnName(expr *parser.Expression) string {
	// 特殊处理：如果是系统变量，使用 Column 字段
	if expr.Column != "" && (strings.HasPrefix(expr.Column, "@@") || strings.HasPrefix(expr.Column, "@")) {
		return expr.Column
	}

	switch expr.Type {
	case parser.ExprTypeValue:
		// 常量值：使用值的字符串表示
		if expr.Value != nil {
			return fmt.Sprintf("%v", expr.Value)
		}
		// Value 为 nil 的情况，可能是系统变量或 NULL
		return "NULL"

	case parser.ExprTypeFunction:
		// 函数调用：使用函数名()
		if expr.Function != "" {
			return expr.Function + "()"
		}
		return "function"

	case parser.ExprTypeOperator:
		// 运算符表达式：递归生成操作数和运算符
		return e.generateOperatorColumnName(expr)

	case parser.ExprTypeColumn:
		// 列引用：使用列名
		if expr.Column != "" {
			return expr.Column
		}
		return "column"

	default:
		return "expr"
	}
}

// generateOperatorColumnName 为运算符表达式生成列名
func (e *OptimizedExecutor) generateOperatorColumnName(expr *parser.Expression) string {
	if expr.Operator == "" {
		return "expr"
	}

	// 将解析器的运算符名称转换为SQL符号
	opSymbol := e.operatorToSQL(expr.Operator)

	if expr.Left != nil {
		leftName := e.generateColumnName(expr.Left)
		if expr.Right != nil {
			rightName := e.generateColumnName(expr.Right)
			// 二元运算符
			return leftName + opSymbol + rightName
		}
		// 一元运算符
		return opSymbol + leftName
	}

	if expr.Right != nil {
		rightName := e.generateColumnName(expr.Right)
		return opSymbol + rightName
	}

	return "expr"
}

// operatorToSQL 将解析器的运算符名称转换为SQL符号
func (e *OptimizedExecutor) operatorToSQL(op string) string {
	switch strings.ToLower(op) {
	case "plus":
		return "+"
	case "minus":
		return "-"
	case "mul":
		return "*"
	case "div":
		return "/"
	case "eq":
		return "="
	case "neq":
		return "!="
	case "gt":
		return ">"
	case "gte":
		return ">="
	case "lt":
		return "<"
	case "lte":
		return "<="
	case "and":
		return " AND "
	case "or":
		return " OR "
	case "not":
		return "NOT "
	case "like":
		return " LIKE "
	default:
		return op
	}
}

// evaluateNoFromExpression 评估无 FROM 子句的表达式
func (e *OptimizedExecutor) evaluateNoFromExpression(expr *parser.Expression, row parser.Row) (interface{}, error) {
	// 特殊处理：如果 Column 字段包含系统变量（即使 Type 不是 COLUMN）
	if expr.Column != "" && (strings.HasPrefix(expr.Column, "@@") || strings.HasPrefix(expr.Column, "@")) {
		return e.evaluateVariable(expr.Column)
	}

	switch expr.Type {
	case parser.ExprTypeValue:
		// 常量值
		fmt.Printf("  [DEBUG] evaluateNoFromExpression: 常量值=%v\n", expr.Value)
		// 特殊处理：如果 Value 为 nil，返回默认的系统变量值
		// 注意：这是启发式方法，因为解析器无法提供原始变量名
		if expr.Value == nil {
			// 返回 @@version_comment 的默认值
			return "sqlexec MySQL-compatible database", nil
		}
		return expr.Value, nil

	case parser.ExprTypeColumn:
		// 变量引用（系统变量、会话变量）
		if expr.Column != "" {
			return e.evaluateVariable(expr.Column)
		}
		return nil, fmt.Errorf("column reference without column name")

	case parser.ExprTypeFunction:
		// 函数调用
		return e.evaluateFunctionExpression(expr, row)

	case parser.ExprTypeOperator:
		// 运算符表达式
		return e.exprEvaluator.Evaluate(expr, row)

	default:
		return nil, fmt.Errorf("unsupported expression type: %s", expr.Type)
	}
}

// evaluateVariable 评估变量（系统变量或会话变量）
func (e *OptimizedExecutor) evaluateVariable(colName string) (interface{}, error) {
	varName := strings.ToUpper(strings.TrimSpace(colName))

	fmt.Printf("  [DEBUG] evaluateVariable: 变量名=%s\n", varName)

	// 处理系统变量（@@variable）
	if strings.HasPrefix(varName, "@@") {
		return e.evaluateSystemVariable(varName)
	}

	// 处理会话变量（@variable）
	if strings.HasPrefix(varName, "@") && !strings.HasPrefix(varName, "@@") {
		return e.evaluateSessionVariable(varName)
	}

	return nil, fmt.Errorf("unsupported variable: %s", colName)
}

// evaluateSystemVariable 评估系统变量
func (e *OptimizedExecutor) evaluateSystemVariable(varName string) (interface{}, error) {
	// 移除 @@ 前缀
	name := strings.TrimPrefix(varName, "@@")

	// 移除作用域前缀（@@global., @@session., @@local.）
	name = strings.TrimPrefix(name, "GLOBAL.")
	name = strings.TrimPrefix(name, "SESSION.")
	name = strings.TrimPrefix(name, "LOCAL.")

	fmt.Printf("  [DEBUG] evaluateSystemVariable: 系统变量=%s\n", name)

	// 处理已知的系统变量
	switch name {
	case "VERSION_COMMENT", "@@VERSION_COMMENT":
		return "sqlexec MySQL-compatible database", nil
	case "VERSION":
		return "8.0.0-sqlexec", nil
	case "PORT":
		return 3307, nil
	case "HOSTNAME":
		return "localhost", nil
	case "DATADIR":
		return "/var/lib/mysql", nil
	case "SERVER_ID":
		return 1, nil
	default:
		return nil, fmt.Errorf("unknown system variable: %s", name)
	}
}

// evaluateSessionVariable 评估会话变量
func (e *OptimizedExecutor) evaluateSessionVariable(varName string) (interface{}, error) {
	// 移除 @ 前缀
	name := strings.TrimPrefix(varName, "@")

	fmt.Printf("  [DEBUG] evaluateSessionVariable: 会话变量=%s\n", name)

	// 当前实现中，我们无法访问 session 对象
	// 这是一个限制，需要在未来改进架构
	return nil, fmt.Errorf("session variables not yet supported in no-FROM queries: %s", name)
}

// evaluateFunctionExpression 评估函数表达式
func (e *OptimizedExecutor) evaluateFunctionExpression(expr *parser.Expression, row parser.Row) (interface{}, error) {
	funcName := strings.ToUpper(expr.Function)

	fmt.Printf("  [DEBUG] evaluateFunctionExpression: 函数名=%s\n", funcName)

	// 特殊处理 DATABASE() 函数（因为它需要当前数据库上下文）
	if funcName == "DATABASE" {
		return e.currentDB, nil
	}

	// 对于其他函数，使用 ExpressionEvaluator
	return e.exprEvaluator.Evaluate(expr, row)
}

// inferType 推断值的类型
func (e *OptimizedExecutor) inferType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch value.(type) {
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "bool"
	case string:
		return "string"
	case time.Time:
		return "datetime"
	default:
		return "string"
	}
}

// executeWithOptimizer 使用优化器执行查询
func (e *OptimizedExecutor) executeWithOptimizer(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error) {
	fmt.Println("  [DEBUG] 开始优化查询...")

	// 处理没有 FROM 子句的查询（如 SELECT DATABASE()）
	if stmt.From == "" {
		fmt.Println("  [DEBUG] 检测到无 FROM 子句的查询")
		return e.handleNoFromQuery(ctx, stmt)
	}

	// 再次检查是否是 information_schema 查询
	// 因为 optimizer 路径不支持 information_schema 虚拟表
	if e.isInformationSchemaQuery(stmt.From) {
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

