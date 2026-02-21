package optimizer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ShowExecutor SHOW 语句执行器
type ShowExecutor struct {
	currentDB  string
	dsManager  interface{} // 实际类型为 *application.DataSourceManager
	executeWithBuilder func(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error)
}

// NewShowExecutor 创建 SHOW 语句执行器
func NewShowExecutor(currentDB string, dsManager interface{}, executeWithBuilder func(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error)) *ShowExecutor {
	return &ShowExecutor{
		currentDB:          currentDB,
		dsManager:          dsManager,
		executeWithBuilder: executeWithBuilder,
	}
}

// SetCurrentDB 设置当前数据库
func (e *ShowExecutor) SetCurrentDB(dbName string) {
	e.currentDB = dbName
}

// ExecuteShow 执行 SHOW 语句 - 转换为 information_schema 查询
func (e *ShowExecutor) ExecuteShow(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	debugf("  [DEBUG] Executing SHOW statement: Type=%s, Table=%s, Like=%s, Where=%s\n",
		showStmt.Type, showStmt.Table, showStmt.Like, showStmt.Where)

	// 根据 SHOW 类型转换为相应的 information_schema 查询
	switch showStmt.Type {
	case "TABLES":
		return e.executeShowTables(ctx, showStmt)
	case "DATABASES":
		return e.executeShowDatabases(ctx, showStmt)
	case "COLUMNS":
		return e.executeShowColumns(ctx, showStmt)
	case "PROCESSLIST":
		return e.executeShowProcessList(ctx, showStmt.Full)
	case "VARIABLES":
		return e.executeShowVariables(ctx, showStmt)
	case "STATUS":
		return e.executeShowStatus(ctx, showStmt)
	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", showStmt.Type)
	}
}

// executeShowTables 执行 SHOW TABLES
func (e *ShowExecutor) executeShowTables(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
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
	debugf("  [DEBUG] SHOW TABLES converted to: %s, currentDB=%s\n", sql, currentDB)

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
}

// executeShowDatabases 执行 SHOW DATABASES
func (e *ShowExecutor) executeShowDatabases(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
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
	debugf("  [DEBUG] SHOW DATABASES converted to: %s\n", sql)

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
}

// executeShowColumns 执行 SHOW COLUMNS
func (e *ShowExecutor) executeShowColumns(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
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

	// 使用当前数据库作为 table_schema 过滤条件，避免跨库列混淆
	schemaFilter := ""
	if e.currentDB != "" {
		schemaFilter = fmt.Sprintf(" AND table_schema = '%s'", e.currentDB)
	}

	sql := fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_name = '%s'%s%s",
		showStmt.Table, schemaFilter, whereClause)
	debugf("  [DEBUG] SHOW COLUMNS converted to: %s\n", sql)

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
}

// executeShowProcessList 执行 SHOW PROCESSLIST
func (e *ShowExecutor) executeShowProcessList(ctx context.Context, full bool) (*domain.QueryResult, error) {
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
			"Id":      int64(threadID),
			"User":    user,
			"Host":    host,
			"db":      db,
			"Command": "Query",
			"Time":    timeSeconds,
			"State":   state,
			"Info":    info,
		}
		rows = append(rows, row)
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

// executeShowVariables executes SHOW VARIABLES
func (e *ShowExecutor) executeShowVariables(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	// Build variables from shared definitions (single source of truth)
	defs := information_schema.GetSystemVariableDefs()
	variables := make([]domain.Row, 0, len(defs))
	for _, v := range defs {
		variables = append(variables, domain.Row{
			"Variable_name": v.Name,
			"Value":         v.Value,
		})
	}

	// Apply LIKE filter if provided
	if showStmt.Like != "" {
		filtered := make([]domain.Row, 0)
		pattern := showStmt.Like
		// Remove quotes if present
		if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
			pattern = pattern[1 : len(pattern)-1]
		}
		for _, row := range variables {
			varName, _ := row["Variable_name"].(string)
			if matchLike(varName, pattern) {
				filtered = append(filtered, row)
			}
		}
		variables = filtered
	}

	columns := []domain.ColumnInfo{
		{Name: "Variable_name", Type: "VARCHAR"},
		{Name: "Value", Type: "VARCHAR"},
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    variables,
		Total:   int64(len(variables)),
	}, nil
}

// executeShowStatus executes SHOW STATUS
func (e *ShowExecutor) executeShowStatus(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	// Return basic status variables (all values must be strings per MySQL protocol)
	status := []domain.Row{
		{"Variable_name": "Threads_connected", "Value": "1"},
		{"Variable_name": "Threads_running", "Value": "1"},
		{"Variable_name": "Queries", "Value": "0"},
		{"Variable_name": "Uptime", "Value": "0"},
		{"Variable_name": "Connections", "Value": "1"},
		{"Variable_name": "Bytes_received", "Value": "0"},
		{"Variable_name": "Bytes_sent", "Value": "0"},
	}

	// Apply LIKE filter if provided
	if showStmt.Like != "" {
		filtered := make([]domain.Row, 0)
		pattern := showStmt.Like
		if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
			pattern = pattern[1 : len(pattern)-1]
		}
		for _, row := range status {
			varName, _ := row["Variable_name"].(string)
			if matchLike(varName, pattern) {
				filtered = append(filtered, row)
			}
		}
		status = filtered
	}

	columns := []domain.ColumnInfo{
		{Name: "Variable_name", Type: "VARCHAR"},
		{Name: "Value", Type: "VARCHAR"},
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    status,
		Total:   int64(len(status)),
	}, nil
}

// matchLike performs simple SQL LIKE pattern matching (case-insensitive)
func matchLike(s, pattern string) bool {
	// Convert both strings to lowercase for case-insensitive matching
	s = strings.ToLower(s)
	pattern = strings.ToLower(pattern)

	// Convert SQL LIKE pattern to simple wildcard matching
	// % matches any sequence, _ matches single character
	i, j := 0, 0
	for i < len(s) && j < len(pattern) {
		if pattern[j] == '%' {
			// Skip consecutive %
			for j < len(pattern) && pattern[j] == '%' {
				j++
			}
			if j == len(pattern) {
				return true
			}
			// Try to match rest of pattern
			for i < len(s) {
				if matchLike(s[i:], pattern[j:]) {
					return true
				}
				i++
			}
			return false
		} else if pattern[j] == '_' || pattern[j] == s[i] {
			i++
			j++
		} else {
			return false
		}
	}
	// Skip trailing %
	for j < len(pattern) && pattern[j] == '%' {
		j++
	}
	return i == len(s) && j == len(pattern)
}
