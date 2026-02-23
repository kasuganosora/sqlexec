package optimizer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/executor"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// DefaultShowProcessor 是 ShowProcessor 接口的默认实现
type DefaultShowProcessor struct {
	dataSource          domain.DataSource
	dsManager           interface{} // *application.DataSourceManager
	currentDB           string
	processListProvider ProcessListProvider
}

// NewDefaultShowProcessor 创建默认的 SHOW 处理器
func NewDefaultShowProcessor(dataSource domain.DataSource) *DefaultShowProcessor {
	return &DefaultShowProcessor{
		dataSource:          dataSource,
		currentDB:           "",
		processListProvider: nil,
	}
}

// NewDefaultShowProcessorWithManager 创建带有数据源管理器的 SHOW 处理器
func NewDefaultShowProcessorWithManager(dataSource domain.DataSource, dsManager interface{}) *DefaultShowProcessor {
	return &DefaultShowProcessor{
		dataSource:          dataSource,
		dsManager:           dsManager,
		currentDB:           "default",
		processListProvider: nil,
	}
}

// SetCurrentDB 设置当前数据库
func (sp *DefaultShowProcessor) SetCurrentDB(dbName string) {
	sp.currentDB = dbName
}

// SetProcessListProvider 设置进程列表提供者
func (sp *DefaultShowProcessor) SetProcessListProvider(provider ProcessListProvider) {
	sp.processListProvider = provider
}

// ProcessShowTables 处理 SHOW TABLES 语句
func (sp *DefaultShowProcessor) ProcessShowTables(ctx context.Context) (executor.ResultSet, error) {
	// SHOW TABLES -> SELECT table_name FROM information_schema.tables WHERE table_schema = ?
	sql := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", sp.currentDB)

	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW TABLES query: %w", err)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("SHOW TABLES conversion failed: not a SELECT statement")
	}

	// 执行转换后的查询
	builder := parser.NewQueryBuilder(sp.dataSource)
	result, err := builder.ExecuteStatement(ctx, parseResult.Statement)
	if err != nil {
		return nil, err
	}

	return sp.convertToResultSet(result), nil
}

// ProcessShowDatabases 处理 SHOW DATABASES 语句
func (sp *DefaultShowProcessor) ProcessShowDatabases(ctx context.Context) (executor.ResultSet, error) {
	// SHOW DATABASES -> SELECT schema_name FROM information_schema.schemata
	sql := "SELECT schema_name FROM information_schema.schemata"

	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW DATABASES query: %w", err)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("SHOW DATABASES conversion failed: not a SELECT statement")
	}

	builder := parser.NewQueryBuilder(sp.dataSource)
	result, err := builder.ExecuteStatement(ctx, parseResult.Statement)
	if err != nil {
		return nil, err
	}

	return sp.convertToResultSet(result), nil
}

// ProcessShowColumns 处理 SHOW COLUMNS 语句
func (sp *DefaultShowProcessor) ProcessShowColumns(ctx context.Context, tableName string) (executor.ResultSet, error) {
	if tableName == "" {
		return nil, fmt.Errorf("SHOW COLUMNS requires a table name")
	}

	// SHOW COLUMNS FROM table -> SELECT * FROM information_schema.columns WHERE table_name = ? AND table_schema = ?
	schemaFilter := ""
	if sp.currentDB != "" {
		schemaFilter = fmt.Sprintf(" AND table_schema = '%s'", sp.currentDB)
	}
	sql := fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_name = '%s'%s", tableName, schemaFilter)

	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW COLUMNS query: %w", err)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("SHOW COLUMNS conversion failed: not a SELECT statement")
	}

	builder := parser.NewQueryBuilder(sp.dataSource)
	result, err := builder.ExecuteStatement(ctx, parseResult.Statement)
	if err != nil {
		return nil, err
	}

	return sp.convertToResultSet(result), nil
}

// ProcessShowIndex 处理 SHOW INDEX 语句
func (sp *DefaultShowProcessor) ProcessShowIndex(ctx context.Context, tableName string) (executor.ResultSet, error) {
	// 默认实现返回空结果
	return &defaultResultSet{
		columns: []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment"},
		rows:    []map[string]interface{}{},
	}, nil
}

// ProcessShowProcessList 处理 SHOW PROCESSLIST 语句
func (sp *DefaultShowProcessor) ProcessShowProcessList(ctx context.Context) (executor.ResultSet, error) {
	// 使用进程列表提供者获取查询列表
	var processList []interface{}
	if sp.processListProvider != nil {
		processList = sp.processListProvider()
	}

	// 定义 PROCESSLIST 字段
	columns := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}

	// 构建结果行
	rows := make([]map[string]interface{}, 0, len(processList))
	for _, item := range processList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
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

		state := "executing"
		if status == "canceled" {
			state = "killed"
		} else if status == "timeout" {
			state = "timeout"
		}

		if user == "" {
			user = "user"
		}
		if host == "" {
			host = "localhost:3306"
		}

		row := map[string]interface{}{
			"Id":      int64(threadID),
			"User":    user,
			"Host":    host,
			"db":      db,
			"Command": "Query",
			"Time":    timeSeconds,
			"State":   state,
			"Info":    sql,
		}
		rows = append(rows, row)
	}

	return &defaultResultSet{
		columns: columns,
		rows:    rows,
	}, nil
}

// ProcessShowVariables 处理 SHOW VARIABLES 语句
func (sp *DefaultShowProcessor) ProcessShowVariables(ctx context.Context) (executor.ResultSet, error) {
	// Use shared system variable definitions as single source of truth
	defs := information_schema.GetSystemVariableDefs()
	variables := make([]map[string]interface{}, 0, len(defs))
	for _, v := range defs {
		variables = append(variables, map[string]interface{}{
			"Variable_name": v.Name,
			"Value":         v.Value,
		})
	}

	return &defaultResultSet{
		columns: []string{"Variable_name", "Value"},
		rows:    variables,
	}, nil
}

// ProcessShowStatus 处理 SHOW STATUS 语句
func (sp *DefaultShowProcessor) ProcessShowStatus(ctx context.Context) (executor.ResultSet, error) {
	// 返回基本的状态变量
	status := []map[string]interface{}{
		{"Variable_name": "Threads_connected", "Value": 1},
		{"Variable_name": "Threads_running", "Value": 1},
		{"Variable_name": "Queries", "Value": 0},
		{"Variable_name": "Uptime", "Value": 0},
	}

	return &defaultResultSet{
		columns: []string{"Variable_name", "Value"},
		rows:    status,
	}, nil
}

// ProcessShowCreateTable 处理 SHOW CREATE TABLE 语句
func (sp *DefaultShowProcessor) ProcessShowCreateTable(ctx context.Context, tableName string) (executor.ResultSet, error) {
	if tableName == "" {
		return nil, fmt.Errorf("SHOW CREATE TABLE requires a table name")
	}

	// 从数据源获取表信息
	tableInfo, err := sp.dataSource.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found: %w", tableName, err)
	}

	// 构建 CREATE TABLE 语句
	var columns []string
	for _, col := range tableInfo.Columns {
		colDef := fmt.Sprintf("`%s` %s", col.Name, col.Type)
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}

	createStmt := fmt.Sprintf("CREATE TABLE `%s` (\n  %s\n)", tableName, strings.Join(columns, ",\n  "))

	rows := []map[string]interface{}{
		{"Table": tableName, "Create Table": createStmt},
	}

	return &defaultResultSet{
		columns: []string{"Table", "Create Table"},
		rows:    rows,
	}, nil
}

// convertToResultSet 将 QueryResult 转换为 ResultSet
func (sp *DefaultShowProcessor) convertToResultSet(result *domain.QueryResult) executor.ResultSet {
	// 提取列名
	columns := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		columns[i] = col.Name
	}

	// 转换行数据
	rows := make([]map[string]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		rowMap := make(map[string]interface{})
		for k, v := range row {
			rowMap[k] = v
		}
		rows[i] = rowMap
	}

	return &defaultResultSet{
		columns: columns,
		rows:    rows,
		total:   result.Total,
	}
}

// defaultResultSet 是 ResultSet 的简单实现
type defaultResultSet struct {
	columns []string
	rows    []map[string]interface{}
	total   int64
}

func (rs *defaultResultSet) Columns() []string {
	return rs.columns
}

func (rs *defaultResultSet) Next() (map[string]interface{}, error) {
	if len(rs.rows) == 0 {
		return nil, fmt.Errorf("no more rows")
	}
	row := rs.rows[0]
	rs.rows = rs.rows[1:]
	return row, nil
}

func (rs *defaultResultSet) HasNext() bool {
	return len(rs.rows) > 0
}

func (rs *defaultResultSet) Total() int64 {
	return rs.total
}

func (rs *defaultResultSet) Close() error {
	return nil
}

// Ensure DefaultShowProcessor implements executor.ShowProcessor
var _ executor.ShowProcessor = (*DefaultShowProcessor)(nil)
