package resource

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLSource MySQL数据源实现
type MySQLSource struct {
	config    *DataSourceConfig
	db        *sql.DB
	connected bool
	mu        sync.RWMutex
}

// MySQLFactory MySQL数据源工厂
type MySQLFactory struct{}

// NewMySQLFactory 创建MySQL数据源工厂
func NewMySQLFactory() *MySQLFactory {
	return &MySQLFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *MySQLFactory) GetType() DataSourceType {
	return DataSourceTypeMySQL
}

// Create 实现DataSourceFactory接口
func (f *MySQLFactory) Create(config *DataSourceConfig) (DataSource, error) {
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == 0 {
		config.Port = 3306
	}
	if config.Database == "" {
		config.Database = "test"
	}
	return &MySQLSource{
		config: config,
	}, nil
}

// Connect 连接数据源
func (s *MySQLSource) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.db != nil {
		return nil
	}
	
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		s.config.Username,
		s.config.Password,
		s.config.Host,
		s.config.Port,
		s.config.Database,
	)
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}
	
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping mysql: %w", err)
	}
	
	s.db = db
	s.connected = true
	return nil
}

// Close 关闭连接
func (s *MySQLSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.db == nil {
		return nil
	}
	
	err := s.db.Close()
	s.db = nil
	s.connected = false
	return err
}

// IsConnected 检查是否已连接
func (s *MySQLSource) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// GetConfig 获取数据源配置
func (s *MySQLSource) GetConfig() *DataSourceConfig {
	return s.config
}

// GetTables 获取所有表
func (s *MySQLSource) GetTables(ctx context.Context) ([]string, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return nil, err
	}
	
	rows, err := s.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()
	
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, table)
	}
	
	return tables, nil
}

// GetTableInfo 获取表信息
func (s *MySQLSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return nil, err
	}
	
	// 查询列信息
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_KEY,
			COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	
	rows, err := s.db.QueryContext(ctx, query, s.config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()
	
	tableInfo := &TableInfo{
		Name:   tableName,
		Schema: s.config.Database,
	}
	
	for rows.Next() {
		var columnName, dataType, isNullable, columnKey sql.NullString
		var columnDefault sql.NullString
		
		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnKey, &columnDefault); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		
		col := ColumnInfo{
			Name:     columnName.String,
			Type:     dataType.String,
			Nullable: isNullable.String == "YES",
			Primary:  columnKey.String == "PRI",
		}
		
		if columnDefault.Valid {
			col.Default = columnDefault.String
		}
		
		tableInfo.Columns = append(tableInfo.Columns, col)
	}
	
	return tableInfo, nil
}

// Query 查询数据
func (s *MySQLSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return nil, err
	}
	
	// 获取表信息
	tableInfo, err := s.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, err
	}
	
	// 构建SQL
	query, args := s.buildSelectSQL(tableName, options)
	
	// 查询总数
	var total int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", 
		s.quoteIdentifier(tableName), 
		s.buildWhereClause(options))
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to query count: %w", err)
	}
	
	// 查询数据
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()
	
	// 读取列
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	
	// 读取数据
	result := &QueryResult{
		Columns: tableInfo.Columns,
		Total:   total,
	}
	
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		row := make(Row)
		for i, col := range columns {
			row[col] = values[i]
		}
		result.Rows = append(result.Rows, row)
	}
	
	return result, nil
}

// Insert 插入数据
func (s *MySQLSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return 0, err
	}
	
	if len(rows) == 0 {
		return 0, nil
	}
	
	// 构建插入SQL
	query, args := s.buildInsertSQL(tableName, rows, options)
	
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to insert data: %w", err)
	}
	
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}
	
	return affected, nil
}

// Update 更新数据
func (s *MySQLSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return 0, err
	}
	
	if len(updates) == 0 {
		return 0, nil
	}
	
	// 构建更新SQL
	query, args := s.buildUpdateSQL(tableName, filters, updates, options)
	
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to update data: %w", err)
	}
	
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}
	
	return affected, nil
}

// Delete 删除数据
func (s *MySQLSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return 0, err
	}
	
	if len(filters) == 0 && (options == nil || !options.Force) {
		return 0, fmt.Errorf("delete operation requires filters or force option")
	}
	
	// 构建删除SQL
	query, args := s.buildDeleteSQL(tableName, filters, options)
	
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}
	
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}
	
	return affected, nil
}

// CreateTable 创建表
func (s *MySQLSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	if err := s.ensureConnected(ctx); err != nil {
		return err
	}
	
	query, args := s.buildCreateTableSQL(tableInfo)
	
	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	return nil
}

// DropTable 删除表
func (s *MySQLSource) DropTable(ctx context.Context, tableName string) error {
	if err := s.ensureConnected(ctx); err != nil {
		return err
	}
	
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", s.quoteIdentifier(tableName))
	
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	
	return nil
}

// TruncateTable 清空表
func (s *MySQLSource) TruncateTable(ctx context.Context, tableName string) error {
	if err := s.ensureConnected(ctx); err != nil {
		return err
	}
	
	query := fmt.Sprintf("TRUNCATE TABLE %s", s.quoteIdentifier(tableName))
	
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}
	
	return nil
}

// Execute 执行自定义SQL语句
func (s *MySQLSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return nil, err
	}
	
	// 判断是否是查询语句
	if len(sql) > 5 {
		prefix := sql[:6]
		if containsIgnoreCase(prefix, "SELECT") || 
		   containsIgnoreCase(prefix, "SHOW") || 
		   containsIgnoreCase(prefix, "DESCRIBE") {
			return s.executeQuery(ctx, sql)
		}
	}
	
	// 执行非查询语句
	_, execErr := s.db.ExecContext(ctx, sql)
	if execErr != nil {
		return nil, fmt.Errorf("failed to execute SQL: %w", execErr)
	}
	
	return &QueryResult{
		Total: 0,
	}, nil
}

// ensureConnected 确保已连接
func (s *MySQLSource) ensureConnected(ctx context.Context) error {
	if !s.IsConnected() {
		return fmt.Errorf("data source is not connected")
	}
	return nil
}

// buildSelectSQL 构建SELECT SQL
func (s *MySQLSource) buildSelectSQL(tableName string, options *QueryOptions) (string, []interface{}) {
	args := []interface{}{}
	
	query := "SELECT * FROM " + s.quoteIdentifier(tableName)
	
	// WHERE子句
	where := s.buildWhereClause(options)
	if where != "" {
		query += " " + where
	}
	
	// ORDER BY子句
	if options != nil && options.OrderBy != "" {
		order := options.Order
		if order == "" {
			order = "ASC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", s.quoteIdentifier(options.OrderBy), order)
	}
	
	// LIMIT和OFFSET子句
	if options != nil {
		if options.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", options.Limit)
			if options.Offset > 0 {
				query += fmt.Sprintf(" OFFSET %d", options.Offset)
			}
		}
	}
	
	return query, args
}

// buildWhereClause 构建WHERE子句
func (s *MySQLSource) buildWhereClause(options *QueryOptions) string {
	if options == nil || len(options.Filters) == 0 {
		return ""
	}
	
	where := "WHERE "
	conditions := []string{}
	
	for _, filter := range options.Filters {
		condition := fmt.Sprintf("%s %s ?", s.quoteIdentifier(filter.Field), filter.Operator)
		conditions = append(conditions, condition)
	}
	
	where += joinConditions(conditions, " AND ")
	return where
}

// buildInsertSQL 构建INSERT SQL
func (s *MySQLSource) buildInsertSQL(tableName string, rows []Row, options *InsertOptions) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	
	// 获取所有列名
	columns := []string{}
	for col := range rows[0] {
		columns = append(columns, col)
	}
	
	// 构建列名列表
	colNames := ""
	for i, col := range columns {
		if i > 0 {
			colNames += ", "
		}
		colNames += s.quoteIdentifier(col)
	}
	
	// 构建值占位符
	valuePlaceholders := ""
	for i := range columns {
		if i > 0 {
			valuePlaceholders += ", "
		}
		valuePlaceholders += "?"
	}
	
	// 构建完整SQL
	keyword := "INSERT"
	if options != nil && options.Replace {
		keyword = "REPLACE"
	}
	
	query := fmt.Sprintf("%s INTO %s (%s) VALUES (%s)", 
		keyword, s.quoteIdentifier(tableName), colNames, valuePlaceholders)
	
	// 构建参数
	args := []interface{}{}
	for _, row := range rows {
		for _, col := range columns {
			args = append(args, row[col])
		}
	}
	
	return query, args
}

// buildUpdateSQL 构建UPDATE SQL
func (s *MySQLSource) buildUpdateSQL(tableName string, filters []Filter, updates Row, options *UpdateOptions) (string, []interface{}) {
	args := []interface{}{}
	
	// 构建SET子句
	setClause := "SET "
	setParts := []string{}
	for col, val := range updates {
		setParts = append(setParts, fmt.Sprintf("%s = ?", s.quoteIdentifier(col)))
		args = append(args, val)
	}
	setClause += joinConditions(setParts, ", ")
	
	// 构建WHERE子句
	whereClause := ""
	queryOpts := &QueryOptions{Filters: filters}
	where := s.buildWhereClause(queryOpts)
	if where != "" {
		whereClause = " " + where
	}
	
	// 构建参数
	for _, filter := range filters {
		args = append(args, filter.Value)
	}
	
	query := fmt.Sprintf("UPDATE %s %s%s", s.quoteIdentifier(tableName), setClause, whereClause)
	return query, args
}

// buildDeleteSQL 构建DELETE SQL
func (s *MySQLSource) buildDeleteSQL(tableName string, filters []Filter, options *DeleteOptions) (string, []interface{}) {
	args := []interface{}{}
	
	query := fmt.Sprintf("DELETE FROM %s", s.quoteIdentifier(tableName))
	
	// WHERE子句
	if len(filters) > 0 {
		queryOpts := &QueryOptions{Filters: filters}
		where := s.buildWhereClause(queryOpts)
		if where != "" {
			query += " " + where
			for _, filter := range filters {
				args = append(args, filter.Value)
			}
		}
	}
	
	return query, args
}

// buildCreateTableSQL 构建CREATE TABLE SQL
func (s *MySQLSource) buildCreateTableSQL(tableInfo *TableInfo) (string, []interface{}) {
	args := []interface{}{}
	
	query := fmt.Sprintf("CREATE TABLE %s (\n", s.quoteIdentifier(tableInfo.Name))
	
	columnDefs := []string{}
	for _, col := range tableInfo.Columns {
		colDef := fmt.Sprintf("  %s %s", s.quoteIdentifier(col.Name), s.mapMySQLType(col.Type))
		
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		
		if col.Primary {
			colDef += " PRIMARY KEY"
		}
		
		if col.Default != "" {
			colDef += fmt.Sprintf(" DEFAULT '%s'", col.Default)
		}
		
		columnDefs = append(columnDefs, colDef)
	}
	
	query += joinConditions(columnDefs, ",\n")
	query += "\n)"
	
	return query, args
}

// mapMySQLType 映射MySQL类型
func (s *MySQLSource) mapMySQLType(typ string) string {
	switch typ {
	case "int", "integer":
		return "INT"
	case "varchar", "string":
		return "VARCHAR(255)"
	case "text":
		return "TEXT"
	case "datetime", "timestamp":
		return "DATETIME"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "decimal":
		return "DECIMAL(10,2)"
	case "float", "double":
		return "DOUBLE"
	case "boolean", "bool":
		return "BOOLEAN"
	default:
		return typ
	}
}

// quoteIdentifier 引用标识符
func (s *MySQLSource) quoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", name)
}

// executeQuery 执行查询
func (s *MySQLSource) executeQuery(ctx context.Context, sql string) (*QueryResult, error) {
	rows, err := s.db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	
	// 读取列
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	
	// 构建列信息
	colInfo := make([]ColumnInfo, len(columns))
	for i, col := range columns {
		colInfo[i] = ColumnInfo{
			Name: col,
			Type: "TEXT", // 简化处理
		}
	}
	
	// 读取数据
	result := &QueryResult{
		Columns: colInfo,
	}
	
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		row := make(Row)
		for i, col := range columns {
			row[col] = values[i]
		}
		result.Rows = append(result.Rows, row)
	}
	
	result.Total = int64(len(result.Rows))
	return result, nil
}

// containsIgnoreCase 忽略大小写包含检查
func containsIgnoreCase(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	return s[:len(substr)] == substr
}

// joinConditions 连接条件
func joinConditions(conditions []string, sep string) string {
	result := ""
	for i, cond := range conditions {
		if i > 0 {
			result += sep
		}
		result += cond
	}
	return result
}

func init() {
	RegisterFactory(NewMySQLFactory())
}
