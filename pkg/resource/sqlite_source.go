package resource

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// SQLiteConfig SQLite数据源配置
type SQLiteConfig struct {
	// DatabasePath 数据库文件路径，":memory:" 表示内存数据库
	DatabasePath string
	// MaxOpenConns 最大打开连接数
	MaxOpenConns int
	// MaxIdleConns 最大空闲连接数
	MaxIdleConns int
	// ConnMaxLifetime 连接最大生命周期
	ConnMaxLifetime time.Duration
}

// DefaultSQLiteConfig 返回默认配置
func DefaultSQLiteConfig(dbPath string) *SQLiteConfig {
	return &SQLiteConfig{
		DatabasePath:    dbPath,
		MaxOpenConns:    1, // SQLite推荐单个连接
		MaxIdleConns:    1,
		ConnMaxLifetime: 1 * time.Hour,
	}
}

// SQLiteSource SQLite数据源实现
type SQLiteSource struct {
	config    *SQLiteConfig
	conn      *sql.DB
	connected bool
	mu        sync.RWMutex
	dataConfig *DataSourceConfig
}

// NewSQLiteSource 创建SQLite数据源
func NewSQLiteSource(config *SQLiteConfig) *SQLiteSource {
	return &SQLiteSource{
		config: config,
		dataConfig: &DataSourceConfig{
			Type:     DataSourceTypeSQLite,
			Name:     "sqlite",
			Database: config.DatabasePath,
			Writable: true,
		},
	}
}

// Connect 连接到SQLite数据库
func (s *SQLiteSource) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil
	}

	// 构建DSN
	dsn := s.config.DatabasePath
	if !strings.HasPrefix(dsn, ":memory:") {
		// 文件模式添加PRAGMA参数
		dsn += "?_foreign_keys=on&_journal_mode=WAL&_synchronous=NORMAL"
	}

	// 打开连接
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// 配置连接池
	conn.SetMaxOpenConns(s.config.MaxOpenConns)
	conn.SetMaxIdleConns(s.config.MaxIdleConns)
	conn.SetConnMaxLifetime(s.config.ConnMaxLifetime)

	// 测试连接
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	s.conn = conn
	s.connected = true
	return nil
}

// Close 关闭连接
func (s *SQLiteSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected {
		return nil
	}

	if err := s.conn.Close(); err != nil {
		return err
	}

	s.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (s *SQLiteSource) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// IsWritable 检查是否可写
func (s *SQLiteSource) IsWritable() bool {
	return true
}

// GetConfig 获取数据源配置
func (s *SQLiteSource) GetConfig() *DataSourceConfig {
	return s.dataConfig
}

// GetTables 获取所有表
func (s *SQLiteSource) GetTables(ctx context.Context) ([]string, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	rows, err := s.conn.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// GetTableInfo 获取表信息
func (s *SQLiteSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 获取列信息
	columnQuery := `
		SELECT name, type, "notnull", dflt_value, pk
		FROM pragma_table_info(?)
		ORDER BY cid
	`

	rows, err := s.conn.QueryContext(ctx, columnQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()

	columns := make([]ColumnInfo, 0)
	for rows.Next() {
		var colName, colType string
		var notNull int
		var defaultVal sql.NullString
		var pk int

		if err := rows.Scan(&colName, &colType, &notNull, &defaultVal, &pk); err != nil {
			return nil, err
		}

		// 转换SQLite类型到标准类型
		fieldType := convertSQLiteType(colType)

		column := ColumnInfo{
			Name:     colName,
			Type:     fieldType,
			Nullable: notNull == 0,
			Primary:  pk > 0,
		}

		if defaultVal.Valid {
			column.Default = defaultVal.String
		}

		columns = append(columns, column)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return &TableInfo{
		Name:    tableName,
		Schema:  "main",
		Columns: columns,
	}, nil
}

// Query 执行查询
func (s *SQLiteSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 构建SQL查询
	query, args := buildQuery(tableName, options)

	// 执行查询
	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	// 获取列类型信息
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// 转换结果
	return s.convertRows(rows, columnTypes)
}

// Insert 插入数据
func (s *SQLiteSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	if len(rows) == 0 {
		return 0, fmt.Errorf("no rows to insert")
	}

	// 获取表信息
	tableInfo, err := s.GetTableInfo(ctx, tableName)
	if err != nil {
		return 0, err
	}

	var totalRows int64

	// 逐行插入
	for _, row := range rows {
		columns := make([]string, 0)
		placeholders := make([]string, 0)
		values := make([]interface{}, 0)

		for _, col := range tableInfo.Columns {
			if val, exists := row[col.Name]; exists {
				columns = append(columns, col.Name)
				placeholders = append(placeholders, "?")
				values = append(values, val)
			}
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)

		result, err := s.conn.ExecContext(ctx, query, values...)
		if err != nil {
			return 0, fmt.Errorf("failed to insert: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalRows += rowsAffected
	}

	return totalRows, nil
}

// Update 更新数据
func (s *SQLiteSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	// 构建UPDATE语句
	sets := make([]string, 0)
	values := make([]interface{}, 0)

	for col, val := range updates {
		sets = append(sets, fmt.Sprintf("%s = ?", col))
		values = append(values, val)
	}

	// 构建WHERE条件
	whereSQL, whereArgs := buildWhereClause(filters)
	values = append(values, whereArgs...)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(sets, ", "),
		whereSQL,
	)

	if whereSQL == "" {
		query = fmt.Sprintf("UPDATE %s SET %s", tableName, strings.Join(sets, ", "))
	}

	result, err := s.conn.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, fmt.Errorf("failed to update: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// Delete 删除数据
func (s *SQLiteSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	// 构建DELETE语句
	whereSQL, whereArgs := buildWhereClause(filters)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, whereSQL)

	if whereSQL == "" {
		return 0, fmt.Errorf("delete without filters is not allowed")
	}

	result, err := s.conn.ExecContext(ctx, query, whereArgs...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// TruncateTable 清空表
func (s *SQLiteSource) TruncateTable(ctx context.Context, tableName string) error {
	if !s.connected {
		return fmt.Errorf("not connected")
	}

	_, err := s.conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", tableName))
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	return nil
}

// CreateTable 创建表
func (s *SQLiteSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	if !s.connected {
		return fmt.Errorf("not connected")
	}

	if tableInfo == nil {
		return fmt.Errorf("table info is required")
	}

	columnDefs := make([]string, 0, len(tableInfo.Columns))
	primaryKeys := make([]string, 0)

	for _, col := range tableInfo.Columns {
		def := fmt.Sprintf("%s %s", col.Name, convertToSQLiteType(col.Type))

		if col.Primary {
			primaryKeys = append(primaryKeys, col.Name)
		}

		if col.Default != "" {
			def += fmt.Sprintf(" DEFAULT %s", col.Default)
		}

		if !col.Nullable {
			def += " NOT NULL"
		}

		columnDefs = append(columnDefs, def)
	}

	// 添加主键约束
	if len(primaryKeys) > 0 {
		pkConstraint := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
		columnDefs = append(columnDefs, pkConstraint)
	}

	columnsStr := strings.Join(columnDefs, ", ")
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableInfo.Name, columnsStr)

	_, err := s.conn.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// DropTable 删除表
func (s *SQLiteSource) DropTable(ctx context.Context, tableName string) error {
	if !s.connected {
		return fmt.Errorf("not connected")
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := s.conn.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	return nil
}

// Execute 执行自定义SQL
func (s *SQLiteSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	rows, err := s.conn.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	return s.convertRows(rows, columnTypes)
}

// BeginTransaction 开始事务
func (s *SQLiteSource) BeginTransaction(ctx context.Context) (interface{}, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &SQLiteTransaction{
		tx: tx,
	}, nil
}

// convertRows 转换查询结果
func (s *SQLiteSource) convertRows(rows *sql.Rows, columns []*sql.ColumnType) (*QueryResult, error) {
	result := make([]Row, 0)

	for rows.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(columns))

		scanArgs := make([]interface{}, len(columns))
		for i := range columns {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		for i, col := range columns {
			colName := col.Name()
			row[colName] = values[i]
		}

		result = append(result, row)
	}

	// 转换ColumnInfo
	columnInfos := make([]ColumnInfo, 0, len(columns))
	for _, col := range columns {
		columnInfos = append(columnInfos, ColumnInfo{
			Name: col.Name(),
			Type: convertSQLiteType(col.DatabaseTypeName()),
		})
	}

	return &QueryResult{
		Columns: columnInfos,
		Rows:    result,
		Total:   int64(len(result)),
	}, nil
}

// convertSQLiteType 转换SQLite类型
func convertSQLiteType(sqliteType string) string {
	upperType := strings.ToUpper(sqliteType)

	switch {
	case strings.Contains(upperType, "INT"):
		return "INTEGER"
	case strings.Contains(upperType, "CHAR") || strings.Contains(upperType, "CLOB") || strings.Contains(upperType, "TEXT"):
		return "TEXT"
	case strings.Contains(upperType, "BLOB"):
		return "BLOB"
	case strings.Contains(upperType, "REAL") || strings.Contains(upperType, "FLOA") || strings.Contains(upperType, "DOUB"):
		return "REAL"
	case strings.Contains(upperType, "NUM") || strings.Contains(upperType, "DEC"):
		return "NUMERIC"
	default:
		return "TEXT" // SQLite默认类型
	}
}

// convertToSQLiteType 转换为SQLite类型
func convertToSQLiteType(typeStr string) string {
	upperType := strings.ToUpper(typeStr)

	switch upperType {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		return "INTEGER"
	case "VARCHAR", "CHAR", "TEXT", "STRING":
		return "TEXT"
	case "FLOAT", "DOUBLE", "REAL":
		return "REAL"
	case "BLOB", "BINARY":
		return "BLOB"
	case "BOOLEAN", "BOOL":
		return "INTEGER"
	default:
		return "TEXT"
	}
}

// buildQuery 构建查询SQL
func buildQuery(tableName string, options *QueryOptions) (string, []interface{}) {
	args := make([]interface{}, 0)

	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// WHERE条件
	if options != nil && len(options.Filters) > 0 {
		whereSQL, whereArgs := buildWhereClause(options.Filters)
		query += " WHERE " + whereSQL
		args = append(args, whereArgs...)
	}

	// ORDER BY
	if options != nil && options.OrderBy != "" {
		if options.Order != "" {
			query += fmt.Sprintf(" ORDER BY %s %s", options.OrderBy, options.Order)
		} else {
			query += fmt.Sprintf(" ORDER BY %s", options.OrderBy)
		}
	}

	// LIMIT和OFFSET
	if options != nil && options.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", options.Limit)
		if options.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", options.Offset)
		}
	}

	return query, args
}

// buildWhereClause 构建WHERE子句
func buildWhereClause(filters []Filter) (string, []interface{}) {
	if len(filters) == 0 {
		return "", nil
	}

	conditions := make([]string, 0)
	args := make([]interface{}, 0)

	for _, filter := range filters {
		switch filter.Operator {
		case "=":
			conditions = append(conditions, fmt.Sprintf("%s = ?", filter.Field))
			args = append(args, filter.Value)
		case "!=":
			conditions = append(conditions, fmt.Sprintf("%s != ?", filter.Field))
			args = append(args, filter.Value)
		case ">":
			conditions = append(conditions, fmt.Sprintf("%s > ?", filter.Field))
			args = append(args, filter.Value)
		case ">=":
			conditions = append(conditions, fmt.Sprintf("%s >= ?", filter.Field))
			args = append(args, filter.Value)
		case "<":
			conditions = append(conditions, fmt.Sprintf("%s < ?", filter.Field))
			args = append(args, filter.Value)
		case "<=":
			conditions = append(conditions, fmt.Sprintf("%s <= ?", filter.Field))
			args = append(args, filter.Value)
		case "LIKE":
			conditions = append(conditions, fmt.Sprintf("%s LIKE ?", filter.Field))
			args = append(args, filter.Value)
		case "IN":
			// IN操作符的Value应该是切片
			if values, ok := filter.Value.([]interface{}); ok {
				placeholders := make([]string, len(values))
				for i := range placeholders {
					placeholders[i] = "?"
				}
				conditions = append(conditions, fmt.Sprintf("%s IN (%s)", filter.Field, strings.Join(placeholders, ", ")))
				args = append(args, values...)
			}
		}
	}

	if len(conditions) == 1 {
		return conditions[0], args
	}

	return strings.Join(conditions, " AND "), args
}

// SQLiteTransaction SQLite事务实现
type SQLiteTransaction struct {
	tx    *sql.Tx
	completed bool
}

// Commit 提交事务
func (t *SQLiteTransaction) Commit() error {
	if t.completed {
		return fmt.Errorf("transaction already completed")
	}
	t.completed = true
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *SQLiteTransaction) Rollback() error {
	if t.completed {
		return fmt.Errorf("transaction already completed")
	}
	t.completed = true
	return t.tx.Rollback()
}

// IsActive 检查事务是否活跃
func (t *SQLiteTransaction) IsActive() bool {
	return !t.completed
}
