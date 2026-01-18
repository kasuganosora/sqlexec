package resource

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ==================== MySQL 数据�?====================

// MySQLSource MySQL 数据�?
type MySQLSource struct {
	config         *DataSourceConfig
	connected      bool
	conn           *sql.DB
	statementCache *StatementCache
	connPool       *ConnectionPool
	slowQueryLog  *SlowQueryLogger
	queryCache     *QueryCache
	mu             sync.RWMutex
}

// NewMySQLSource 创建 MySQL 数据�?
func NewMySQLSource(config *DataSourceConfig) *MySQLSource {
	return &MySQLSource{
		config:         config,
		statementCache: NewStatementCache(),
		connPool:       NewConnectionPool(),
		slowQueryLog:  NewSlowQueryLogger(),
		queryCache:     NewQueryCache(),
	}
}

// Connect 连接数据�?
func (s *MySQLSource) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil
	}

	// 构建连接字符�?
	dsn := s.buildDSN()

	// 打开数据库连�?
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	// 测试连接
	if err := conn.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}

	// 设置连接池参数（使用默认值）
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(30 * time.Minute)

	s.conn = conn
	s.connected = true

	return nil
}

// Close 关闭连接
func (s *MySQLSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected {
		return nil
	}

	// 关闭数据库连�?
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			return err
		}
	}

	// 清理缓存
	s.statementCache.Clear()
	s.queryCache.Clear()
	s.connPool.Close()

	s.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (s *MySQLSource) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// GetConfig 获取数据源配�?
func (s *MySQLSource) GetConfig() *DataSourceConfig {
	return s.config
}

// IsWritable 检查是否可�?
func (s *MySQLSource) IsWritable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.Writable
}

// GetTables 获取所有表
func (s *MySQLSource) GetTables(ctx context.Context) ([]string, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	rows, err := s.conn.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// GetTableInfo 获取表信�?
func (s *MySQLSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 查询表结�?
	query := fmt.Sprintf("DESCRIBE %s", tableName)
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]ColumnInfo, 0)
	for rows.Next() {
		var (
			colName    string
			fieldType string
			null       string
			key        string
			defaultVal sql.NullString
			extra      string
		)
		if err := rows.Scan(&colName, &fieldType, &null, &key, &defaultVal, &extra); err != nil {
			return nil, err
		}

		column := ColumnInfo{
			Name:    colName,
			Type:    fieldType,
			Primary:  key == "PRI",
		}

		if defaultVal.Valid {
			column.Default = defaultVal.String
		}

		columns = append(columns, column)
	}

	return &TableInfo{
		Name:    tableName,
		Schema:  s.config.Database,
		Columns: columns,
	}, nil
}

// Query 查询数据
func (s *MySQLSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 构建SQL查询
	query := s.buildSelectQuery(tableName, options)

	// 检查查询缓�?
	if options != nil && options.SelectAll {
		if cached, exists := s.queryCache.Get(query); exists {
			return cached, nil
		}
	}

	// 记录慢查�?
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if s.slowQueryLog != nil && elapsed > 100*time.Millisecond {
			s.slowQueryLog.Log(query, elapsed)
		}
	}()

	// 执行查询
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 获取列类型信�?
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// 转换结果
	result, err := s.convertRows(rows, columnTypes)
	if err != nil {
		return nil, err
	}

	// 应用分页限制
	if options != nil && options.Limit > 0 {
		total := len(result.Rows)
		if options.Offset < len(result.Rows) {
			end := options.Offset + options.Limit
			if end > len(result.Rows) {
				end = len(result.Rows)
			}
			result.Rows = result.Rows[options.Offset:end]
		}
		result.Total = int64(total)
	}

	// 缓存结果
	if options != nil && options.SelectAll {
		s.queryCache.Set(query, result)
	}

	return result, nil
}

// Insert 插入数据
func (s *MySQLSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return 0, fmt.Errorf("data source is read-only")
	}

	// 获取表信�?
	tableInfo, err := s.GetTableInfo(ctx, tableName)
	if err != nil {
		return 0, err
	}

	// 构建插入SQL
	columns, values, args := s.buildInsertQuery(tableName, tableInfo.Columns, rows)

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columns, values)

	// 使用预编译语�?
	stmt, err := s.statementCache.Get(s.conn, query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// 执行插入
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		return 0, err
	}

	// 清除相关缓存
	s.queryCache.Invalidate(tableName)

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// Update 更新数据
func (s *MySQLSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return 0, fmt.Errorf("data source is read-only")
	}

	// 构建更新SQL
	setClause := s.buildSetClause(updates)
	whereClause := s.buildWhereClause(filters)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause)

	// 收集参数
	args := make([]interface{}, 0)
	args = append(args, s.buildUpdateArgs(updates, filters)...)

	// 执行更新
	result, err := s.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	// 清除相关缓存
	s.queryCache.Invalidate(tableName)

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// Delete 删除数据
func (s *MySQLSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return 0, fmt.Errorf("data source is read-only")
	}

	// 构建删除SQL
	whereClause := s.buildWhereClause(filters)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, whereClause)

	// 收集参数
	args := s.buildFilterArgs(filters)

	// 执行删除
	result, err := s.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	// 清除相关缓存
	s.queryCache.Invalidate(tableName)

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// CreateTable 创建�?
func (s *MySQLSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	if !s.connected {
		return fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return fmt.Errorf("data source is read-only")
	}

	// 构建创建表SQL
	query := s.buildCreateTableSQL(tableInfo)

	// 执行创建
	_, err := s.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

// DropTable 删除�?
func (s *MySQLSource) DropTable(ctx context.Context, tableName string) error {
	if !s.connected {
		return fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return fmt.Errorf("data source is read-only")
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)

	_, err := s.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	// 清除相关缓存
	s.queryCache.Invalidate(tableName)
	s.statementCache.InvalidateTable(tableName)

	return nil
}

// TruncateTable 清空�?
func (s *MySQLSource) TruncateTable(ctx context.Context, tableName string) error {
	if !s.connected {
		return fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return fmt.Errorf("data source is read-only")
	}

	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)

	_, err := s.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	// 清除相关缓存
	s.queryCache.Invalidate(tableName)

	return nil
}

// Execute 执行自定义SQL语句
func (s *MySQLSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 记录慢查�?
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if s.slowQueryLog != nil && elapsed > 100*time.Millisecond {
			s.slowQueryLog.Log(sql, elapsed)
		}
	}()

	// 执行SQL
	rows, err := s.conn.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 获取列类型信�?
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// 转换结果
	result, err := s.convertRows(rows, columnTypes)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ==================== 事务支持 ====================

// BeginTransaction 开始事�?
func (s *MySQLSource) BeginTransaction(ctx context.Context, isolationLevel string) (interface{}, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	if !s.config.Writable {
		return nil, fmt.Errorf("data source is read-only")
	}

	// 开始事�?
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// CommitTransaction 提交事务
func (s *MySQLSource) CommitTransaction(ctx context.Context, txn interface{}) error {
	tx, ok := txn.(*sql.Tx)
	if !ok {
		return fmt.Errorf("invalid transaction")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// RollbackTransaction 回滚事务
func (s *MySQLSource) RollbackTransaction(ctx context.Context, txn interface{}) error {
	tx, ok := txn.(*sql.Tx)
	if !ok {
		return fmt.Errorf("invalid transaction")
	}

	if err := tx.Rollback(); err != nil {
		return err
	}

	return nil
}

// ==================== SQL构建 ====================

// buildDSN 构建连接字符�?
func (s *MySQLSource) buildDSN() string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		s.config.Username,
		s.config.Password,
		s.config.Host,
		s.config.Port,
		s.config.Database,
	)

	// 添加参数
	dsn += "?charset=utf8mb4"

	return dsn
}

// buildSelectQuery 构建SELECT查询
func (s *MySQLSource) buildSelectQuery(tableName string, options *QueryOptions) string {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// 添加WHERE子句
	if options != nil && len(options.Filters) > 0 {
		query += " WHERE " + s.buildWhereClause(options.Filters)
	}

	// 添加ORDER BY
	if options != nil && options.OrderBy != "" {
		order := options.Order
		if order == "" {
			order = "ASC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", options.OrderBy, order)
	}

	// 添加LIMIT
	if options != nil && options.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", options.Limit)
		if options.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", options.Offset)
		}
	}

	return query
}

// buildWhereClause 构建WHERE子句
func (s *MySQLSource) buildWhereClause(filters []Filter) string {
	if len(filters) == 0 {
		return "1=1"
	}

	conditions := make([]string, 0)
	for _, f := range filters {
		conditions = append(conditions, s.buildFilterCondition(f))
	}

	return "(" + s.joinConditions(conditions, filters[0].LogicOp) + ")"
}

// buildFilterCondition 构建过滤条件
func (s *MySQLSource) buildFilterCondition(filter Filter) string {
	return fmt.Sprintf("%s %s ?", filter.Field, filter.Operator)
}

// joinConditions 连接条件
func (s *MySQLSource) joinConditions(conditions []string, logicalOp string) string {
	if logicalOp == "" || logicalOp == "AND" {
		return joinWith(conditions, " AND ")
	} else if logicalOp == "OR" {
		return joinWith(conditions, " OR ")
	}
	return joinWith(conditions, " AND ")
}

// buildInsertQuery 构建INSERT查询
func (s *MySQLSource) buildInsertQuery(tableName string, columns []ColumnInfo, rows []Row) (string, string, []interface{}) {
	// 获取列名
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
	}
	columnsStr := joinWith(columnNames, ", ")

	// 构建占位�?
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	for _, row := range rows {
		rowValues := make([]string, 0)
		for _, col := range columns {
			rowValues = append(rowValues, "?")
			args = append(args, row[col.Name])
		}
		placeholders = append(placeholders, "("+joinWith(rowValues, ", ")+")")
	}

	valuesStr := joinWith(placeholders, ", ")

	return columnsStr, valuesStr, args
}

// buildSetClause 构建SET子句
func (s *MySQLSource) buildSetClause(updates Row) string {
	clauses := make([]string, 0)
	for field := range updates {
		clauses = append(clauses, fmt.Sprintf("%s = ?", field))
	}
	return joinWith(clauses, ", ")
}

// buildFilterArgs 构建过滤条件参数
func (s *MySQLSource) buildFilterArgs(filters []Filter) []interface{} {
	args := make([]interface{}, 0)
	for _, filter := range filters {
		args = append(args, filter.Value)
	}
	return args
}

// buildUpdateArgs 构建UPDATE参数
func (s *MySQLSource) buildUpdateArgs(updates Row, filters []Filter) []interface{} {
	args := make([]interface{}, 0)

	// UPDATE参数
	for _, value := range updates {
		args = append(args, value)
	}

	// WHERE参数
	for _, filter := range filters {
		args = append(args, filter.Value)
	}

	return args
}

// buildCreateTableSQL 构建CREATE TABLE语句
func (s *MySQLSource) buildCreateTableSQL(tableInfo *TableInfo) string {
	columnDefs := make([]string, 0)
	for _, col := range tableInfo.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)

		// 添加主键
		if col.Primary {
			def += " PRIMARY KEY"
		}

		// 添加默认�?
		if col.Default != "" {
			def += fmt.Sprintf(" DEFAULT '%s'", col.Default)
		}

		// 添加非空
		if !col.Nullable {
			def += " NOT NULL"
		}

		columnDefs = append(columnDefs, def)
	}

	columnsStr := joinWith(columnDefs, ", ")
	query := fmt.Sprintf("CREATE TABLE %s (%s)", tableInfo.Name, columnsStr)

	return query
}

// convertRows 转换查询结果
func (s *MySQLSource) convertRows(rows *sql.Rows, columns []*sql.ColumnType) (*QueryResult, error) {
	result := make([]Row, 0)

	for rows.Next() {
		// 创建映射
		row := make(map[string]interface{})
		values := make([]interface{}, len(columns))

		// 扫描所有列
		scanArgs := make([]interface{}, len(columns))
		for i := range columns {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		// 填充映射
		for i, col := range columns {
			// 获取列名
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
			Type: col.DatabaseTypeName(),
		})
	}

	return &QueryResult{
		Columns: columnInfos,
		Rows:    result,
		Total:   int64(len(result)),
	}, nil
}

// ==================== 工厂注册 ====================

func init() {
	RegisterFactory(&MySQLFactory{})
}

// MySQLFactory MySQL 数据源工�?
type MySQLFactory struct{}

// GetType 实现DataSourceFactory接口
func (f *MySQLFactory) GetType() DataSourceType {
	return DataSourceTypeMySQL
}

// Create 实现DataSourceFactory接口
func (f *MySQLFactory) Create(config *DataSourceConfig) (DataSource, error) {
	return NewMySQLSource(config), nil
}
