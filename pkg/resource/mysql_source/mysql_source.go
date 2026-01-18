package mysql_source

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource"
	_ "github.com/go-sql-driver/mysql"
)

// MySQLSource MySQL数据库数据源实现
type MySQLSource struct {
	config    *resource.DataSourceConfig
	connected  bool
	writable   bool
	dsn        string
	db         *sql.DB
}

// NewMySQLSource 创建MySQL数据源
func NewMySQLSource(dsn string) *MySQLSource {
	return &MySQLSource{
		dsn:      dsn,
		writable: true,
	}
}

// Connect 连接数据源
func (m *MySQLSource) Connect(ctx context.Context) error {
	db, err := sql.Open("mysql", m.dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}

	m.db = db
	m.config = &resource.DataSourceConfig{
		Name:    "mysql",
		Type:    resource.DataSourceTypeMySQL,
		Options: map[string]interface{}{
			"dsn": m.dsn,
		},
	}
	m.connected = true
	return nil
}

// Close 关闭数据源
func (m *MySQLSource) Close(ctx context.Context) error {
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			return fmt.Errorf("failed to close MySQL connection: %w", err)
		}
	}
	m.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (m *MySQLSource) IsConnected() bool {
	return m.connected
}

// IsWritable 检查是否可写
func (m *MySQLSource) IsWritable() bool {
	return m.writable
}

// GetConfig 获取配置
func (m *MySQLSource) GetConfig() *resource.DataSourceConfig {
	return m.config
}

// SetConfig 设置配置
func (m *MySQLSource) SetConfig(config *resource.DataSourceConfig) error {
	m.config = config
	return nil
}

// GetDB 获取数据库连接
func (m *MySQLSource) GetDB() *sql.DB {
	return m.db
}

// GetColumns 获取列信息
func (m *MySQLSource) GetColumns() ([]resource.ColumnInfo, error) {
	return []resource.ColumnInfo{}, fmt.Errorf("use Query to get columns")
}

// Query 执行查询
func (m *MySQLSource) Query(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	if !m.connected || m.db == nil {
		return nil, fmt.Errorf("MySQL source not connected")
	}

	rows, err := m.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL: %w", err)
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	colInfos := make([]resource.ColumnInfo, len(columns))
	for i, col := range columns {
		colInfos[i] = resource.ColumnInfo{
			Name:     col.Name(),
			Type:      col.DatabaseTypeName(),
			Nullable:  col.Nullable(),
			Index:     i,
		}
	}

	var results [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range scanArgs {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, values)
	}

	return &resource.QueryResult{
		Columns: colInfos,
		Rows:    results,
		Total:   int64(len(results)),
	}, nil
}

// Insert 插入数据
func (m *MySQLSource) Insert(ctx context.Context, table string, columns []string, values []interface{}) (int64, error) {
	if !m.connected || m.db == nil {
		return 0, fmt.Errorf("MySQL source not connected")
	}

	result, err := m.db.ExecContext(ctx, buildInsertSQL(table, columns), values...)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into MySQL: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// Update 更新数据
func (m *MySQLSource) Update(ctx context.Context, table string, set map[string]interface{}, where map[string]interface{}) (int64, error) {
	if !m.connected || m.db == nil {
		return 0, fmt.Errorf("MySQL source not connected")
	}

	setSQL := buildSetSQL(set)
	whereSQL := buildWhereSQL(where)
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, setSQL, whereSQL)

	result, err := m.db.ExecContext(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("failed to update MySQL: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// Delete 删除数据
func (m *MySQLSource) Delete(ctx context.Context, table string, where map[string]interface{}) (int64, error) {
	if !m.connected || m.db == nil {
		return 0, fmt.Errorf("MySQL source not connected")
	}

	whereSQL := buildWhereSQL(where)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", table, whereSQL)

	result, err := m.db.ExecContext(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("failed to delete from MySQL: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// Execute 执行语句
func (m *MySQLSource) Execute(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	if !m.connected || m.db == nil {
		return nil, fmt.Errorf("MySQL source not connected")
	}

	result, err := m.db.ExecContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute MySQL: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return &resource.QueryResult{
		Rows:  [][]interface{}{},
		Total: rowsAffected,
	}, nil
}

// Begin 开始事务
func (m *MySQLSource) Begin(ctx context.Context) (resource.Transaction, error) {
	if !m.connected || m.db == nil {
		return nil, fmt.Errorf("MySQL source not connected")
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return tx, nil
}

// Commit 提交事务
func (m *MySQLSource) Commit() error {
	return fmt.Errorf("use transaction object to commit")
}

// Rollback 回滚事务
func (m *MySQLSource) Rollback() error {
	return fmt.Errorf("use transaction object to rollback")
}

// buildInsertSQL 构建INSERT语句
func buildInsertSQL(table string, columns []string) string {
	columnStr := ""
	for i, col := range columns {
		if i > 0 {
			columnStr += ", "
		}
		columnStr += col
	}
	placeholders := ""
	for i := 0; i < len(columns); i++ {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += "?"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columnStr, placeholders)
}

// buildSetSQL 构建SET子句
func buildSetSQL(set map[string]interface{}) string {
	setStr := ""
	i := 0
	for k := range set {
		if i > 0 {
			setStr += ", "
		}
		setStr += k + " = ?"
		i++
	}
	return setStr
}

// buildWhereSQL 构建WHERE子句
func buildWhereSQL(where map[string]interface{}) string {
	whereStr := ""
	i := 0
	for k := range where {
		if i > 0 {
			whereStr += " AND "
		}
		whereStr += k + " = ?"
		i++
	}
	return whereStr
}
