package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// SQLCommonDataSource implements domain.DataSource and domain.FilterableDataSource
// using database/sql. Both MySQL and PostgreSQL embed this struct.
type SQLCommonDataSource struct {
	mu        sync.RWMutex
	config    *domain.DataSourceConfig
	sqlCfg    *SQLConfig
	dialect   Dialect
	db        *sql.DB
	connected bool
}

// NewSQLCommonDataSource creates a new shared SQL datasource.
func NewSQLCommonDataSource(dsCfg *domain.DataSourceConfig, sqlCfg *SQLConfig, dialect Dialect) *SQLCommonDataSource {
	return &SQLCommonDataSource{
		config:  dsCfg,
		sqlCfg:  sqlCfg,
		dialect: dialect,
	}
}

// Connect opens the database connection and configures the pool.
func (ds *SQLCommonDataSource) Connect(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	dsn, err := ds.dialect.BuildDSN(ds.config, ds.sqlCfg)
	if err != nil {
		return &domain.ErrConnectionFailed{
			DataSourceType: ds.dialect.DriverName(),
			Reason:         fmt.Sprintf("build DSN: %v", err),
		}
	}

	db, err := sql.Open(ds.dialect.DriverName(), dsn)
	if err != nil {
		return &domain.ErrConnectionFailed{
			DataSourceType: ds.dialect.DriverName(),
			Reason:         err.Error(),
		}
	}

	// Configure pool
	db.SetMaxOpenConns(ds.sqlCfg.MaxOpenConns)
	db.SetMaxIdleConns(ds.sqlCfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(ds.sqlCfg.ConnMaxLifetime) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(ds.sqlCfg.ConnMaxIdleTime) * time.Second)

	// Verify connectivity
	pingCtx, cancel := context.WithTimeout(ctx, time.Duration(ds.sqlCfg.ConnectTimeout)*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return &domain.ErrConnectionFailed{
			DataSourceType: ds.dialect.DriverName(),
			Reason:         err.Error(),
		}
	}

	ds.db = db
	ds.connected = true
	return nil
}

// Close closes the database connection.
func (ds *SQLCommonDataSource) Close(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.connected = false
	if ds.db != nil {
		return ds.db.Close()
	}
	return nil
}

// IsConnected returns whether the datasource is connected.
func (ds *SQLCommonDataSource) IsConnected() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.connected
}

// IsWritable returns whether the datasource allows writes.
func (ds *SQLCommonDataSource) IsWritable() bool {
	return ds.config.Writable
}

// GetConfig returns the datasource configuration.
func (ds *SQLCommonDataSource) GetConfig() *domain.DataSourceConfig {
	return ds.config
}

// GetTables returns all user table names in the current database.
func (ds *SQLCommonDataSource) GetTables(ctx context.Context) ([]string, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(ds.dialect.DriverName())
	}

	rows, err := ds.db.QueryContext(ctx, ds.dialect.GetTablesQuery())
	if err != nil {
		return nil, fmt.Errorf("get tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// GetTableInfo returns column metadata for a table.
func (ds *SQLCommonDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(ds.dialect.DriverName())
	}

	rows, err := ds.db.QueryContext(ctx, ds.dialect.GetTableInfoQuery(), tableName)
	if err != nil {
		return nil, fmt.Errorf("get table info: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	var columns []domain.ColumnInfo
	for rows.Next() {
		values := make([]interface{}, len(colTypes))
		scanTargets := make([]interface{}, len(colTypes))
		for i := range values {
			scanTargets[i] = &values[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf("scan column info: %w", err)
		}

		col := ds.parseColumnInfo(values, colTypes)
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &domain.TableInfo{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func (ds *SQLCommonDataSource) parseColumnInfo(values []interface{}, colTypes []*sql.ColumnType) domain.ColumnInfo {
	// Build a map from column name to value
	m := make(map[string]interface{}, len(colTypes))
	for i, ct := range colTypes {
		m[strings.ToLower(ct.Name())] = normalizeValue(values[i])
	}

	colName, _ := m["column_name"].(string)
	colType, _ := m["column_type"].(string)
	if colType == "" {
		// PostgreSQL uses data_type instead of column_type
		colType, _ = m["data_type"].(string)
	}

	isNullable := false
	if n, ok := m["is_nullable"].(string); ok {
		isNullable = strings.EqualFold(n, "YES")
	}

	isPrimary := false
	if k, ok := m["column_key"].(string); ok {
		isPrimary = strings.EqualFold(k, "PRI")
	}

	isAutoInc := false
	if extra, ok := m["extra"].(string); ok {
		isAutoInc = strings.Contains(strings.ToLower(extra), "auto_increment")
	}

	colDefault := ""
	if d, ok := m["column_default"].(string); ok {
		colDefault = d
	}

	return domain.ColumnInfo{
		Name:          colName,
		Type:          ds.dialect.MapColumnType(colType, nil),
		Nullable:      isNullable,
		Primary:       isPrimary,
		Default:       colDefault,
		AutoIncrement: isAutoInc,
	}
}

// Query executes a SELECT query built from QueryOptions.
func (ds *SQLCommonDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(ds.dialect.DriverName())
	}

	querySQL, params := BuildSelectSQL(ds.dialect, tableName, options, 0)

	rows, err := ds.db.QueryContext(ctx, querySQL, params...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	data, columns, err := ScanRows(rows, ds.dialect)
	if err != nil {
		return nil, err
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    data,
		Total:   int64(len(data)),
	}, nil
}

// Insert inserts rows into a table.
func (ds *SQLCommonDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !ds.IsConnected() {
		return 0, domain.NewErrNotConnected(ds.dialect.DriverName())
	}
	if !ds.config.Writable {
		return 0, domain.NewErrReadOnly(ds.dialect.DriverName(), "INSERT")
	}

	insertSQL, params, _ := BuildInsertSQL(ds.dialect, tableName, rows)
	if insertSQL == "" {
		return 0, nil
	}

	result, err := ds.db.ExecContext(ctx, insertSQL, params...)
	if err != nil {
		return 0, fmt.Errorf("insert: %w", err)
	}

	return result.RowsAffected()
}

// Update updates rows matching filters.
func (ds *SQLCommonDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !ds.IsConnected() {
		return 0, domain.NewErrNotConnected(ds.dialect.DriverName())
	}
	if !ds.config.Writable {
		return 0, domain.NewErrReadOnly(ds.dialect.DriverName(), "UPDATE")
	}

	updateSQL, params := BuildUpdateSQL(ds.dialect, tableName, filters, updates)

	result, err := ds.db.ExecContext(ctx, updateSQL, params...)
	if err != nil {
		return 0, fmt.Errorf("update: %w", err)
	}

	return result.RowsAffected()
}

// Delete removes rows matching filters.
func (ds *SQLCommonDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !ds.IsConnected() {
		return 0, domain.NewErrNotConnected(ds.dialect.DriverName())
	}
	if !ds.config.Writable {
		return 0, domain.NewErrReadOnly(ds.dialect.DriverName(), "DELETE")
	}

	deleteSQL, params := BuildDeleteSQL(ds.dialect, tableName, filters)

	result, err := ds.db.ExecContext(ctx, deleteSQL, params...)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// CreateTable creates a table. Builds CREATE TABLE from TableInfo.
func (ds *SQLCommonDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	if !ds.IsConnected() {
		return domain.NewErrNotConnected(ds.dialect.DriverName())
	}
	if !ds.config.Writable {
		return domain.NewErrReadOnly(ds.dialect.DriverName(), "CREATE TABLE")
	}

	sql := ds.buildCreateTableSQL(tableInfo)
	_, err := ds.db.ExecContext(ctx, sql)
	return err
}

// DropTable drops a table.
func (ds *SQLCommonDataSource) DropTable(ctx context.Context, tableName string) error {
	if !ds.IsConnected() {
		return domain.NewErrNotConnected(ds.dialect.DriverName())
	}
	if !ds.config.Writable {
		return domain.NewErrReadOnly(ds.dialect.DriverName(), "DROP TABLE")
	}

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", ds.dialect.QuoteIdentifier(tableName))
	_, err := ds.db.ExecContext(ctx, sql)
	return err
}

// TruncateTable truncates a table.
func (ds *SQLCommonDataSource) TruncateTable(ctx context.Context, tableName string) error {
	if !ds.IsConnected() {
		return domain.NewErrNotConnected(ds.dialect.DriverName())
	}
	if !ds.config.Writable {
		return domain.NewErrReadOnly(ds.dialect.DriverName(), "TRUNCATE TABLE")
	}

	sql := fmt.Sprintf("TRUNCATE TABLE %s", ds.dialect.QuoteIdentifier(tableName))
	_, err := ds.db.ExecContext(ctx, sql)
	return err
}

// Execute runs raw SQL. SELECT-like statements return rows; DML/DDL returns affected count.
func (ds *SQLCommonDataSource) Execute(ctx context.Context, rawSQL string) (*domain.QueryResult, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(ds.dialect.DriverName())
	}

	trimmed := strings.TrimSpace(strings.ToUpper(rawSQL))
	isQuery := strings.HasPrefix(trimmed, "SELECT") ||
		strings.HasPrefix(trimmed, "SHOW") ||
		strings.HasPrefix(trimmed, "DESCRIBE") ||
		strings.HasPrefix(trimmed, "DESC ") ||
		strings.HasPrefix(trimmed, "EXPLAIN") ||
		strings.HasPrefix(trimmed, "WITH")

	if isQuery {
		rows, err := ds.db.QueryContext(ctx, rawSQL)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}
		defer rows.Close()

		data, columns, err := ScanRows(rows, ds.dialect)
		if err != nil {
			return nil, err
		}

		return &domain.QueryResult{
			Columns: columns,
			Rows:    data,
			Total:   int64(len(data)),
		}, nil
	}

	// DML/DDL
	if !ds.config.Writable {
		return nil, domain.NewErrReadOnly(ds.dialect.DriverName(), "EXECUTE")
	}

	result, err := ds.db.ExecContext(ctx, rawSQL)
	if err != nil {
		return nil, fmt.Errorf("execute: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &domain.QueryResult{
		Total: affected,
	}, nil
}

// ── FilterableDataSource ──

// SupportsFiltering always returns true for SQL datasources.
func (ds *SQLCommonDataSource) SupportsFiltering(tableName string) bool {
	return true
}

// Filter executes a filtered query with offset/limit.
func (ds *SQLCommonDataSource) Filter(ctx context.Context, tableName string, filter domain.Filter, offset, limit int) ([]domain.Row, int64, error) {
	if !ds.IsConnected() {
		return nil, 0, domain.NewErrNotConnected(ds.dialect.DriverName())
	}

	var filters []domain.Filter
	if filter.Field != "" || filter.Logic != "" || len(filter.SubFilters) > 0 {
		filters = []domain.Filter{filter}
	}

	options := &domain.QueryOptions{
		Filters: filters,
		Offset:  offset,
		Limit:   limit,
	}

	querySQL, params := BuildSelectSQL(ds.dialect, tableName, options, 0)

	rows, err := ds.db.QueryContext(ctx, querySQL, params...)
	if err != nil {
		return nil, 0, fmt.Errorf("filter: %w", err)
	}
	defer rows.Close()

	data, _, err := ScanRows(rows, ds.dialect)
	if err != nil {
		return nil, 0, err
	}

	return data, int64(len(data)), nil
}

// GetDatabaseName returns the virtual database name for this datasource.
func (ds *SQLCommonDataSource) GetDatabaseName() string {
	return ds.dialect.GetDatabaseName(ds.config, ds.sqlCfg)
}

// buildCreateTableSQL generates CREATE TABLE SQL from domain.TableInfo.
func (ds *SQLCommonDataSource) buildCreateTableSQL(info *domain.TableInfo) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(ds.dialect.QuoteIdentifier(info.Name))
	sb.WriteString(" (\n")

	var colDefs []string
	var pkCols []string

	for _, col := range info.Columns {
		def := "  " + ds.dialect.QuoteIdentifier(col.Name) + " " + ds.mapDomainTypeToSQL(col.Type)
		if !col.Nullable {
			def += " NOT NULL"
		}
		if col.AutoIncrement {
			if ds.dialect.DriverName() == "mysql" {
				def += " AUTO_INCREMENT"
			}
			// PostgreSQL uses SERIAL type instead
		}
		if col.Default != "" {
			def += " DEFAULT " + col.Default
		}
		colDefs = append(colDefs, def)

		if col.Primary {
			pkCols = append(pkCols, ds.dialect.QuoteIdentifier(col.Name))
		}
	}

	if len(pkCols) > 0 {
		colDefs = append(colDefs, "  PRIMARY KEY ("+strings.Join(pkCols, ", ")+")")
	}

	sb.WriteString(strings.Join(colDefs, ",\n"))
	sb.WriteString("\n)")

	return sb.String()
}

func (ds *SQLCommonDataSource) mapDomainTypeToSQL(domainType string) string {
	switch strings.ToLower(domainType) {
	case "int", "integer", "int64", "bigint":
		if ds.dialect.DriverName() == "postgres" {
			return "BIGINT"
		}
		return "BIGINT"
	case "int32", "smallint":
		return "INT"
	case "string", "text", "varchar":
		return "TEXT"
	case "float64", "double", "decimal", "numeric":
		if ds.dialect.DriverName() == "postgres" {
			return "DOUBLE PRECISION"
		}
		return "DOUBLE"
	case "float32", "float":
		return "REAL"
	case "bool", "boolean":
		return "BOOLEAN"
	case "datetime", "timestamp":
		if ds.dialect.DriverName() == "postgres" {
			return "TIMESTAMP"
		}
		return "DATETIME"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	default:
		return "TEXT"
	}
}
