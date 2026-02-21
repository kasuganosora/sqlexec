package information_schema

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// TablesTable represents information_schema.tables
// It lists all tables across all data sources
type TablesTable struct {
	dsManager   *application.DataSourceManager
	vdbRegistry *virtual.VirtualDatabaseRegistry
}

// NewTablesTable creates a new TablesTable
func NewTablesTable(dsManager *application.DataSourceManager, registry *virtual.VirtualDatabaseRegistry) virtual.VirtualTable {
	return &TablesTable{
		dsManager:   dsManager,
		vdbRegistry: registry,
	}
}

// GetName returns table name
func (t *TablesTable) GetName() string {
	return "tables"
}

// GetSchema returns table schema
func (t *TablesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "table_catalog", Type: "varchar(512)", Nullable: false},
		{Name: "table_schema", Type: "varchar(64)", Nullable: false},
		{Name: "table_name", Type: "varchar(64)", Nullable: false},
		{Name: "table_type", Type: "varchar(64)", Nullable: false},
		{Name: "engine", Type: "varchar(64)", Nullable: true},
		{Name: "version", Type: "bigint", Nullable: true},
		{Name: "row_format", Type: "varchar(10)", Nullable: true},
		{Name: "table_rows", Type: "bigint", Nullable: true},
		{Name: "avg_row_length", Type: "bigint", Nullable: true},
		{Name: "data_length", Type: "bigint", Nullable: true},
		{Name: "max_data_length", Type: "bigint", Nullable: true},
		{Name: "index_length", Type: "bigint", Nullable: true},
		{Name: "data_free", Type: "bigint", Nullable: true},
		{Name: "auto_increment", Type: "bigint", Nullable: true},
		{Name: "create_time", Type: "datetime", Nullable: true},
		{Name: "update_time", Type: "datetime", Nullable: true},
		{Name: "check_time", Type: "datetime", Nullable: true},
		{Name: "table_collation", Type: "varchar(32)", Nullable: true},
		{Name: "checksum", Type: "bigint", Nullable: true},
		{Name: "create_options", Type: "varchar(255)", Nullable: true},
		{Name: "table_comment", Type: "varchar(2048)", Nullable: true},
		{Name: "table_attributes", Type: "json", Nullable: true},
	}
}

// Query executes a query against tables table
func (t *TablesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Build result rows
	rows := make([]domain.Row, 0)

	// 权限检查：只有 root 用户可以看到所有表，包括权限表
	// 非特权用户只能看到基本的 information_schema 表
	allowPrivilegeTables := false
	if options != nil && options.User != "" {
		// root 用户可以看到所有表
		if options.User == "root" {
			allowPrivilegeTables = true
		}
	}

	// Add information_schema's own tables
	infoSchemaTables := []struct {
		name    string
		comment string
	}{
		{"schemata", "Databases"},
		{"tables", "Tables"},
		{"columns", "Table columns"},
		{"table_constraints", "Table constraints"},
		{"key_column_usage", "Key column usage"},
	}

	// 只有特权用户（如 root）才能看到权限表
	if allowPrivilegeTables {
		privilegeTables := []struct {
			name    string
			comment string
		}{
			{"USER_PRIVILEGES", "User privileges"},
			{"SCHEMA_PRIVILEGES", "Schema privileges"},
			{"TABLE_PRIVILEGES", "Table privileges"},
			{"COLUMN_PRIVILEGES", "Column privileges"},
		}
		infoSchemaTables = append(infoSchemaTables, privilegeTables...)
	}

	for _, table := range infoSchemaTables {
		row := domain.Row{
			"table_catalog":     "def",
			"table_schema":      "information_schema",
			"table_name":       table.name,
			"table_type":        "SYSTEM VIEW",
			"engine":            "MEMORY",
			"version":           int64(10),
			"row_format":        "Fixed",
			"table_rows":        int64(0),
			"avg_row_length":    int64(0),
			"data_length":       int64(0),
			"max_data_length":   int64(0),
			"index_length":      int64(0),
			"data_free":         int64(0),
			"auto_increment":    nil,
			"create_time":       time.Now(),
			"update_time":       nil,
			"check_time":        nil,
			"table_collation":   "utf8mb4_general_ci",
			"checksum":          nil,
			"create_options":    "",
			"table_comment":     table.comment,
			"table_attributes":  nil,
		}
		rows = append(rows, row)
	}

	// Get all data source names
	dsNames := t.dsManager.List()

	for _, dsName := range dsNames {
		// Get tables from this data source
		tables, err := t.dsManager.GetTables(ctx, dsName)
		if err != nil {
			// Skip data sources that fail
			continue
		}

		// Get table info for each table
		for _, tableName := range tables {
			tableInfo, err := t.dsManager.GetTableInfo(ctx, dsName, tableName)
			if err != nil {
				// Skip tables that fail
				continue
			}

		row := domain.Row{
			"table_catalog":     "def",
			"table_schema":      dsName,
			"table_name":       tableName,
			"table_type":        "BASE TABLE",
			"engine":            "MEMORY",
			"version":           int64(10),
			"row_format":        "Fixed",
			"table_rows":        int64(len(tableInfo.Columns)),
			"avg_row_length":    int64(0),
			"data_length":       int64(0),
			"max_data_length":   int64(0),
			"index_length":      int64(0),
			"data_free":         int64(0),
			"auto_increment":    nil,
			"create_time":       time.Now(),
			"update_time":       nil,
			"check_time":        nil,
			"table_collation":   getTableCollation(tableInfo),
			"checksum":          nil,
			"create_options":    "",
			"table_comment":     "",
			"table_attributes":  serializeTableAttributes(tableInfo.Atts),
		}

			rows = append(rows, row)
		}
	}

	// Add tables from all registered virtual databases
	if t.vdbRegistry != nil {
		for _, entry := range t.vdbRegistry.List() {
			for _, vTableName := range entry.Provider.ListVirtualTables() {
				row := domain.Row{
					"table_catalog":    "def",
					"table_schema":     entry.Name,
					"table_name":       vTableName,
					"table_type":       "SYSTEM VIEW",
					"engine":           "VIRTUAL",
					"version":          int64(10),
					"row_format":       "Dynamic",
					"table_rows":       int64(0),
					"avg_row_length":   int64(0),
					"data_length":      int64(0),
					"max_data_length":  int64(0),
					"index_length":     int64(0),
					"data_free":        int64(0),
					"auto_increment":   nil,
					"create_time":      time.Now(),
					"update_time":      nil,
					"check_time":       nil,
					"table_collation":  "utf8mb4_general_ci",
					"checksum":         nil,
					"create_options":   "",
					"table_comment":    "",
					"table_attributes": nil,
				}
				rows = append(rows, row)
			}
		}
	}

	// Apply filters if provided
	var err error
	if len(filters) > 0 {
		rows, err = t.applyFilters(rows, filters)
		if err != nil {
			return nil, fmt.Errorf("failed to apply filters: %w", err)
		}
	}

	// Apply limit/offset if specified
	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(rows) {
			end = len(rows)
		}
		if start >= len(rows) {
			rows = []domain.Row{}
		} else {
			rows = rows[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

// applyFilters applies filters to result rows (using utils package)
func (t *TablesTable) applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	return utils.ApplyFilters(rows, filters)
}

// matchesFilter checks if a row matches a filter (using utils package)
func (t *TablesTable) matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
	return utils.MatchesFilter(row, filter)
}

// matchesLike implements simple LIKE pattern matching (using utils package)
func (t *TablesTable) matchesLike(value, pattern string) bool {
	return utils.MatchesLike(value, pattern)
}

// serializeTableAttributes converts table attributes map to JSON string
func serializeTableAttributes(atts map[string]interface{}) interface{} {
	if atts == nil {
		return nil
	}

	// Convert to JSON
	jsonBytes, err := json.Marshal(atts)
	if err != nil {
		return nil
	}

	// Return as string
	return string(jsonBytes)
}

// getTableCollation returns the table's collation, falling back to the default
func getTableCollation(tableInfo *domain.TableInfo) string {
	if tableInfo != nil && tableInfo.Collation != "" {
		return tableInfo.Collation
	}
	return "utf8mb4_general_ci"
}
