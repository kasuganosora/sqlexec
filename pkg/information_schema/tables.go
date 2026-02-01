package information_schema

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// TablesTable represents information_schema.tables
// It lists all tables across all data sources
type TablesTable struct {
	dsManager *application.DataSourceManager
}

// NewTablesTable creates a new TablesTable
func NewTablesTable(dsManager *application.DataSourceManager) virtual.VirtualTable {
	return &TablesTable{
		dsManager: dsManager,
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
			"table_collation":   "utf8mb4_general_ci",
			"checksum":          nil,
			"create_options":    "",
			"table_comment":     "",
			"table_attributes":  serializeTableAttributes(tableInfo.Atts),
		}

			rows = append(rows, row)
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

// applyFilters applies filters to result rows
func (t *TablesTable) applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	for _, filter := range filters {
		var filteredRows []domain.Row

		for _, row := range rows {
			matches, err := t.matchesFilter(row, filter)
			if err != nil {
				return nil, err
			}
			if matches {
				filteredRows = append(filteredRows, row)
			}
		}

		rows = filteredRows
	}

	return rows, nil
}

// matchesFilter checks if a row matches a filter
func (t *TablesTable) matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
	value, exists := row[filter.Field]
	if !exists {
		return false, nil
	}

	// Convert value to string for comparison
	var strValue string
	if value == nil {
		strValue = ""
	} else {
		strValue = fmt.Sprintf("%v", value)
	}

	// Apply operator
	switch filter.Operator {
	case "=":
		return strValue == fmt.Sprintf("%v", filter.Value), nil
	case "!=":
		return strValue != fmt.Sprintf("%v", filter.Value), nil
	case "like":
		return t.matchesLike(strValue, fmt.Sprintf("%v", filter.Value)), nil
	default:
		return false, fmt.Errorf("unsupported filter operator: %s", filter.Operator)
	}
}

// matchesLike implements simple LIKE pattern matching
func (t *TablesTable) matchesLike(value, pattern string) bool {
	// Simple implementation - can be enhanced for full LIKE support
	if pattern == "%" {
		return true
	}
	if pattern == value {
		return true
	}
	if len(pattern) > 0 && pattern[0] == '%' && len(pattern) > 1 {
		suffix := pattern[1:]
		return len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix
	}
	if len(pattern) > 0 && pattern[len(pattern)-1] == '%' && len(pattern) > 1 {
		prefix := pattern[:len(pattern)-1]
		return len(value) >= len(prefix) && value[:len(prefix)] == prefix
	}
	return false
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
