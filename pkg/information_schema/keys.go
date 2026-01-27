package information_schema

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// KeyColumnUsageTable represents information_schema.key_column_usage
// It lists all key columns (primary key, unique key, foreign key) across all tables
type KeyColumnUsageTable struct {
	dsManager *application.DataSourceManager
}

// NewKeyColumnUsageTable creates a new KeyColumnUsageTable
func NewKeyColumnUsageTable(dsManager *application.DataSourceManager) virtual.VirtualTable {
	return &KeyColumnUsageTable{
		dsManager: dsManager,
	}
}

// GetName returns table name
func (t *KeyColumnUsageTable) GetName() string {
	return "key_column_usage"
}

// GetSchema returns table schema
func (t *KeyColumnUsageTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "constraint_catalog", Type: "varchar(512)", Nullable: true},
		{Name: "constraint_schema", Type: "varchar(64)", Nullable: true},
		{Name: "constraint_name", Type: "varchar(64)", Nullable: true},
		{Name: "table_catalog", Type: "varchar(512)", Nullable: false},
		{Name: "table_schema", Type: "varchar(64)", Nullable: false},
		{Name: "table_name", Type: "varchar(64)", Nullable: false},
		{Name: "column_name", Type: "varchar(64)", Nullable: false},
		{Name: "ordinal_position", Type: "bigint", Nullable: true},
		{Name: "position_in_unique_constraint", Type: "bigint", Nullable: true},
		{Name: "referenced_table_schema", Type: "varchar(64)", Nullable: true},
		{Name: "referenced_table_name", Type: "varchar(64)", Nullable: true},
		{Name: "referenced_column_name", Type: "varchar(64)", Nullable: true},
	}
}

// Query executes a query against key_column_usage table
func (t *KeyColumnUsageTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Get all data source names
	dsNames := t.dsManager.List()

	// Build result rows
	rows := make([]domain.Row, 0)

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

			// Add primary key columns
			ordinalPos := 1
			for i, column := range tableInfo.Columns {
				if column.Primary {
					row := domain.Row{
						"constraint_catalog":              "def",
						"constraint_schema":               dsName,
						"constraint_name":                "PRIMARY",
						"table_catalog":                  "def",
						"table_schema":                   dsName,
						"table_name":                     tableName,
						"column_name":                    column.Name,
						"ordinal_position":               ordinalPos,
						"position_in_unique_constraint":   ordinalPos,
						"referenced_table_schema":        nil,
						"referenced_table_name":          nil,
						"referenced_column_name":         nil,
					}
					rows = append(rows, row)
					ordinalPos++
					_ = i // Use index to avoid linter warning
				}
			}

			// Add unique constraint columns
			for i, column := range tableInfo.Columns {
				if column.Unique && !column.Primary {
					row := domain.Row{
						"constraint_catalog":              "def",
						"constraint_schema":               dsName,
						"constraint_name":                fmt.Sprintf("unique_%s", column.Name),
						"table_catalog":                  "def",
						"table_schema":                   dsName,
						"table_name":                     tableName,
						"column_name":                    column.Name,
						"ordinal_position":               1,
						"position_in_unique_constraint":   1,
						"referenced_table_schema":        nil,
						"referenced_table_name":          nil,
						"referenced_column_name":         nil,
					}
					rows = append(rows, row)
					_ = i // Use index to avoid linter warning
				}
			}

				// Add foreign key columns (if any)
				// Note: Foreign key information can be extended when full FK support is added
				for i, column := range tableInfo.Columns {
					if column.ForeignKey != nil {
						row := domain.Row{
							"constraint_catalog":              "def",
							"constraint_schema":               dsName,
							"constraint_name":                fmt.Sprintf("fk_%s_%s", tableName, column.Name),
							"table_catalog":                  "def",
							"table_schema":                   dsName,
							"table_name":                     tableName,
							"column_name":                    column.Name,
							"ordinal_position":               1,
							"position_in_unique_constraint":   nil,
							"referenced_table_schema":        column.ForeignKey.Table,
							"referenced_table_name":          column.ForeignKey.Table,
							"referenced_column_name":         column.ForeignKey.Column,
						}
						rows = append(rows, row)
						_ = i // Use index to avoid linter warning
					}
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

// applyFilters applies filters to result rows
func (t *KeyColumnUsageTable) applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
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
func (t *KeyColumnUsageTable) matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
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
func (t *KeyColumnUsageTable) matchesLike(value, pattern string) bool {
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
