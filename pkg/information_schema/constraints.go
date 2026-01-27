package information_schema

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// TableConstraintsTable represents information_schema.table_constraints
// It lists all constraints (PRIMARY, UNIQUE, FOREIGN KEY) across all tables
type TableConstraintsTable struct {
	dsManager *application.DataSourceManager
}

// NewTableConstraintsTable creates a new TableConstraintsTable
func NewTableConstraintsTable(dsManager *application.DataSourceManager) virtual.VirtualTable {
	return &TableConstraintsTable{
		dsManager: dsManager,
	}
}

// GetName returns table name
func (t *TableConstraintsTable) GetName() string {
	return "table_constraints"
}

// GetSchema returns table schema
func (t *TableConstraintsTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "constraint_catalog", Type: "varchar(512)", Nullable: false},
		{Name: "constraint_schema", Type: "varchar(64)", Nullable: false},
		{Name: "constraint_name", Type: "varchar(64)", Nullable: false},
		{Name: "table_schema", Type: "varchar(64)", Nullable: false},
		{Name: "table_name", Type: "varchar(64)", Nullable: false},
		{Name: "constraint_type", Type: "varchar(64)", Nullable: false},
	}
}

// Query executes a query against table_constraints table
func (t *TableConstraintsTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
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

			// Add PRIMARY KEY constraint if table has primary key
			hasPrimaryKey := false
			for _, column := range tableInfo.Columns {
				if column.Primary {
					hasPrimaryKey = true
					break
				}
			}

			if hasPrimaryKey {
				row := domain.Row{
					"constraint_catalog": "def",
					"constraint_schema":  dsName,
					"constraint_name":   "PRIMARY",
					"table_schema":      dsName,
					"table_name":        tableName,
					"constraint_type":    "PRIMARY KEY",
				}
				rows = append(rows, row)
			}

			// Add UNIQUE constraints for columns with unique flag
			for i, column := range tableInfo.Columns {
				if column.Unique && !column.Primary {
					row := domain.Row{
						"constraint_catalog": "def",
						"constraint_schema":  dsName,
						"constraint_name":   fmt.Sprintf("unique_%s", column.Name),
						"table_schema":      dsName,
						"table_name":        tableName,
						"constraint_type":    "UNIQUE",
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
func (t *TableConstraintsTable) applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
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
func (t *TableConstraintsTable) matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
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
func (t *TableConstraintsTable) matchesLike(value, pattern string) bool {
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
