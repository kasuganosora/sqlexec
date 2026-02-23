package information_schema

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// SchemataTable represents information_schema.schemata
// It lists all databases (data sources) in the system
type SchemataTable struct {
	dsManager   *application.DataSourceManager
	vdbRegistry *virtual.VirtualDatabaseRegistry
}

// NewSchemataTable creates a new SchemataTable
func NewSchemataTable(dsManager *application.DataSourceManager, registry *virtual.VirtualDatabaseRegistry) virtual.VirtualTable {
	return &SchemataTable{
		dsManager:   dsManager,
		vdbRegistry: registry,
	}
}

// GetName returns the table name
func (t *SchemataTable) GetName() string {
	return "schemata"
}

// GetSchema returns the table schema
func (t *SchemataTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "catalog_name", Type: "varchar(512)", Nullable: false},
		{Name: "schema_name", Type: "varchar(64)", Nullable: false},
		{Name: "default_character_set_name", Type: "varchar(32)", Nullable: false},
		{Name: "default_collation_name", Type: "varchar(32)", Nullable: false},
		{Name: "sql_path", Type: "varchar(512)", Nullable: true},
	}
}

// Query executes a query against the schemata table
func (t *SchemataTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Get all data source names
	dsNames := t.dsManager.List()

	// Build result rows
	rows := make([]domain.Row, 0, len(dsNames)+2)

	// Add information_schema (always available)
	rows = append(rows, domain.Row{
		"catalog_name":               "def",
		"schema_name":                "information_schema",
		"default_character_set_name": "utf8mb4",
		"default_collation_name":     "utf8mb4_general_ci",
		"sql_path":                   nil,
	})

	// Add all registered virtual databases from registry
	if t.vdbRegistry != nil {
		for _, entry := range t.vdbRegistry.List() {
			rows = append(rows, domain.Row{
				"catalog_name":               "def",
				"schema_name":                entry.Name,
				"default_character_set_name": "utf8mb4",
				"default_collation_name":     "utf8mb4_general_ci",
				"sql_path":                   nil,
			})
		}
	}

	// Add all registered data sources
	for _, name := range dsNames {
		// Skip virtual databases (already added above)
		if name == "information_schema" {
			continue
		}
		if t.vdbRegistry != nil && t.vdbRegistry.IsVirtualDB(name) {
			continue
		}
		row := domain.Row{
			"catalog_name":               "def",
			"schema_name":                name,
			"default_character_set_name": "utf8mb4",
			"default_collation_name":     "utf8mb4_general_ci",
			"sql_path":                   nil,
		}
		rows = append(rows, row)
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

// applyFilters applies filters to the result rows
func (t *SchemataTable) applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
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
func (t *SchemataTable) matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
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
func (t *SchemataTable) matchesLike(value, pattern string) bool {
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
