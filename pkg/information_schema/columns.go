package information_schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// ColumnsTable represents information_schema.columns
// It lists all columns across all tables in all data sources
type ColumnsTable struct {
	dsManager   *application.DataSourceManager
	vdbRegistry *virtual.VirtualDatabaseRegistry
}

// NewColumnsTable creates a new ColumnsTable
func NewColumnsTable(dsManager *application.DataSourceManager, registry *virtual.VirtualDatabaseRegistry) virtual.VirtualTable {
	return &ColumnsTable{
		dsManager:   dsManager,
		vdbRegistry: registry,
	}
}

// GetName returns table name
func (t *ColumnsTable) GetName() string {
	return "columns"
}

// GetSchema returns table schema
func (t *ColumnsTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "table_catalog", Type: "varchar(512)", Nullable: false},
		{Name: "table_schema", Type: "varchar(64)", Nullable: false},
		{Name: "table_name", Type: "varchar(64)", Nullable: false},
		{Name: "column_name", Type: "varchar(64)", Nullable: false},
		{Name: "ordinal_position", Type: "int", Nullable: false},
		{Name: "column_default", Type: "text", Nullable: true},
		{Name: "is_nullable", Type: "varchar(3)", Nullable: false},
		{Name: "data_type", Type: "varchar(64)", Nullable: false},
		{Name: "character_maximum_length", Type: "int", Nullable: true},
		{Name: "character_octet_length", Type: "int", Nullable: true},
		{Name: "numeric_precision", Type: "int", Nullable: true},
		{Name: "numeric_scale", Type: "int", Nullable: true},
		{Name: "datetime_precision", Type: "int", Nullable: true},
		{Name: "character_set_name", Type: "varchar(64)", Nullable: true},
		{Name: "collation_name", Type: "varchar(64)", Nullable: true},
		{Name: "column_type", Type: "varchar(64)", Nullable: false},
		{Name: "column_key", Type: "varchar(10)", Nullable: true},
		{Name: "extra", Type: "varchar(256)", Nullable: true},
		{Name: "privileges", Type: "varchar(80)", Nullable: true},
		{Name: "column_comment", Type: "varchar(1024)", Nullable: true},
		{Name: "generation_expression", Type: "text", Nullable: true},
	}
}

// Query executes a query against columns table
func (t *ColumnsTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
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

			// Generate a row for each column
			for i, column := range tableInfo.Columns {
				columnType := t.getColumnType(column)
				charMaxLen := t.getCharacterMaxLength(column.Type)
				numPrecision := t.getNumericPrecision(column.Type)

				extra := ""
				if column.AutoIncrement {
					extra = "auto_increment"
				}

				columnKey := ""
				if column.Primary {
					columnKey = "PRI"
				} else if column.Unique {
					columnKey = "UNI"
				}

				nullable := "NO"
				if column.Nullable {
					nullable = "YES"
				}

				defaultValue := ""
				if column.Default != "" {
					defaultValue = column.Default
				}

				row := domain.Row{
					"table_catalog":            "def",
					"table_schema":             dsName,
					"table_name":               tableName,
					"column_name":              column.Name,
					"ordinal_position":         i + 1,
					"column_default":           defaultValue,
					"is_nullable":              nullable,
					"data_type":                t.getDataType(column.Type),
					"character_maximum_length": charMaxLen,
					"character_octet_length":   charMaxLen * 4, // Assuming UTF-8 (4 bytes per char)
					"numeric_precision":        numPrecision,
					"numeric_scale":            0,
					"datetime_precision":       nil,
					"character_set_name":       getColumnCharset(column),
					"collation_name":           getColumnCollation(column),
					"column_type":              columnType,
					"column_key":               columnKey,
					"extra":                    extra,
					"privileges":               "select,insert,update,references",
					"column_comment":           "",
					"generation_expression":    nil,
				}

				rows = append(rows, row)
			}
		}
	}

	// Add columns from all registered virtual databases
	if t.vdbRegistry != nil {
		for _, entry := range t.vdbRegistry.List() {
			for _, vTableName := range entry.Provider.ListVirtualTables() {
				vt, vtErr := entry.Provider.GetVirtualTable(vTableName)
				if vtErr != nil {
					continue
				}
				for i, column := range vt.GetSchema() {
					columnType := t.getColumnType(column)
					charMaxLen := t.getCharacterMaxLength(column.Type)
					numPrecision := t.getNumericPrecision(column.Type)

					columnKey := ""
					if column.Primary {
						columnKey = "PRI"
					}
					nullable := "NO"
					if column.Nullable {
						nullable = "YES"
					}

					row := domain.Row{
						"table_catalog":            "def",
						"table_schema":             entry.Name,
						"table_name":               vTableName,
						"column_name":              column.Name,
						"ordinal_position":         i + 1,
						"column_default":           "",
						"is_nullable":              nullable,
						"data_type":                t.getDataType(column.Type),
						"character_maximum_length": charMaxLen,
						"character_octet_length":   charMaxLen * 4,
						"numeric_precision":        numPrecision,
						"numeric_scale":            0,
						"datetime_precision":       nil,
						"character_set_name":       "utf8mb4",
						"collation_name":           "utf8mb4_general_ci",
						"column_type":              columnType,
						"column_key":               columnKey,
						"extra":                    "",
						"privileges":               "select,insert,update,references",
						"column_comment":           "",
						"generation_expression":    nil,
					}
					rows = append(rows, row)
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

// applyFilters applies filters to result rows (using utils package)
func (t *ColumnsTable) applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	return utils.ApplyFilters(rows, filters)
}

// matchesFilter checks if a row matches a filter (using utils package)
func (t *ColumnsTable) matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
	return utils.MatchesFilter(row, filter)
}

// matchesLike implements simple LIKE pattern matching (using utils package)
func (t *ColumnsTable) matchesLike(value, pattern string) bool {
	return utils.MatchesLike(value, pattern)
}

// getDataType extracts the data type from column type string
func (t *ColumnsTable) getDataType(columnType string) string {
	columnType = strings.ToUpper(columnType)
	switch {
	case strings.HasPrefix(columnType, "INT"):
		return "int"
	case strings.HasPrefix(columnType, "BIGINT"):
		return "bigint"
	case strings.HasPrefix(columnType, "VARCHAR"):
		return "varchar"
	case strings.HasPrefix(columnType, "TEXT"):
		return "text"
	case strings.HasPrefix(columnType, "CHAR"):
		return "char"
	case strings.HasPrefix(columnType, "DECIMAL"):
		return "decimal"
	case strings.HasPrefix(columnType, "FLOAT"):
		return "float"
	case strings.HasPrefix(columnType, "DOUBLE"):
		return "double"
	case strings.HasPrefix(columnType, "DATETIME"):
		return "datetime"
	case strings.HasPrefix(columnType, "TIMESTAMP"):
		return "timestamp"
	case strings.HasPrefix(columnType, "DATE"):
		return "date"
	case strings.HasPrefix(columnType, "BOOLEAN") || strings.HasPrefix(columnType, "BOOL"):
		return "boolean"
	default:
		return columnType
	}
}

// getColumnType returns the full column type string
func (t *ColumnsTable) getColumnType(column domain.ColumnInfo) string {
	if !strings.Contains(column.Type, "(") {
		return column.Type
	}

	// Already has length specification
	return column.Type
}

// getCharacterMaxLength returns the maximum character length for the column
func (t *ColumnsTable) getCharacterMaxLength(columnType string) int64 {
	columnType = strings.ToUpper(columnType)

	switch {
	case strings.HasPrefix(columnType, "VARCHAR"):
		// Extract length from VARCHAR(n)
		if i := strings.Index(columnType, "("); i > 0 {
			if j := strings.Index(columnType, ")"); j > i {
				var length int64
				fmt.Sscanf(columnType[i+1:j], "%d", &length)
				return length
			}
		}
		return 65535
	case strings.HasPrefix(columnType, "CHAR"):
		// Extract length from CHAR(n)
		if i := strings.Index(columnType, "("); i > 0 {
			if j := strings.Index(columnType, ")"); j > i {
				var length int64
				fmt.Sscanf(columnType[i+1:j], "%d", &length)
				return length
			}
		}
		return 255
	case strings.HasPrefix(columnType, "TEXT"):
		return 65535
	case strings.HasPrefix(columnType, "MEDIUMTEXT"):
		return 16777215
	case strings.HasPrefix(columnType, "LONGTEXT"):
		return 4294967295
	default:
		return 0
	}
}

// getNumericPrecision returns the numeric precision for the column
func (t *ColumnsTable) getNumericPrecision(columnType string) int64 {
	columnType = strings.ToUpper(columnType)

	switch {
	case strings.HasPrefix(columnType, "TINYINT"):
		return 3
	case strings.HasPrefix(columnType, "SMALLINT"):
		return 5
	case strings.HasPrefix(columnType, "MEDIUMINT"):
		return 7
	case strings.HasPrefix(columnType, "INT"):
		return 10
	case strings.HasPrefix(columnType, "BIGINT"):
		return 19
	case strings.HasPrefix(columnType, "FLOAT"):
		return 7
	case strings.HasPrefix(columnType, "DOUBLE"):
		return 15
	case strings.HasPrefix(columnType, "DECIMAL"):
		// Extract precision from DECIMAL(p,s)
		if i := strings.Index(columnType, "("); i > 0 {
			if j := strings.Index(columnType, ")"); j > i {
				var precision int64
				fmt.Sscanf(columnType[i+1:j], "%d", &precision)
				return precision
			}
		}
		return 10
	default:
		return 0
	}
}

// getColumnCollation returns the column's collation, falling back to the default
func getColumnCollation(col domain.ColumnInfo) string {
	if col.Collation != "" {
		return col.Collation
	}
	return "utf8mb4_general_ci"
}

// getColumnCharset returns the column's charset, falling back to the default
func getColumnCharset(col domain.ColumnInfo) string {
	if col.Charset != "" {
		return col.Charset
	}
	return "utf8mb4"
}
