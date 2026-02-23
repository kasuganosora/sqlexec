package information_schema

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// ViewsTable represents INFORMATION_SCHEMA.VIEWS
type ViewsTable struct {
	dsManager *application.DataSourceManager
}

// NewViewsTable creates a new VIEWS table
func NewViewsTable(dsManager *application.DataSourceManager) virtual.VirtualTable {
	return &ViewsTable{dsManager: dsManager}
}

// GetName returns table name
func (v *ViewsTable) GetName() string {
	return "VIEWS"
}

// GetSchema returns table schema
func (v *ViewsTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "TABLE_CATALOG", Type: "varchar(512)", Nullable: false},
		{Name: "TABLE_SCHEMA", Type: "varchar(64)", Nullable: false},
		{Name: "TABLE_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "VIEW_DEFINITION", Type: "longtext", Nullable: true},
		{Name: "CHECK_OPTION", Type: "varchar(8)", Nullable: true},
		{Name: "IS_UPDATABLE", Type: "varchar(3)", Nullable: false},
		{Name: "DEFINER", Type: "varchar(288)", Nullable: false},
		{Name: "SECURITY_TYPE", Type: "varchar(7)", Nullable: false},
		{Name: "CHARACTER_SET_CLIENT", Type: "varchar(64)", Nullable: true},
		{Name: "COLLATION_CONNECTION", Type: "varchar(64)", Nullable: true},
	}
}

// Query executes a query against VIEWS table
func (v *ViewsTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	rows := make([]domain.Row, 0)

	// Get all data sources
	dsNames := v.dsManager.List()
	for _, dsName := range dsNames {
		// Get tables from this data source
		tables, err := v.dsManager.GetTables(ctx, dsName)
		if err != nil {
			continue
		}

		for _, tableName := range tables {
			// Get table info
			tableInfo, err := v.dsManager.GetTableInfo(ctx, dsName, tableName)
			if err != nil {
				continue
			}

			// Check if it's a view (has view metadata in Atts)
			if tableInfo.Atts == nil {
				continue
			}
			viewData, ok := tableInfo.Atts[domain.ViewMetaKey]
			if !ok {
				continue
			}

			// Handle view metadata - can be either domain.ViewInfo or JSON string
			var viewInfo domain.ViewInfo
			switch v := viewData.(type) {
			case domain.ViewInfo:
				viewInfo = v
			case string:
				if err := json.Unmarshal([]byte(v), &viewInfo); err != nil {
					continue
				}
			default:
				continue
			}

			// Build view row
			row := domain.Row{
				"TABLE_CATALOG":        "def",
				"TABLE_SCHEMA":         dsName,
				"TABLE_NAME":           tableName,
				"VIEW_DEFINITION":      viewInfo.SelectStmt,
				"CHECK_OPTION":         checkOptionToString(viewInfo.CheckOption),
				"IS_UPDATABLE":         boolToYN(viewInfo.Updatable),
				"DEFINER":              viewInfo.Definer,
				"SECURITY_TYPE":        securityTypeToString(viewInfo.Security),
				"CHARACTER_SET_CLIENT": viewInfo.Charset,
				"COLLATION_CONNECTION": viewInfo.Collate,
			}
			rows = append(rows, row)
		}
	}

	// Apply filters
	var err error
	if len(filters) > 0 {
		rows, err = applyViewsFilters(rows, filters)
		if err != nil {
			return nil, err
		}
	}

	// Apply limit/offset
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
		Columns: v.GetSchema(),
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

// applyViewsFilters applies filters to view rows
func applyViewsFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	filtered := make([]domain.Row, 0, len(rows))

	for _, row := range rows {
		match := true
		for _, filter := range filters {
			val, exists := row[filter.Field]
			if !exists {
				match = false
				break
			}

			// Simple string comparison for now
			filterVal, ok := filter.Value.(string)
			if !ok {
				match = false
				break
			}

			rowVal, ok := val.(string)
			if !ok {
				match = false
				break
			}

			// Apply operator
			switch filter.Operator {
			case "=":
				match = match && rowVal == filterVal
			case "!=":
				match = match && rowVal != filterVal
			case "LIKE":
				match = match && matchLike(rowVal, filterVal)
			default:
				match = match && rowVal == filterVal
			}

			if !match {
				break
			}
		}

		if match {
			filtered = append(filtered, row)
		}
	}

	return filtered, nil
}

// matchLike performs case-insensitive LIKE matching
func matchLike(value, pattern string) bool {
	return utils.MatchesLike(strings.ToLower(value), strings.ToLower(pattern))
}

// checkOptionToString converts ViewCheckOption to string
func checkOptionToString(opt domain.ViewCheckOption) string {
	switch opt {
	case domain.ViewCheckOptionNone:
		return "NONE"
	case domain.ViewCheckOptionLocal:
		return "LOCAL"
	case domain.ViewCheckOptionCascaded:
		return "CASCADED"
	default:
		return ""
	}
}

// securityTypeToString converts ViewSecurity to string
func securityTypeToString(sec domain.ViewSecurity) string {
	switch sec {
	case domain.ViewSecurityDefiner:
		return "DEFINER"
	case domain.ViewSecurityInvoker:
		return "INVOKER"
	default:
		return ""
	}
}
