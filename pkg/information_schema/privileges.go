package information_schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
	"github.com/kasuganosora/sqlexec/server/acl"
)

// UserPrivilegesTable represents INFORMATION_SCHEMA.USER_PRIVILEGES
type UserPrivilegesTable struct {
	aclMgr ACLManager
}

// NewUserPrivilegesTable creates a new USER_PRIVILEGES table
func NewUserPrivilegesTable(aclMgr ACLManager) virtual.VirtualTable {
	return &UserPrivilegesTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *UserPrivilegesTable) GetName() string {
	return "USER_PRIVILEGES"
}

// GetSchema returns table schema
func (t *UserPrivilegesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "GRANTEE", Type: "varchar(292)", Nullable: false},
		{Name: "TABLE_CATALOG", Type: "varchar(512)", Nullable: false},
		{Name: "PRIVILEGE_TYPE", Type: "varchar(64)", Nullable: false},
		{Name: "IS_GRANTABLE", Type: "varchar(3)", Nullable: false},
	}
}

// Query executes a query against USER_PRIVILEGES table
func (t *UserPrivilegesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Return empty result if ACL manager is not available or not loaded
	if t.aclMgr == nil || !t.aclMgr.IsLoaded() {
		return &domain.QueryResult{
			Columns: t.GetSchema(),
			Rows:    []domain.Row{},
			Total:   0,
		}, nil
	}

	rows := make([]domain.Row, 0)

	// Get all users
	users := t.aclMgr.GetUsers()

	// Convert to privilege rows
	for _, user := range users {
		// Get privileges that are granted
		for privType, granted := range user.Privileges {
			if granted && privType != "GRANT OPTION" {
				row := domain.Row{
					"GRANTEE":        fmt.Sprintf("'%s'@'%s'", user.User, user.Host),
					"TABLE_CATALOG":  "def",
					"PRIVILEGE_TYPE": privType,
					"IS_GRANTABLE":   boolToYN(t.hasGrantOption(*user)),
				}
				rows = append(rows, row)
			}
		}
	}

	// Apply filters
	var err error
	if len(filters) > 0 {
		rows, err = applyPrivilegeFilters(rows, filters)
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
		Columns: t.GetSchema(),
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

// hasGrantOption checks if user has GRANT OPTION
func (t *UserPrivilegesTable) hasGrantOption(user acl.User) bool {
	if grant, exists := user.Privileges["GRANT OPTION"]; exists {
		return grant
	}
	return false
}

// SchemaPrivilegesTable represents INFORMATION_SCHEMA.SCHEMA_PRIVILEGES
type SchemaPrivilegesTable struct {
	aclMgr ACLManager
}

// NewSchemaPrivilegesTable creates a new SCHEMA_PRIVILEGES table
func NewSchemaPrivilegesTable(aclMgr ACLManager) virtual.VirtualTable {
	return &SchemaPrivilegesTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *SchemaPrivilegesTable) GetName() string {
	return "SCHEMA_PRIVILEGES"
}

// GetSchema returns table schema
func (t *SchemaPrivilegesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "GRANTEE", Type: "varchar(292)", Nullable: false},
		{Name: "TABLE_CATALOG", Type: "varchar(512)", Nullable: false},
		{Name: "TABLE_SCHEMA", Type: "varchar(64)", Nullable: false},
		{Name: "PRIVILEGE_TYPE", Type: "varchar(64)", Nullable: false},
		{Name: "IS_GRANTABLE", Type: "varchar(3)", Nullable: false},
	}
}

// Query executes a query against SCHEMA_PRIVILEGES table
func (t *SchemaPrivilegesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Note: This will need to query database permissions from ACLManager
	// For now, return empty result
	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// TablePrivilegesTable represents INFORMATION_SCHEMA.TABLE_PRIVILEGES
type TablePrivilegesTable struct {
	aclMgr ACLManager
}

// NewTablePrivilegesTable creates a new TABLE_PRIVILEGES table
func NewTablePrivilegesTable(aclMgr ACLManager) virtual.VirtualTable {
	return &TablePrivilegesTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *TablePrivilegesTable) GetName() string {
	return "TABLE_PRIVILEGES"
}

// GetSchema returns table schema
func (t *TablePrivilegesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "GRANTEE", Type: "varchar(292)", Nullable: false},
		{Name: "TABLE_CATALOG", Type: "varchar(512)", Nullable: false},
		{Name: "TABLE_SCHEMA", Type: "varchar(64)", Nullable: false},
		{Name: "TABLE_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "PRIVILEGE_TYPE", Type: "varchar(64)", Nullable: false},
		{Name: "IS_GRANTABLE", Type: "varchar(3)", Nullable: false},
	}
}

// Query executes a query against TABLE_PRIVILEGES table
func (t *TablePrivilegesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Note: This will need to query table permissions from ACLManager
	// For now, return empty result
	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// ColumnPrivilegesTable represents INFORMATION_SCHEMA.COLUMN_PRIVILEGES
type ColumnPrivilegesTable struct {
	aclMgr ACLManager
}

// NewColumnPrivilegesTable creates a new COLUMN_PRIVILEGES table
func NewColumnPrivilegesTable(aclMgr ACLManager) virtual.VirtualTable {
	return &ColumnPrivilegesTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *ColumnPrivilegesTable) GetName() string {
	return "COLUMN_PRIVILEGES"
}

// GetSchema returns table schema
func (t *ColumnPrivilegesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "GRANTEE", Type: "varchar(292)", Nullable: false},
		{Name: "TABLE_CATALOG", Type: "varchar(512)", Nullable: false},
		{Name: "TABLE_SCHEMA", Type: "varchar(64)", Nullable: false},
		{Name: "TABLE_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "COLUMN_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "PRIVILEGE_TYPE", Type: "varchar(64)", Nullable: false},
		{Name: "IS_GRANTABLE", Type: "varchar(3)", Nullable: false},
	}
}

// Query executes a query against COLUMN_PRIVILEGES table
func (t *ColumnPrivilegesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Note: This will need to query column permissions from ACLManager
	// For now, return empty result
	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// Helper functions

// boolToYN converts bool to 'Y'/'N'
func boolToYN(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

// applyPrivilegeFilters applies filters to privilege rows
func applyPrivilegeFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	for _, filter := range filters {
		var filteredRows []domain.Row

		for _, row := range rows {
			matches, err := matchesPrivilegeFilter(row, filter)
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

// matchesPrivilegeFilter checks if a row matches a filter
func matchesPrivilegeFilter(row domain.Row, filter domain.Filter) (bool, error) {
	value, exists := row[filter.Field]
	if !exists {
		return false, nil
	}

	var strValue string
	if value == nil {
		strValue = ""
	} else {
		strValue = fmt.Sprintf("%v", value)
	}

	switch filter.Operator {
	case "=":
		return strValue == fmt.Sprintf("%v", filter.Value), nil
	case "!=":
		return strValue != fmt.Sprintf("%v", filter.Value), nil
	case "like":
		return matchesPrivilegeLike(strValue, fmt.Sprintf("%v", filter.Value)), nil
	default:
		return false, fmt.Errorf("unsupported filter operator: %s", filter.Operator)
	}
}

// matchesPrivilegeLike performs case-insensitive LIKE matching for privileges
func matchesPrivilegeLike(value, pattern string) bool {
	return utils.MatchesLike(strings.ToUpper(value), strings.ToUpper(pattern))
}
