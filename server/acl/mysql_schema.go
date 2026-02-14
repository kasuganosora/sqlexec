package acl

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// MySQLUserTable represents mysql.user virtual table
type MySQLUserTable struct {
	aclMgr *ACLManager
}

// NewMySQLUserTable creates a new mysql.user virtual table
func NewMySQLUserTable(aclMgr *ACLManager) virtual.VirtualTable {
	return &MySQLUserTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *MySQLUserTable) GetName() string {
	return "user"
}

// GetSchema returns table schema (matching MySQL 5.7+ structure)
func (t *MySQLUserTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "Host", Type: "char(255)", Nullable: false},
		{Name: "User", Type: "char(32)", Nullable: false},
		{Name: "Password", Type: "char(41)", Nullable: true},
		{Name: "Select_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Insert_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Update_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Delete_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Drop_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Reload_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Shutdown_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Process_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "File_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Grant_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "References_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Index_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Alter_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Show_db_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Super_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_tmp_table_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Lock_tables_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Execute_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Repl_slave_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Repl_client_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_view_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Show_view_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_routine_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Alter_routine_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_user_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Event_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Trigger_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_tablespace_priv", Type: "enum('N','Y')", Default: "N"},
	}
}

// Query executes a query against mysql.user table
func (t *MySQLUserTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	if t.aclMgr == nil || !t.aclMgr.IsLoaded() {
		return nil, fmt.Errorf("ACL manager not initialized")
	}

	// Get all users
	users := t.aclMgr.GetUsers()

	// Convert to rows
	rows := make([]domain.Row, 0, len(users))
	for _, user := range users {
		row := domain.Row{
			"Host":                    user.Host,
			"User":                    user.User,
			"Password":                user.Password,
			"Select_priv":             boolToYN(user.Privileges["SELECT"]),
			"Insert_priv":             boolToYN(user.Privileges["INSERT"]),
			"Update_priv":             boolToYN(user.Privileges["UPDATE"]),
			"Delete_priv":             boolToYN(user.Privileges["DELETE"]),
			"Create_priv":             boolToYN(user.Privileges["CREATE"]),
			"Drop_priv":               boolToYN(user.Privileges["DROP"]),
			"Reload_priv":             boolToYN(user.Privileges["RELOAD"]),
			"Shutdown_priv":           boolToYN(user.Privileges["SHUTDOWN"]),
			"Process_priv":            boolToYN(user.Privileges["PROCESS"]),
			"File_priv":              boolToYN(user.Privileges["FILE"]),
			"Grant_priv":              boolToYN(user.Privileges["GRANT OPTION"]),
			"References_priv":         boolToYN(user.Privileges["REFERENCES"]),
			"Index_priv":             boolToYN(user.Privileges["INDEX"]),
			"Alter_priv":             boolToYN(user.Privileges["ALTER"]),
			"Show_db_priv":           boolToYN(user.Privileges["SHOW DATABASES"]),
			"Super_priv":             boolToYN(user.Privileges["SUPER"]),
			"Create_tmp_table_priv":   boolToYN(user.Privileges["CREATE TEMPORARY TABLES"]),
			"Lock_tables_priv":       boolToYN(user.Privileges["LOCK TABLES"]),
			"Execute_priv":           boolToYN(user.Privileges["EXECUTE"]),
			"Repl_slave_priv":        boolToYN(user.Privileges["REPLICATION SLAVE"]),
			"Repl_client_priv":       boolToYN(user.Privileges["REPLICATION CLIENT"]),
			"Create_view_priv":       boolToYN(user.Privileges["CREATE VIEW"]),
			"Show_view_priv":         boolToYN(user.Privileges["SHOW VIEW"]),
			"Create_routine_priv":     boolToYN(user.Privileges["CREATE ROUTINE"]),
			"Alter_routine_priv":      boolToYN(user.Privileges["ALTER ROUTINE"]),
			"Create_user_priv":       boolToYN(user.Privileges["CREATE USER"]),
			"Event_priv":             boolToYN(user.Privileges["EVENT"]),
			"Trigger_priv":           boolToYN(user.Privileges["TRIGGER"]),
			"Create_tablespace_priv": boolToYN(user.Privileges["CREATE TABLESPACE"]),
		}
		rows = append(rows, row)
	}

	// Apply filters
	var err error
	if len(filters) > 0 {
		rows, err = applyFilters(rows, filters)
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

// MySQLDBTable represents mysql.db virtual table
type MySQLDBTable struct {
	aclMgr *ACLManager
}

// NewMySQLDBTable creates a new mysql.db virtual table
func NewMySQLDBTable(aclMgr *ACLManager) virtual.VirtualTable {
	return &MySQLDBTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *MySQLDBTable) GetName() string {
	return "db"
}

// GetSchema returns table schema
func (t *MySQLDBTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "Host", Type: "char(255)", Nullable: false},
		{Name: "Db", Type: "char(64)", Nullable: false},
		{Name: "User", Type: "char(32)", Nullable: false},
		{Name: "Select_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Insert_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Update_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Delete_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Drop_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Grant_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "References_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Index_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Alter_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_tmp_table_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Lock_tables_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_view_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Show_view_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Create_routine_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Alter_routine_priv", Type: "enum('N','Y')", Default: "N"},
		{Name: "Execute_priv", Type: "enum('N','Y')", Default: "N"},
	}
}

// Query executes a query against mysql.db table
func (t *MySQLDBTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Note: This will need to access db permissions from ACLManager
	// For now, return empty result
	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// MySQLTablesPrivTable represents mysql.tables_priv virtual table
type MySQLTablesPrivTable struct {
	aclMgr *ACLManager
}

// NewMySQLTablesPrivTable creates a new mysql.tables_priv virtual table
func NewMySQLTablesPrivTable(aclMgr *ACLManager) virtual.VirtualTable {
	return &MySQLTablesPrivTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *MySQLTablesPrivTable) GetName() string {
	return "tables_priv"
}

// GetSchema returns table schema
func (t *MySQLTablesPrivTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "Host", Type: "char(60)", Nullable: false},
		{Name: "Db", Type: "char(64)", Nullable: false},
		{Name: "User", Type: "char(32)", Nullable: false},
		{Name: "Table_name", Type: "char(64)", Nullable: false},
		{Name: "Grantor", Type: "char(93)", Nullable: false},
		{Name: "Timestamp", Type: "timestamp", Nullable: false},
		{Name: "Table_priv", Type: "set('Select','Insert','Update','Delete','Create','Drop','Grant','References','Index','Alter','Create View','Show view','Trigger')", Nullable: false},
		{Name: "Column_priv", Type: "set('Select','Insert','Update','References')", Nullable: false},
	}
}

// Query executes a query against mysql.tables_priv table
func (t *MySQLTablesPrivTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Note: This will need to access table permissions from ACLManager
	// For now, return empty result
	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// MySQLColumnsPrivTable represents mysql.columns_priv virtual table
type MySQLColumnsPrivTable struct {
	aclMgr *ACLManager
}

// NewMySQLColumnsPrivTable creates a new mysql.columns_priv virtual table
func NewMySQLColumnsPrivTable(aclMgr *ACLManager) virtual.VirtualTable {
	return &MySQLColumnsPrivTable{aclMgr: aclMgr}
}

// GetName returns table name
func (t *MySQLColumnsPrivTable) GetName() string {
	return "columns_priv"
}

// GetSchema returns table schema
func (t *MySQLColumnsPrivTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "Host", Type: "char(60)", Nullable: false},
		{Name: "Db", Type: "char(64)", Nullable: false},
		{Name: "User", Type: "char(32)", Nullable: false},
		{Name: "Table_name", Type: "char(64)", Nullable: false},
		{Name: "Column_name", Type: "char(64)", Nullable: false},
		{Name: "Timestamp", Type: "timestamp", Nullable: false},
		{Name: "Column_priv", Type: "set('Select','Insert','Update','References')", Nullable: false},
	}
}

// Query executes a query against mysql.columns_priv table
func (t *MySQLColumnsPrivTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Note: This will need to access column permissions from ACLManager
	// For now, return empty result
	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

// Helper functions

// boolToYN converts bool to MySQL 'Y'/'N' string
func boolToYN(b bool) string {
	if b {
		return "Y"
	}
	return "N"
}

// applyFilters applies filters to rows
func applyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	for _, filter := range filters {
		var filteredRows []domain.Row

		for _, row := range rows {
			matches, err := matchesFilter(row, filter)
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
func matchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
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
		return matchesLike(strValue, fmt.Sprintf("%v", filter.Value)), nil
	default:
		return false, fmt.Errorf("unsupported filter operator: %s", filter.Operator)
	}
}

// matchesLike delegates to utils.MatchesLike for full LIKE pattern matching
func matchesLike(value, pattern string) bool {
	return utils.MatchesLike(value, pattern)
}
