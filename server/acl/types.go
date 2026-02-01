package acl

// PermissionLevel represents the scope of a permission
type PermissionLevel int

const (
	// PermissionLevelGlobal represents global permissions (mysql.user)
	PermissionLevelGlobal PermissionLevel = iota
	// PermissionLevelDatabase represents database-level permissions (mysql.db)
	PermissionLevelDatabase
	// PermissionLevelTable represents table-level permissions (mysql.tables_priv)
	PermissionLevelTable
	// PermissionLevelColumn represents column-level permissions (mysql.columns_priv)
	PermissionLevelColumn
)

// PermissionType represents MySQL permission types
type PermissionType string

const (
	PrivSelect   PermissionType = "SELECT"
	PrivInsert   PermissionType = "INSERT"
	PrivUpdate   PermissionType = "UPDATE"
	PrivDelete   PermissionType = "DELETE"
	PrivCreate   PermissionType = "CREATE"
	PrivDrop     PermissionType = "DROP"
	PrivReload   PermissionType = "RELOAD"
	PrivShutdown PermissionType = "SHUTDOWN"
	PrivProcess  PermissionType = "PROCESS"
	PrivFile     PermissionType = "FILE"
	PrivGrant    PermissionType = "GRANT OPTION"
	PrivReferences PermissionType = "REFERENCES"
	PrivIndex    PermissionType = "INDEX"
	PrivAlter    PermissionType = "ALTER"
	PrivShowDB   PermissionType = "SHOW DATABASES"
	PrivSuper    PermissionType = "SUPER"
	PrivCreateTmpTable PermissionType = "CREATE TEMPORARY TABLES"
	PrivLockTables    PermissionType = "LOCK TABLES"
	PrivExecute      PermissionType = "EXECUTE"
	PrivReplSlave    PermissionType = "REPLICATION SLAVE"
	PrivReplClient   PermissionType = "REPLICATION CLIENT"
	PrivCreateView   PermissionType = "CREATE VIEW"
	PrivShowView     PermissionType = "SHOW VIEW"
	PrivCreateRoutine PermissionType = "CREATE ROUTINE"
	PrivAlterRoutine  PermissionType = "ALTER ROUTINE"
	PrivCreateUser    PermissionType = "CREATE USER"
	PrivEvent     PermissionType = "EVENT"
	PrivTrigger   PermissionType = "TRIGGER"
	PrivAllPrivileges PermissionType = "ALL PRIVILEGES"
)

// User represents a user with global permissions (from mysql.user)
type User struct {
	Host       string            `json:"host"`
	User       string            `json:"user"`
	Password   string            `json:"password"` // Empty string means no password
	Privileges map[string]bool  `json:"privileges"` // Map of PermissionType -> true/false (global level)
}

// DatabasePermission represents database-level permissions (from mysql.db)
type DatabasePermission struct {
	Host       string            `json:"host"`
	Db         string            `json:"db"`
	User       string            `json:"user"`
	Privileges map[string]bool  `json:"privileges"` // Map of PermissionType -> true/false
}

// TablePermission represents table-level permissions (from mysql.tables_priv)
type TablePermission struct {
	Host       string            `json:"host"`
	Db         string            `json:"db"`
	User       string            `json:"user"`
	TableName  string            `json:"table_name"`
	Grantor    string            `json:"grantor"` // User who granted this permission
	Timestamp  string            `json:"timestamp"`
	Privileges map[string]bool  `json:"privileges"` // Map of PermissionType -> true/false
}

// ColumnPermission represents column-level permissions (from mysql.columns_priv)
type ColumnPermission struct {
	Host       string            `json:"host"`
	Db         string            `json:"db"`
	User       string            `json:"user"`
	TableName  string            `json:"table_name"`
	ColumnName string            `json:"column_name"`
	Timestamp  string            `json:"timestamp"`
	Privileges map[string]bool  `json:"privileges"` // Map of PermissionType -> true/false
}

// PermissionScope defines the scope for permission checking
type PermissionScope struct {
	Database string
	Table    string
	Column   string
	Level    PermissionLevel
}

// AllPermissionTypes returns all valid permission types
func AllPermissionTypes() []PermissionType {
	return []PermissionType{
		PrivSelect, PrivInsert, PrivUpdate, PrivDelete,
		PrivCreate, PrivDrop, PrivReload, PrivShutdown,
		PrivProcess, PrivFile, PrivGrant, PrivReferences,
		PrivIndex, PrivAlter, PrivShowDB, PrivSuper,
		PrivCreateTmpTable, PrivLockTables, PrivExecute,
		PrivReplSlave, PrivReplClient, PrivCreateView,
		PrivShowView, PrivCreateRoutine, PrivAlterRoutine,
		PrivCreateUser, PrivEvent, PrivTrigger,
	}
}

// IsPrivilegeType checks if a string is a valid privilege type
func IsPrivilegeType(priv string) bool {
	for _, p := range AllPermissionTypes() {
		if string(p) == priv {
			return true
		}
	}
	if priv == "ALL" || priv == "ALL PRIVILEGES" {
		return true
	}
	return false
}

// NormalizePrivilege normalizes privilege strings (e.g., "ALL" -> "ALL PRIVILEGES")
func NormalizePrivilege(priv string) PermissionType {
	if priv == "ALL" {
		return PrivAllPrivileges
	}
	return PermissionType(priv)
}

// DefaultPrivileges returns the default privilege map with all permissions set to false
func DefaultPrivileges() map[string]bool {
	privs := make(map[string]bool)
	for _, p := range AllPermissionTypes() {
		privs[string(p)] = false
	}
	return privs
}

// AllPrivilegesMap returns a privilege map with all permissions set to true
func AllPrivilegesMap() map[string]bool {
	privs := make(map[string]bool)
	for _, p := range AllPermissionTypes() {
		privs[string(p)] = true
	}
	return privs
}
