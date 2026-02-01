package acl

import (
	"fmt"
	"strings"
	"sync"
)

// PermissionManager handles permission grants, revokes, and checks
type PermissionManager struct {
	dbPermissions     map[string]*DatabasePermission // Key: "host:db:user"
	tablePermissions  map[string]*TablePermission  // Key: "host:db:user:table"
	columnPermissions map[string]*ColumnPermission // Key: "host:db:user:table:column"
	mu               sync.RWMutex
}

// NewPermissionManager creates a new permission manager
func NewPermissionManager() *PermissionManager {
	return &PermissionManager{
		dbPermissions:     make(map[string]*DatabasePermission),
		tablePermissions:  make(map[string]*TablePermission),
		columnPermissions: make(map[string]*ColumnPermission),
	}
}

// Grant grants permissions to a user at specified level
func (pm *PermissionManager) Grant(host, user string, permissions []PermissionType, level PermissionLevel, db, table, column string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Handle ALL PRIVILEGES
	if contains(permissions, PrivAllPrivileges) {
		permissions = AllPermissionTypes()
	}

	switch level {
	case PermissionLevelDatabase:
		return pm.grantDatabasePermission(host, user, db, permissions)
	case PermissionLevelTable:
		return pm.grantTablePermission(host, user, db, table, permissions)
	case PermissionLevelColumn:
		return pm.grantColumnPermission(host, user, db, table, column, permissions)
	default:
		return fmt.Errorf("unsupported permission level: %v", level)
	}
}

// Revoke revokes permissions from a user at specified level
func (pm *PermissionManager) Revoke(host, user string, permissions []PermissionType, level PermissionLevel, db, table, column string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Handle ALL PRIVILEGES
	if contains(permissions, PrivAllPrivileges) {
		permissions = AllPermissionTypes()
	}

	switch level {
	case PermissionLevelDatabase:
		return pm.revokeDatabasePermission(host, user, db, permissions)
	case PermissionLevelTable:
		return pm.revokeTablePermission(host, user, db, table, permissions)
	case PermissionLevelColumn:
		return pm.revokeColumnPermission(host, user, db, table, column, permissions)
	default:
		return fmt.Errorf("unsupported permission level: %v", level)
	}
}

// CheckPermission checks if user has specified permission
// Checks in order: global -> database -> table -> column
func (pm *PermissionManager) CheckPermission(host, user string, priv PermissionType, db, table, column string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	privStr := string(priv)

	// First check database-level permission
	for _, dbPerm := range pm.dbPermissions {
		if dbPerm.Host == host || dbPerm.Host == "%" {
			if dbPerm.User == user {
				if dbPerm.Db == db || dbPerm.Db == "%" {
					if granted, exists := dbPerm.Privileges[privStr]; exists && granted {
						return true
					}
				}
			}
		}
	}

	// Then check table-level permission
	for _, tablePerm := range pm.tablePermissions {
		if tablePerm.Host == host || tablePerm.Host == "%" {
			if tablePerm.User == user {
				if tablePerm.Db == db || tablePerm.Db == "%" {
					if tablePerm.TableName == table || tablePerm.TableName == "%" {
						if granted, exists := tablePerm.Privileges[privStr]; exists && granted {
							return true
						}
					}
				}
			}
		}
	}

	// Finally check column-level permission
	for _, colPerm := range pm.columnPermissions {
		if colPerm.Host == host || colPerm.Host == "%" {
			if colPerm.User == user {
				if colPerm.Db == db || colPerm.Db == "%" {
					if colPerm.TableName == table || colPerm.TableName == "%" {
						if colPerm.ColumnName == column || colPerm.ColumnName == "%" {
							if granted, exists := colPerm.Privileges[privStr]; exists && granted {
								return true
							}
						}
					}
				}
			}
		}
	}

	return false
}

// HasGrantOption checks if user can grant permissions to others
func (pm *PermissionManager) HasGrantOption(host, user string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Check database-level GRANT OPTION
	for _, dbPerm := range pm.dbPermissions {
		if (dbPerm.Host == host || dbPerm.Host == "%") && dbPerm.User == user {
			if granted, exists := dbPerm.Privileges[string(PrivGrant)]; exists && granted {
				return true
			}
		}
	}

	return false
}

// LoadPermissions loads permissions from data structures
func (pm *PermissionManager) LoadPermissions(dbPerms []DatabasePermission, tablePerms []TablePermission, columnPerms []ColumnPermission) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.dbPermissions = make(map[string]*DatabasePermission)
	pm.tablePermissions = make(map[string]*TablePermission)
	pm.columnPermissions = make(map[string]*ColumnPermission)

	for i := range dbPerms {
		p := &dbPerms[i]
		key := pm.makeDBKey(p.Host, p.Db, p.User)
		pm.dbPermissions[key] = p
	}

	for i := range tablePerms {
		p := &tablePerms[i]
		key := pm.makeTableKey(p.Host, p.Db, p.User, p.TableName)
		pm.tablePermissions[key] = p
	}

	for i := range columnPerms {
		p := &columnPerms[i]
		key := pm.makeColumnKey(p.Host, p.Db, p.User, p.TableName, p.ColumnName)
		pm.columnPermissions[key] = p
	}

	return nil
}

// ExportPermissions exports permissions to data structures
func (pm *PermissionManager) ExportPermissions() ([]DatabasePermission, []TablePermission, []ColumnPermission) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	dbPerms := make([]DatabasePermission, 0, len(pm.dbPermissions))
	for _, p := range pm.dbPermissions {
		dbPerms = append(dbPerms, *p)
	}

	tablePerms := make([]TablePermission, 0, len(pm.tablePermissions))
	for _, p := range pm.tablePermissions {
		tablePerms = append(tablePerms, *p)
	}

	columnPerms := make([]ColumnPermission, 0, len(pm.columnPermissions))
	for _, p := range pm.columnPermissions {
		columnPerms = append(columnPerms, *p)
	}

	return dbPerms, tablePerms, columnPerms
}

// grantDatabasePermission grants database-level permissions
func (pm *PermissionManager) grantDatabasePermission(host, user, db string, permissions []PermissionType) error {
	key := pm.makeDBKey(host, db, user)

	perm, exists := pm.dbPermissions[key]
	if !exists {
		// Create new database permission
		perm = &DatabasePermission{
			Host:       host,
			Db:         db,
			User:       user,
			Privileges: DefaultPrivileges(),
		}
		pm.dbPermissions[key] = perm
	}

	// Update permissions
	for _, priv := range permissions {
		perm.Privileges[string(priv)] = true
	}

	return nil
}

// grantTablePermission grants table-level permissions
func (pm *PermissionManager) grantTablePermission(host, user, db, table string, permissions []PermissionType) error {
	key := pm.makeTableKey(host, db, user, table)

	perm, exists := pm.tablePermissions[key]
	if !exists {
		// Create new table permission
		perm = &TablePermission{
			Host:       host,
			Db:         db,
			User:       user,
			TableName:  table,
			Grantor:    "",
			Timestamp:  "",
			Privileges: DefaultPrivileges(),
		}
		pm.tablePermissions[key] = perm
	}

	// Update permissions
	for _, priv := range permissions {
		perm.Privileges[string(priv)] = true
	}

	return nil
}

// grantColumnPermission grants column-level permissions
func (pm *PermissionManager) grantColumnPermission(host, user, db, table, column string, permissions []PermissionType) error {
	key := pm.makeColumnKey(host, db, user, table, column)

	perm, exists := pm.columnPermissions[key]
	if !exists {
		// Create new column permission
		perm = &ColumnPermission{
			Host:       host,
			Db:         db,
			User:       user,
			TableName:  table,
			ColumnName: column,
			Timestamp:  "",
			Privileges: DefaultPrivileges(),
		}
		pm.columnPermissions[key] = perm
	}

	// Update permissions
	for _, priv := range permissions {
		perm.Privileges[string(priv)] = true
	}

	return nil
}

// revokeDatabasePermission revokes database-level permissions
func (pm *PermissionManager) revokeDatabasePermission(host, user, db string, permissions []PermissionType) error {
	key := pm.makeDBKey(host, db, user)

	perm, exists := pm.dbPermissions[key]
	if !exists {
		return fmt.Errorf("no database permission found for '%s'@'%s' on '%s'", user, host, db)
	}

	// Revoke permissions
	for _, priv := range permissions {
		perm.Privileges[string(priv)] = false
	}

	return nil
}

// revokeTablePermission revokes table-level permissions
func (pm *PermissionManager) revokeTablePermission(host, user, db, table string, permissions []PermissionType) error {
	key := pm.makeTableKey(host, db, user, table)

	perm, exists := pm.tablePermissions[key]
	if !exists {
		return fmt.Errorf("no table permission found for '%s'@'%s' on '%s.%s'", user, host, db, table)
	}

	// Revoke permissions
	for _, priv := range permissions {
		perm.Privileges[string(priv)] = false
	}

	return nil
}

// revokeColumnPermission revokes column-level permissions
func (pm *PermissionManager) revokeColumnPermission(host, user, db, table, column string, permissions []PermissionType) error {
	key := pm.makeColumnKey(host, db, user, table, column)

	perm, exists := pm.columnPermissions[key]
	if !exists {
		return fmt.Errorf("no column permission found for '%s'@'%s' on '%s.%s.%s'", user, host, db, table, column)
	}

	// Revoke permissions
	for _, priv := range permissions {
		perm.Privileges[string(priv)] = false
	}

	return nil
}

// makeDBKey creates a map key for database permissions
func (pm *PermissionManager) makeDBKey(host, db, user string) string {
	return strings.ToLower(host) + ":" + strings.ToLower(db) + ":" + strings.ToLower(user)
}

// makeTableKey creates a map key for table permissions
func (pm *PermissionManager) makeTableKey(host, db, user, table string) string {
	return strings.ToLower(host) + ":" + strings.ToLower(db) + ":" + strings.ToLower(user) + ":" + strings.ToLower(table)
}

// makeColumnKey creates a map key for column permissions
func (pm *PermissionManager) makeColumnKey(host, db, user, table, column string) string {
	return strings.ToLower(host) + ":" + strings.ToLower(db) + ":" + strings.ToLower(user) + ":" + strings.ToLower(table) + ":" + strings.ToLower(column)
}

// contains checks if a permission is in a slice
func contains(permissions []PermissionType, priv PermissionType) bool {
	for _, p := range permissions {
		if p == priv {
			return true
		}
	}
	return false
}
