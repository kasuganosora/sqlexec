package acl

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DataFile represents the structure of users.json and permissions.json
type DataFile struct {
	Users              []User              `json:"users"`
	DBPermissions      []DatabasePermission `json:"db"`
	TablePermissions   []TablePermission  `json:"tables_priv"`
	ColumnPermissions []ColumnPermission `json:"columns_priv"`
}

// ACLManager integrates user and permission management with JSON persistence
type ACLManager struct {
	userManager      *UserManager
	permissionMgr   *PermissionManager
	authenticator    *Authenticator
	dataDir         string
	usersFilePath    string
	permsFilePath    string
	loaded          bool
	mu              sync.RWMutex
}

// NewACLManager creates a new ACL manager
func NewACLManager(dataDir string) (*ACLManager, error) {
	if dataDir == "" {
		dataDir = "."
	}

	am := &ACLManager{
		userManager:    NewUserManager(),
		permissionMgr:   NewPermissionManager(),
		authenticator:  NewAuthenticator(),
		dataDir:       dataDir,
		usersFilePath:  filepath.Join(dataDir, "users.json"),
		permsFilePath:  filepath.Join(dataDir, "permissions.json"),
		loaded:        false,
	}

	// Initialize JSON files if they don't exist
	if err := am.initializeFiles(); err != nil {
		return nil, fmt.Errorf("failed to initialize ACL files: %w", err)
	}

	// Load data from JSON files
	if err := am.Load(); err != nil {
		return nil, fmt.Errorf("failed to load ACL data: %w", err)
	}

	return am, nil
}

// Initialize files creates default files if they don't exist
func (am *ACLManager) initializeFiles() error {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(am.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Check if users.json exists
	if _, err := os.Stat(am.usersFilePath); os.IsNotExist(err) {
		// Create default users.json with root user
		defaultUser := User{
			Host:     "%",
			User:     "root",
			Password:  "", // No password for root
			Privileges: DefaultPrivileges(),
		}

		// Grant all privileges to root
		for priv := range defaultUser.Privileges {
			defaultUser.Privileges[priv] = true
		}

		data := DataFile{
			Users: []User{defaultUser},
		}

		if err := am.writeUsersFile(&data); err != nil {
			return fmt.Errorf("failed to create default users.json: %w", err)
		}
	}

	// Check if permissions.json exists
	if _, err := os.Stat(am.permsFilePath); os.IsNotExist(err) {
		// Create empty permissions.json
		data := DataFile{
			DBPermissions:      []DatabasePermission{},
			TablePermissions:   []TablePermission{},
			ColumnPermissions: []ColumnPermission{},
		}

		if err := am.writePermissionsFile(&data); err != nil {
			return fmt.Errorf("failed to create default permissions.json: %w", err)
		}
	}

	return nil
}

// Load loads user and permission data from JSON files
func (am *ACLManager) Load() error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Load users.json
	usersData, err := os.ReadFile(am.usersFilePath)
	if err != nil {
		return fmt.Errorf("failed to read users.json: %w", err)
	}

	var usersFile DataFile
	if err := json.Unmarshal(usersData, &usersFile); err != nil {
		return fmt.Errorf("failed to parse users.json: %w", err)
	}

	// Load permissions.json
	permsData, err := os.ReadFile(am.permsFilePath)
	if err != nil {
		return fmt.Errorf("failed to read permissions.json: %w", err)
	}

	var permsFile DataFile
	if err := json.Unmarshal(permsData, &permsFile); err != nil {
		return fmt.Errorf("failed to parse permissions.json: %w", err)
	}

	// Load into managers
	if err := am.userManager.LoadUsers(usersFile.Users); err != nil {
		return fmt.Errorf("failed to load users: %w", err)
	}

	if err := am.permissionMgr.LoadPermissions(
		permsFile.DBPermissions,
		permsFile.TablePermissions,
		permsFile.ColumnPermissions,
	); err != nil {
		return fmt.Errorf("failed to load permissions: %w", err)
	}

	am.loaded = true
	return nil
}

// Save persists user and permission data to JSON files
func (am *ACLManager) Save() error {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.saveWithoutLock()
}

// saveWithoutLock persists data without acquiring lock (for internal use when already locked)
func (am *ACLManager) saveWithoutLock() error {
	// Prepare data structure for users.json
	usersData := DataFile{
		Users: am.userManager.ExportUsers(),
	}

	// Prepare data structure for permissions.json
	dbPerms, tablePerms, colPerms := am.permissionMgr.ExportPermissions()
	permsData := DataFile{
		DBPermissions:      dbPerms,
		TablePermissions:   tablePerms,
		ColumnPermissions: colPerms,
	}

	// Write to files
	if err := am.writeUsersFile(&usersData); err != nil {
		return err
	}

	if err := am.writePermissionsFile(&permsData); err != nil {
		return err
	}

	return nil
}

// Authenticate verifies user credentials
func (am *ACLManager) Authenticate(username, password string) (*User, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if !am.loaded {
		return nil, fmt.Errorf("ACL not loaded")
	}

	// Try to find user (allow wildcard host)
	user, err := am.userManager.GetUser("%", username)
	if err != nil {
		// Try exact host match (will be refined later with actual client host)
		user, err = am.userManager.GetUser("localhost", username)
		if err != nil {
			return nil, fmt.Errorf("access denied for user '%s'", username)
		}
	}

	// Check password (if user has a password)
	if user.Password != "" {
		// For now, simple password check (can be enhanced with salt later)
		if !am.authenticator.VerifyPasswordWithHash(user.Password, password) {
			return nil, fmt.Errorf("access denied for user '%s'", username)
		}
	}

	return user, nil
}

// CheckPermission checks if user has specified permission
func (am *ACLManager) CheckPermission(username, host string, priv PermissionType, db, table, column string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if !am.loaded {
		return false
	}

	// First check global permissions (User.Privileges)
	hasGlobal, _ := am.userManager.HasPrivilege(host, username, priv)
	if hasGlobal {
		return true
	}

	// Then check lower-level permissions
	return am.permissionMgr.CheckPermission(host, username, priv, db, table, column)
}

// HasGrantOption checks if user can grant permissions
func (am *ACLManager) HasGrantOption(username, host string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if !am.loaded {
		return false
	}

	// Check global GRANT OPTION first
	hasGlobal, _ := am.userManager.HasPrivilege(host, username, PrivGrant)
	if hasGlobal {
		return true
	}

	// Check database-level GRANT OPTION
	return am.permissionMgr.HasGrantOption(host, username)
}

// IsPrivilegedUser checks if user has any admin privileges
func (am *ACLManager) IsPrivilegedUser(username, host string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if !am.loaded {
		return false
	}

	// Check for SUPER or CREATE USER privilege
	hasSuper, _ := am.userManager.HasPrivilege(host, username, PrivSuper)
	hasCreateUser, _ := am.userManager.HasPrivilege(host, username, PrivCreateUser)

	// Use inline grant check to avoid re-acquiring am.mu.RLock via HasGrantOption
	hasGrant, _ := am.userManager.HasPrivilege(host, username, PrivGrant)
	if !hasGrant {
		hasGrant = am.permissionMgr.HasGrantOption(host, username)
	}

	return hasSuper || hasCreateUser || hasGrant
}

// CreateUser creates a new user
func (am *ACLManager) CreateUser(host, user, password string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Hash password
	var passwordHash string
	if password != "" {
		passwordHash = am.authenticator.GenerateHashedPassword(password)
	}

	// Create user with no permissions
	if err := am.userManager.CreateUser(host, user, passwordHash, DefaultPrivileges()); err != nil {
		return err
	}

	// Save to file (already have write lock)
	if err := am.saveWithoutLock(); err != nil {
		return err
	}

	return nil
}

// DropUser removes a user
func (am *ACLManager) DropUser(host, user string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if err := am.userManager.DropUser(host, user); err != nil {
		return err
	}

	// Save to file (already have write lock)
	if err := am.saveWithoutLock(); err != nil {
		return err
	}

	return nil
}

// SetPassword updates user's password
func (am *ACLManager) SetPassword(host, user, newPassword string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Hash new password
	var passwordHash string
	if newPassword != "" {
		passwordHash = am.authenticator.GenerateHashedPassword(newPassword)
	}

	if err := am.userManager.SetPassword(host, user, passwordHash); err != nil {
		return err
	}

	// Save to file (already have write lock)
	if err := am.saveWithoutLock(); err != nil {
		return err
	}

	return nil
}

// Grant grants permissions to a user
func (am *ACLManager) Grant(host, user string, privileges []PermissionType, level PermissionLevel, db, table, column string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Global-level permissions go directly to User.Privileges
	if level == PermissionLevelGlobal {
		for _, priv := range privileges {
			if err := am.userManager.SetPrivilege(host, user, priv, true); err != nil {
				return err
			}
		}
	} else {
		// Database, table, and column level permissions go to PermissionManager
		if err := am.permissionMgr.Grant(host, user, privileges, level, db, table, column); err != nil {
			return err
		}
	}

	// Save to file (already have write lock)
	if err := am.saveWithoutLock(); err != nil {
		return err
	}

	return nil
}

// Revoke revokes permissions from a user
func (am *ACLManager) Revoke(host, user string, privileges []PermissionType, level PermissionLevel, db, table, column string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Global-level permissions are revoked from User.Privileges
	if level == PermissionLevelGlobal {
		for _, priv := range privileges {
			if err := am.userManager.SetPrivilege(host, user, priv, false); err != nil {
				return err
			}
		}
	} else {
		// Database, table, and column level permissions
		if err := am.permissionMgr.Revoke(host, user, privileges, level, db, table, column); err != nil {
			return err
		}
	}

	// Save to file (already have write lock)
	if err := am.saveWithoutLock(); err != nil {
		return err
	}

	return nil
}

// GetUsers returns all users (for privileged users)
func (am *ACLManager) GetUsers() []*User {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.userManager.ListUsers()
}

// IsLoaded returns true if ACL data has been loaded
func (am *ACLManager) IsLoaded() bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.loaded
}

// writeUsersFile writes user data to JSON file
func (am *ACLManager) writeUsersFile(data *DataFile) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal users data: %w", err)
	}

	if err := os.WriteFile(am.usersFilePath, jsonData, 0600); err != nil {
		return fmt.Errorf("failed to write users.json: %w", err)
	}

	return nil
}

// writePermissionsFile writes permission data to JSON file
func (am *ACLManager) writePermissionsFile(data *DataFile) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal permissions data: %w", err)
	}

	if err := os.WriteFile(am.permsFilePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write permissions.json: %w", err)
	}

	return nil
}
