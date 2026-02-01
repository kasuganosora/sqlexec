package acl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewACLManager(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() error = %v, want nil", err)
	}
	if am == nil {
		t.Fatal("NewACLManager() returned nil")
	}
	if !am.IsLoaded() {
		t.Error("NewACLManager() should load data by default")
	}
}

func TestNewACLManagerDefaultDir(t *testing.T) {
	am, err := NewACLManager("")
	if err != nil {
		t.Fatalf("NewACLManager() error = %v, want nil", err)
	}
	if am == nil {
		t.Fatal("NewACLManager() returned nil")
	}
	if am.dataDir != "." {
		t.Errorf("NewACLManager() dataDir = %v, want .", am.dataDir)
	}
}

func TestInitializeFiles(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	usersFile := filepath.Join(tmpDir, "users.json")
	permsFile := filepath.Join(tmpDir, "permissions.json")

	// Files should not exist initially
	if _, err := os.Stat(usersFile); !os.IsNotExist(err) {
		t.Fatal("users.json already exists")
	}
	if _, err := os.Stat(permsFile); !os.IsNotExist(err) {
		t.Fatal("permissions.json already exists")
	}

	// Create ACL manager (should initialize files)
	_, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Files should now exist
	if _, err := os.Stat(usersFile); os.IsNotExist(err) {
		t.Error("users.json was not created")
	}
	if _, err := os.Stat(permsFile); os.IsNotExist(err) {
		t.Error("permissions.json was not created")
	}
}

func TestInitializeDefaultRootUser(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Verify root user was created
	users := am.GetUsers()
	if len(users) == 0 {
		t.Fatal("No users created")
	}

	var rootUser *User
	rootFound := false
	for _, u := range users {
		if u.User == "root" && u.Host == "%" {
			rootFound = true
			rootUser = u
			if u.Password != "" {
				t.Error("Root user should have no password")
			}
			break
		}
	}
	if !rootFound {
		t.Error("Root user was not created")
	}

	// Debug: print root user's privileges
	if rootUser != nil {
		t.Logf("Root user privileges map size: %d", len(rootUser.Privileges))
	}

	// Check directly through userManager
	hasGlobalSelect, _ := am.userManager.HasPrivilege("%", "root", PrivSelect)
	hasGlobalSuper, _ := am.userManager.HasPrivilege("%", "root", PrivSuper)
	t.Logf("HasPrivilege(PrivSelect): %v", hasGlobalSelect)
	t.Logf("HasPrivilege(PrivSuper): %v", hasGlobalSuper)

	// Verify root can check any permission (which means they have it)
	if !am.CheckPermission("root", "%", PrivSelect, "", "", "") {
		t.Error("Root should have SELECT privilege")
	}
	if !am.CheckPermission("root", "%", PrivInsert, "", "", "") {
		t.Error("Root should have INSERT privilege")
	}
	if !am.CheckPermission("root", "%", PrivUpdate, "", "", "") {
		t.Error("Root should have UPDATE privilege")
	}
}

func TestLoad(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Verify data was loaded
	if !am.IsLoaded() {
		t.Error("Load() did not set loaded flag")
	}

	// Verify root user was loaded
	users := am.GetUsers()
	if len(users) == 0 {
		t.Error("Load() did not load users")
	}
}

func TestSave(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create a test user
	err = am.CreateUser("localhost", "testuser", "password123")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Save to file
	err = am.Save()
	if err != nil {
		t.Errorf("Save() error = %v, want nil", err)
	}

	// Create new ACL manager to reload data
	am2, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Verify user was persisted
	users := am2.GetUsers()
	userFound := false
	for _, u := range users {
		if u.User == "testuser" && u.Host == "localhost" {
			userFound = true
			if u.Password == "" {
				t.Error("User password was not persisted")
			}
			break
		}
	}
	if !userFound {
		t.Error("User was not persisted to file")
	}
}

func TestAuthenticate(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test user with password
	err = am.CreateUser("%", "testuser", "password123")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	tests := []struct {
		name     string
		username string
		password string
		wantErr  bool
	}{
		{
			name:     "Correct credentials",
			username: "testuser",
			password: "password123",
			wantErr:  false,
		},
		{
			name:     "Wrong password",
			username: "testuser",
			password: "wrongpassword",
			wantErr:  true,
		},
		{
			name:     "Non-existent user",
			username: "nonexistent",
			password: "password",
			wantErr:  true,
		},
		{
			name:     "Root with no password",
			username: "root",
			password: "",
			wantErr:  false,
		},
		{
			name:     "Root with any password when no password set",
			username: "root",
			password: "wrong", // root has no password set, so any password works
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, err := am.Authenticate(tt.username, tt.password)
			if (err != nil) != tt.wantErr {
				t.Errorf("Authenticate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && user == nil {
				t.Error("Authenticate() returned nil user")
			}
			if !tt.wantErr && user != nil && user.User != tt.username {
				t.Errorf("Authenticate() user.User = %v, want %v", user.User, tt.username)
			}
		})
	}
}

func TestACLManagerCheckPermission(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test user
	err = am.CreateUser("%", "testuser", "")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Grant database-level permissions
	err = am.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Grant table-level permissions
	err = am.Grant("%", "testuser", []PermissionType{PrivUpdate}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	tests := []struct {
		name   string
		host   string
		user   string
		priv   PermissionType
		db     string
		table  string
		column string
		want   bool
	}{
		{
			name:   "Has database-level SELECT",
			host:   "%",
			user:   "testuser",
			priv:   PrivSelect,
			db:     "testdb",
			table:  "",
			column: "",
			want:   true,
		},
		{
			name:   "Has database-level INSERT",
			host:   "%",
			user:   "testuser",
			priv:   PrivInsert,
			db:     "testdb",
			table:  "",
			column: "",
			want:   true,
		},
		{
			name:   "Has table-level UPDATE",
			host:   "%",
			user:   "testuser",
			priv:   PrivUpdate,
			db:     "testdb",
			table:  "testtable",
			column: "",
			want:   true,
		},
		{
			name:   "Does not have DELETE",
			host:   "%",
			user:   "testuser",
			priv:   PrivDelete,
			db:     "testdb",
			table:  "",
			column: "",
			want:   false,
		},
		{
			name:   "Root has all privileges",
			host:   "%",
			user:   "root",
			priv:   PrivSelect,
			db:     "",
			table:  "",
			column: "",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := am.CheckPermission(tt.user, tt.host, tt.priv, tt.db, tt.table, tt.column)
			if got != tt.want {
				t.Errorf("CheckPermission() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestACLManagerHasGrantOption(t *testing.T) {
	tests := []struct {
		name        string
		setupGrant  bool
		host        string
		user        string
		want        bool
	}{
		{
			name:       "Has global GRANT OPTION",
			setupGrant: true,
			host:       "%",
			user:       "testuser",
			want:       true,
		},
		{
			name:       "No GRANT OPTION",
			setupGrant: false,
			host:       "%",
			user:       "testuser",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a fresh temp directory for each test
			testTmpDir := t.TempDir()
			testAm, err := NewACLManager(testTmpDir)
			if err != nil {
				t.Fatalf("NewACLManager() failed: %v", err)
			}

			err = testAm.CreateUser("%", "testuser", "")
			if err != nil {
				t.Fatalf("CreateUser() failed: %v", err)
			}

			if tt.setupGrant {
				err = testAm.Grant(tt.host, tt.user, []PermissionType{PrivGrant}, PermissionLevelDatabase, "testdb", "", "")
				if err != nil {
					t.Fatalf("Grant() failed: %v", err)
				}
			}

			got := testAm.HasGrantOption(tt.user, tt.host)
			if got != tt.want {
				t.Errorf("HasGrantOption() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPrivilegedUser(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test users
	err = am.CreateUser("%", "superuser", "")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}
	err = am.CreateUser("%", "normaluser", "")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Grant SUPER to superuser (global level)
	err = am.Grant("%", "superuser", []PermissionType{PrivSuper}, PermissionLevelGlobal, "", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	tests := []struct {
		name string
		host string
		user string
		want bool
	}{
		{
			name: "Root is privileged",
			host: "%",
			user: "root",
			want: true,
		},
		{
			name: "Super user is privileged",
			host: "%",
			user: "superuser",
			want: true,
		},
		{
			name: "Normal user is not privileged",
			host: "%",
			user: "normaluser",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := am.IsPrivilegedUser(tt.user, tt.host)
			if got != tt.want {
				t.Errorf("IsPrivilegedUser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestACLManagerCreateUser(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	tests := []struct {
		name     string
		host     string
		user     string
		password string
		wantErr  bool
	}{
		{
			name:     "Create user with password",
			host:     "%",
			user:     "testuser1",
			password: "password123",
			wantErr:  false,
		},
		{
			name:     "Create user without password",
			host:     "localhost",
			user:     "testuser2",
			password: "",
			wantErr:  false,
		},
		{
			name:     "Create user with wildcard host",
			host:     "%",
			user:     "testuser3",
			password: "",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			am, err = NewACLManager(tmpDir)
			if err != nil {
				t.Fatalf("NewACLManager() failed: %v", err)
			}

			err = am.CreateUser(tt.host, tt.user, tt.password)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// Verify user was created
				users := am.GetUsers()
				found := false
				for _, u := range users {
					if u.User == tt.user && u.Host == tt.host {
						found = true
						if tt.password != "" && u.Password == "" {
							t.Error("CreateUser() did not hash password")
						}
						break
					}
				}
				if !found {
					t.Error("CreateUser() did not create user")
				}
			}
		})
	}
}

func TestACLManagerCreateUserDuplicate(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	host := "%"
	user := "testuser"
	password := "password123"

	// Create user first time
	err = am.CreateUser(host, user, password)
	if err != nil {
		t.Fatalf("First CreateUser() failed: %v", err)
	}

	// Try to create same user again
	err = am.CreateUser(host, user, password)
	if err == nil {
		t.Error("CreateUser() should return error for duplicate user")
	}
}

func TestACLManagerDropUser(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	host := "%"
	user := "testuser"

	// Create user
	err = am.CreateUser(host, user, "password123")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Verify user exists
	users := am.GetUsers()
	found := false
	for _, u := range users {
		if u.User == user && u.Host == host {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("User was not created")
	}

	// Drop user
	err = am.DropUser(host, user)
	if err != nil {
		t.Errorf("DropUser() error = %v, want nil", err)
	}

	// Verify user was deleted
	users = am.GetUsers()
	found = false
	for _, u := range users {
		if u.User == user && u.Host == host {
			found = true
			break
		}
	}
	if found {
		t.Error("DropUser() did not delete user")
	}
}

func TestACLManagerDropUserNotExists(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	err = am.DropUser("%", "nonexistent")
	if err == nil {
		t.Error("DropUser() should return error for non-existent user")
	}
}

func TestACLManagerSetPassword(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	host := "%"
	user := "testuser"
	oldPassword := "oldpassword"
	newPassword := "newpassword"

	// Create user
	err = am.CreateUser(host, user, oldPassword)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Set new password
	err = am.SetPassword(host, user, newPassword)
	if err != nil {
		t.Errorf("SetPassword() error = %v, want nil", err)
	}

	// Verify password was changed
	_, err = am.Authenticate(user, newPassword)
	if err != nil {
		t.Errorf("SetPassword() did not change password, authentication failed: %v", err)
	}

	// Verify old password no longer works
	_, err = am.Authenticate(user, oldPassword)
	if err == nil {
		t.Error("SetPassword() old password still works")
	}
}

func TestACLManagerSetPasswordNonExistentUser(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	err = am.SetPassword("%", "nonexistent", "newpassword")
	if err == nil {
		t.Error("SetPassword() should return error for non-existent user")
	}
}

func TestGrant(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create user
	err = am.CreateUser("%", "testuser", "")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	tests := []struct {
		name        string
		host        string
		user        string
		privileges  []PermissionType
		level       PermissionLevel
		db          string
		table       string
		column      string
		wantErr     bool
	}{
		{
			name:       "Grant database-level permissions",
			host:       "%",
			user:       "testuser",
			privileges: []PermissionType{PrivSelect, PrivInsert},
			level:      PermissionLevelDatabase,
			db:         "testdb",
			table:      "",
			column:     "",
			wantErr:    false,
		},
		{
			name:       "Grant table-level permissions",
			host:       "%",
			user:       "testuser",
			privileges: []PermissionType{PrivUpdate, PrivDelete},
			level:      PermissionLevelTable,
			db:         "testdb",
			table:      "testtable",
			column:     "",
			wantErr:    false,
		},
		{
			name:       "Grant column-level permissions",
			host:       "%",
			user:       "testuser",
			privileges: []PermissionType{PrivReferences},
			level:      PermissionLevelColumn,
			db:         "testdb",
			table:      "testtable",
			column:     "col1",
			wantErr:    false,
		},
		{
			name:       "Grant ALL PRIVILEGES",
			host:       "%",
			user:       "testuser",
			privileges: []PermissionType{PrivAllPrivileges},
			level:      PermissionLevelDatabase,
			db:         "testdb",
			table:      "",
			column:     "",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := am.Grant(tt.host, tt.user, tt.privileges, tt.level, tt.db, tt.table, tt.column)
			if (err != nil) != tt.wantErr {
				t.Errorf("Grant() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRevoke(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create user
	err = am.CreateUser("%", "testuser", "")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Grant permissions first
	err = am.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke SELECT
	err = am.Revoke("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Errorf("Revoke() error = %v, want nil", err)
	}

	// Verify SELECT was revoked but INSERT remains
	if am.CheckPermission("testuser", "%", PrivSelect, "testdb", "", "") {
		t.Error("Revoke() did not revoke SELECT")
	}
	if !am.CheckPermission("testuser", "%", PrivInsert, "testdb", "", "") {
		t.Error("Revoke() incorrectly revoked INSERT")
	}
}

func TestACLManagerRevokeAllPrivileges(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create user
	err = am.CreateUser("%", "testuser", "")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Grant all privileges
	err = am.Grant("%", "testuser", []PermissionType{PrivAllPrivileges}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke all privileges
	err = am.Revoke("%", "testuser", []PermissionType{PrivAllPrivileges}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Errorf("Revoke() error = %v, want nil", err)
	}

	// Verify all privileges were revoked
	allPrivTypes := AllPermissionTypes()
	for _, privType := range allPrivTypes {
		if am.CheckPermission("testuser", "%", privType, "testdb", "", "") {
			t.Errorf("Revoke() did not revoke privilege %v", privType)
		}
	}
}

func TestGetUsers(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Initially should have only root user
	users := am.GetUsers()
	if len(users) != 1 {
		t.Errorf("GetUsers() initial length = %v, want 1", len(users))
	}

	// Create some users
	am.CreateUser("%", "user1", "")
	am.CreateUser("localhost", "user2", "")
	am.CreateUser("192.168.1.1", "user3", "")

	// Get users
	users = am.GetUsers()
	if len(users) != 4 { // 3 new + root
		t.Errorf("GetUsers() length = %v, want 4", len(users))
	}

	// Verify user names
	userNames := make(map[string]bool)
	for _, u := range users {
		userNames[u.User] = true
	}
	if !userNames["root"] || !userNames["user1"] || !userNames["user2"] || !userNames["user3"] {
		t.Error("GetUsers() did not return all users")
	}
}

func TestIsLoaded(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	if !am.IsLoaded() {
		t.Error("IsLoaded() should return true after NewACLManager")
	}
}

func TestPersistence(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create first ACL manager
	am1, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create a user
	err = am1.CreateUser("%", "testuser", "password123")
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Grant permissions
	err = am1.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Verify changes
	if !am1.CheckPermission("testuser", "%", PrivSelect, "testdb", "", "") {
		t.Error("Permission not granted in first ACL manager")
	}

	// Create second ACL manager (should load from files)
	am2, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Verify user was persisted
	users := am2.GetUsers()
	userFound := false
	for _, u := range users {
		if u.User == "testuser" && u.Host == "%" {
			userFound = true
			break
		}
	}
	if !userFound {
		t.Error("User was not persisted")
	}

	// Verify permissions were persisted
	if !am2.CheckPermission("testuser", "%", PrivSelect, "testdb", "", "") {
		t.Error("Permissions were not persisted")
	}

	// Verify authentication works
	_, err = am2.Authenticate("testuser", "password123")
	if err != nil {
		t.Errorf("Authentication failed after persistence: %v", err)
	}
}
