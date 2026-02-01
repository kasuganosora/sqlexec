package acl

import (
	"strconv"
	"testing"
)

func TestNewUserManager(t *testing.T) {
	um := NewUserManager()
	if um == nil {
		t.Fatal("NewUserManager() returned nil")
	}
	if um.users == nil {
		t.Fatal("NewUserManager() users map is nil")
	}
}

func TestUserManagerCreateUser(t *testing.T) {
	um := NewUserManager()

	tests := []struct {
		name         string
		host         string
		user         string
		passwordHash string
		privileges   map[string]bool
		wantErr      bool
	}{
		{
			name:         "Create user with no password",
			host:         "%",
			user:         "testuser1",
			passwordHash: "",
			privileges:   map[string]bool{"SELECT": true},
			wantErr:      false,
		},
		{
			name:         "Create user with password",
			host:         "localhost",
			user:         "testuser2",
			passwordHash: "*hashedpassword",
			privileges:   map[string]bool{"SELECT": true, "INSERT": true},
			wantErr:      false,
		},
		{
			name:         "Create user with wildcard host",
			host:         "%",
			user:         "testuser3",
			passwordHash: "",
			privileges:   nil,
			wantErr:      false,
		},
		{
			name:         "Create user with specific IP",
			host:         "192.168.1.1",
			user:         "testuser4",
			passwordHash: "",
			privileges:   map[string]bool{},
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := um.CreateUser(tt.host, tt.user, tt.passwordHash, tt.privileges)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateUser() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && err == nil {
				// Verify user was created
				key := um.makeKey(tt.host, tt.user)
				if _, exists := um.users[key]; !exists {
					t.Error("CreateUser() did not create user")
				}
			}
		})
	}
}

func TestUserManagerCreateUserDuplicate(t *testing.T) {
	um := NewUserManager()

	host := "%"
	user := "testuser"
	passwordHash := ""
	privileges := map[string]bool{"SELECT": true}

	// Create user first time
	err := um.CreateUser(host, user, passwordHash, privileges)
	if err != nil {
		t.Fatalf("First CreateUser() failed: %v", err)
	}

	// Try to create same user again
	err = um.CreateUser(host, user, passwordHash, privileges)
	if err == nil {
		t.Error("CreateUser() should return error for duplicate user")
	}
}

func TestUserManagerDropUser(t *testing.T) {
	um := NewUserManager()

	host := "%"
	user := "testuser"
	passwordHash := ""
	privileges := map[string]bool{}

	// Create user
	err := um.CreateUser(host, user, passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Verify user exists
	key := um.makeKey(host, user)
	if _, exists := um.users[key]; !exists {
		t.Fatal("User was not created")
	}

	// Drop user
	err = um.DropUser(host, user)
	if err != nil {
		t.Errorf("DropUser() error = %v, want nil", err)
	}

	// Verify user is deleted
	if _, exists := um.users[key]; exists {
		t.Error("DropUser() did not delete user")
	}
}

func TestUserManagerDropUserNotExists(t *testing.T) {
	um := NewUserManager()

	err := um.DropUser("%", "nonexistent")
	if err == nil {
		t.Error("DropUser() should return error for non-existent user")
	}
}

func TestGetUser(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{"SELECT": true}

	// Create test users
	err := um.CreateUser("%", "testuser1", passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}
	err = um.CreateUser("localhost", "testuser2", passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	tests := []struct {
		name    string
		host    string
		user    string
		wantErr bool
	}{
		{
			name:    "Get existing user with exact host",
			host:    "%",
			user:    "testuser1",
			wantErr: false,
		},
		{
			name:    "Get existing user with localhost",
			host:    "localhost",
			user:    "testuser2",
			wantErr: false,
		},
		{
			name:    "Get user with wildcard host match",
			host:    "192.168.1.1",
			user:    "testuser1",
			wantErr: false,
		},
		{
			name:    "Get non-existent user",
			host:    "%",
			user:    "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, err := um.GetUser(tt.host, tt.user)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if user == nil {
					t.Error("GetUser() returned nil user")
				}
				if user.User != tt.user {
					t.Errorf("GetUser() user.User = %v, want %v", user.User, tt.user)
				}
			}
		})
	}
}

func TestGetUserCaseInsensitive(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{}

	// Create user with lowercase
	err := um.CreateUser("localhost", "TestUser", passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Try to get with different case
	user, err := um.GetUser("LOCALHOST", "testuser")
	if err != nil {
		t.Errorf("GetUser() should be case insensitive, got error: %v", err)
	}
	if user == nil {
		t.Error("GetUser() should find user with different case")
	}
}

func TestListUsers(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{}

	// Initially should be empty
	users := um.ListUsers()
	if len(users) != 0 {
		t.Errorf("ListUsers() initial length = %v, want 0", len(users))
	}

	// Create some users
	um.CreateUser("%", "user1", passwordHash, privileges)
	um.CreateUser("localhost", "user2", passwordHash, privileges)
	um.CreateUser("192.168.1.1", "user3", passwordHash, privileges)

	// List users
	users = um.ListUsers()
	if len(users) != 3 {
		t.Errorf("ListUsers() length = %v, want 3", len(users))
	}

	// Verify user names
	userNames := make(map[string]bool)
	for _, u := range users {
		userNames[u.User] = true
	}
	if !userNames["user1"] || !userNames["user2"] || !userNames["user3"] {
		t.Error("ListUsers() did not return all users")
	}
}

func TestHasPrivilege(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{
		"SELECT":   true,
		"INSERT":   true,
		"UPDATE":   false,
		"DELETE":   false,
		"CREATE":   true,
	}

	err := um.CreateUser("%", "testuser", passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	tests := []struct {
		name    string
		host    string
		user    string
		priv    PermissionType
		want    bool
		wantErr bool
	}{
		{
			name:    "User has SELECT privilege",
			host:    "%",
			user:    "testuser",
			priv:    PrivSelect,
			want:    true,
			wantErr: false,
		},
		{
			name:    "User has INSERT privilege",
			host:    "%",
			user:    "testuser",
			priv:    PrivInsert,
			want:    true,
			wantErr: false,
		},
		{
			name:    "User does not have UPDATE privilege",
			host:    "%",
			user:    "testuser",
			priv:    PrivUpdate,
			want:    false,
			wantErr: false,
		},
		{
			name:    "User has CREATE privilege",
			host:    "%",
			user:    "testuser",
			priv:    PrivCreate,
			want:    true,
			wantErr: false,
		},
		{
			name:    "Non-existent user",
			host:    "%",
			user:    "nonexistent",
			priv:    PrivSelect,
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := um.HasPrivilege(tt.host, tt.user, tt.priv)
			if (err != nil) != tt.wantErr {
				t.Errorf("HasPrivilege() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HasPrivilege() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserManagerSetPassword(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{}

	host := "%"
	user := "testuser"

	// Create user
	err := um.CreateUser(host, user, passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Set password
	newPasswordHash := "*newhashedpassword"
	err = um.SetPassword(host, user, newPasswordHash)
	if err != nil {
		t.Errorf("SetPassword() error = %v, want nil", err)
	}

	// Verify password was changed
	retrievedUser, err := um.GetUser(host, user)
	if err != nil {
		t.Fatalf("GetUser() failed: %v", err)
	}
	if retrievedUser.Password != newPasswordHash {
		t.Errorf("SetPassword() password = %v, want %v", retrievedUser.Password, newPasswordHash)
	}
}

func TestUserManagerSetPasswordNonExistentUser(t *testing.T) {
	um := NewUserManager()

	err := um.SetPassword("%", "nonexistent", "*hashedpassword")
	if err == nil {
		t.Error("SetPassword() should return error for non-existent user")
	}
}

func TestSetPrivilege(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{"SELECT": false}

	host := "%"
	user := "testuser"

	// Create user
	err := um.CreateUser(host, user, passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Set SELECT privilege to true
	err = um.SetPrivilege(host, user, PrivSelect, true)
	if err != nil {
		t.Errorf("SetPrivilege() error = %v, want nil", err)
	}

	// Verify privilege was set
	hasPriv, err := um.HasPrivilege(host, user, PrivSelect)
	if err != nil {
		t.Fatalf("HasPrivilege() failed: %v", err)
	}
	if !hasPriv {
		t.Error("SetPrivilege() did not set privilege to true")
	}

	// Set privilege to false
	err = um.SetPrivilege(host, user, PrivSelect, false)
	if err != nil {
		t.Errorf("SetPrivilege() error = %v, want nil", err)
	}

	// Verify privilege was cleared
	hasPriv, err = um.HasPrivilege(host, user, PrivSelect)
	if err != nil {
		t.Fatalf("HasPrivilege() failed: %v", err)
	}
	if hasPriv {
		t.Error("SetPrivilege() did not clear privilege")
	}
}

func TestSetPrivilegeNonExistentUser(t *testing.T) {
	um := NewUserManager()

	err := um.SetPrivilege("%", "nonexistent", PrivSelect, true)
	if err == nil {
		t.Error("SetPrivilege() should return error for non-existent user")
	}
}

func TestLoadUsers(t *testing.T) {
	um := NewUserManager()

	users := []User{
		{
			Host:     "%",
			User:     "user1",
			Password: "",
			Privileges: map[string]bool{
				"SELECT": true,
				"INSERT": false,
			},
		},
		{
			Host:     "localhost",
			User:     "user2",
			Password: "*hashedpassword",
			Privileges: map[string]bool{
				"SELECT": false,
				"INSERT": true,
			},
		},
	}

	err := um.LoadUsers(users)
	if err != nil {
		t.Errorf("LoadUsers() error = %v, want nil", err)
	}

	// Verify users were loaded
	if len(um.users) != 2 {
		t.Errorf("LoadUsers() loaded %v users, want 2", len(um.users))
	}

	// Verify user1
	user1, err := um.GetUser("%", "user1")
	if err != nil {
		t.Errorf("GetUser() failed for user1: %v", err)
	}
	if user1 == nil {
		t.Fatal("user1 not found")
	}
	if user1.Privileges["SELECT"] != true {
		t.Error("user1 SELECT privilege not loaded correctly")
	}

	// Verify user2
	user2, err := um.GetUser("localhost", "user2")
	if err != nil {
		t.Errorf("GetUser() failed for user2: %v", err)
	}
	if user2 == nil {
		t.Fatal("user2 not found")
	}
	if user2.Password != "*hashedpassword" {
		t.Error("user2 password not loaded correctly")
	}
}

func TestExportUsers(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{"SELECT": true}

	// Create some users
	um.CreateUser("%", "user1", passwordHash, privileges)
	um.CreateUser("localhost", "user2", passwordHash, privileges)
	um.CreateUser("192.168.1.1", "user3", passwordHash, privileges)

	// Export users
	exported := um.ExportUsers()
	if len(exported) != 3 {
		t.Errorf("ExportUsers() length = %v, want 3", len(exported))
	}

	// Verify user names
	userNames := make(map[string]bool)
	for _, u := range exported {
		userNames[u.User] = true
	}
	if !userNames["user1"] || !userNames["user2"] || !userNames["user3"] {
		t.Error("ExportUsers() did not export all users")
	}
}

func TestUserPrivilegesInitialization(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{}

	err := um.CreateUser("%", "testuser", passwordHash, privileges)
	if err != nil {
		t.Fatalf("CreateUser() failed: %v", err)
	}

	// Get user
	user, err := um.GetUser("%", "testuser")
	if err != nil {
		t.Fatalf("GetUser() failed: %v", err)
	}

	// Verify all privilege types are initialized
	allPrivTypes := AllPermissionTypes()
	for _, privType := range allPrivTypes {
		if _, exists := user.Privileges[string(privType)]; !exists {
			t.Errorf("Privilege type %v not initialized", privType)
		}
	}
}

func TestMakeKey(t *testing.T) {
	um := NewUserManager()

	tests := []struct {
		host string
		user string
		want string
	}{
		{"%", "root", "%:root"},
		{"localhost", "test", "localhost:test"},
		{"LOCALHOST", "TEST", "localhost:test"},
		{"192.168.1.1", "user@domain", "192.168.1.1:user@domain"},
	}

	for _, tt := range tests {
		t.Run(tt.host+"@"+tt.user, func(t *testing.T) {
			got := um.makeKey(tt.host, tt.user)
			if got != tt.want {
				t.Errorf("makeKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserManagerConcurrentOperations(t *testing.T) {
	um := NewUserManager()
	passwordHash := ""
	privileges := map[string]bool{}

	// Create users concurrently
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(n int) {
			err := um.CreateUser("%", "user"+strconv.Itoa(n), passwordHash, privileges)
			done <- (err == nil)
		}(i)
	}

	// Wait for all operations
	successCount := 0
	for i := 0; i < 100; i++ {
		if <-done {
			successCount++
		}
	}

	// All should succeed
	if successCount != 100 {
		t.Errorf("Concurrent CreateUser() success count = %v, want 100", successCount)
	}

	// Verify all users exist
	users := um.ListUsers()
	if len(users) != 100 {
		t.Errorf("Concurrent operations created %v users, want 100", len(users))
	}
}
