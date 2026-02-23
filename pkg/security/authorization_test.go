package security

import (
	"testing"
	"time"
)

func TestNewAuthorizationManager(t *testing.T) {
	am := NewAuthorizationManager()
	if am == nil {
		t.Fatal("NewAuthorizationManager returned nil")
	}
	if am.users == nil {
		t.Error("users map should be initialized")
	}
	if am.roles == nil {
		t.Error("roles map should be initialized")
	}
}

func TestInitDefaultRoles(t *testing.T) {
	am := NewAuthorizationManager()

	// 检查默认角色是否初始化
	expectedRoles := []Role{RoleAdmin, RoleModerator, RoleUser, RoleReadOnly, RoleGuest}
	for _, role := range expectedRoles {
		if _, exists := am.roles[role]; !exists {
			t.Errorf("Role %s should be initialized", role)
		}
	}

	// 检查admin权限
	adminPerms := am.roles[RoleAdmin]
	if len(adminPerms) != 1 || adminPerms[0] != PermissionAll {
		t.Error("Admin should have PermissionAll")
	}
}

func TestCreateUser(t *testing.T) {
	am := NewAuthorizationManager()

	tests := []struct {
		name        string
		username    string
		password    string
		roles       []Role
		expectError bool
	}{
		{"Valid user", "testuser", "password123", []Role{RoleUser}, false},
		{"Empty username", "", "password123", []Role{RoleUser}, true},
		{"Duplicate user", "testuser", "password456", []Role{RoleAdmin}, true},
		{"User with multiple roles", "multirole", "password", []Role{RoleUser, RoleModerator}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := am.CreateUser(tt.username, tt.password, tt.roles)
			if (err != nil) != tt.expectError {
				t.Errorf("CreateUser() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

func TestGetUser(t *testing.T) {
	am := NewAuthorizationManager()

	// 创建测试用户
	username := "testuser"
	password := "password123"
	roles := []Role{RoleUser}
	am.CreateUser(username, password, roles)

	// 测试获取存在的用户
	user, err := am.GetUser(username)
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if user.Username != username {
		t.Errorf("Got username %s, want %s", user.Username, username)
	}
	if user.PasswordHash != password {
		t.Error("Password hash mismatch")
	}

	// 测试获取不存在的用户
	_, err = am.GetUser("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent user")
	}
}

func TestDeleteUser(t *testing.T) {
	am := NewAuthorizationManager()

	username := "testuser"
	am.CreateUser(username, "password", []Role{RoleUser})

	// 删除存在的用户
	err := am.DeleteUser(username)
	if err != nil {
		t.Errorf("DeleteUser() error = %v", err)
	}

	// 验证用户已删除
	_, err = am.GetUser(username)
	if err == nil {
		t.Error("User should be deleted")
	}

	// 删除不存在的用户
	err = am.DeleteUser("nonexistent")
	if err == nil {
		t.Error("Expected error for deleting nonexistent user")
	}
}

func TestHasPermission(t *testing.T) {
	am := NewAuthorizationManager()

	// 创建不同角色的用户
	am.CreateUser("admin", "password", []Role{RoleAdmin})
	am.CreateUser("moderator", "password", []Role{RoleModerator})
	am.CreateUser("user", "password", []Role{RoleUser})
	am.CreateUser("readonly", "password", []Role{RoleReadOnly})
	am.CreateUser("guest", "password", []Role{RoleGuest})

	tests := []struct {
		username   string
		permission Permission
		table      string
		expected   bool
	}{
		{"admin", PermissionRead, "users", true},
		{"admin", PermissionWrite, "users", true},
		{"admin", PermissionDelete, "users", true},
		{"moderator", PermissionRead, "users", true},
		{"moderator", PermissionWrite, "users", true},
		{"moderator", PermissionDelete, "users", true},
		{"moderator", PermissionCreate, "users", false},
		{"user", PermissionRead, "users", true},
		{"user", PermissionWrite, "users", true},
		{"user", PermissionDelete, "users", false},
		{"readonly", PermissionRead, "users", true},
		{"readonly", PermissionWrite, "users", false},
		{"guest", PermissionRead, "users", false},
		{"nonexistent", PermissionRead, "users", false},
	}

	for _, tt := range tests {
		t.Run(tt.username, func(t *testing.T) {
			result := am.HasPermission(tt.username, tt.permission, tt.table)
			if result != tt.expected {
				t.Errorf("HasPermission(%s, %v, %s) = %v, want %v",
					tt.username, tt.permission, tt.table, result, tt.expected)
			}
		})
	}
}

func TestGrantPermission(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleUser})

	tests := []struct {
		name        string
		username    string
		permission  Permission
		table       string
		expectError bool
	}{
		{"Valid grant", "testuser", PermissionDelete, "users", false},
		{"Nonexistent user", "nonexistent", PermissionRead, "users", true},
		{"Empty table", "testuser", PermissionRead, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := am.GrantPermission(tt.username, tt.permission, tt.table)
			if (err != nil) != tt.expectError {
				t.Errorf("GrantPermission() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}

	// 验证权限已授予
	if !am.HasPermission("testuser", PermissionDelete, "users") {
		t.Error("Permission should be granted")
	}
}

func TestRevokePermission(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleAdmin})

	// 先授予权限
	am.GrantPermission("testuser", PermissionRead, "users")

	// 撤销权限
	err := am.RevokePermission("testuser", PermissionRead, "users")
	if err != nil {
		t.Errorf("RevokePermission() error = %v", err)
	}

	// 验证权限已撤销（admin角色仍有权限，所以这里测试表级权限撤销）
	user, _ := am.GetUser("testuser")
	if user.Permissions["users"] != 0 {
		t.Error("Table-level permission should be revoked")
	}
}

func TestAssignRole(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleUser})

	err := am.AssignRole("testuser", RoleModerator)
	if err != nil {
		t.Errorf("AssignRole() error = %v", err)
	}

	// 验证角色已分配
	user, _ := am.GetUser("testuser")
	if len(user.Roles) != 2 {
		t.Errorf("Expected 2 roles, got %d", len(user.Roles))
	}

	// 测试重复分配
	err = am.AssignRole("testuser", RoleModerator)
	if err == nil {
		t.Error("Expected error for duplicate role")
	}
}

func TestRemoveRole(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleUser, RoleModerator})

	err := am.RemoveRole("testuser", RoleModerator)
	if err != nil {
		t.Errorf("RemoveRole() error = %v", err)
	}

	// 验证角色已移除
	user, _ := am.GetUser("testuser")
	if len(user.Roles) != 1 {
		t.Errorf("Expected 1 role, got %d", len(user.Roles))
	}

	// 测试移除不存在的角色
	err = am.RemoveRole("testuser", RoleAdmin)
	if err == nil {
		t.Error("Expected error for nonexistent role")
	}
}

func TestActivateUser(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleUser})

	// 停用用户
	am.DeactivateUser("testuser")
	user, _ := am.GetUser("testuser")
	if user.IsActive {
		t.Error("User should be inactive")
	}

	// 激活用户
	err := am.ActivateUser("testuser")
	if err != nil {
		t.Errorf("ActivateUser() error = %v", err)
	}

	user, _ = am.GetUser("testuser")
	if !user.IsActive {
		t.Error("User should be active")
	}
}

func TestDeactivateUser(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleUser})

	err := am.DeactivateUser("testuser")
	if err != nil {
		t.Errorf("DeactivateUser() error = %v", err)
	}

	user, _ := am.GetUser("testuser")
	if user.IsActive {
		t.Error("User should be inactive")
	}
}

func TestListUsers(t *testing.T) {
	am := NewAuthorizationManager()

	am.CreateUser("user1", "password", []Role{RoleUser})
	am.CreateUser("user2", "password", []Role{RoleAdmin})
	am.CreateUser("user3", "password", []Role{RoleModerator})

	users := am.ListUsers()
	if len(users) != 3 {
		t.Errorf("Expected 3 users, got %d", len(users))
	}
}

func TestUserTimestamps(t *testing.T) {
	am := NewAuthorizationManager()

	beforeCreation := time.Now()
	am.CreateUser("testuser", "password", []Role{RoleUser})
	user, _ := am.GetUser("testuser")

	if user.CreatedAt.Before(beforeCreation) {
		t.Error("CreatedAt should be after creation time")
	}

	if user.UpdatedAt.Before(user.CreatedAt) {
		t.Error("UpdatedAt should not be before CreatedAt")
	}

	// 更新用户并检查UpdatedAt
	beforeUpdate := time.Now()
	am.AssignRole("testuser", RoleModerator)
	user, _ = am.GetUser("testuser")

	if user.UpdatedAt.Before(beforeUpdate) {
		t.Error("UpdatedAt should be after update")
	}
}

func TestPermissionConstants(t *testing.T) {
	// 测试权限常量
	expected := map[Permission]int{
		PermissionNone:   0,
		PermissionRead:   1,
		PermissionWrite:  2,
		PermissionDelete: 4,
		PermissionCreate: 8,
		PermissionDrop:   16,
		PermissionAlter:  32,
		PermissionGrant:  64,
		PermissionAll:    0xFF,
	}

	for perm, expectedVal := range expected {
		if int(perm) != expectedVal {
			t.Errorf("Permission %d = %d, want %d", perm, int(perm), expectedVal)
		}
	}
}

func TestRoleConstants(t *testing.T) {
	// 测试角色常量
	if RoleAdmin != "admin" {
		t.Errorf("RoleAdmin = %s, want admin", RoleAdmin)
	}
	if RoleModerator != "moderator" {
		t.Errorf("RoleModerator = %s, want moderator", RoleModerator)
	}
	if RoleUser != "user" {
		t.Errorf("RoleUser = %s, want user", RoleUser)
	}
	if RoleReadOnly != "readonly" {
		t.Errorf("RoleReadOnly = %s, want readonly", RoleReadOnly)
	}
	if RoleGuest != "guest" {
		t.Errorf("RoleGuest = %s, want guest", RoleGuest)
	}
}

func TestInactiveUserHasPermission(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleAdmin})

	// 停用用户
	am.DeactivateUser("testuser")

	// 不活跃用户不应该有权限
	if am.HasPermission("testuser", PermissionRead, "users") {
		t.Error("Inactive user should not have permissions")
	}
}

func TestTableLevelPermissionPriority(t *testing.T) {
	am := NewAuthorizationManager()
	am.CreateUser("testuser", "password", []Role{RoleReadOnly})

	// 授予表级别的写权限
	am.GrantPermission("testuser", PermissionWrite, "users")

	// 应该有表级别权限
	if !am.HasPermission("testuser", PermissionWrite, "users") {
		t.Error("Should have table-level Write permission")
	}
}
