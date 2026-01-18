package security

import (
	"errors"
	"sync"
	"time"
)

// Permission 权限类型
type Permission int

const (
	PermissionNone Permission = iota
	PermissionRead
	PermissionWrite
	PermissionDelete
	PermissionCreate
	PermissionDrop
	PermissionAlter
	PermissionGrant
	PermissionAll = 0xFF
)

// Role 角色类型
type Role string

const (
	RoleAdmin      Role = "admin"
	RoleModerator  Role = "moderator"
	RoleUser       Role = "user"
	RoleReadOnly   Role = "readonly"
	RoleGuest      Role = "guest"
)

// User 用户
type User struct {
	Username    string
	PasswordHash string
	Roles       []Role
	Permissions map[string]Permission // table -> permission
	CreatedAt   time.Time
	UpdatedAt   time.Time
	IsActive    bool
}

// AuthorizationManager 授权管理�?
type AuthorizationManager struct {
	users  map[string]*User
	roles  map[Role][]Permission
	rwLock sync.RWMutex
}

// NewAuthorizationManager 创建授权管理�?
func NewAuthorizationManager() *AuthorizationManager {
	am := &AuthorizationManager{
		users:  make(map[string]*User),
		roles:  make(map[Role][]Permission),
	}

	// 初始化默认角色权�?
	am.initDefaultRoles()

	return am
}

// initDefaultRoles 初始化默认角色权�?
func (am *AuthorizationManager) initDefaultRoles() {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	// 管理员：所有权�?
	am.roles[RoleAdmin] = []Permission{PermissionAll}

	// 版主：读写删除权�?
	am.roles[RoleModerator] = []Permission{
		PermissionRead,
		PermissionWrite,
		PermissionDelete,
	}

	// 普通用户：读写权限
	am.roles[RoleUser] = []Permission{
		PermissionRead,
		PermissionWrite,
	}

	// 只读用户：只读权�?
	am.roles[RoleReadOnly] = []Permission{
		PermissionRead,
	}

	// 访客：无权限
	am.roles[RoleGuest] = []Permission{
		PermissionNone,
	}
}

// CreateUser 创建用户
func (am *AuthorizationManager) CreateUser(username, passwordHash string, roles []Role) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	if username == "" {
		return errors.New("username cannot be empty")
	}

	if _, exists := am.users[username]; exists {
		return errors.New("user already exists")
	}

	am.users[username] = &User{
		Username:    username,
		PasswordHash: passwordHash,
		Roles:       roles,
		Permissions: make(map[string]Permission),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		IsActive:    true,
	}

	return nil
}

// GetUser 获取用户
func (am *AuthorizationManager) GetUser(username string) (*User, error) {
	am.rwLock.RLock()
	defer am.rwLock.RUnlock()

	user, exists := am.users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	return user, nil
}

// DeleteUser 删除用户
func (am *AuthorizationManager) DeleteUser(username string) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	if _, exists := am.users[username]; !exists {
		return errors.New("user not found")
	}

	delete(am.users, username)
	return nil
}

// HasPermission 检查用户是否有指定权限
func (am *AuthorizationManager) HasPermission(username string, permission Permission, table string) bool {
	am.rwLock.RLock()
	defer am.rwLock.RUnlock()

	user, exists := am.users[username]
	if !exists || !user.IsActive {
		return false
	}

	// 检查表级别权限
	if table != "" {
		if tablePerm, ok := user.Permissions[table]; ok {
			return (tablePerm & permission) != 0
		}
	}

	// 检查角色权�?
	for _, role := range user.Roles {
		if rolePerms, ok := am.roles[role]; ok {
			for _, perm := range rolePerms {
				if perm == PermissionAll || perm == permission {
					return true
				}
			}
		}
	}

	return false
}

// GrantPermission 授予权限
func (am *AuthorizationManager) GrantPermission(username string, permission Permission, table string) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	if table == "" {
		return errors.New("table cannot be empty")
	}

	if user.Permissions == nil {
		user.Permissions = make(map[string]Permission)
	}

	currentPerm := user.Permissions[table]
	user.Permissions[table] = currentPerm | permission
	user.UpdatedAt = time.Now()

	return nil
}

// RevokePermission 撤销权限
func (am *AuthorizationManager) RevokePermission(username string, permission Permission, table string) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	if table == "" {
		return errors.New("table cannot be empty")
	}

	currentPerm := user.Permissions[table]
	user.Permissions[table] = currentPerm &^ permission
	user.UpdatedAt = time.Now()

	return nil
}

// AssignRole 分配角色
func (am *AuthorizationManager) AssignRole(username string, role Role) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	for _, r := range user.Roles {
		if r == role {
			return errors.New("user already has this role")
		}
	}

	user.Roles = append(user.Roles, role)
	user.UpdatedAt = time.Now()

	return nil
}

// RemoveRole 移除角色
func (am *AuthorizationManager) RemoveRole(username string, role Role) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	for i, r := range user.Roles {
		if r == role {
			user.Roles = append(user.Roles[:i], user.Roles[i+1:]...)
			user.UpdatedAt = time.Now()
			return nil
		}
	}

	return errors.New("role not found for user")
}

// ActivateUser 激活用�?
func (am *AuthorizationManager) ActivateUser(username string) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	user.IsActive = true
	user.UpdatedAt = time.Now()

	return nil
}

// DeactivateUser 停用用户
func (am *AuthorizationManager) DeactivateUser(username string) error {
	am.rwLock.Lock()
	defer am.rwLock.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	user.IsActive = false
	user.UpdatedAt = time.Now()

	return nil
}

// ListUsers 列出所有用�?
func (am *AuthorizationManager) ListUsers() []string {
	am.rwLock.RLock()
	defer am.rwLock.RUnlock()

	users := make([]string, 0, len(am.users))
	for username := range am.users {
		users = append(users, username)
	}
	return users
}
