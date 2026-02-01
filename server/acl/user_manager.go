package acl

import (
	"fmt"
	"strings"
	"sync"
)

// UserManager handles user CRUD operations
type UserManager struct {
	users map[string]*User // Key: "host:user"
	mu    sync.RWMutex
}

// NewUserManager creates a new user manager
func NewUserManager() *UserManager {
	return &UserManager{
		users: make(map[string]*User),
	}
}

// CreateUser creates a new user with given privileges
func (um *UserManager) CreateUser(host, user, passwordHash string, privileges map[string]bool) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	key := um.makeKey(host, user)

	// Check if user already exists
	if _, exists := um.users[key]; exists {
		return fmt.Errorf("user '%s'@'%s' already exists", user, host)
	}

	// Create new user
	newUser := &User{
		Host:       host,
		User:       user,
		Password:   passwordHash,
		Privileges: make(map[string]bool),
	}

	// Initialize all permissions to false
	for _, privType := range AllPermissionTypes() {
		newUser.Privileges[string(privType)] = false
	}

	// Apply given privileges
	for priv, granted := range privileges {
		if _, exists := newUser.Privileges[priv]; exists {
			newUser.Privileges[priv] = granted
		}
	}

	um.users[key] = newUser
	return nil
}

// DropUser removes a user
func (um *UserManager) DropUser(host, user string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	key := um.makeKey(host, user)

	// Check if user exists
	if _, exists := um.users[key]; !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", user, host)
	}

	delete(um.users, key)
	return nil
}

// GetUser retrieves a user by host and username
func (um *UserManager) GetUser(host, user string) (*User, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	// Try exact match first
	key := um.makeKey(host, user)
	if u, exists := um.users[key]; exists {
		return u, nil
	}

	// Try wildcard host match
	for _, u := range um.users {
		if u.User == user && (u.Host == "%" || u.Host == host) {
			return u, nil
		}
	}

	return nil, fmt.Errorf("user '%s'@'%s' does not exist", user, host)
}

// ListUsers returns all users (only for privileged users)
func (um *UserManager) ListUsers() []*User {
	um.mu.RLock()
	defer um.mu.RUnlock()

	users := make([]*User, 0, len(um.users))
	for _, u := range um.users {
		users = append(users, u)
	}
	return users
}

// HasPrivilege checks if user has a specific global privilege
func (um *UserManager) HasPrivilege(host, user string, priv PermissionType) (bool, error) {
	u, err := um.GetUser(host, user)
	if err != nil {
		return false, err
	}

	granted, exists := u.Privileges[string(priv)]
	if !exists {
		return false, nil
	}

	return granted, nil
}

// SetPassword updates user's password hash
func (um *UserManager) SetPassword(host, user, newPasswordHash string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	key := um.makeKey(host, user)

	u, exists := um.users[key]
	if !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", user, host)
	}

	u.Password = newPasswordHash
	return nil
}

// SetPrivilege sets a specific privilege for a user
func (um *UserManager) SetPrivilege(host, user string, priv PermissionType, granted bool) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	key := um.makeKey(host, user)

	u, exists := um.users[key]
	if !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", user, host)
	}

	u.Privileges[string(priv)] = granted
	return nil
}

// LoadUsers loads users from data slice
func (um *UserManager) LoadUsers(users []User) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	um.users = make(map[string]*User)

	for i := range users {
		u := &users[i]
		key := um.makeKey(u.Host, u.User)
		um.users[key] = u
	}

	return nil
}

// ExportUsers exports users to data slice
func (um *UserManager) ExportUsers() []User {
	um.mu.RLock()
	defer um.mu.RUnlock()

	users := make([]User, 0, len(um.users))
	for _, u := range um.users {
		users = append(users, *u)
	}
	return users
}

// makeKey creates a map key from host and user
func (um *UserManager) makeKey(host, user string) string {
	return strings.ToLower(host) + ":" + strings.ToLower(user)
}
