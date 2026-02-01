package information_schema

import (
	"sync"
)

// MockACLManager is a mock implementation of ACLManager for testing
type MockACLManager struct {
	users     []*MockUser
	mu        sync.RWMutex
	isLoaded   bool
}

// MockUser is a mock user for testing
type MockUser struct {
	User       string
	Host       string
	Password   string
	Privileges map[string]bool
}

// NewMockACLManager creates a new mock ACL manager
func NewMockACLManager() *MockACLManager {
	return &MockACLManager{
		users:   make([]*MockUser, 0),
		isLoaded: true,
	}
}

// CheckPermission checks if user has specified permission
func (m *MockACLManager) CheckPermission(user, host, permission, db, table, column string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, u := range m.users {
		if u.User == user && (u.Host == host || u.Host == "%") {
			if granted, exists := u.Privileges[permission]; exists && granted {
				return true
			}
		}
	}
	return false
}

// HasGrantOption checks if user has GRANT OPTION
func (m *MockACLManager) HasGrantOption(user, host string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, u := range m.users {
		if u.User == user && (u.Host == host || u.Host == "%") {
			if granted, exists := u.Privileges["GRANT OPTION"]; exists && granted {
				return true
			}
		}
	}
	return false
}

// IsLoaded returns true if ACL data has been loaded
func (m *MockACLManager) IsLoaded() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isLoaded
}

// GetUsers returns all users
func (m *MockACLManager) GetUsers() interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	users := make([]interface{}, len(m.users))
	for i, u := range m.users {
		users[i] = &MockUser{
			User:       u.User,
			Host:       u.Host,
			Password:   u.Password,
			Privileges: copyPrivileges(u.Privileges),
		}
	}
	return users
}

// AddUser adds a mock user
func (m *MockACLManager) AddUser(host, user, password string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mockUser := &MockUser{
		User:       user,
		Host:       host,
		Password:   password,
		Privileges: make(map[string]bool),
	}
	m.users = append(m.users, mockUser)
}

// Grant grants privileges to a user
func (m *MockACLManager) Grant(host, user string, privileges []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, u := range m.users {
		if u.User == user && u.Host == host {
			for _, priv := range privileges {
				if u.Privileges == nil {
					u.Privileges = make(map[string]bool)
				}
				u.Privileges[priv] = true
			}
			break
		}
	}
}

// copyPrivileges creates a copy of the privileges map
func copyPrivileges(src map[string]bool) map[string]bool {
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
