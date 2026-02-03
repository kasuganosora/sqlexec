package mock

import (
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/api"
)

// MockSession implements pkg/session.Session interface for testing
type MockSession struct {
	mu           sync.Mutex
	ID           string
	ThreadID     uint32
	User         string
	SequenceID   uint8
	apiSession   interface{}
	data         map[string]interface{}
	closed       bool
}

// NewMockSession creates a new mock session
func NewMockSession() *MockSession {
	return &MockSession{
		ID:         "mock-session-1",
		ThreadID:   1,
		User:       "",
		SequenceID: 0,
		data:       make(map[string]interface{}),
		closed:     false,
	}
}

// GetID returns Session ID
func (m *MockSession) GetID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ID
}

// SetID sets Session ID
func (m *MockSession) SetID(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ID = id
}

// GetThreadID returns thread ID
func (m *MockSession) GetThreadID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ThreadID
}

// SetThreadID sets thread ID
func (m *MockSession) SetThreadID(threadID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ThreadID = threadID
}

// GetUser returns username
func (m *MockSession) GetUser() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.User
}

// SetUser sets username
func (m *MockSession) SetUser(user string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.User = user
}

// GetSequenceID returns current sequence ID
func (m *MockSession) GetSequenceID() uint8 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SequenceID
}

// GetNextSequenceID gets next sequence ID and increments
func (m *MockSession) GetNextSequenceID() uint8 {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.SequenceID
	m.SequenceID++
	if m.SequenceID > 255 {
		m.SequenceID = 0
	}
	return current
}

// ResetSequenceID resets sequence ID to 0
func (m *MockSession) ResetSequenceID() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SequenceID = 0
}

// SetSequenceID sets sequence ID (for testing specific scenarios)
func (m *MockSession) SetSequenceID(seqID uint8) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SequenceID = seqID
}

// Get gets session data
func (m *MockSession) Get(key string) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key]
}

// Set sets session data
func (m *MockSession) Set(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete deletes session data
func (m *MockSession) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// GetAPISession returns API Session
func (m *MockSession) GetAPISession() interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.apiSession
}

// SetAPISession sets API Session
func (m *MockSession) SetAPISession(apiSession interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apiSession = apiSession
}

// Close closes session
func (m *MockSession) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// IsClosed checks if session is closed
func (m *MockSession) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Clone clones session (for testing)
func (m *MockSession) Clone() *MockSession {
	m.mu.Lock()
	defer m.mu.Unlock()

	clone := &MockSession{
		ID:         m.ID,
		ThreadID:   m.ThreadID,
		User:       m.User,
		SequenceID: m.SequenceID,
		apiSession: m.apiSession,
		data:       make(map[string]interface{}),
		closed:     m.closed,
	}

	for k, v := range m.data {
		clone.data[k] = v
	}

	return clone
}

// NewMockAPISession creates a mock API session
func NewMockAPISession() *api.Session {
	// Use real API session but no real DB needed
	// In unit tests, we can mock the DB part
	return nil
}
