package testing

import (
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/api"
)

// MockSession 实现pkg/session.Session接口用于测试
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

// NewMockSession 创建一个新的Mock Session
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

// GetID 获取Session ID
func (m *MockSession) GetID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ID
}

// SetID 设置Session ID
func (m *MockSession) SetID(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ID = id
}

// GetThreadID 获取线程ID
func (m *MockSession) GetThreadID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ThreadID
}

// SetThreadID 设置线程ID
func (m *MockSession) SetThreadID(threadID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ThreadID = threadID
}

// GetUser 获取用户名
func (m *MockSession) GetUser() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.User
}

// SetUser 设置用户名
func (m *MockSession) SetUser(user string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.User = user
}

// GetSequenceID 获取当前序列号
func (m *MockSession) GetSequenceID() uint8 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SequenceID
}

// GetNextSequenceID 获取下一个序列号并递增
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

// ResetSequenceID 重置序列号为0
func (m *MockSession) ResetSequenceID() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SequenceID = 0
}

// SetSequenceID 设置序列号（用于测试特定场景）
func (m *MockSession) SetSequenceID(seqID uint8) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SequenceID = seqID
}

// Get 获取Session数据
func (m *MockSession) Get(key string) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key]
}

// Set 设置Session数据
func (m *MockSession) Set(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete 删除Session数据
func (m *MockSession) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// GetAPISession 获取API Session
func (m *MockSession) GetAPISession() interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.apiSession
}

// SetAPISession 设置API Session
func (m *MockSession) SetAPISession(apiSession interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apiSession = apiSession
}

// Close 关闭Session
func (m *MockSession) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// IsClosed 检查Session是否已关闭
func (m *MockSession) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Clone 克隆Session（用于测试）
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

// NewMockAPISession 创建一个Mock的API Session
func NewMockAPISession() *api.Session {
	// 使用真实API Session但不需要真实DB
	// 在单元测试中，我们可以mock DB部分
	return nil
}
