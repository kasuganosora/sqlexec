package session

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockSessionDriver 用于测试的模拟驱动
type MockSessionDriver struct {
	sessions  map[string]*Session
	values    map[string]map[string]any
	threadIds map[string]*Session
}

func NewMockSessionDriver() *MockSessionDriver {
	return &MockSessionDriver{
		sessions:  make(map[string]*Session),
		values:    make(map[string]map[string]any),
		threadIds: make(map[string]*Session),
	}
}

func (m *MockSessionDriver) CreateSession(ctx context.Context, session *Session) error {
	m.sessions[session.ID] = session
	m.values[session.ID] = make(map[string]any)
	return nil
}

func (m *MockSessionDriver) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	sess, ok := m.sessions[sessionID]
	if !ok {
		return nil, NewSessionNotFoundError()
	}
	return sess, nil
}

func (m *MockSessionDriver) GetSessions(ctx context.Context) ([]*Session, error) {
	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	return sessions, nil
}

func (m *MockSessionDriver) DeleteSession(ctx context.Context, sessionID string) error {
	delete(m.sessions, sessionID)
	delete(m.values, sessionID)
	return nil
}

func (m *MockSessionDriver) GetKey(ctx context.Context, sessionID string, key string) (any, error) {
	sess, ok := m.sessions[sessionID]
	if !ok {
		return nil, NewSessionNotFoundError()
	}

	values, ok := m.values[sessionID]
	if !ok {
		return nil, NewKeyNotFoundError()
	}

	val, ok := values[key]
	if !ok {
		return nil, NewKeyNotFoundError()
	}

	// Update LastUsed
	sess.LastUsed = time.Now()
	return val, nil
}

func (m *MockSessionDriver) SetKey(ctx context.Context, sessionID string, key string, value any) error {
	_, ok := m.sessions[sessionID]
	if !ok {
		return NewSessionNotFoundError()
	}

	values, ok := m.values[sessionID]
	if !ok {
		m.values[sessionID] = make(map[string]any)
		values = m.values[sessionID]
	}

	values[key] = value

	// Update LastUsed
	m.sessions[sessionID].LastUsed = time.Now()
	return nil
}

func (m *MockSessionDriver) DeleteKey(ctx context.Context, sessionID string, key string) error {
	_, ok := m.sessions[sessionID]
	if !ok {
		return NewSessionNotFoundError()
	}

	values, ok := m.values[sessionID]
	if !ok {
		return nil
	}

	delete(values, key)
	m.sessions[sessionID].LastUsed = time.Now()
	return nil
}

func (m *MockSessionDriver) GetAllKeys(ctx context.Context, sessionID string) ([]string, error) {
	_, ok := m.sessions[sessionID]
	if !ok {
		return nil, NewSessionNotFoundError()
	}

	values, ok := m.values[sessionID]
	if !ok {
		return []string{}, nil
	}

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}

	m.sessions[sessionID].LastUsed = time.Now()
	return keys, nil
}

func (m *MockSessionDriver) Touch(ctx context.Context, sessionID string) error {
	sess, ok := m.sessions[sessionID]
	if !ok {
		return NewSessionNotFoundError()
	}
	sess.LastUsed = time.Now()
	return nil
}

func (m *MockSessionDriver) GetThreadId(ctx context.Context, threadID uint32) (uint32, error) {
	sess, ok := m.threadIds[strconv.FormatUint(uint64(threadID), 10)]
	if !ok {
		return 0, NewThreadIdNotFoundError()
	}
	return sess.ThreadID, nil
}

func (m *MockSessionDriver) SetThreadId(ctx context.Context, threadID uint32, sess *Session) error {
	m.threadIds[strconv.FormatUint(uint64(threadID), 10)] = sess
	return nil
}

func (m *MockSessionDriver) DeleteThreadId(ctx context.Context, threadID uint32) error {
	delete(m.threadIds, strconv.FormatUint(uint64(threadID), 10))
	return nil
}

// Custom error functions
func NewSessionNotFoundError() error {
	return &sessionError{msg: "session not found"}
}

func NewKeyNotFoundError() error {
	return &keyError{msg: "key not found"}
}

func NewThreadIdNotFoundError() error {
	return &threadIdError{msg: "thread id not found"}
}

type sessionError struct {
	msg string
}

func (e *sessionError) Error() string {
	return e.msg
}

type keyError struct {
	msg string
}

func (e *keyError) Error() string {
	return e.msg
}

type threadIdError struct {
	msg string
}

func (e *threadIdError) Error() string {
	return e.msg
}

func TestInitSessionConfig(t *testing.T) {
	// 测试默认配置
	oldMaxAge := SessionMaxAge
	oldGCInterval := SessionGCInterval
	defer func() {
		SessionMaxAge = oldMaxAge
		SessionGCInterval = oldGCInterval
	}()

	cfg := &config.SessionConfig{
		MaxAge:     1 * time.Hour,
		GCInterval: 30 * time.Second,
	}

	InitSessionConfig(cfg)

	assert.Equal(t, 1*time.Hour, SessionMaxAge)
	assert.Equal(t, 30*time.Second, SessionGCInterval)
}

func TestInitSessionConfig_Nil(t *testing.T) {
	// 测试nil配置不应该改变默认值
	oldMaxAge := SessionMaxAge
	oldGCInterval := SessionGCInterval
	defer func() {
		SessionMaxAge = oldMaxAge
		SessionGCInterval = oldGCInterval
	}()

	InitSessionConfig(nil)

	// 应该保持不变
	assert.Equal(t, oldMaxAge, SessionMaxAge)
	assert.Equal(t, oldGCInterval, SessionGCInterval)
}

func TestNewSessionMgr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	assert.NotNil(t, mgr)
	assert.NotNil(t, mgr.driver)
}

func TestSessionMgr_CreateSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "127.0.0.1", "3306")

	assert.NoError(t, err)
	assert.NotNil(t, sess)
	assert.NotEmpty(t, sess.ID)
	assert.Equal(t, "127.0.0.1", sess.RemoteIP)
	assert.Equal(t, "3306", sess.RemotePort)
	assert.True(t, time.Since(sess.Created) < time.Second)
	assert.True(t, time.Since(sess.LastUsed) < time.Second)
	assert.Equal(t, uint32(1), sess.ThreadID)
}

func TestSessionMgr_GetOrCreateSession_NewSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.GetOrCreateSession(ctx, "192.168.1.1", "5432")

	assert.NoError(t, err)
	assert.NotNil(t, sess)
	assert.NotEmpty(t, sess.ID)
}

func TestSessionMgr_GetOrCreateSession_ExistingSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	// 创建会话
	sess1, err := mgr.CreateSession(ctx, "10.0.0.1", "8080")
	assert.NoError(t, err)

	// 获取已存在的会话
	sess2, err := mgr.GetOrCreateSession(ctx, "10.0.0.1", "8080")

	assert.NoError(t, err)
	assert.NotNil(t, sess2)
	assert.Equal(t, sess1.ID, sess2.ID)
}

func TestSessionMgr_GetSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	// 创建会话
	createdSess, err := mgr.CreateSession(ctx, "192.168.2.1", "3306")
	require.NoError(t, err)

	// 获取会话
	sess, err := mgr.GetSession(ctx, createdSess.ID)

	assert.NoError(t, err)
	assert.NotNil(t, sess)
	assert.Equal(t, createdSess.ID, sess.ID)
}

func TestSessionMgr_GetSession_NotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.GetSession(ctx, "non_existent_session_id")

	assert.Error(t, err)
	assert.Nil(t, sess)
	assert.Contains(t, err.Error(), "session not found")
}

func TestSessionMgr_DeleteSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	// 创建会话
	sess, err := mgr.CreateSession(ctx, "192.168.3.1", "3306")
	require.NoError(t, err)

	// 删除会话
	err = mgr.DeleteSession(ctx, sess.ID)
	assert.NoError(t, err)

	// 验证会话已被删除
	_, err = mgr.GetSession(ctx, sess.ID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session not found")
}

func TestSessionMgr_GetSessions(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	// 创建多个会话
	_, err := mgr.CreateSession(ctx, "192.168.4.1", "3306")
	require.NoError(t, err)
	_, err = mgr.CreateSession(ctx, "192.168.4.2", "3306")
	require.NoError(t, err)
	_, err = mgr.CreateSession(ctx, "192.168.4.3", "3306")
	require.NoError(t, err)

	// 获取所有会话
	sessions, err := mgr.GetSessions(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, sessions)
	assert.Len(t, sessions, 3)
}

func TestSessionMgr_GenerateSessionID(t *testing.T) {
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(context.Background(), driver)

	id1 := mgr.GenerateSessionID("127.0.0.1", "3306")
	id2 := mgr.GenerateSessionID("127.0.0.1", "3306")
	id3 := mgr.GenerateSessionID("192.168.1.1", "3306")

	// 相同的地址和端口应该生成相同的ID
	assert.Equal(t, id1, id2)
	// 不同的地址或端口应该生成不同的ID
	assert.NotEqual(t, id1, id3)
	// ID应该是32字符的MD5哈希
	assert.Len(t, id1, 32)
}

func TestSessionMgr_GetThreadId(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	// 第一个会话应该获得ThreadID 1
	sess1, err := mgr.CreateSession(ctx, "192.168.5.1", "3306")
	require.NoError(t, err)
	assert.Equal(t, uint32(1), sess1.ThreadID)

	// 第二个会话应该获得ThreadID 2
	sess2, err := mgr.CreateSession(ctx, "192.168.5.2", "3306")
	require.NoError(t, err)
	assert.Equal(t, uint32(2), sess2.ThreadID)
}

func TestSession_SetAndGet(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.6.1", "3306")
	require.NoError(t, err)

	// 设置值
	err = sess.Set("username", "testuser")
	assert.NoError(t, err)

	// 获取值
	val, err := sess.Get("username")
	assert.NoError(t, err)
	assert.Equal(t, "testuser", val)
}

func TestSession_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.7.1", "3306")
	require.NoError(t, err)

	val, err := sess.Get("nonexistent_key")
	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Contains(t, err.Error(), "key not found")
}

func TestSession_Delete(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.8.1", "3306")
	require.NoError(t, err)

	// 设置值
	err = sess.Set("test_key", "test_value")
	require.NoError(t, err)

	// 删除值
	err = sess.Delete("test_key")
	assert.NoError(t, err)

	// 验证值已被删除
	_, err = sess.Get("test_key")
	assert.Error(t, err)
}

func TestSession_SetUser(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.9.1", "3306")
	require.NoError(t, err)

	sess.SetUser("testuser")

	assert.Equal(t, "testuser", sess.User)
}

func TestSession_SetAndGetVariable(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.10.1", "3306")
	require.NoError(t, err)

	// 设置变量
	err = sess.SetVariable("autocommit", true)
	assert.NoError(t, err)

	// 获取变量
	val, err := sess.GetVariable("autocommit")
	assert.NoError(t, err)
	assert.Equal(t, true, val)
}

func TestSession_DeleteVariable(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.11.1", "3306")
	require.NoError(t, err)

	// 设置变量
	err = sess.SetVariable("test_var", "test_value")
	require.NoError(t, err)

	// 删除变量
	err = sess.DeleteVariable("test_var")
	assert.NoError(t, err)

	// 验证变量已被删除
	_, err = sess.GetVariable("test_var")
	assert.Error(t, err)
}

func TestSession_GetAllVariables(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.12.1", "3306")
	require.NoError(t, err)

	// 设置多个变量
	sess.SetVariable("var1", "value1")
	sess.SetVariable("var2", 123)
	sess.SetVariable("var3", true)

	// 获取所有变量
	vars, err := sess.GetAllVariables()
	assert.NoError(t, err)
	assert.Len(t, vars, 3)
	assert.Equal(t, "value1", vars["var1"])
	assert.Equal(t, 123, vars["var2"])
	assert.Equal(t, true, vars["var3"])
}

func TestSession_GetNextSequenceID(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.13.1", "3306")
	require.NoError(t, err)

	// 初始序列号应该是0
	assert.Equal(t, uint8(0), sess.SequenceID)

	// 获取下一个序列号
	id1 := sess.GetNextSequenceID()
	assert.Equal(t, uint8(1), id1)
	assert.Equal(t, uint8(1), sess.SequenceID)

	id2 := sess.GetNextSequenceID()
	assert.Equal(t, uint8(2), id2)
	assert.Equal(t, uint8(2), sess.SequenceID)
}

func TestSession_ResetSequenceID(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	sess, err := mgr.CreateSession(ctx, "192.168.14.1", "3306")
	require.NoError(t, err)

	// 增加序列号
	sess.GetNextSequenceID()
	sess.GetNextSequenceID()
	assert.Equal(t, uint8(2), sess.SequenceID)

	// 重置序列号
	sess.ResetSequenceID()
	assert.Equal(t, uint8(0), sess.SequenceID)
}

func TestSessionMgr_GC(t *testing.T) {
	ctx := context.Background()
	driver := NewMockSessionDriver()
	mgr := NewSessionMgr(ctx, driver)

	// 修改GC配置以便快速测试
	oldMaxAge := SessionMaxAge
	oldGCInterval := SessionGCInterval
	defer func() {
		SessionMaxAge = oldMaxAge
		SessionGCInterval = oldGCInterval
	}()

	SessionMaxAge = 100 * time.Millisecond

	// 创建会话
	sess1, err := mgr.CreateSession(ctx, "192.168.15.1", "3306")
	require.NoError(t, err)

	// 等待会话过期
	time.Sleep(150 * time.Millisecond)

	// 创建新会话
	sess2, err := mgr.CreateSession(ctx, "192.168.15.2", "3306")
	require.NoError(t, err)

	// 执行GC
	err = mgr.GC()
	assert.NoError(t, err)

	// 验证旧会话已被删除
	_, err = mgr.GetSession(ctx, sess1.ID)
	assert.Error(t, err)

	// 验证新会话仍然存在
	_, err = mgr.GetSession(ctx, sess2.ID)
	assert.NoError(t, err)
}
