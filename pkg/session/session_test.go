package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/config"
)

// MockSessionDriver implements SessionDriver for testing
type MockSessionDriver struct {
	sessions  map[string]*Session
	threadIDs map[uint32]*Session
	keys      map[string]map[string]interface{}
	mu        sync.RWMutex
}

func NewMockSessionDriver() *MockSessionDriver {
	return &MockSessionDriver{
		sessions:  make(map[string]*Session),
		threadIDs: make(map[uint32]*Session),
		keys:      make(map[string]map[string]interface{}),
	}
}

func (m *MockSessionDriver) CreateSession(ctx context.Context, session *Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[session.ID] = session
	m.keys[session.ID] = make(map[string]interface{})
	return nil
}

func (m *MockSessionDriver) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if sess, ok := m.sessions[sessionID]; ok {
		return sess, nil
	}
	return nil, errors.New("session not found")
}

func (m *MockSessionDriver) GetSessions(ctx context.Context) ([]*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		result = append(result, sess)
	}
	return result, nil
}

func (m *MockSessionDriver) DeleteSession(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
	delete(m.keys, sessionID)
	return nil
}

func (m *MockSessionDriver) GetKey(ctx context.Context, sessionID string, key string) (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if keys, ok := m.keys[sessionID]; ok {
		if val, ok := keys[key]; ok {
			return val, nil
		}
		return nil, errors.New("key not found")
	}
	return nil, errors.New("session not found")
}

func (m *MockSessionDriver) SetKey(ctx context.Context, sessionID string, key string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if keys, ok := m.keys[sessionID]; ok {
		keys[key] = value
		return nil
	}
	return errors.New("session not found")
}

func (m *MockSessionDriver) DeleteKey(ctx context.Context, sessionID string, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if keys, ok := m.keys[sessionID]; ok {
		delete(keys, key)
		return nil
	}
	return errors.New("session not found")
}

func (m *MockSessionDriver) GetAllKeys(ctx context.Context, sessionID string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if keys, ok := m.keys[sessionID]; ok {
		result := make([]string, 0, len(keys))
		for k := range keys {
			result = append(result, k)
		}
		return result, nil
	}
	return nil, errors.New("session not found")
}

func (m *MockSessionDriver) Touch(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok := m.sessions[sessionID]; ok {
		sess.LastUsed = time.Now()
		return nil
	}
	return errors.New("session not found")
}

func (m *MockSessionDriver) GetThreadId(ctx context.Context, threadID uint32) (uint32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.threadIDs[threadID]; ok {
		return threadID, nil
	}
	return 0, errors.New("thread id not found")
}

func (m *MockSessionDriver) SetThreadId(ctx context.Context, threadID uint32, sess *Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.threadIDs[threadID] = sess
	return nil
}

func (m *MockSessionDriver) DeleteThreadId(ctx context.Context, threadID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.threadIDs, threadID)
	return nil
}

// TestSessionMgr_Close tests graceful shutdown
func TestSessionMgr_Close(t *testing.T) {
	driver := NewMockSessionDriver()
	ctx := context.Background()

	mgr := NewSessionMgr(ctx, driver)

	// Verify manager is created
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}

	// Close should not panic and should complete
	done := make(chan struct{})
	go func() {
		mgr.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete within timeout")
	}
}

// TestSessionMgr_GC_ContextCancellation tests GC stops when context is cancelled
func TestSessionMgr_GC_ContextCancellation(t *testing.T) {
	driver := NewMockSessionDriver()
	ctx, cancel := context.WithCancel(context.Background())

	mgr := NewSessionMgr(ctx, driver)

	// Cancel context
	cancel()

	// Wait a bit for goroutine to detect cancellation
	time.Sleep(100 * time.Millisecond)

	// Close should complete quickly since goroutine should have exited
	done := make(chan struct{})
	go func() {
		mgr.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Close did not complete within timeout after context cancellation")
	}
}

// TestSession_GetNextSequenceID_ConcurrentSafety tests concurrent access to SequenceID
func TestSession_GetNextSequenceID_ConcurrentSafety(t *testing.T) {
	driver := NewMockSessionDriver()
	sess := &Session{
		ID:         "test-session",
		driver:     driver,
		SequenceID: 0,
	}

	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				id := sess.GetNextSequenceID()
				// Just ensure no race condition (will be caught by race detector)
				_ = id
			}
		}()
	}

	wg.Wait()

	// Final sequence should be goroutines * iterations (mod 256 for uint8 wrap)
	expected := uint8((goroutines * iterations) % 256)
	if sess.SequenceID != expected {
		t.Errorf("expected SequenceID=%d, got %d", expected, sess.SequenceID)
	}
}

// TestSession_GetNextSequenceID_WrapAround tests uint8 wrap-around behavior
func TestSession_GetNextSequenceID_WrapAround(t *testing.T) {
	driver := NewMockSessionDriver()
	sess := &Session{
		ID:         "test-session",
		driver:     driver,
		SequenceID: 254,
	}

	// Should wrap around
	id1 := sess.GetNextSequenceID()
	if id1 != 255 {
		t.Errorf("expected 255, got %d", id1)
	}

	id2 := sess.GetNextSequenceID()
	if id2 != 0 {
		t.Errorf("expected 0 (wrap-around), got %d", id2)
	}

	id3 := sess.GetNextSequenceID()
	if id3 != 1 {
		t.Errorf("expected 1, got %d", id3)
	}
}

// TestSessionMgr_CreateSession tests session creation
func TestSessionMgr_CreateSession(t *testing.T) {
	driver := NewMockSessionDriver()
	ctx := context.Background()
	defer driver.DeleteSession(ctx, "test-id")

	mgr := NewSessionMgr(ctx, driver)
	defer mgr.Close()

	sess, err := mgr.CreateSession(ctx, "127.0.0.1", "3306")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if sess == nil {
		t.Fatal("expected non-nil session")
	}

	if sess.RemoteIP != "127.0.0.1" {
		t.Errorf("expected RemoteIP=127.0.0.1, got %s", sess.RemoteIP)
	}

	if sess.RemotePort != "3306" {
		t.Errorf("expected RemotePort=3306, got %s", sess.RemotePort)
	}
}

// TestSession_SetVariable tests session variable operations
func TestSession_SetVariable(t *testing.T) {
	driver := NewMockSessionDriver()
	sess := &Session{
		ID:     "test-session",
		driver: driver,
	}
	driver.sessions["test-session"] = sess
	driver.keys["test-session"] = make(map[string]interface{})

	err := sess.SetVariable("autocommit", 1)
	if err != nil {
		t.Fatalf("failed to set variable: %v", err)
	}

	val, err := sess.GetVariable("autocommit")
	if err != nil {
		t.Fatalf("failed to get variable: %v", err)
	}

	if val != 1 {
		t.Errorf("expected autocommit=1, got %v", val)
	}
}

// TestInitSessionConfig tests session config initialization
func TestInitSessionConfig(t *testing.T) {
	// Save original values
	origMaxAge := SessionMaxAge
	origGCInterval := SessionGCInterval
	defer func() {
		SessionMaxAge = origMaxAge
		SessionGCInterval = origGCInterval
	}()

	cfg := &config.SessionConfig{
		MaxAge:     2 * time.Hour,
		GCInterval: 30 * time.Second,
	}

	InitSessionConfig(cfg)

	if SessionMaxAge != 2*time.Hour {
		t.Errorf("expected SessionMaxAge=2h, got %v", SessionMaxAge)
	}

	if SessionGCInterval != 30*time.Second {
		t.Errorf("expected SessionGCInterval=30s, got %v", SessionGCInterval)
	}

	// Test nil config
	InitSessionConfig(nil)
	// Should not change values
	if SessionMaxAge != 2*time.Hour {
		t.Error("SessionMaxAge should not change with nil config")
	}
}
