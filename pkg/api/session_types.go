package api

import (
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/session"
)

// IsolationLevel represents transaction isolation level
type IsolationLevel int

const (
	IsolationReadUncommitted IsolationLevel = iota
	IsolationReadCommitted
	IsolationRepeatableRead
	IsolationSerializable
)

func (l IsolationLevel) String() string {
	switch l {
	case IsolationReadUncommitted:
		return "READ UNCOMMITTED"
	case IsolationReadCommitted:
		return "READ COMMITTED"
	case IsolationRepeatableRead:
		return "REPEATABLE READ"
	case IsolationSerializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// SessionOptions contains configuration options for creating a session
type SessionOptions struct {
	DataSourceName string
	Isolation      IsolationLevel
	ReadOnly       bool
	CacheEnabled   bool
	QueryTimeout   time.Duration // 会话级查询超时, 覆盖DB配置
}

// Session represents a database session (like a MySQL connection)
// It is concurrent safe and can be used across multiple goroutines
type Session struct {
	db          *DB
	coreSession *session.CoreSession
	options     *SessionOptions
	cacheEnabled bool
	logger      Logger
	mu          sync.RWMutex
	err         error // Error state if session creation failed
	queryTimeout time.Duration // 实际生效的超时时间
	threadID     uint32        // 关联的线程ID (用于KILL)
}

// SetThreadID 设置线程ID (用于KILL查询)
func (s *Session) SetThreadID(threadID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threadID = threadID
	if s.coreSession != nil {
		s.coreSession.SetThreadID(threadID)
	}
}

// GetThreadID 获取线程ID
func (s *Session) GetThreadID() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.threadID
}
