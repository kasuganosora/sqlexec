package api

import (
	"sync"

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
}
