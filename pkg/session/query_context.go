package session

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// QueryContext 查询上下文,用于追踪和控制查询执行
type QueryContext struct {
	QueryID    string        // 查询唯一ID (生成格式: {ThreadID}_{timestamp}_{sequence})
	ThreadID   uint32        // 关联的线程ID
	SQL        string        // 执行的SQL
	StartTime  time.Time     // 开始时间
	CancelFunc context.CancelFunc // 取消函数
	User       string        // 执行该查询的用户
	Host       string        // 客户端主机地址 (格式: host:port)
	DB         string        // 当前使用的数据库
	mu         sync.RWMutex
	canceled   bool
	timeout    bool
}

// IsCanceled 检查查询是否被取消
func (qc *QueryContext) IsCanceled() bool {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.canceled
}

// IsTimeout 检查查询是否超时
func (qc *QueryContext) IsTimeout() bool {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.timeout
}

// SetCanceled 标记查询被取消
func (qc *QueryContext) SetCanceled() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.canceled = true
}

// SetTimeout 标记查询超时
func (qc *QueryContext) SetTimeout() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.timeout = true
}

// GetDuration 获取查询执行时长
func (qc *QueryContext) GetDuration() time.Duration {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return time.Since(qc.StartTime)
}

// QueryStatus 查询状态
type QueryStatus struct {
	QueryID   string
	ThreadID  uint32
	SQL       string
	StartTime time.Time
	Duration  time.Duration
	Status    string // "running", "canceled", "timeout", "completed"
	User      string
	Host      string
	DB        string
}

// GetStatus 获取查询状态
func (qc *QueryContext) GetStatus() QueryStatus {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	status := "running"
	if qc.canceled {
		status = "canceled"
	} else if qc.timeout {
		status = "timeout"
	}

	return QueryStatus{
		QueryID:   qc.QueryID,
		ThreadID:  qc.ThreadID,
		SQL:       qc.SQL,
		StartTime: qc.StartTime,
		Duration:  time.Since(qc.StartTime),
		Status:    status,
		User:      qc.User,
		Host:      qc.Host,
		DB:        qc.DB,
	}
}

// QueryContextManager 查询上下文管理器
type QueryContextManager struct {
	mu        sync.RWMutex
	queries   map[string]*QueryContext // QueryID -> QueryContext
	threadMap map[uint32]*QueryContext  // ThreadID -> 当前查询
}

// NewQueryContextManager 创建查询上下文管理器
func NewQueryContextManager() *QueryContextManager {
	return &QueryContextManager{
		queries:   make(map[string]*QueryContext),
		threadMap: make(map[uint32]*QueryContext),
	}
}

// RegisterQuery 注册查询
func (m *QueryContextManager) RegisterQuery(qc *QueryContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查线程ID是否已有正在执行的查询
	if existing, ok := m.threadMap[qc.ThreadID]; ok {
		// 如果已有查询,先取消它
		existing.CancelFunc()
		delete(m.queries, existing.QueryID)
	}

	m.queries[qc.QueryID] = qc
	m.threadMap[qc.ThreadID] = qc
	return nil
}

// UnregisterQuery 注销查询
func (m *QueryContextManager) UnregisterQuery(queryID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if qc, ok := m.queries[queryID]; ok {
		// 从线程映射中删除
		if existing, ok := m.threadMap[qc.ThreadID]; ok && existing.QueryID == queryID {
			delete(m.threadMap, qc.ThreadID)
		}
		delete(m.queries, queryID)
	}
}

// GetQuery 获取查询
func (m *QueryContextManager) GetQuery(queryID string) *QueryContext {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queries[queryID]
}

// GetQueryByThreadID 通过ThreadID获取查询
func (m *QueryContextManager) GetQueryByThreadID(threadID uint32) *QueryContext {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.threadMap[threadID]
}

// KillQueryByThreadID 通过ThreadID取消查询
func (m *QueryContextManager) KillQueryByThreadID(threadID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	qc, ok := m.threadMap[threadID]
	if !ok {
		return fmt.Errorf("query not found for thread %d", threadID)
	}

	qc.SetCanceled()
	qc.CancelFunc()
	return nil
}

// GetAllQueries 获取所有查询
func (m *QueryContextManager) GetAllQueries() []*QueryContext {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queries := make([]*QueryContext, 0, len(m.queries))
	for _, qc := range m.queries {
		queries = append(queries, qc)
	}
	return queries
}

// GetQueryCount 获取查询数量
func (m *QueryContextManager) GetQueryCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.queries)
}
