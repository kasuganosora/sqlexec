package session

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// 全局查询注册表单例
	globalQueryRegistry *QueryRegistry
	registryOnce        sync.Once
	querySequence       uint64 // 原子计数器,用于生成唯一查询ID
)

// GetProcessListForOptimizer 获取进程列表（供 optimizer 使用）
func GetProcessListForOptimizer() []interface{} {
	registry := GetGlobalQueryRegistry()
	queries := registry.GetAllQueries()

	result := make([]interface{}, 0, len(queries))
	for _, qc := range queries {
		status := qc.GetStatus()
		result = append(result, map[string]interface{}{
			"QueryID":   status.QueryID,
			"ThreadID":  status.ThreadID,
			"SQL":       status.SQL,
			"StartTime": status.StartTime,
			"Duration":  status.Duration,
			"Status":    status.Status,
			"User":      status.User,
			"Host":      status.Host,
			"DB":        status.DB,
		})
	}
	return result
}

// GetGlobalQueryRegistry 获取全局查询注册表
func GetGlobalQueryRegistry() *QueryRegistry {
	registryOnce.Do(func() {
		globalQueryRegistry = NewQueryRegistry()
	})
	return globalQueryRegistry
}

// QueryRegistry 全局查询注册表
type QueryRegistry struct {
	mu        sync.RWMutex
	queries   map[string]*QueryContext // QueryID -> QueryContext
	threadMap map[uint32]*QueryContext  // ThreadID -> 当前查询
}

// NewQueryRegistry 创建查询注册表
func NewQueryRegistry() *QueryRegistry {
	return &QueryRegistry{
		queries:   make(map[string]*QueryContext),
		threadMap: make(map[uint32]*QueryContext),
	}
}

// RegisterQuery 注册查询
func (r *QueryRegistry) RegisterQuery(qc *QueryContext) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查线程ID是否已有正在执行的查询
	if existing, ok := r.threadMap[qc.ThreadID]; ok {
		// 如果已有查询,先取消它
		existing.CancelFunc()
		delete(r.queries, existing.QueryID)
	}

	r.queries[qc.QueryID] = qc
	r.threadMap[qc.ThreadID] = qc
	return nil
}

// UnregisterQuery 注销查询
func (r *QueryRegistry) UnregisterQuery(queryID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if qc, ok := r.queries[queryID]; ok {
		// 从线程映射中删除
		if existing, ok := r.threadMap[qc.ThreadID]; ok && existing.QueryID == queryID {
			delete(r.threadMap, qc.ThreadID)
		}
		delete(r.queries, queryID)
	}
}

// GetQuery 获取查询
func (r *QueryRegistry) GetQuery(queryID string) *QueryContext {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.queries[queryID]
}

// GetQueryByThreadID 通过ThreadID获取查询
func (r *QueryRegistry) GetQueryByThreadID(threadID uint32) *QueryContext {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.threadMap[threadID]
}

// KillQueryByThreadID 通过ThreadID取消查询
func (r *QueryRegistry) KillQueryByThreadID(threadID uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	qc, ok := r.threadMap[threadID]
	if !ok {
		return fmt.Errorf("query not found for thread %d", threadID)
	}

	qc.SetCanceled()
	qc.CancelFunc()
	return nil
}

// GetAllQueries 获取所有查询
func (r *QueryRegistry) GetAllQueries() []*QueryContext {
	r.mu.RLock()
	defer r.mu.RUnlock()

	queries := make([]*QueryContext, 0, len(r.queries))
	for _, qc := range r.queries {
		queries = append(queries, qc)
	}
	return queries
}

// GetQueryCount 获取查询数量
func (r *QueryRegistry) GetQueryCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.queries)
}

// GenerateQueryID 生成查询ID
func GenerateQueryID(threadID uint32) string {
	timestamp := time.Now().UnixNano()
	// 原子递增序列号
	seq := atomic.AddUint64(&querySequence, 1)
	return fmt.Sprintf("%d_%d_%d", threadID, timestamp, seq)
}

// KillQueryByThreadID 通过ThreadID取消查询(全局)
func KillQueryByThreadID(threadID uint32) error {
	return GetGlobalQueryRegistry().KillQueryByThreadID(threadID)
}

// GetQueryByThreadID 通过ThreadID获取查询(全局)
func GetQueryByThreadID(threadID uint32) *QueryContext {
	return GetGlobalQueryRegistry().GetQueryByThreadID(threadID)
}

// GetAllQueries 获取所有查询(全局)
func GetAllQueries() []*QueryContext {
	return GetGlobalQueryRegistry().GetAllQueries()
}
