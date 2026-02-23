package executor

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Runtime 执行运行时
type Runtime struct {
	activeQueries map[string]*QueryContext
	mu            sync.RWMutex
}

// QueryContext 查询上下文
type QueryContext struct {
	QueryID    string
	StartTime  time.Time
	CancelFunc context.CancelFunc
	Status     string
	Progress   float64
}

// NewRuntime 创建执行运行时
func NewRuntime() *Runtime {
	return &Runtime{
		activeQueries: make(map[string]*QueryContext),
	}
}

// RegisterQuery 注册查询
func (r *Runtime) RegisterQuery(queryID string, cancelFunc context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.activeQueries[queryID] = &QueryContext{
		QueryID:    queryID,
		StartTime:  time.Now(),
		CancelFunc: cancelFunc,
		Status:     "running",
		Progress:   0.0,
	}
}

// UnregisterQuery 注销查询
func (r *Runtime) UnregisterQuery(queryID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.activeQueries, queryID)
}

// UpdateProgress 更新查询进度
func (r *Runtime) UpdateProgress(queryID string, progress float64, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx, ok := r.activeQueries[queryID]; ok {
		ctx.Progress = progress
		if status != "" {
			ctx.Status = status
		}
	}
}

// CancelQuery 取消查询
func (r *Runtime) CancelQuery(queryID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ctx, ok := r.activeQueries[queryID]
	if !ok {
		return fmt.Errorf("query not found: %s", queryID)
	}
	ctx.CancelFunc()
	return nil
}

// GetQueryStatus 获取查询状态
func (r *Runtime) GetQueryStatus(queryID string) (*QueryContext, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ctx, ok := r.activeQueries[queryID]
	if !ok {
		return nil, fmt.Errorf("query not found: %s", queryID)
	}
	// 返回副本，避免调用者竞争修改内部状态
	cp := *ctx
	return &cp, nil
}

// GetAllQueries 获取所有活跃查询
func (r *Runtime) GetAllQueries() []*QueryContext {
	r.mu.RLock()
	defer r.mu.RUnlock()
	queries := make([]*QueryContext, 0, len(r.activeQueries))
	for _, ctx := range r.activeQueries {
		cp := *ctx
		queries = append(queries, &cp)
	}
	return queries
}
