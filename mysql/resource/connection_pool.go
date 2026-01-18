package resource

import (
	"container/list"
	"database/sql"
	"sync"
	"time"
)

// ==================== 连接池 ====================

// ConnectionWrapper 连接包装器
type ConnectionWrapper struct {
	conn      *sql.DB
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
}

// ==================== 连接池 ====================

// ConnectionPool 连接池
type ConnectionPool struct {
	maxOpen     int
	maxIdle     int
	lifetime    time.Duration
	idleTimeout time.Duration
	connections *list.List
	mu          sync.RWMutex
	metrics      *PoolMetrics
}

// PoolMetrics 池指标
type PoolMetrics struct {
	Created     int64
	Destroyed   int64
	Acquired   int64
	Released   int64
	Errors      int64
	mu          sync.RWMutex
}

// NewConnectionPool 创建连接池
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		maxOpen:     10,
		maxIdle:     5,
		lifetime:    30 * time.Minute,
		idleTimeout: 5 * time.Minute,
		connections: list.New(),
		metrics:      &PoolMetrics{},
	}
}

// SetMaxOpenConns 设置最大连接数
func (p *ConnectionPool) SetMaxOpenConns(max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxOpen = max
}

// SetMaxIdleConns 设置最大空闲连接数
func (p *ConnectionPool) SetMaxIdleConns(max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxIdle = max
}

// SetConnMaxLifetime 设置连接最大生命周期
func (p *ConnectionPool) SetConnMaxLifetime(lifetime time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lifetime = lifetime
}

// SetIdleTimeout 设置空闲超时
func (p *ConnectionPool) SetIdleTimeout(timeout time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.idleTimeout = timeout
}

// Get 获取连接
func (p *ConnectionPool) Get() (*sql.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查连接数
	if p.connections.Len() >= p.maxOpen {
		p.metrics.IncrementErrors()
		return nil, &PoolError{Message: "connection pool exhausted"}
	}

	// 查找空闲连接
	for e := p.connections.Front(); e != nil; e = e.Next() {
		conn, ok := e.Value.(*ConnectionWrapper)
		if ok && !conn.inUse {
			// 检查连接是否过期
			if time.Since(conn.lastUsed) > p.idleTimeout {
				p.connections.Remove(e)
				p.metrics.DecrementDestroyed()
				continue
			}

			// 检查连接生命周期
			if time.Since(conn.createdAt) > p.lifetime {
				p.connections.Remove(e)
				p.metrics.DecrementDestroyed()
				continue
			}

			// 重用连接
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.metrics.IncrementAcquired()
			return conn.conn, nil
		}
	}

	// 没有空闲连接，需要在实际使用中由调用者提供新连接
	p.metrics.IncrementCreated()
	return nil, nil
}

// Release 释放连接
func (p *ConnectionPool) Release(conn *sql.DB) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 查找连接
	for e := p.connections.Front(); e != nil; e = e.Next() {
		wrapper, ok := e.Value.(*ConnectionWrapper)
		if ok && wrapper.conn == conn {
			wrapper.inUse = false
			wrapper.lastUsed = time.Now()
			p.metrics.IncrementReleased()
			return
		}
	}
}

// Close 关闭连接池
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for e := p.connections.Front(); e != nil; e = e.Next() {
		wrapper, ok := e.Value.(*ConnectionWrapper)
		if ok {
			p.connections.Remove(e)
			// 注意：这里不关闭实际的数据库连接，因为它由外部管理
			p.metrics.IncrementDestroyed()
		}
	}

	return nil
}

// GetMetrics 获取池指标
func (p *ConnectionPool) GetMetrics() *PoolMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metrics
}

// Stats 获取统计信息
func (p *ConnectionPool) Stats() map[string]interface{} {
	metrics := p.GetMetrics()
	return map[string]interface{}{
		"max_open":       p.maxOpen,
		"max_idle":       p.maxIdle,
		"current_open":    p.connections.Len(),
		"total_created":   metrics.GetCreated(),
		"total_destroyed": metrics.GetDestroyed(),
		"total_acquired":  metrics.GetAcquired(),
		"total_released":  metrics.GetReleased(),
		"total_errors":    metrics.GetErrors(),
	}
}

// ==================== 池错误 ====================

// PoolError 池错误
type PoolError struct {
	Message string
}

// Error 实现error接口
func (e *PoolError) Error() string {
	return e.Message
}

// ==================== 池指标 ====================

// IncrementCreated 增加创建计数
func (m *PoolMetrics) IncrementCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Created++
}

// DecrementCreated 减少创建计数
func (m *PoolMetrics) DecrementCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Created > 0 {
		m.Created--
	}
}

// IncrementDestroyed 增加销毁计数
func (m *PoolMetrics) IncrementDestroyed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Destroyed++
}

// DecrementDestroyed 减少销毁计数
func (m *PoolMetrics) DecrementDestroyed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Destroyed > 0 {
		m.Destroyed--
	}
}

// IncrementAcquired 增加获取计数
func (m *PoolMetrics) IncrementAcquired() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Acquired++
}

// IncrementReleased 增加释放计数
func (m *PoolMetrics) IncrementReleased() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Released++
}

// IncrementErrors 增加错误计数
func (m *PoolMetrics) IncrementErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Errors++
}

// GetCreated 获取创建计数
func (m *PoolMetrics) GetCreated() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Created
}

// GetDestroyed 获取销毁计数
func (m *PoolMetrics) GetDestroyed() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Destroyed
}

// GetAcquired 获取获取计数
func (m *PoolMetrics) GetAcquired() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Acquired
}

// GetReleased 获取释放计数
func (m *PoolMetrics) GetReleased() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Released
}

// GetErrors 获取错误计数
func (m *PoolMetrics) GetErrors() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Errors
}
