package pool

import (
	"container/list"
	"database/sql"
	"sync"
	"time"

	errors "github.com/kasuganosora/sqlexec/pkg/resource/infrastructure/errors"
)

// safeCloseDB attempts to close a sql.DB, recovering from panics caused by
// improperly initialized (zero-value) *sql.DB instances.
func safeCloseDB(db *sql.DB) {
	if db == nil {
		return
	}
	defer func() { recover() }()
	db.Close()
}

// ==================== 连接池 ====================

// ConnectionWrapper 连接包装器
type ConnectionWrapper struct {
	conn      *sql.DB
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
}

// ConnectionPool 连接池
type ConnectionPool struct {
	maxOpen     int
	maxIdle     int
	lifetime    time.Duration
	idleTimeout time.Duration
	connections *list.List
	mu          sync.RWMutex
	metrics     *PoolMetrics
}

// PoolMetrics 池指标
type PoolMetrics struct {
	Created   int64
	Destroyed int64
	Acquired  int64
	Released  int64
	Errors    int64
	mu        sync.RWMutex
}

// NewConnectionPool 创建连接池
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		maxOpen:     10,
		maxIdle:     5,
		lifetime:    30 * time.Minute,
		idleTimeout: 5 * time.Minute,
		connections: list.New(),
		metrics:     &PoolMetrics{},
	}
}

// NewConnectionPoolWithConfig 使用配置创建连接池
func NewConnectionPoolWithConfig(maxOpen, maxIdle int, lifetime, idleTimeout time.Duration) *ConnectionPool {
	return &ConnectionPool{
		maxOpen:     maxOpen,
		maxIdle:     maxIdle,
		lifetime:    lifetime,
		idleTimeout: idleTimeout,
		connections: list.New(),
		metrics:     &PoolMetrics{},
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
		p.metrics.IncDeIncrementErrors()
		return nil, &errors.ErrPoolExhausted{Message: "connection pool exhausted"}
	}

	// 查找空闲连接
	for e := p.connections.Front(); e != nil; {
		next := e.Next() // Save next before potential Remove (Remove clears links)
		conn, ok := e.Value.(*ConnectionWrapper)
		if ok && !conn.inUse {
			// 检查连接是否过期
			if time.Since(conn.lastUsed) > p.idleTimeout {
				p.connections.Remove(e)
				safeCloseDB(conn.conn)
				p.metrics.IncDeIncrementDestroyed()
				e = next
				continue
			}

			// 检查连接生命周期
			if time.Since(conn.createdAt) > p.lifetime {
				p.connections.Remove(e)
				safeCloseDB(conn.conn)
				p.metrics.IncDeIncrementDestroyed()
				e = next
				continue
			}

			// 重用连接
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.metrics.IncDeIncrementAcquired()
			return conn.conn, nil
		}
		e = next
	}

	// 没有空闲连接，需要在实际使用中由调用者提供新连接
	p.metrics.IncDeIncrementCreated()
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
			p.metrics.IncDeIncrementReleased()
			return
		}
	}
}

// AddConnection 添加连接到池
func (p *ConnectionPool) AddConnection(conn *sql.DB) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.connections.PushBack(&ConnectionWrapper{
		conn:      conn,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		inUse:     false,
	})

	// 更新metrics
	p.metrics.IncDeIncrementCreated()
}

// Close 关闭连接池
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for e := p.connections.Front(); e != nil; {
		next := e.Next() // Save next before Remove (Remove clears links)
		if wrapper, ok := e.Value.(*ConnectionWrapper); ok {
			safeCloseDB(wrapper.conn)
			p.connections.Remove(e)
			p.metrics.IncDeIncrementDestroyed()
		}
		e = next
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
	// Snapshot pool fields under lock to avoid races with setters/mutations
	p.mu.RLock()
	maxOpen := p.maxOpen
	maxIdle := p.maxIdle
	currentOpen := p.connections.Len()
	p.mu.RUnlock()

	return map[string]interface{}{
		"max_open":        maxOpen,
		"max_idle":        maxIdle,
		"current_open":    currentOpen,
		"total_created":   p.metrics.GetCreated(),
		"total_destroyed": p.metrics.GetDestroyed(),
		"total_acquired":  p.metrics.GetAcquired(),
		"total_released":  p.metrics.GetReleased(),
		"total_errors":    p.metrics.GetErrors(),
	}
}

// ==================== 池指标 ====================

// IncDeIncrementCreated 增加创建计数
func (m *PoolMetrics) IncDeIncrementCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Created++
}

// DecDeIncrementCreated 减少创建计数
func (m *PoolMetrics) DecDeIncrementCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Created > 0 {
		m.Created--
	}
}

// IncDeIncrementDestroyed 增加销毁计数
func (m *PoolMetrics) IncDeIncrementDestroyed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Destroyed++
}

// DecDeIncrementDestroyed 减少销毁计数
func (m *PoolMetrics) DecDeIncrementDestroyed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Destroyed > 0 {
		m.Destroyed--
	}
}

// IncDeIncrementAcquired 增加获取计数
func (m *PoolMetrics) IncDeIncrementAcquired() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Acquired++
}

// IncDeIncrementReleased 增加释放计数
func (m *PoolMetrics) IncDeIncrementReleased() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Released++
}

// IncDeIncrementErrors 增加错误计数
func (m *PoolMetrics) IncDeIncrementErrors() {
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
