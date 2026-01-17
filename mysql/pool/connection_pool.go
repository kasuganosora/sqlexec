package pool

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// ConnectionPool 连接池
type ConnectionPool struct {
	connections  chan *sql.Conn
	factory     func() (*sql.Conn, error)
	destroy     func(*sql.Conn) error
	maxSize     int
	minIdle     int
	currentSize int
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
}

// NewConnectionPool 创建连接池
func NewConnectionPool(ctx context.Context, factory func() (*sql.Conn, error), maxSize int) *ConnectionPool {
	poolCtx, cancel := context.WithCancel(ctx)

	return &ConnectionPool{
		connections: make(chan *sql.Conn, maxSize),
		factory:     factory,
		destroy:     func(conn *sql.Conn) error { return conn.Close() },
		maxSize:     maxSize,
		minIdle:     2,
		ctx:         poolCtx,
		cancel:      cancel,
	}
}

// Get 获取连接
func (p *ConnectionPool) Get(ctx context.Context) (*sql.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	// 尝试从池中获取
	select {
	case conn := <-p.connections:
		// 验证连接是否有效
		if err := conn.PingContext(ctx); err == nil {
			p.mu.Unlock()
			return conn, nil
		}
		// 连接无效，关闭并创建新的
		p.destroy(conn)
		p.currentSize--
	default:
		// 池中没有连接
	}

	p.mu.Unlock()

	// 创建新连接
	conn, err := p.createConnection(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Put 归还连接
func (p *ConnectionPool) Put(conn *sql.Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return p.destroy(conn)
	}

	// 检查连接是否有效
	if err := conn.PingContext(context.Background()); err != nil {
		p.destroy(conn)
		p.currentSize--
		return nil
	}

	// 放回池中
	select {
	case p.connections <- conn:
		return nil
	default:
		// 池已满，关闭连接
		return p.destroy(conn)
	}
}

// createConnection 创建新连接
func (p *ConnectionPool) createConnection(ctx context.Context) (*sql.Conn, error) {
	p.mu.Lock()

	if p.currentSize >= p.maxSize {
		p.mu.Unlock()
		// 等待连接释放
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case conn := <-p.connections:
			p.mu.Lock()
			if err := conn.PingContext(ctx); err != nil {
				p.destroy(conn)
				p.currentSize--
				p.mu.Unlock()
				return p.createConnection(ctx)
			}
			p.mu.Unlock()
			return conn, nil
		}
	}

	p.mu.Unlock()

	// 创建新连接
	conn, err := p.factory()
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	p.mu.Lock()
	p.currentSize++
	p.mu.Unlock()

	return conn, nil
}

// Stats 获取连接池统计
func (p *ConnectionPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return PoolStats{
		CurrentSize: p.currentSize,
		MaxSize:     p.maxSize,
		IdleCount:   len(p.connections),
		ActiveCount:  p.currentSize - len(p.connections),
	}
}

// Close 关闭连接池
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	p.cancel()

	// 关闭所有连接
	close(p.connections)
	for conn := range p.connections {
		p.destroy(conn)
		p.currentSize--
	}

	return nil
}

// SQLConnectionPool SQL连接池（基于database/sql）
type SQLConnectionPool struct {
	db      *sql.DB
	maxOpen int
	maxIdle int
}

// NewSQLConnectionPool 创建SQL连接池
func NewSQLConnectionPool(db *sql.DB, maxOpen, maxIdle int) *SQLConnectionPool {
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(time.Minute * 30)

	return &SQLConnectionPool{
		db:      db,
		maxOpen: maxOpen,
		maxIdle: maxIdle,
	}
}

// GetDB 获取数据库连接
func (p *SQLConnectionPool) GetDB() *sql.DB {
	return p.db
}

// Stats 获取连接池统计
func (p *SQLConnectionPool) Stats() sql.DBStats {
	return p.db.Stats()
}

// Close 关闭连接池
func (p *SQLConnectionPool) Close() error {
	return p.db.Close()
}

// ConnectionManager 连接管理器
type ConnectionManager struct {
	pools map[string]*SQLConnectionPool
	mu    sync.RWMutex
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		pools: make(map[string]*SQLConnectionPool),
	}
}

// RegisterPool 注册连接池
func (m *ConnectionManager) RegisterPool(name string, db *sql.DB, maxOpen, maxIdle int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pools[name] = NewSQLConnectionPool(db, maxOpen, maxIdle)
}

// GetPool 获取连接池
func (m *ConnectionManager) GetPool(name string) (*SQLConnectionPool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pool, ok := m.pools[name]
	return pool, ok
}

// Close 关闭所有连接池
func (m *ConnectionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for _, pool := range m.pools {
		if err := pool.Close(); err != nil {
			lastErr = err
		}
	}
	m.pools = make(map[string]*SQLConnectionPool)

	return lastErr
}
