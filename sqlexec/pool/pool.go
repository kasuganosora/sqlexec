package pool

import (
	"context"
	"sync"
	"time"
)

// Pool 通用对象池接口
type Pool interface {
	Get(ctx context.Context) (interface{}, error)
	Put(obj interface{}) error
	Close() error
	Stats() PoolStats
}

// PoolStats 池统计信息
type PoolStats struct {
	TotalCreated   int64
	TotalAcquired  int64
	TotalReleased  int64
	CurrentSize    int
	MaxSize        int
	IdleCount      int
	ActiveCount    int
	WaitCount      int64
	WaitDuration   time.Duration
}

// ObjectPool 对象池实现
type ObjectPool struct {
	factory     func() (interface{}, error)
	destroy     func(interface{}) error
	idle        []interface{}
	active      map[interface{}]struct{}
	mu          sync.RWMutex
	maxSize     int
	minIdle     int
	maxIdle     int
	createCount int64
	acquireCount int64
	releaseCount int64
	waitCount   int64
	closed      bool
}

// NewObjectPool 创建对象池
func NewObjectPool(factory func() (interface{}, error), destroy func(interface{}) error, maxSize int) *ObjectPool {
	return &ObjectPool{
		factory: factory,
		destroy: destroy,
		idle:    make([]interface{}, 0, maxSize),
		active:  make(map[interface{}]struct{}),
		maxSize: maxSize,
		minIdle: 2,
		maxIdle: maxSize / 2,
	}
}

// Get 获取对象
func (p *ObjectPool) Get(ctx context.Context) (interface{}, error) {
	p.mu.Lock()

	// 检查池是否已关闭
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	// 尝试从空闲队列获取
	if len(p.idle) > 0 {
		obj := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.active[obj] = struct{}{}
		p.acquireCount++
		p.mu.Unlock()
		return obj, nil
	}

	// 检查是否达到最大限制
	if len(p.active) >= p.maxSize {
		// 等待对象释放
		p.waitCount++
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond * 10):
			// 超时重试
			return p.Get(ctx)
		}
	}

	// 创建新对象
	p.mu.Unlock()
	obj, err := p.factory()
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.active[obj] = struct{}{}
	p.createCount++
	p.acquireCount++
	p.mu.Unlock()

	return obj, nil
}

// Put 归还对象
func (p *ObjectPool) Put(obj interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	// 从活跃集合移除
	delete(p.active, obj)
	p.releaseCount++

	// 检查空闲队列是否已满
	if len(p.idle) >= p.maxIdle {
		// 销毁对象
		if p.destroy != nil {
			p.destroy(obj)
		}
		return nil
	}

	// 放回空闲队列
	p.idle = append(p.idle, obj)
	return nil
}

// Stats 获取池统计信息
func (p *ObjectPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return PoolStats{
		TotalCreated:  p.createCount,
		TotalAcquired: p.acquireCount,
		TotalReleased: p.releaseCount,
		CurrentSize:   len(p.idle) + len(p.active),
		MaxSize:       p.maxSize,
		IdleCount:     len(p.idle),
		ActiveCount:   len(p.active),
		WaitCount:     p.waitCount,
	}
}

// Close 关闭池
func (p *ObjectPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	// 销毁所有空闲对象
	for _, obj := range p.idle {
		if p.destroy != nil {
			p.destroy(obj)
		}
	}
	p.idle = nil

	// 活跃对象将在使用完后自动清理
	return nil
}

// GoroutinePool goroutine池
type GoroutinePool struct {
	workerChan  chan func()
	taskQueue   chan func()
	workerCount int
	maxWorkers  int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewGoroutinePool 创建goroutine池
func NewGoroutinePool(maxWorkers int, queueSize int) *GoroutinePool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &GoroutinePool{
		workerChan: make(chan func(), maxWorkers),
		taskQueue:  make(chan func(), queueSize),
		maxWorkers: maxWorkers,
		ctx:        ctx,
		cancel:     cancel,
	}

	// 启动worker
	for i := 0; i < maxWorkers; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
		pool.workerCount++
	}

	return pool
}

// Submit 提交任务
func (p *GoroutinePool) Submit(task func()) error {
	select {
	case p.taskQueue <- task:
		return nil
	case <-p.ctx.Done():
		return ErrPoolClosed
	}
}

// worker worker协程
func (p *GoroutinePool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case task := <-p.taskQueue:
			task()
		case <-p.ctx.Done():
			return
		}
	}
}

// Stats 获取池统计
func (p *GoroutinePool) Stats() PoolStats {
	return PoolStats{
		MaxSize:    p.maxWorkers,
		ActiveCount: p.workerCount,
	}
}

// Close 关闭池
func (p *GoroutinePool) Close() error {
	p.cancel()
	p.wg.Wait()
	close(p.taskQueue)
	return nil
}

// RetryPool 重试池
type RetryPool struct {
	maxRetries int
	retryDelay time.Duration
}

// NewRetryPool 创建重试池
func NewRetryPool(maxRetries int, retryDelay time.Duration) *RetryPool {
	return &RetryPool{
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

// Execute 执行带重试的任务
func (p *RetryPool) Execute(ctx context.Context, task func() error) error {
	var lastErr error

	for i := 0; i <= p.maxRetries; i++ {
		err := task()
		if err == nil {
			return nil
		}

		lastErr = err

		// 检查是否应该重试
		if i < p.maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.retryDelay):
				continue
			}
		}
	}

	return lastErr
}

// 错误定义
var (
	ErrPoolClosed = &PoolError{Message: "pool is closed"}
	ErrPoolEmpty = &PoolError{Message: "pool is empty"}
)

// PoolError 池错误
type PoolError struct {
	Message string
}

func (e *PoolError) Error() string {
	return e.Message
}


