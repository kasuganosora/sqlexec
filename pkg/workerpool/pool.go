// Package workerpool provides a reusable, high-performance worker pool
// for parallel task execution with dynamic scaling and graceful shutdown.
package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Common errors
var (
	ErrPoolClosed   = errors.New("workerpool: pool is closed")
	ErrPoolRunning  = errors.New("workerpool: pool is already running")
	ErrInvalidSize  = errors.New("workerpool: invalid pool size")
	ErrTaskPanic    = errors.New("workerpool: task panicked")
	ErrTaskCanceled = errors.New("workerpool: task canceled")
)

// Task represents a unit of work to be executed by the pool
type Task func(ctx context.Context) error

// Result represents the result of a task execution
type Result struct {
	Value interface{}
	Error error
}

// TaskFunc is a function that produces a result
type TaskFunc func(ctx context.Context) (interface{}, error)

// Config holds worker pool configuration
type Config struct {
	// Size is the number of workers in the pool
	Size int
	// QueueSize is the task queue buffer size (0 = unbuffered)
	QueueSize int
	// IdleTimeout is the duration after which idle workers are reduced
	IdleTimeout time.Duration
	// EnableDynamicScaling allows the pool to scale up/down based on load
	EnableDynamicScaling bool
	// MinWorkers is the minimum number of workers when dynamic scaling is enabled
	MinWorkers int
	// MaxWorkers is the maximum number of workers when dynamic scaling is enabled
	MaxWorkers int
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig() Config {
	return Config{
		Size:                 4,
		QueueSize:            100,
		IdleTimeout:          30 * time.Second,
		EnableDynamicScaling: false,
		MinWorkers:           1,
		MaxWorkers:           16,
	}
}

// Pool represents a worker pool
type Pool struct {
	config  Config
	tasks   chan taskWrapper
	results chan Result
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	running atomic.Bool
	closed  atomic.Bool
	mu      sync.RWMutex
	workers int32
	taskCnt int64
	errCnt  int64
}

// taskWrapper wraps a task with its result channel
type taskWrapper struct {
	task   Task
	result chan Result
	ctx    context.Context
}

// New creates a new worker pool with the given configuration
func New(config Config) (*Pool, error) {
	if config.Size <= 0 {
		return nil, ErrInvalidSize
	}
	if config.EnableDynamicScaling {
		if config.MinWorkers <= 0 || config.MaxWorkers < config.MinWorkers {
			return nil, ErrInvalidSize
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool{
		config:  config,
		tasks:   make(chan taskWrapper, config.QueueSize),
		results: make(chan Result, config.QueueSize),
		ctx:     ctx,
		cancel:  cancel,
	}

	return p, nil
}

// NewWithSize creates a new worker pool with a specific size
func NewWithSize(size int) (*Pool, error) {
	return New(Config{Size: size})
}

// Start starts the worker pool
func (p *Pool) Start() error {
	if p.running.Load() {
		return ErrPoolRunning
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed.Load() {
		return ErrPoolClosed
	}

	// Start initial workers
	for i := 0; i < p.config.Size; i++ {
		p.startWorker()
	}

	p.running.Store(true)
	return nil
}

// startWorker starts a new worker goroutine
func (p *Pool) startWorker() {
	p.wg.Add(1)
	atomic.AddInt32(&p.workers, 1)

	go func() {
		defer p.wg.Done()
		defer atomic.AddInt32(&p.workers, -1)

		idleTimer := time.NewTimer(p.config.IdleTimeout)
		defer idleTimer.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-idleTimer.C:
				// Check if we should scale down
				if p.config.EnableDynamicScaling && p.shouldScaleDown() {
					return
				}
				idleTimer.Reset(p.config.IdleTimeout)
			case wrapper, ok := <-p.tasks:
				if !ok {
					return
				}
				p.executeTask(wrapper)
				idleTimer.Reset(p.config.IdleTimeout)
			}
		}
	}()
}

// shouldScaleDown checks if a worker should exit due to low load
func (p *Pool) shouldScaleDown() bool {
	workers := atomic.LoadInt32(&p.workers)
	return workers > int32(p.config.MinWorkers)
}

// executeTask executes a task and sends the result
func (p *Pool) executeTask(wrapper taskWrapper) {
	atomic.AddInt64(&p.taskCnt, 1)

	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&p.errCnt, 1)
			p.sendResult(wrapper.result, Result{
				Value: nil,
				Error: ErrTaskPanic,
			})
		}
	}()

	select {
	case <-wrapper.ctx.Done():
		atomic.AddInt64(&p.errCnt, 1)
		p.sendResult(wrapper.result, Result{
			Value: nil,
			Error: ErrTaskCanceled,
		})
	default:
		err := wrapper.task(wrapper.ctx)
		if err != nil {
			atomic.AddInt64(&p.errCnt, 1)
		}
		p.sendResult(wrapper.result, Result{
			Value: nil,
			Error: err,
		})
	}
}

// sendResult safely sends a result to the result channel
func (p *Pool) sendResult(ch chan Result, result Result) {
	if ch == nil {
		return
	}
	select {
	case ch <- result:
	case <-p.ctx.Done():
	}
}

// Submit submits a task to the pool and returns a channel for the result
func (p *Pool) Submit(ctx context.Context, task Task) (<-chan Result, error) {
	if !p.running.Load() || p.closed.Load() {
		return nil, ErrPoolClosed
	}

	resultCh := make(chan Result, 1)
	wrapper := taskWrapper{
		task:   task,
		result: resultCh,
		ctx:    ctx,
	}

	select {
	case p.tasks <- wrapper:
		// Possibly scale up
		if p.config.EnableDynamicScaling {
			go p.maybeScaleUp()
		}
		return resultCh, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, ErrPoolClosed
	}
}

// SubmitWait submits a task and waits for the result
func (p *Pool) SubmitWait(ctx context.Context, task Task) error {
	resultCh, err := p.Submit(ctx, task)
	if err != nil {
		return err
	}

	select {
	case result := <-resultCh:
		return result.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubmitFunc submits a function that returns a value
func (p *Pool) SubmitFunc(ctx context.Context, fn TaskFunc) (<-chan Result, error) {
	if !p.running.Load() || p.closed.Load() {
		return nil, ErrPoolClosed
	}

	resultCh := make(chan Result, 1)
	wrapper := taskWrapper{
		task: func(ctx context.Context) error {
			val, err := fn(ctx)
			select {
			case resultCh <- Result{Value: val, Error: err}:
			default:
			}
			return err
		},
		result: nil, // result is sent directly to resultCh by the task
		ctx:    ctx,
	}

	select {
	case p.tasks <- wrapper:
		if p.config.EnableDynamicScaling {
			go p.maybeScaleUp()
		}
		return resultCh, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, ErrPoolClosed
	}
}

// SubmitBatch submits multiple tasks and returns a channel for all results
func (p *Pool) SubmitBatch(ctx context.Context, tasks []Task) (<-chan Result, error) {
	if !p.running.Load() || p.closed.Load() {
		return nil, ErrPoolClosed
	}

	results := make(chan Result, len(tasks))

	go func() {
		var wg sync.WaitGroup
		wg.Add(len(tasks))

		for _, task := range tasks {
			go func(t Task) {
				defer wg.Done()
				resultCh, err := p.Submit(ctx, t)
				if err != nil {
					results <- Result{Error: err}
					return
				}
				select {
				case result := <-resultCh:
					results <- result
				case <-ctx.Done():
					results <- Result{Error: ctx.Err()}
				}
			}(task)
		}

		wg.Wait()
		close(results)
	}()

	return results, nil
}

// maybeScaleUp checks if the pool should add more workers
func (p *Pool) maybeScaleUp() {
	if !p.config.EnableDynamicScaling {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	currentWorkers := atomic.LoadInt32(&p.workers)
	if int(currentWorkers) < p.config.MaxWorkers && len(p.tasks) > 0 {
		p.startWorker()
	}
}

// Close gracefully shuts down the worker pool
func (p *Pool) Close() error {
	if p.closed.Swap(true) {
		return nil
	}

	p.running.Store(false)
	p.cancel()
	close(p.tasks)
	p.wg.Wait()
	close(p.results)

	return nil
}

// CloseWithTimeout shuts down the pool with a timeout
func (p *Pool) CloseWithTimeout(timeout time.Duration) error {
	if p.closed.Swap(true) {
		return nil
	}

	p.running.Store(false)
	p.cancel()
	close(p.tasks)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(p.results)
		return nil
	case <-time.After(timeout):
		close(p.results)
		return context.DeadlineExceeded
	}
}

// Stats returns current pool statistics
func (p *Pool) Stats() Stats {
	return Stats{
		Workers:       int(atomic.LoadInt32(&p.workers)),
		TasksExecuted: atomic.LoadInt64(&p.taskCnt),
		TasksFailed:   atomic.LoadInt64(&p.errCnt),
		QueueSize:     len(p.tasks),
		MaxQueueSize:  p.config.QueueSize,
		IsRunning:     p.running.Load(),
		IsClosed:      p.closed.Load(),
	}
}

// Stats holds pool statistics
type Stats struct {
	Workers       int
	TasksExecuted int64
	TasksFailed   int64
	QueueSize     int
	MaxQueueSize  int
	IsRunning     bool
	IsClosed      bool
}

// IsRunning returns true if the pool is running
func (p *Pool) IsRunning() bool {
	return p.running.Load()
}

// IsClosed returns true if the pool is closed
func (p *Pool) IsClosed() bool {
	return p.closed.Load()
}

// WorkerCount returns the current number of workers
func (p *Pool) WorkerCount() int {
	return int(atomic.LoadInt32(&p.workers))
}

// QueueLen returns the current queue length
func (p *Pool) QueueLen() int {
	return len(p.tasks)
}

// Results returns the results channel
func (p *Pool) Results() <-chan Result {
	return p.results
}
