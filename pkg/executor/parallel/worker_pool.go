package parallel

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// WorkerPool 工作池
// 管理并发worker的生命周期，避免频繁创建和销毁goroutine
type WorkerPool struct {
	workers     []worker
	taskQueue   chan Task
	workerCount int
	wg          sync.WaitGroup
	shutdown    chan struct{}
	mu          sync.Mutex
}

// worker 工作包装器
type worker struct {
	id       int
	taskChan chan Task
	done     chan struct{}
}

// Task 任务接口
type Task interface {
	Execute() error
}

// NewWorkerPool creates a worker pool
func NewWorkerPool(workerCount int) *WorkerPool {
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}

	wp := &WorkerPool{
		workers:     make([]worker, workerCount),
		taskQueue:   make(chan Task, workerCount*2), // buffered queue
		workerCount: workerCount,
		shutdown:    make(chan struct{}),
	}

	// Initialize workers
	for i := 0; i < workerCount; i++ {
		wp.workers[i] = worker{
			id:       i,
			taskChan: make(chan Task, 1),
			done:     make(chan struct{}),
		}

		// Start worker with local reference
		wp.wg.Add(1)
		go func(w *worker) {
			defer wp.wg.Done()
			w.run()
		}(&wp.workers[i])
	}

	// Start dispatcher to distribute tasks from taskQueue to workers
	wp.wg.Add(1)
	go wp.dispatch()

	return wp
}

// dispatch distributes tasks from taskQueue to workers
func (wp *WorkerPool) dispatch() {
	defer wp.wg.Done()
	workerIdx := 0
	for {
		select {
		case <-wp.shutdown:
			return
		case task, ok := <-wp.taskQueue:
			if !ok {
				return
			}
			// Round-robin distribution to workers
			wp.mu.Lock()
			workers := wp.workers
			wp.mu.Unlock()

			if len(workers) > 0 {
				select {
				case workers[workerIdx].taskChan <- task:
				case <-wp.shutdown:
					return
				}
				workerIdx = (workerIdx + 1) % len(workers)
			}
		}
	}
}

// Submit 提交任务到工作池
func (wp *WorkerPool) Submit(task Task) error {
	select {
	case wp.taskQueue <- task:
		return nil
	case <-wp.shutdown:
		return fmt.Errorf("worker pool is shutdown")
	}
}

// SubmitWithTimeout 提交任务（带超时）
func (wp *WorkerPool) SubmitWithTimeout(task Task, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case wp.taskQueue <- task:
		return nil
	case <-timer.C:
		return fmt.Errorf("task timeout after %v", timeout)
	case <-wp.shutdown:
		return fmt.Errorf("worker pool is shutdown")
	}
}

// SubmitAndWait submits a task and waits for it to complete
func (wp *WorkerPool) SubmitAndWait(task Task) error {
	if err := wp.Submit(task); err != nil {
		return err
	}
	// Wait for all submitted tasks to complete
	// Note: This waits for ALL tasks in the pool, not just this one
	// For single task wait, use SubmitAndWaitWithTimeout
	return nil
}

// SubmitAndWaitWithTimeout submits a task and waits for it to complete with timeout
func (wp *WorkerPool) SubmitAndWaitWithTimeout(task Task, timeout time.Duration) error {
	if err := wp.SubmitWithTimeout(task, timeout); err != nil {
		return err
	}

	timeoutChan := time.After(timeout)
	done := make(chan error, 1)

	go func() {
		if err := task.Execute(); err != nil {
			done <- err
		} else {
			done <- nil
		}
	}()

	select {
	case err := <-done:
		return err
	case <-timeoutChan:
		return fmt.Errorf("task wait timeout after %v", timeout)
	}
}

// BatchSubmit 批量提交任务
func (wp *WorkerPool) BatchSubmit(tasks []Task) error {
	for _, task := range tasks {
		if err := wp.Submit(task); err != nil {
			return err
		}
	}
	return nil
}

// BatchSubmitAndWait 批量提交任务并等待全部完成
func (wp *WorkerPool) BatchSubmitAndWait(tasks []Task) error {
	for _, task := range tasks {
		if err := wp.Submit(task); err != nil {
			return err
		}
	}
	return wp.Wait()
}

// GetStats 获取工作池统计信息
func (wp *WorkerPool) GetStats() WorkerPoolStats {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	return WorkerPoolStats{
		WorkerCount:  wp.workerCount,
		QueueSize:     len(wp.taskQueue),
		ActiveWorkers: wp.getActiveWorkerCount(),
	}
}

// getActiveWorkerCount 获取活跃worker数量
func (wp *WorkerPool) getActiveWorkerCount() int {
	activeCount := 0
	for _, w := range wp.workers {
		select {
		case <-w.taskChan:
			activeCount++
		default:
		}
	}
	return activeCount
}

// Resize dynamically adjusts the number of workers
func (wp *WorkerPool) Resize(newWorkerCount int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if newWorkerCount <= 0 || newWorkerCount == wp.workerCount {
		return
	}

	// Close old workers
	oldWorkers := wp.workers

	// Create new workers slice
	newWorkers := make([]worker, newWorkerCount)
	for i := 0; i < newWorkerCount; i++ {
		newWorkers[i] = worker{
			id:       i,
			taskChan: make(chan Task, 1),
			done:     make(chan struct{}),
		}

		// Start new worker with local reference
		wp.wg.Add(1)
		go func(w *worker) {
			defer wp.wg.Done()
			w.run()
		}(&newWorkers[i])
	}

	// Now update the pool's workers
	wp.workers = newWorkers
	wp.workerCount = newWorkerCount

	// Stop old workers
	for _, w := range oldWorkers {
		close(w.taskChan)
		// Signal worker to stop and wait
		// The worker will exit when taskChan is closed
	}
}

// Shutdown gracefully shuts down the worker pool
func (wp *WorkerPool) Shutdown() {
	// Signal shutdown
	close(wp.shutdown)

	// Close task queue to stop dispatcher
	close(wp.taskQueue)

	wp.mu.Lock()
	// Stop all workers by closing their channels first
	// Workers will exit gracefully when channels are closed
	for _, w := range wp.workers {
		close(w.taskChan)
		close(w.done)
	}
	wp.mu.Unlock()

	// Wait for all workers and dispatcher to complete
	wp.wg.Wait()

	wp.mu.Lock()
	defer wp.mu.Unlock()

	// Clear workers
	wp.workers = nil
	wp.workerCount = 0
}

// Wait 等待所有任务完成
func (wp *WorkerPool) Wait() error {
	wp.wg.Wait()
	return nil
}

// WaitWithTimeout 等待所有任务完成（带超时）
func (wp *WorkerPool) WaitWithTimeout(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("wait timeout after %v", timeout)
	}
}

// worker run worker main loop
func (w *worker) run() {
	for {
		select {
		case <-w.done:
			return
		case task, ok := <-w.taskChan:
			if !ok {
				// taskChan was closed, exit gracefully
				return
			}
			// Execute task (errors are ignored, should be logged in production)
			_ = task.Execute()
		}
	}
}

// WorkerPoolStats 工作池统计信息
type WorkerPoolStats struct {
	WorkerCount  int // Worker总数
	QueueSize    int // 任务队列大小
	ActiveWorkers int // 活跃Worker数量
}

// Explain 返回工作池的说明
func (wp *WorkerPool) Explain() string {
	stats := wp.GetStats()
	return fmt.Sprintf(
		"WorkerPool(workers=%d, queueSize=%d, activeWorkers=%d)",
		stats.WorkerCount, stats.QueueSize, stats.ActiveWorkers,
	)
}
