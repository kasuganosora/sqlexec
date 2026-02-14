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

// NewWorkerPool 创建工作池
func NewWorkerPool(workerCount int) *WorkerPool {
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}

	wp := &WorkerPool{
		workers:     make([]worker, workerCount),
		taskQueue:   make(chan Task, workerCount*2), // 缓冲队列
		workerCount: workerCount,
		shutdown:    make(chan struct{}),
	}

	// 初始化workers
	for i := 0; i < workerCount; i++ {
		wp.workers[i] = worker{
			id:       i,
			taskChan: make(chan Task, 1),
			done:     make(chan struct{}),
		}

		// 启动worker
		wp.wg.Add(1)
		go func(idx int) {
			defer wp.wg.Done()
			wp.workers[idx].run()
		}(i)
	}

	return wp
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

// SubmitAndWait 提交任务并等待完成
func (wp *WorkerPool) SubmitAndWait(task Task) error {
	wp.wg.Add(1)
	defer wp.wg.Done()

	if err := wp.Submit(task); err != nil {
		return err
	}

	// 等待任务完成
	return nil
}

// SubmitAndWaitWithTimeout 提交任务并等待完成（带超时）
func (wp *WorkerPool) SubmitAndWaitWithTimeout(task Task, timeout time.Duration) error {
	if err := wp.SubmitWithTimeout(task, timeout); err != nil {
		return err
	}

	wp.wg.Add(1)
	defer wp.wg.Done()

	// 等待任务完成（带超时）
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

// Resize 动态调整worker数量
func (wp *WorkerPool) Resize(newWorkerCount int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if newWorkerCount <= 0 || newWorkerCount == wp.workerCount {
		return
	}

	// 关闭旧workers
	oldWorkers := wp.workers
	wp.workers = make([]worker, newWorkerCount)
	wp.workerCount = newWorkerCount

	// 创建新workers
	for i := 0; i < newWorkerCount; i++ {
		wp.workers[i] = worker{
			id:       i,
			taskChan: make(chan Task, 1),
			done:     make(chan struct{}),
		}

		// 启动新worker
		wp.wg.Add(1)
		go func(idx int) {
			defer wp.wg.Done()
			wp.workers[idx].run()
		}(i)
	}

	// 停止旧workers: close taskChan to signal, wait on done for completion
	for _, w := range oldWorkers {
		close(w.taskChan)
		<-w.done
	}
}

// Shutdown 优雅关闭工作池
func (wp *WorkerPool) Shutdown() {
	close(wp.shutdown)
	wp.wg.Wait()

	wp.mu.Lock()
	defer wp.mu.Unlock()

	// 停止所有workers
	for _, w := range wp.workers {
		close(w.taskChan)
		<-w.done
	}

	// 清空workers
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

// worker run worker主循环
func (w *worker) run() {
	defer close(w.done)
	for task := range w.taskChan {
		_ = task.Execute()
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
