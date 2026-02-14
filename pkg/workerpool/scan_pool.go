package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ScanTask represents a data scanning task with partition info
type ScanTask struct {
	ID         int
	StartIndex int
	EndIndex   int
	Data       interface{}
}

// ScanResult represents the result of a scan task
type ScanResult struct {
	TaskID int
	Items  []interface{}
	Error  error
}

// ScanFunc is the function type for processing a scan task
type ScanFunc func(ctx context.Context, task ScanTask) (ScanResult, error)

// ScanPool is a specialized pool for parallel scanning operations
type ScanPool struct {
	pool     *Pool
	scanFunc ScanFunc
}

// NewScanPool creates a new scan pool
func NewScanPool(size int, scanFunc ScanFunc) (*ScanPool, error) {
	pool, err := New(Config{
		Size:                 size,
		QueueSize:            size * 2,
		IdleTimeout:          30 * 1e9, // 30 seconds
		EnableDynamicScaling: false,
	})
	if err != nil {
		return nil, err
	}

	sp := &ScanPool{
		pool:     pool,
		scanFunc: scanFunc,
	}

	return sp, nil
}

// Start starts the scan pool
func (sp *ScanPool) Start() error {
	return sp.pool.Start()
}

// ExecuteParallel executes scan tasks in parallel and collects results
func (sp *ScanPool) ExecuteParallel(ctx context.Context, tasks []ScanTask) ([]ScanResult, error) {
	if sp.pool.IsClosed() || !sp.pool.IsRunning() {
		return nil, ErrPoolClosed
	}

	results := make([]ScanResult, len(tasks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errCount int64
	var panicCount int64

	wg.Add(len(tasks))

	for i, task := range tasks {
		task := task
		idx := i

		go func() {
			defer wg.Done()

			// Recover from panics
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&panicCount, 1)
					mu.Lock()
					results[idx] = ScanResult{
						TaskID: task.ID,
						Error:  ErrTaskPanic,
					}
					mu.Unlock()
				}
			}()

			result, err := sp.scanFunc(ctx, task)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				mu.Lock()
				results[idx] = ScanResult{
					TaskID: task.ID,
					Error:  err,
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			results[idx] = result
			mu.Unlock()
		}()
	}

	wg.Wait()

	if panicCount > 0 {
		return results, ErrTaskPanic
	}
	if errCount > 0 {
		return results, errors.New("workerpool: one or more scan tasks failed")
	}

	return results, nil
}

// ExecuteParallelWithPool uses the worker pool to execute tasks
func (sp *ScanPool) ExecuteParallelWithPool(ctx context.Context, tasks []ScanTask) ([]ScanResult, error) {
	if sp.pool.IsClosed() || !sp.pool.IsRunning() {
		return nil, ErrPoolClosed
	}

	results := make([]ScanResult, len(tasks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errCount int64

	for i, task := range tasks {
		task := task
		idx := i

		wg.Add(1)

		go func() {
			defer wg.Done()

			err := sp.pool.SubmitWait(ctx, func(ctx context.Context) error {
				result, err := sp.scanFunc(ctx, task)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					mu.Lock()
					results[idx] = ScanResult{
						TaskID: task.ID,
						Error:  err,
					}
					mu.Unlock()
					return err
				}

				mu.Lock()
				results[idx] = result
				mu.Unlock()
				return nil
			})

			if err != nil {
				atomic.AddInt64(&errCount, 1)
				mu.Lock()
				results[idx] = ScanResult{
					TaskID: task.ID,
					Error:  err,
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if errCount > 0 {
		return results, errors.New("workerpool: one or more scan tasks failed")
	}

	return results, nil
}

// Close closes the scan pool
func (sp *ScanPool) Close() error {
	return sp.pool.Close()
}

// Stats returns pool statistics
func (sp *ScanPool) Stats() Stats {
	return sp.pool.Stats()
}
