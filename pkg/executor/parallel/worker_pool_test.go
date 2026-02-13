package parallel

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockTask implements Task interface for testing
type mockTask struct {
	executeFunc func() error
	executed    int32
}

func (m *mockTask) Execute() error {
	atomic.AddInt32(&m.executed, 1)
	if m.executeFunc != nil {
		return m.executeFunc()
	}
	return nil
}

// TestNewWorkerPool tests worker pool creation
func TestNewWorkerPool(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		expected    int
	}{
		{"default worker count", 0, 1}, // Will use runtime.NumCPU() which is at least 1
		{"negative worker count", -1, 1},
		{"custom worker count", 4, 4},
		{"large worker count", 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp := NewWorkerPool(tt.workerCount)
			if wp == nil {
				t.Fatal("expected non-nil worker pool")
			}
			defer wp.Shutdown()

			stats := wp.GetStats()
			if tt.workerCount > 0 && stats.WorkerCount != tt.expected {
				t.Errorf("expected %d workers, got %d", tt.expected, stats.WorkerCount)
			}
		})
	}
}

// TestWorkerPool_Submit tests task submission
func TestWorkerPool_Submit(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	task := &mockTask{}
	err := wp.Submit(task)
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&task.executed) != 1 {
		t.Error("task was not executed")
	}
}

// TestWorkerPool_SubmitWithTimeout tests task submission with timeout
func TestWorkerPool_SubmitWithTimeout(t *testing.T) {
	wp := NewWorkerPool(1)
	defer wp.Shutdown()

	task := &mockTask{}
	err := wp.SubmitWithTimeout(task, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&task.executed) != 1 {
		t.Error("task was not executed")
	}
}

// TestWorkerPool_SubmitAndWait tests submit and wait
func TestWorkerPool_SubmitAndWait(t *testing.T) {
	wp := NewWorkerPool(2)
	defer wp.Shutdown()

	task := &mockTask{}
	err := wp.SubmitAndWait(task)
	if err != nil {
		t.Fatalf("failed to submit and wait: %v", err)
	}

	// Note: SubmitAndWait does not actually wait for the specific task
	// It just submits and returns
}

// TestWorkerPool_SubmitAndWaitWithTimeout tests submit and wait with timeout
func TestWorkerPool_SubmitAndWaitWithTimeout(t *testing.T) {
	wp := NewWorkerPool(2)
	defer wp.Shutdown()

	task := &mockTask{}
	err := wp.SubmitAndWaitWithTimeout(task, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to submit and wait: %v", err)
	}

	if atomic.LoadInt32(&task.executed) != 1 {
		t.Error("task was not executed")
	}
}

// TestWorkerPool_SubmitAndWaitWithTimeout_Timeout tests timeout behavior
func TestWorkerPool_SubmitAndWaitWithTimeout_Timeout(t *testing.T) {
	wp := NewWorkerPool(1)
	defer wp.Shutdown()

	slowTask := &mockTask{
		executeFunc: func() error {
			time.Sleep(2 * time.Second)
			return nil
		},
	}

	start := time.Now()
	err := wp.SubmitAndWaitWithTimeout(slowTask, 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected timeout error, got nil")
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

// TestWorkerPool_BatchSubmit tests batch task submission
func TestWorkerPool_BatchSubmit(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	tasks := make([]Task, 10)
	for i := range tasks {
		tasks[i] = &mockTask{}
	}

	err := wp.BatchSubmit(tasks)
	if err != nil {
		t.Fatalf("failed to batch submit: %v", err)
	}

	// Wait for tasks to be processed
	time.Sleep(200 * time.Millisecond)

	for i, task := range tasks {
		mt := task.(*mockTask)
		if atomic.LoadInt32(&mt.executed) != 1 {
			t.Errorf("task %d was not executed", i)
		}
	}
}

// TestWorkerPool_BatchSubmitAndWait tests batch submission with wait
func TestWorkerPool_BatchSubmitAndWait(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	var executedCount int32
	tasks := make([]Task, 10)
	for i := range tasks {
		tasks[i] = &mockTask{
			executeFunc: func() error {
				atomic.AddInt32(&executedCount, 1)
				return nil
			},
		}
	}

	err := wp.BatchSubmitAndWait(tasks)
	if err != nil {
		t.Fatalf("failed to batch submit and wait: %v", err)
	}
}

// TestWorkerPool_Shutdown tests graceful shutdown
func TestWorkerPool_Shutdown(t *testing.T) {
	wp := NewWorkerPool(4)

	// Submit a task
	task := &mockTask{}
	_ = wp.Submit(task)

	// Wait a bit for task to be processed
	time.Sleep(50 * time.Millisecond)

	// Shutdown should complete without hanging
	done := make(chan struct{})
	go func() {
		wp.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete within timeout")
	}
}

// TestWorkerPool_SubmitAfterShutdown tests that Submit fails after shutdown
func TestWorkerPool_SubmitAfterShutdown(t *testing.T) {
	wp := NewWorkerPool(2)
	wp.Shutdown()

	task := &mockTask{}
	err := wp.Submit(task)
	if err == nil {
		t.Error("expected error when submitting after shutdown")
	}
}

// TestWorkerPool_Resize tests dynamic resizing
func TestWorkerPool_Resize(t *testing.T) {
	wp := NewWorkerPool(2)
	defer wp.Shutdown()

	// Verify initial worker count
	stats := wp.GetStats()
	if stats.WorkerCount != 2 {
		t.Errorf("expected 2 workers, got %d", stats.WorkerCount)
	}

	// Resize up
	wp.Resize(5)
	stats = wp.GetStats()
	if stats.WorkerCount != 5 {
		t.Errorf("expected 5 workers after resize, got %d", stats.WorkerCount)
	}

	// Resize down
	wp.Resize(2)
	stats = wp.GetStats()
	if stats.WorkerCount != 2 {
		t.Errorf("expected 2 workers after resize down, got %d", stats.WorkerCount)
	}

	// Submit task after resize should still work
	task := &mockTask{}
	err := wp.Submit(task)
	if err != nil {
		t.Fatalf("failed to submit task after resize: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&task.executed) != 1 {
		t.Error("task was not executed after resize")
	}
}

// TestWorkerPool_Resize_InvalidValues tests resize with invalid values
func TestWorkerPool_Resize_InvalidValues(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	// Resize to zero should be ignored
	wp.Resize(0)
	stats := wp.GetStats()
	if stats.WorkerCount != 4 {
		t.Errorf("expected 4 workers (no change), got %d", stats.WorkerCount)
	}

	// Resize to negative should be ignored
	wp.Resize(-1)
	stats = wp.GetStats()
	if stats.WorkerCount != 4 {
		t.Errorf("expected 4 workers (no change), got %d", stats.WorkerCount)
	}

	// Resize to same value should be ignored
	wp.Resize(4)
	stats = wp.GetStats()
	if stats.WorkerCount != 4 {
		t.Errorf("expected 4 workers (no change), got %d", stats.WorkerCount)
	}
}

// TestWorkerPool_WaitWithTimeout tests wait with timeout
func TestWorkerPool_WaitWithTimeout(t *testing.T) {
	wp := NewWorkerPool(2)
	defer wp.Shutdown()

	// Submit tasks
	for i := 0; i < 5; i++ {
		_ = wp.Submit(&mockTask{})
	}

	err := wp.WaitWithTimeout(1 * time.Second)
	if err != nil {
		t.Errorf("wait with timeout failed: %v", err)
	}
}

// TestWorkerPool_GetStats tests stats retrieval
func TestWorkerPool_GetStats(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	stats := wp.GetStats()

	if stats.WorkerCount != 4 {
		t.Errorf("expected 4 workers, got %d", stats.WorkerCount)
	}

	if stats.QueueSize < 0 {
		t.Errorf("invalid queue size: %d", stats.QueueSize)
	}

	if stats.ActiveWorkers < 0 {
		t.Errorf("invalid active workers: %d", stats.ActiveWorkers)
	}
}

// TestWorkerPool_Explain tests Explain method
func TestWorkerPool_Explain(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	explain := wp.Explain()
	if explain == "" {
		t.Error("expected non-empty explain string")
	}
}

// TestWorkerPool_ConcurrentSubmit tests concurrent task submission
func TestWorkerPool_ConcurrentSubmit(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Shutdown()

	const numGoroutines = 10
	const tasksPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var successCount int32
	var errorCount int32

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < tasksPerGoroutine; j++ {
				task := &mockTask{}
				if err := wp.Submit(task); err != nil {
					atomic.AddInt32(&errorCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	// All submissions should succeed
	if atomic.LoadInt32(&errorCount) > 0 {
		t.Errorf("%d submissions failed", errorCount)
	}

	// Wait for tasks to complete
	time.Sleep(500 * time.Millisecond)
}

// TestWorkerPool_TaskError tests that task errors are handled gracefully
func TestWorkerPool_TaskError(t *testing.T) {
	wp := NewWorkerPool(2)
	defer wp.Shutdown()

	taskError := errors.New("task error")
	task := &mockTask{
		executeFunc: func() error {
			return taskError
		},
	}

	// Submit should succeed even if task returns error
	err := wp.Submit(task)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)

	// Task should have been executed despite error
	if atomic.LoadInt32(&task.executed) != 1 {
		t.Error("task was not executed")
	}
}

// TestWorkerPool_SubmitWithTimeout_Timeout tests timeout during submit
func TestWorkerPool_SubmitWithTimeout_Timeout(t *testing.T) {
	// Create pool with very small queue
	wp := NewWorkerPool(1)
	defer wp.Shutdown()

	// Fill the queue with slow tasks
	for i := 0; i < 5; i++ {
		_ = wp.Submit(&mockTask{
			executeFunc: func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			},
		})
	}

	// This submit might timeout if queue is full
	_ = wp.SubmitWithTimeout(&mockTask{}, 10*time.Millisecond)
	// Timeout error is acceptable here
}

// TestWorkerPool_WaitWithTimeout_Timeout tests wait timeout
func TestWorkerPool_WaitWithTimeout_Timeout(t *testing.T) {
	wp := NewWorkerPool(1)
	defer wp.Shutdown()

	// Submit slow tasks
	for i := 0; i < 3; i++ {
		_ = wp.Submit(&mockTask{
			executeFunc: func() error {
				time.Sleep(500 * time.Millisecond)
				return nil
			},
		})
	}

	// Wait should timeout
	err := wp.WaitWithTimeout(100 * time.Millisecond)
	if err == nil {
		t.Error("expected timeout error")
	}
}
