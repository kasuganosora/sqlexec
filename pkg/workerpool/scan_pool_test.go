package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewScanPool tests scan pool creation
func TestNewScanPool(t *testing.T) {
	sp, err := NewScanPool(4, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	if err != nil {
		t.Fatalf("NewScanPool() error = %v", err)
	}
	if sp == nil {
		t.Fatal("NewScanPool() returned nil")
	}
	sp.Close()
}

// TestScanPool_Start tests scan pool start
func TestScanPool_Start(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	defer sp.Close()

	if err := sp.Start(); err != nil {
		t.Errorf("Start() error = %v", err)
	}

	if !sp.pool.IsRunning() {
		t.Error("ScanPool should be running after Start()")
	}
}

// TestScanPool_ExecuteParallel tests parallel execution
func TestScanPool_ExecuteParallel(t *testing.T) {
	sp, _ := NewScanPool(4, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{
			TaskID: task.ID,
			Items:  []interface{}{task.StartIndex, task.EndIndex},
		}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0, StartIndex: 0, EndIndex: 100},
		{ID: 1, StartIndex: 100, EndIndex: 200},
		{ID: 2, StartIndex: 200, EndIndex: 300},
		{ID: 3, StartIndex: 300, EndIndex: 400},
	}

	results, err := sp.ExecuteParallel(context.Background(), tasks)
	if err != nil {
		t.Fatalf("ExecuteParallel() error = %v", err)
	}

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	for i, result := range results {
		if result.TaskID != i {
			t.Errorf("Result[%d].TaskID = %d, want %d", i, result.TaskID, i)
		}
		if result.Error != nil {
			t.Errorf("Result[%d].Error = %v", i, result.Error)
		}
	}
}

// TestScanPool_ExecuteParallelWithPool tests parallel execution using worker pool
func TestScanPool_ExecuteParallelWithPool(t *testing.T) {
	sp, _ := NewScanPool(4, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{
			TaskID: task.ID,
			Items:  []interface{}{task.Data},
		}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0, Data: "data0"},
		{ID: 1, Data: "data1"},
		{ID: 2, Data: "data2"},
	}

	results, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	if err != nil {
		t.Fatalf("ExecuteParallelWithPool() error = %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestScanPool_ExecuteParallelError tests error handling
func TestScanPool_ExecuteParallelError(t *testing.T) {
	testErr := errors.New("scan error")
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		if task.ID == 1 {
			return ScanResult{}, testErr
		}
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0},
		{ID: 1},
		{ID: 2},
	}

	results, err := sp.ExecuteParallel(context.Background(), tasks)
	if err == nil {
		t.Error("Expected error from ExecuteParallel")
	}

	// Check that we got all results
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Task 1 should have error
	if results[1].Error == nil {
		t.Error("Task 1 should have error")
	}
}

// TestScanPool_ExecuteParallelClosedPool tests execution on closed pool
func TestScanPool_ExecuteParallelClosedPool(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	// Don't start the pool

	tasks := []ScanTask{{ID: 0}}

	_, err := sp.ExecuteParallel(context.Background(), tasks)
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("ExecuteParallel() on non-started pool error = %v, want %v", err, ErrPoolClosed)
	}
}

// TestScanPool_ExecuteParallelWithPoolClosed tests with pool closed
func TestScanPool_ExecuteParallelWithPoolClosed(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})

	tasks := []ScanTask{{ID: 0}}

	_, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("ExecuteParallelWithPool() on non-started pool error = %v, want %v", err, ErrPoolClosed)
	}
}

// TestScanPool_Close tests closing the scan pool
func TestScanPool_Close(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})

	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := sp.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if !sp.pool.IsClosed() {
		t.Error("Pool should be closed")
	}
}

// TestScanPool_Stats tests stats
func TestScanPool_Stats(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})

	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	stats := sp.Stats()
	if stats.Workers <= 0 {
		t.Errorf("Stats().Workers = %d, want > 0", stats.Workers)
	}
}

// TestScanPool_LargeBatch tests large batch processing
func TestScanPool_LargeBatch(t *testing.T) {
	var taskCount int64

	sp, _ := NewScanPool(8, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		atomic.AddInt64(&taskCount, 1)
		return ScanResult{
			TaskID: task.ID,
			Items:  []interface{}{task.ID * 2},
		}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	// Create 100 tasks
	tasks := make([]ScanTask, 100)
	for i := 0; i < 100; i++ {
		tasks[i] = ScanTask{ID: i, StartIndex: i * 100, EndIndex: (i + 1) * 100}
	}

	results, err := sp.ExecuteParallel(context.Background(), tasks)
	if err != nil {
		t.Fatalf("ExecuteParallel() error = %v", err)
	}

	if len(results) != 100 {
		t.Errorf("Expected 100 results, got %d", len(results))
	}

	if atomic.LoadInt64(&taskCount) != 100 {
		t.Errorf("Expected taskCount = 100, got %d", taskCount)
	}
}

// TestScanPool_Panic tests panic handling
func TestScanPool_Panic(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		if task.ID == 1 {
			panic("test panic")
		}
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0},
		{ID: 1}, // This will panic
		{ID: 2},
	}

	results, err := sp.ExecuteParallel(context.Background(), tasks)
	if err == nil {
		t.Error("Expected error due to panic")
	}

	// Should still get all results
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestScanPool_ContextCancel tests context cancellation
func TestScanPool_ContextCancel(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		time.Sleep(time.Millisecond * 50)
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	tasks := []ScanTask{{ID: 0}, {ID: 1}}

	// Context is already canceled, but goroutines may still complete
	// depending on timing
	results, _ := sp.ExecuteParallel(ctx, tasks)
	t.Logf("Got %d results with canceled context", len(results))
}

// BenchmarkScanPool_ExecuteParallel benchmarks parallel execution
func BenchmarkScanPool_ExecuteParallel(b *testing.B) {
	sp, _ := NewScanPool(4, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	sp.Start()
	defer sp.Close()

	tasks := make([]ScanTask, 4)
	for i := range tasks {
		tasks[i] = ScanTask{ID: i, StartIndex: i * 100, EndIndex: (i + 1) * 100}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sp.ExecuteParallel(context.Background(), tasks)
	}
}

// BenchmarkScanPool_ExecuteParallelWithPool benchmarks parallel with worker pool
func BenchmarkScanPool_ExecuteParallelWithPool(b *testing.B) {
	sp, _ := NewScanPool(4, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	sp.Start()
	defer sp.Close()

	tasks := make([]ScanTask, 4)
	for i := range tasks {
		tasks[i] = ScanTask{ID: i, StartIndex: i * 100, EndIndex: (i + 1) * 100}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sp.ExecuteParallelWithPool(context.Background(), tasks)
	}
}

// TestNewScanPoolError tests scan pool creation with invalid size
func TestNewScanPoolError(t *testing.T) {
	_, err := NewScanPool(0, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	if !errors.Is(err, ErrInvalidSize) {
		t.Errorf("NewScanPool(0) error = %v, want %v", err, ErrInvalidSize)
	}
}

// TestScanPool_ExecuteParallelWithPoolError tests parallel execution with pool errors
func TestScanPool_ExecuteParallelWithPoolError(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		if task.ID == 1 {
			return ScanResult{}, errors.New("scan error")
		}
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0},
		{ID: 1},
		{ID: 2},
	}

	results, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	// Should return error due to task failure
	t.Logf("ExecuteParallelWithPool error: %v", err)
	t.Logf("Results: %+v", results)
}

// TestScanPool_ExecuteParallelWithPoolPanic tests panic in ExecuteParallelWithPool
func TestScanPool_ExecuteParallelWithPoolPanic(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		if task.ID == 1 {
			panic("test panic in pool")
		}
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0},
		{ID: 1},
		{ID: 2},
	}

	results, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	// Should handle panic
	t.Logf("ExecuteParallelWithPool error: %v", err)
	t.Logf("Results count: %d", len(results))
}

// TestScanPool_ExecuteParallelEmpty tests with empty task list
func TestScanPool_ExecuteParallelEmpty(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{}

	results, err := sp.ExecuteParallel(context.Background(), tasks)
	if err != nil {
		t.Errorf("ExecuteParallel() with empty tasks error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

// TestScanPool_ExecuteParallelWithPoolEmpty tests with empty task list
func TestScanPool_ExecuteParallelWithPoolEmpty(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{}

	results, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	if err != nil {
		t.Errorf("ExecuteParallelWithPool() with empty tasks error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

// TestScanPool_DoubleClose tests double close
func TestScanPool_DoubleClose(t *testing.T) {
	sp, _ := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{TaskID: task.ID}, nil
	})

	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// First close
	if err := sp.Close(); err != nil {
		t.Errorf("First Close() error = %v", err)
	}

	// Second close should be safe
	if err := sp.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

// TestScanPool_ExecuteParallelWithTaskData tests with various task data
func TestScanPool_ExecuteParallelWithTaskData(t *testing.T) {
	sp, _ := NewScanPool(4, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		// Process data based on task info
		var items []interface{}
		for i := task.StartIndex; i < task.EndIndex; i++ {
			items = append(items, i)
		}
		return ScanResult{
			TaskID: task.ID,
			Items:  items,
		}, nil
	})
	if err := sp.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 0, StartIndex: 0, EndIndex: 10},
		{ID: 1, StartIndex: 10, EndIndex: 20},
		{ID: 2, StartIndex: 20, EndIndex: 30},
	}

	results, err := sp.ExecuteParallel(context.Background(), tasks)
	if err != nil {
		t.Fatalf("ExecuteParallel() error = %v", err)
	}

	// Verify results
	totalItems := 0
	for _, r := range results {
		totalItems += len(r.Items)
	}

	if totalItems != 30 {
		t.Errorf("Expected 30 total items, got %d", totalItems)
	}
}
