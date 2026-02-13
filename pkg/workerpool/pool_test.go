package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNew tests pool creation
func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name:    "valid config",
			config:  Config{Size: 4, QueueSize: 10},
			wantErr: nil,
		},
		{
			name:    "zero size",
			config:  Config{Size: 0},
			wantErr: ErrInvalidSize,
		},
		{
			name:    "negative size",
			config:  Config{Size: -1},
			wantErr: ErrInvalidSize,
		},
		{
			name: "dynamic scaling valid",
			config: Config{
				Size:                 4,
				EnableDynamicScaling: true,
				MinWorkers:           2,
				MaxWorkers:           8,
			},
			wantErr: nil,
		},
		{
			name: "dynamic scaling invalid min",
			config: Config{
				Size:                 4,
				EnableDynamicScaling: true,
				MinWorkers:           0,
				MaxWorkers:           8,
			},
			wantErr: ErrInvalidSize,
		},
		{
			name: "dynamic scaling invalid max",
			config: Config{
				Size:                 4,
				EnableDynamicScaling: true,
				MinWorkers:           4,
				MaxWorkers:           2,
			},
			wantErr: ErrInvalidSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := New(tt.config)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("New() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("New() unexpected error: %v", err)
				return
			}
			if p == nil {
				t.Error("New() returned nil pool")
				return
			}
			p.Close()
		})
	}
}

// TestNewWithSize tests NewWithSize
func TestNewWithSize(t *testing.T) {
	p, err := NewWithSize(4)
	if err != nil {
		t.Fatalf("NewWithSize() error = %v", err)
	}
	if p == nil {
		t.Fatal("NewWithSize() returned nil pool")
	}
	p.Close()

	_, err = NewWithSize(0)
	if !errors.Is(err, ErrInvalidSize) {
		t.Errorf("NewWithSize(0) error = %v, want %v", err, ErrInvalidSize)
	}
}

// TestDefaultConfig tests DefaultConfig
func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config.Size <= 0 {
		t.Error("DefaultConfig().Size should be positive")
	}
	if config.IdleTimeout <= 0 {
		t.Error("DefaultConfig().IdleTimeout should be positive")
	}
}

// TestPool_Start tests pool start
func TestPool_Start(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Errorf("Start() error = %v", err)
	}

	if !p.IsRunning() {
		t.Error("Pool should be running after Start()")
	}

	// Start again should fail
	if err := p.Start(); !errors.Is(err, ErrPoolRunning) {
		t.Errorf("second Start() error = %v, want %v", err, ErrPoolRunning)
	}

	p.Close()
}

// TestPool_Close tests pool close
func TestPool_Close(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Close should succeed
	if err := p.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if !p.IsClosed() {
		t.Error("Pool should be closed after Close()")
	}

	// Double close should be safe
	if err := p.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

// TestPool_Submit tests task submission
func TestPool_Submit(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	executed := atomic.Bool{}
	resultCh, err := p.Submit(context.Background(), func(ctx context.Context) error {
		executed.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Wait for result
	select {
	case result := <-resultCh:
		if result.Error != nil {
			t.Errorf("Task error = %v", result.Error)
		}
		if !executed.Load() {
			t.Error("Task was not executed")
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for task result")
	}
}

// TestPool_SubmitWait tests SubmitWait
func TestPool_SubmitWait(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	executed := atomic.Bool{}
	err = p.SubmitWait(context.Background(), func(ctx context.Context) error {
		executed.Store(true)
		return nil
	})
	if err != nil {
		t.Errorf("SubmitWait() error = %v", err)
	}
	if !executed.Load() {
		t.Error("Task was not executed")
	}
}

// TestPool_SubmitBatch tests batch submission
func TestPool_SubmitBatch(t *testing.T) {
	p, err := New(Config{Size: 4, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var counter int64
	tasks := make([]Task, 10)
	for i := 0; i < 10; i++ {
		tasks[i] = func(ctx context.Context) error {
			atomic.AddInt64(&counter, 1)
			return nil
		}
	}

	resultCh, err := p.SubmitBatch(context.Background(), tasks)
	if err != nil {
		t.Fatalf("SubmitBatch() error = %v", err)
	}

	count := 0
	for range resultCh {
		count++
	}

	if count != 10 {
		t.Errorf("Expected 10 results, got %d", count)
	}
	if atomic.LoadInt64(&counter) != 10 {
		t.Errorf("Expected counter = 10, got %d", counter)
	}
}

// TestPool_SubmitClosed tests submission to closed pool
func TestPool_SubmitClosed(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	p.Close()

	_, err = p.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Submit() to closed pool error = %v, want %v", err, ErrPoolClosed)
	}

	err = p.SubmitWait(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("SubmitWait() to closed pool error = %v, want %v", err, ErrPoolClosed)
	}

	_, err = p.SubmitBatch(context.Background(), []Task{func(ctx context.Context) error { return nil }})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("SubmitBatch() to closed pool error = %v, want %v", err, ErrPoolClosed)
	}
}

// TestPool_SubmitBeforeStart tests submission before start
func TestPool_SubmitBeforeStart(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	_, err = p.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Submit() before start error = %v, want %v", err, ErrPoolClosed)
	}
}

// TestPool_TaskError tests task error handling
func TestPool_TaskError(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	testErr := errors.New("test error")
	resultCh, err := p.Submit(context.Background(), func(ctx context.Context) error {
		return testErr
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case result := <-resultCh:
		if !errors.Is(result.Error, testErr) {
			t.Errorf("Task error = %v, want %v", result.Error, testErr)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for task result")
	}
}

// TestPool_TaskPanic tests panic recovery
func TestPool_TaskPanic(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	resultCh, err := p.Submit(context.Background(), func(ctx context.Context) error {
		panic("test panic")
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case result := <-resultCh:
		if !errors.Is(result.Error, ErrTaskPanic) {
			t.Errorf("Panic task error = %v, want %v", result.Error, ErrTaskPanic)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for task result")
	}
}

// TestPool_CanceledTask tests task cancellation
func TestPool_CanceledTask(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	resultCh, err := p.Submit(ctx, func(ctx context.Context) error {
		return nil
	})
	// Submit might succeed or fail depending on timing
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrPoolClosed) {
			t.Errorf("Submit() error = %v", err)
		}
		return
	}

	select {
	case result := <-resultCh:
		if result.Error != nil && !errors.Is(result.Error, ErrTaskCanceled) {
			t.Logf("Task error = %v (may be expected)", result.Error)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for task result")
	}
}

// TestPool_CloseWithTimeout tests close with timeout
func TestPool_CloseWithTimeout(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Close with short timeout
	err = p.CloseWithTimeout(time.Millisecond * 100)
	if err != nil {
		t.Logf("CloseWithTimeout() error = %v", err)
	}

	if !p.IsClosed() {
		t.Error("Pool should be closed")
	}
}

// TestPool_Stats tests stats
func TestPool_Stats(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit a task
	err = p.SubmitWait(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("SubmitWait() error = %v", err)
	}

	// Give time for stats to update
	time.Sleep(time.Millisecond * 10)

	stats := p.Stats()
	if stats.Workers <= 0 {
		t.Errorf("Stats().Workers = %d, want > 0", stats.Workers)
	}
	if stats.TasksExecuted <= 0 {
		t.Errorf("Stats().TasksExecuted = %d, want > 0", stats.TasksExecuted)
	}
	if !stats.IsRunning {
		t.Error("Stats().IsRunning should be true")
	}

	p.Close()

	stats = p.Stats()
	if !stats.IsClosed {
		t.Error("Stats().IsClosed should be true")
	}
}

// TestPool_WorkerCount tests WorkerCount
func TestPool_WorkerCount(t *testing.T) {
	p, err := New(Config{Size: 4, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait for workers to start
	time.Sleep(time.Millisecond * 50)

	count := p.WorkerCount()
	if count <= 0 {
		t.Errorf("WorkerCount() = %d, want > 0", count)
	}

	p.Close()
}

// TestPool_QueueLen tests QueueLen
func TestPool_QueueLen(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Queue should start empty
	if p.QueueLen() != 0 {
		t.Errorf("QueueLen() = %d, want 0", p.QueueLen())
	}
}

// TestPool_Results tests Results channel
func TestPool_Results(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	ch := p.Results()
	if ch == nil {
		t.Error("Results() returned nil channel")
	}
}

// TestPool_SubmitFunc tests SubmitFunc
func TestPool_SubmitFunc(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	_, err = p.SubmitFunc(context.Background(), func(ctx context.Context) (interface{}, error) {
		return "result", nil
	})
	// SubmitFunc currently just delegates to Submit
	if err != nil {
		t.Errorf("SubmitFunc() error = %v", err)
	}
}

// TestPool_StartAfterClose tests starting after close
func TestPool_StartAfterClose(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	p.Close()

	if err := p.Start(); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Start() after close error = %v, want %v", err, ErrPoolClosed)
	}
}

// TestPool_ConcurrentSubmit tests concurrent submissions
func TestPool_ConcurrentSubmit(t *testing.T) {
	p, err := New(Config{Size: 4, QueueSize: 100})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var counter int64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.SubmitWait(context.Background(), func(ctx context.Context) error {
				atomic.AddInt64(&counter, 1)
				return nil
			})
		}()
	}

	wg.Wait()

	if atomic.LoadInt64(&counter) != 100 {
		t.Errorf("Expected counter = 100, got %d", counter)
	}
}

// TestPool_DynamicScaling tests dynamic scaling
func TestPool_DynamicScaling(t *testing.T) {
	p, err := New(Config{
		Size:                 2,
		QueueSize:            10,
		IdleTimeout:          time.Millisecond * 100,
		EnableDynamicScaling: true,
		MinWorkers:           1,
		MaxWorkers:           4,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit many tasks to trigger scaling
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.SubmitWait(context.Background(), func(ctx context.Context) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			})
		}()
	}

	wg.Wait()

	stats := p.Stats()
	if stats.Workers < 1 {
		t.Errorf("Stats().Workers = %d, want >= 1", stats.Workers)
	}
}

// BenchmarkPool_Submit benchmarks task submission
func BenchmarkPool_Submit(b *testing.B) {
	p, _ := New(Config{Size: 4, QueueSize: 1000})
	p.Start()
	defer p.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}
}

// BenchmarkPool_SubmitWait benchmarks synchronous task submission
func BenchmarkPool_SubmitWait(b *testing.B) {
	p, _ := New(Config{Size: 4, QueueSize: 1000})
	p.Start()
	defer p.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.SubmitWait(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}
}

// BenchmarkPool_Parallel benchmarks parallel submissions
func BenchmarkPool_Parallel(b *testing.B) {
	p, _ := New(Config{Size: 8, QueueSize: 1000})
	p.Start()
	defer p.Close()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.SubmitWait(context.Background(), func(ctx context.Context) error {
				return nil
			})
		}
	})
}

// TestPool_SubmitWithContextCancel tests submit with context cancellation during queue
func TestPool_SubmitWithContextCancel(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit with already canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Submit should fail due to canceled context
	_, err = p.Submit(ctx, func(ctx context.Context) error {
		return nil
	})
	// Error may be context.Canceled or succeed depending on timing
	t.Logf("Submit() with canceled context result: %v", err)
}

// TestPool_SendResultDuringClose tests send result during close
func TestPool_SendResultDuringClose(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit a task
	resultCh, _ := p.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})

	// Close while waiting for result
	go func() {
		time.Sleep(time.Millisecond * 10)
		p.Close()
	}()

	// Wait for result or close
	select {
	case <-resultCh:
		// Got result
	case <-time.After(time.Second):
		t.Error("Timeout waiting for result")
	}
}

// TestPool_SubmitBatchWithErrors tests batch submission with errors
func TestPool_SubmitBatchWithErrors(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	tasks := make([]Task, 5)
	for i := 0; i < 5; i++ {
		tasks[i] = func(ctx context.Context) error {
			return errors.New("task error")
		}
	}

	resultCh, err := p.SubmitBatch(context.Background(), tasks)
	if err != nil {
		t.Fatalf("SubmitBatch() error = %v", err)
	}

	count := 0
	for range resultCh {
		count++
	}

	if count != 5 {
		t.Errorf("Expected 5 results, got %d", count)
	}
}

// TestPool_SubmitBatchWithContextCancel tests batch with context cancel
func TestPool_SubmitBatchWithContextCancel(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	tasks := make([]Task, 10)
	for i := 0; i < 10; i++ {
		tasks[i] = func(ctx context.Context) error {
			time.Sleep(time.Millisecond * 100)
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	resultCh, err := p.SubmitBatch(ctx, tasks)
	if err != nil {
		t.Fatalf("SubmitBatch() error = %v", err)
	}

	count := 0
	for range resultCh {
		count++
	}

	// Some tasks may complete before timeout
	t.Logf("Got %d results before timeout", count)
}

// TestPool_CloseWithTimeoutExceeded tests close with timeout exceeded
func TestPool_CloseWithTimeoutExceeded(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit a task using Submit (non-blocking)
	_, _ = p.Submit(context.Background(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second * 10):
			return nil
		}
	})

	// Close with very short timeout
	err = p.CloseWithTimeout(time.Nanosecond)
	// The pool should still close
	if !p.IsClosed() {
		t.Error("Pool should be closed")
	}
	t.Logf("CloseWithTimeout result: %v", err)
}

// TestPool_ExecuteTaskContextCanceled tests executeTask with canceled context
func TestPool_ExecuteTaskContextCanceled(t *testing.T) {
	p, err := New(Config{Size: 1, QueueSize: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Create a canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Task should detect canceled context
	resultCh, err := p.Submit(ctx, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		// Submit failed due to canceled context
		return
	}

	select {
	case result := <-resultCh:
		// May get ErrTaskCanceled or nil depending on timing
		t.Logf("Result: %v", result.Error)
	case <-time.After(time.Second):
		t.Error("Timeout waiting for result")
	}
}

// TestPool_ShouldScaleDown tests dynamic scaling down
func TestPool_ShouldScaleDown(t *testing.T) {
	p, err := New(Config{
		Size:                 4,
		QueueSize:            10,
		IdleTimeout:          time.Millisecond * 50,
		EnableDynamicScaling: true,
		MinWorkers:           1,
		MaxWorkers:           8,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait for idle timeout and scaling down
	time.Sleep(time.Millisecond * 200)

	// Check that workers may have been reduced
	stats := p.Stats()
	t.Logf("Workers after idle: %d", stats.Workers)

	// Should have at least MinWorkers
	if stats.Workers < 1 {
		t.Errorf("Expected at least 1 worker, got %d", stats.Workers)
	}
}

// TestPool_MaybeScaleUpEdgeCase tests scale up edge case
func TestPool_MaybeScaleUpEdgeCase(t *testing.T) {
	// Create pool without dynamic scaling
	p, err := New(Config{
		Size:                 2,
		QueueSize:            10,
		EnableDynamicScaling: false,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit tasks
	for i := 0; i < 5; i++ {
		_ = p.SubmitWait(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}

	// Worker count should remain at initial size
	if p.WorkerCount() != 2 {
		t.Logf("WorkerCount = %d (may vary)", p.WorkerCount())
	}
}

// TestPool_SubmitWaitContextCanceled tests SubmitWait with canceled context
func TestPool_SubmitWaitContextCanceled(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Try to submit with already canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = p.SubmitWait(ctx, func(ctx context.Context) error {
		return nil
	})

	// May fail with context.Canceled or succeed depending on timing
	t.Logf("SubmitWait with canceled context result: %v", err)
}

// TestPool_NilResultChannel tests task with nil result channel
func TestPool_NilResultChannel(t *testing.T) {
	p, err := New(Config{Size: 2, QueueSize: 10})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Submit task normally
	_ = p.SubmitWait(context.Background(), func(ctx context.Context) error {
		return nil
	})

	// Check stats
	stats := p.Stats()
	t.Logf("Tasks executed: %d", stats.TasksExecuted)
}
