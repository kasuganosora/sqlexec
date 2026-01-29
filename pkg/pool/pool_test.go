package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewObjectPool(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	pool := NewObjectPool(factory, nil, 10)

	assert.NotNil(t, pool)
	assert.Equal(t, 10, pool.maxSize)
	assert.Equal(t, 2, pool.minIdle)
	assert.Equal(t, 5, pool.maxIdle)
	assert.False(t, pool.closed)
}

func TestObjectPool_Get_FromIdle(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	// 预先放入一个对象
	testObj := "idle_object"
	pool.idle = append(pool.idle, testObj)

	obj, err := pool.Get(ctx)

	assert.NoError(t, err)
	assert.Equal(t, testObj, obj)
	assert.Contains(t, pool.active, obj)
	assert.Empty(t, pool.idle)
}

func TestObjectPool_Get_CreateNew(t *testing.T) {
	factory := func() (interface{}, error) {
		return "new_object", nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	obj, err := pool.Get(ctx)

	assert.NoError(t, err)
	assert.Equal(t, "new_object", obj)
	assert.Contains(t, pool.active, obj)
	assert.Equal(t, int64(1), pool.createCount)
	assert.Equal(t, int64(1), pool.acquireCount)
}

func TestObjectPool_Get_PoolClosed(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	pool.Close()
	ctx := context.Background()

	obj, err := pool.Get(ctx)

	assert.Error(t, err)
	assert.Nil(t, obj)
	assert.Equal(t, ErrPoolClosed, err)
}

func TestObjectPool_Get_FactoryError(t *testing.T) {
	factory := func() (interface{}, error) {
		return nil, errors.New("factory error")
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	obj, err := pool.Get(ctx)

	assert.Error(t, err)
	assert.Nil(t, obj)
	assert.Contains(t, err.Error(), "factory error")
}

func TestObjectPool_Put(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	obj, _ := pool.Get(ctx)

	err := pool.Put(obj)

	assert.NoError(t, err)
	assert.NotContains(t, pool.active, obj)
	assert.Equal(t, int64(1), pool.releaseCount)
}

func TestObjectPool_Put_DestroyWhenIdleFull(t *testing.T) {
	factory := func() (interface{}, error) {
		return &struct{}{}, nil
	}

	var destroyed []interface{}
	destroy := func(obj interface{}) error {
		destroyed = append(destroyed, obj)
		return nil
	}

	pool := NewObjectPool(factory, destroy, 10)
	ctx := context.Background()

	// 直接创建并手动添加 maxIdle 个对象到 idle 队列
	// 模拟空闲队列已满的情况
	for i := 0; i < pool.maxIdle; i++ {
		obj, _ := factory()
		pool.idle = append(pool.idle, obj)
	}

	// 验证初始状态：idle 已满
	stats := pool.Stats()
	t.Logf("After manual fill: Idle=%d, MaxIdle=%d", stats.IdleCount, pool.maxIdle)

	// 获取一个对象，会从 idle 拿走
	_, _ = pool.Get(ctx)
	stats = pool.Stats()
	t.Logf("After Get obj1: Idle=%d, Active=%d", stats.IdleCount, stats.ActiveCount)

	// 创建一个新对象并尝试放回
	obj2, _ := factory()
	pool.active[obj2] = struct{}{} // 标记为活跃

	stats = pool.Stats()
	t.Logf("Before Put obj2: Idle=%d, Active=%d", stats.IdleCount, stats.ActiveCount)

	// 归还 obj2，此时 idle 不满（因为之前拿走了一个），应该放回
	pool.Put(obj2)
	stats = pool.Stats()
	t.Logf("After Put obj2 (should be added): Idle=%d, Destroyed=%d", stats.IdleCount, len(destroyed))

	// 再创建一个对象并放回
	obj3, _ := factory()
	pool.active[obj3] = struct{}{}

	// 归还 obj3，此时 idle 应该已满，这个对象应该被销毁
	pool.Put(obj3)

	stats = pool.Stats()
	t.Logf("After Put obj3 (should be destroyed): Idle=%d, Destroyed=%d", stats.IdleCount, len(destroyed))

	// 验证：obj3 被销毁
	assert.Len(t, destroyed, 1)
	assert.Equal(t, obj3, destroyed[0])
}

func TestObjectPool_Put_PoolClosed(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	obj, _ := pool.Get(ctx)
	pool.Close()

	err := pool.Put(obj)

	assert.Error(t, err)
	assert.Equal(t, ErrPoolClosed, err)
}

func TestObjectPool_Stats(t *testing.T) {
	var counter int
	factory := func() (interface{}, error) {
		counter++
		return counter, nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	obj1, _ := pool.Get(ctx)
	_, _ = pool.Get(ctx) // obj2 not used intentionally
	pool.Put(obj1)

	stats := pool.Stats()

	assert.Equal(t, int64(2), stats.TotalCreated)
	assert.Equal(t, int64(2), stats.TotalAcquired)
	assert.Equal(t, int64(1), stats.TotalReleased)
	assert.Equal(t, 5, stats.MaxSize)
	assert.Equal(t, 1, stats.IdleCount)
	assert.Equal(t, 1, stats.ActiveCount)
	assert.Equal(t, 2, stats.CurrentSize)
}

func TestObjectPool_Close(t *testing.T) {
	var destroyed []interface{}
	factory := func() (interface{}, error) {
		return "test", nil
	}

	destroy := func(obj interface{}) error {
		destroyed = append(destroyed, obj)
		return nil
	}

	pool := NewObjectPool(factory, destroy, 5)
	ctx := context.Background()

	// 创建并归还一些对象
	obj1, _ := pool.Get(ctx)
	obj2, _ := pool.Get(ctx)
	pool.Put(obj1)
	pool.Put(obj2)

	err := pool.Close()

	assert.NoError(t, err)
	assert.True(t, pool.closed)
	assert.Len(t, destroyed, 2)
	assert.Nil(t, pool.idle)
}

func TestObjectPool_Close_Idempotent(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	pool := NewObjectPool(factory, nil, 5)

	err1 := pool.Close()
	err2 := pool.Close()

	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestObjectPool_ConcurrentAccess(t *testing.T) {
	factory := func() (interface{}, error) {
		return "test", nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 10)
	ctx := context.Background()

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			obj, err := pool.Get(ctx)
			assert.NoError(t, err)
			time.Sleep(10 * time.Millisecond)
			pool.Put(obj)
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	stats := pool.Stats()
	assert.Equal(t, int64(10), stats.TotalAcquired)
	assert.Equal(t, int64(10), stats.TotalReleased)
}

func TestGoroutinePool_NewGoroutinePool(t *testing.T) {
	pool := NewGoroutinePool(5, 100)

	assert.NotNil(t, pool)
	assert.Equal(t, 5, pool.maxWorkers)
	assert.Equal(t, 100, cap(pool.taskQueue))
	assert.Equal(t, 5, pool.workerCount)
}

func TestGoroutinePool_Submit(t *testing.T) {
	pool := NewGoroutinePool(2, 10)

	var executed int32
	task := func() {
		atomic.AddInt32(&executed, 1)
	}

	err := pool.Submit(task)
	assert.NoError(t, err)

	// 等待任务执行
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
}

func TestGoroutinePool_Submit_MultipleTasks(t *testing.T) {
	pool := NewGoroutinePool(3, 20)

	var executed int32
	for i := 0; i < 10; i++ {
		task := func() {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&executed, 1)
		}
		err := pool.Submit(task)
		assert.NoError(t, err)
	}

	// 等待所有任务完成
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(10), atomic.LoadInt32(&executed))
}

func TestGoroutinePool_Submit_PoolClosed(t *testing.T) {
	pool := NewGoroutinePool(2, 10)
	pool.Close()

	task := func() {}

	err := pool.Submit(task)

	assert.Error(t, err)
	assert.Equal(t, ErrPoolClosed, err)
}

func TestGoroutinePool_Stats(t *testing.T) {
	pool := NewGoroutinePool(5, 100)

	stats := pool.Stats()

	assert.Equal(t, 5, stats.MaxSize)
	assert.Equal(t, 5, stats.ActiveCount)
}

func TestGoroutinePool_Close(t *testing.T) {
	pool := NewGoroutinePool(2, 10)

	err := pool.Close()
	assert.NoError(t, err)

	// 验证任务队列已关闭
	task := func() {}
	err = pool.Submit(task)
	assert.Error(t, err)
}

func TestGoroutinePool_Close_WaitForWorkers(t *testing.T) {
	pool := NewGoroutinePool(2, 10)

	var completed int32
	task := func() {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&completed, 1)
	}

	pool.Submit(task)
	pool.Submit(task)

	// 立即关闭
	start := time.Now()
	err := pool.Close()
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, elapsed >= 100*time.Millisecond, "Close should wait for workers to finish")
	assert.Equal(t, int32(2), atomic.LoadInt32(&completed))
}

func TestRetryPool_NewRetryPool(t *testing.T) {
	pool := NewRetryPool(3, 100*time.Millisecond)

	assert.NotNil(t, pool)
	assert.Equal(t, 3, pool.maxRetries)
	assert.Equal(t, 100*time.Millisecond, pool.retryDelay)
}

func TestRetryPool_Execute_Success(t *testing.T) {
	pool := NewRetryPool(3, 10*time.Millisecond)

	executed := 0
	task := func() error {
		executed++
		if executed < 2 {
			return errors.New("temporary error")
		}
		return nil
	}

	ctx := context.Background()
	err := pool.Execute(ctx, task)

	assert.NoError(t, err)
	assert.Equal(t, 2, executed)
}

func TestRetryPool_Execute_AllRetriesFailed(t *testing.T) {
	pool := NewRetryPool(3, 10*time.Millisecond)

	executed := 0
	task := func() error {
		executed++
		return errors.New("permanent error")
	}

	ctx := context.Background()
	err := pool.Execute(ctx, task)

	assert.Error(t, err)
	assert.Equal(t, 4, executed) // initial + 3 retries
	assert.Contains(t, err.Error(), "permanent error")
}

func TestRetryPool_Execute_ContextCanceled(t *testing.T) {
	pool := NewRetryPool(10, 100*time.Millisecond)

	task := func() error {
		return errors.New("error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()
	err := pool.Execute(ctx, task)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.True(t, elapsed < 500*time.Millisecond, "Should be canceled quickly")
}

func TestRetryPool_Execute_NoRetries(t *testing.T) {
	pool := NewRetryPool(0, 10*time.Millisecond)

	executed := 0
	task := func() error {
		executed++
		return errors.New("error")
	}

	ctx := context.Background()
	err := pool.Execute(ctx, task)

	assert.Error(t, err)
	assert.Equal(t, 1, executed) // only initial attempt
}

func TestPoolError(t *testing.T) {
	err := &PoolError{Message: "test error"}

	assert.Equal(t, "test error", err.Error())
}

func TestErrPoolClosed(t *testing.T) {
	assert.Equal(t, "pool is closed", ErrPoolClosed.Error())
}

func TestErrPoolEmpty(t *testing.T) {
	assert.Equal(t, "pool is empty", ErrPoolEmpty.Error())
}

func TestObjectPool_MaxSizeLimit(t *testing.T) {
	var counter int
	factory := func() (interface{}, error) {
		counter++
		return counter, nil
	}

	destroy := func(obj interface{}) error {
		return nil
	}

	pool := NewObjectPool(factory, destroy, 3)
	ctx := context.Background()

	// 获取最大数量的对象
	objs := make([]interface{}, 3)
	for i := 0; i < 3; i++ {
		obj, err := pool.Get(ctx)
		assert.NoError(t, err)
		objs[i] = obj
	}

	stats := pool.Stats()
	assert.Equal(t, 3, stats.CurrentSize)
	assert.Equal(t, 3, stats.ActiveCount)

	// 尝试获取更多对象应该等待
	done := make(chan bool, 1)
	go func() {
		_, err := pool.Get(ctx)
		assert.NoError(t, err)
		done <- true
	}()

	// 等待一下，确保goroutine正在等待
	time.Sleep(50 * time.Millisecond)

	// 归还一个对象
	pool.Put(objs[0])

	// 等待goroutine完成
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for Get to complete")
	}
}

func TestGoroutinePool_ContextCanceled(t *testing.T) {
	pool := NewGoroutinePool(2, 10)

	ctx, cancel := context.WithCancel(context.Background())
	_ = ctx // avoid unused variable error

	// 提交一个会长时间运行的任务
	task := func() {
		time.Sleep(100 * time.Millisecond)
	}

	pool.Submit(task)

	// 取消context - workers应该继续处理任务直到完成
	cancel()

	// 等待足够长的时间让任务完成
	time.Sleep(150 * time.Millisecond)

	// 验证池仍然可以关闭（不会阻塞）
	err := pool.Close()
	assert.NoError(t, err)
}
