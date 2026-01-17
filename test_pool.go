package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mysql-proxy/mysql/pool"
)

func main() {
	fmt.Println("=== 池性能测试 ===\n")

	// 测试1: ObjectPool
	testObjectPool()

	// 测试2: GoroutinePool
	testGoroutinePool()

	// 测试3: SyncPool
	testSyncPool()

	// 测试4: RetryPool
	testRetryPool()

	fmt.Println("\n=== 所有测试完成 ===")
}

func testObjectPool() {
	fmt.Println("【测试1】ObjectPool")

	// 创建对象池
	objectPool := pool.NewObjectPool(
		func() (interface{}, error) {
			return &TestObject{id: time.Now().UnixNano()}, nil
		},
		func(obj interface{}) error {
			return nil
		},
		100,
	)

	ctx := context.Background()

	// 并发获取和释放对象
	var wg sync.WaitGroup
	var successCount int64
	var failCount int64

	start := time.Now()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			obj, err := objectPool.Get(ctx)
			if err != nil {
				atomic.AddInt64(&failCount, 1)
				return
			}

			time.Sleep(time.Millisecond * time.Duration(1+rand.Intn(5)))

			if err := objectPool.Put(obj); err != nil {
				atomic.AddInt64(&failCount, 1)
				return
			}

			atomic.AddInt64(&successCount, 1)
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	stats := objectPool.Stats()
	fmt.Printf("  成功操作: %d\n", successCount)
	fmt.Printf("  失败操作: %d\n", failCount)
	fmt.Printf("  耗时: %v\n", duration)
	fmt.Printf("  创建对象: %d\n", stats.TotalCreated)
	fmt.Printf("  获取对象: %d\n", stats.TotalAcquired)
	fmt.Printf("  释放对象: %d\n", stats.TotalReleased)
	fmt.Printf("  当前大小: %d/%d\n", stats.CurrentSize, stats.MaxSize)
	fmt.Printf("  空闲对象: %d\n", stats.IdleCount)
	fmt.Printf("  活跃对象: %d\n", stats.ActiveCount)
	fmt.Printf("  吞吐量: %.2f ops/sec\n", float64(successCount)/duration.Seconds())
	fmt.Println()

	objectPool.Close()
}

func testGoroutinePool() {
	fmt.Println("【测试2】GoroutinePool")

	// 创建goroutine池
	goroutinePool := pool.NewGoroutinePool(10, 1000)

	var taskCount int64
	var wg sync.WaitGroup

	start := time.Now()

	// 提交1000个任务
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		task := func() {
			defer wg.Done()
			atomic.AddInt64(&taskCount, 1)
			time.Sleep(time.Millisecond * time.Duration(1+rand.Intn(5)))
		}

		if err := goroutinePool.Submit(task); err != nil {
			fmt.Printf("  提交任务失败: %v\n", err)
			wg.Done()
		}
	}

	wg.Wait()
	duration := time.Since(start)

	stats := goroutinePool.Stats()
	fmt.Printf("  完成任务: %d\n", taskCount)
	fmt.Printf("  耗时: %v\n", duration)
	fmt.Printf("  最大Worker数: %d\n", stats.MaxSize)
	fmt.Printf("  当前Worker数: %d\n", stats.ActiveCount)
	fmt.Printf("  吞吐量: %.2f tasks/sec\n", float64(taskCount)/duration.Seconds())
	fmt.Println()

	goroutinePool.Close()
}

func testSyncPool() {
	fmt.Println("【测试3】对象池高并发测试")

	objectPool := pool.NewObjectPool(
		func() (interface{}, error) {
			return &TestObject{id: time.Now().UnixNano()}, nil
		},
		func(obj interface{}) error {
			return nil
		},
		1000,
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	var getCount int64
	var putCount int64

	start := time.Now()

	// 高并发获取和释放
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			obj, err := objectPool.Get(ctx)
			if err != nil {
				return
			}

			atomic.AddInt64(&getCount, 1)

			time.Sleep(time.Microsecond)

			if err := objectPool.Put(obj); err != nil {
				return
			}

			atomic.AddInt64(&putCount, 1)
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	stats := objectPool.Stats()
	fmt.Printf("  获取次数: %d\n", getCount)
	fmt.Printf("  释放次数: %d\n", putCount)
	fmt.Printf("  耗时: %v\n", duration)
	fmt.Printf("  创建对象: %d\n", stats.TotalCreated)
	fmt.Printf("  吞吐量: %.2f ops/sec\n", float64(getCount)/duration.Seconds())
	fmt.Println()

	objectPool.Close()
}

func testRetryPool() {
	fmt.Println("【测试4】RetryPool")

	// 创建重试池
	retryPool := pool.NewRetryPool(3, time.Millisecond*100)

	var successCount int64
	var failCount int64
	ctx := context.Background()

	tests := []struct {
		name   string
		task   func() error
		success bool
	}{
		{
			name: "成功任务",
			task: func() error {
				return nil
			},
			success: true,
		},
		{
			name: "失败任务",
			task: func() error {
				return fmt.Errorf("task failed")
			},
			success: false,
		},
		{
			name: "重试后成功",
			task: func() error {
				if atomic.AddInt64(&successCount, 1) == 1 {
					return fmt.Errorf("first attempt failed")
				}
				return nil
			},
			success: true,
		},
	}

	for _, test := range tests {
		err := retryPool.Execute(ctx, test.task)
		if err == nil && test.success {
			atomic.AddInt64(&successCount, 1)
			fmt.Printf("  %s: 成功\n", test.name)
		} else if err != nil && !test.success {
			atomic.AddInt64(&failCount, 1)
			fmt.Printf("  %s: 失败 (预期)\n", test.name)
		} else {
			fmt.Printf("  %s: 意外结果: %v\n", test.name, err)
		}
	}

	fmt.Printf("  成功: %d, 失败: %d\n", successCount, failCount)
	fmt.Println()
}

// TestObject 测试对象
type TestObject struct {
	id int64
}
