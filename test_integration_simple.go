package main

import (
	"context"
	"fmt"
	"mysql-proxy/mysql"
	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/pool"
	"time"
)

func main() {
	fmt.Println("=== 池系统和监控系统集成测试（简化版）===\n")

	// 测试1: 创建服务器并验证组件
	testServerCreation()

	// 测试2: 验证池系统
	testPoolSystem()

	// 测试3: 验证监控系统
	testMonitorSystem()

	fmt.Println("\n=== 所有测试完成 ===")
}

func testServerCreation() {
	fmt.Println("【测试1】创建服务器并验证组件")

	// 创建服务器
	server := mysql.NewServer()

	// 验证组件存在
	if server.GetGoroutinePoolStats().MaxSize > 0 {
		fmt.Println("  ✅ GoroutinePool 已创建")
	} else {
		fmt.Println("  ❌ GoroutinePool 未创建")
	}

	if server.GetObjectPoolStats().MaxSize > 0 {
		fmt.Println("  ✅ ObjectPool 已创建")
	} else {
		fmt.Println("  ❌ ObjectPool 未创建")
	}

	if server.GetMetricsCollector() != nil {
		fmt.Println("  ✅ MetricsCollector 已创建")
	} else {
		fmt.Println("  ❌ MetricsCollector 未创建")
	}

	if server.GetCacheManager() != nil {
		fmt.Println("  ✅ CacheManager 已创建")
	} else {
		fmt.Println("  ❌ CacheManager 未创建")
	}

	fmt.Println()
}

func testPoolSystem() {
	fmt.Println("【测试2】验证池系统")

	// 创建 goroutine 池
	goroutinePool := pool.NewGoroutinePool(5, 100)
	stats := goroutinePool.Stats()

	fmt.Printf("  GoroutinePool: MaxSize=%d, Active=%d\n", stats.MaxSize, stats.ActiveCount)

	// 提交任务
	taskCount := 0
	for i := 0; i < 10; i++ {
		err := goroutinePool.Submit(func() {
			taskCount++
		})
		if err != nil {
			fmt.Printf("  ❌ 提交任务失败: %v\n", err)
		}
	}

	// 等待任务完成
	time.Sleep(100 * time.Millisecond)

	if taskCount == 10 {
		fmt.Printf("  ✅ GoroutinePool 执行了 %d 个任务\n", taskCount)
	} else {
		fmt.Printf("  ⚠️  GoroutinePool 执行了 %d 个任务（期望10）\n", taskCount)
	}

	// 创建对象池
	objectPool := pool.NewObjectPool(
		func() (interface{}, error) {
			return make([]int, 0), nil
		},
		func(obj interface{}) error {
			return nil
		},
		100,
	)

	obj, err := objectPool.Get(context.Background())
	if err == nil && obj != nil {
		fmt.Println("  ✅ ObjectPool 成功获取对象")
		objectPool.Put(obj)
	} else {
		fmt.Printf("  ❌ ObjectPool 获取对象失败: %v\n", err)
	}

	// 关闭池
	goroutinePool.Close()
	objectPool.Close()

	fmt.Println()
}

func testMonitorSystem() {
	fmt.Println("【测试3】验证监控系统")

	// 创建监控器
	metricsCollector := monitor.NewMetricsCollector()
	cacheManager := monitor.NewCacheManager(100, 100, 100)

	// 记录一些查询
	for i := 0; i < 10; i++ {
		start := time.Now()
		time.Sleep(time.Duration(i) * time.Millisecond)
		metricsCollector.RecordQuery(time.Since(start), true, "test_table")
	}

	// 获取快照
	snapshot := metricsCollector.GetSnapshot()

	fmt.Printf("  查询统计: 总计=%d, 成功=%d, 成功率=%.2f%%\n",
		snapshot.QueryCount, snapshot.QuerySuccess, snapshot.SuccessRate)
	fmt.Printf("  性能统计: 平均耗时=%v\n", snapshot.AvgDuration)

	if snapshot.QueryCount == 10 && snapshot.QuerySuccess == 10 {
		fmt.Println("  ✅ MetricsCollector 记录正确")
	} else {
		fmt.Printf("  ❌ MetricsCollector 记录错误: 总计=%d, 成功=%d\n",
			snapshot.QueryCount, snapshot.QuerySuccess)
	}

	// 测试缓存
	queryCache := cacheManager.GetQueryCache()
	queryCache.Set("test_key", "test_value", time.Minute)

	value, found := queryCache.Get("test_key")
	if found && value == "test_value" {
		fmt.Println("  ✅ CacheManager 缓存功能正常")
	} else {
		fmt.Println("  ❌ CacheManager 缓存功能异常")
	}

	// 获取缓存统计
	cacheStats := queryCache.GetStats()
	fmt.Printf("  缓存统计: Size=%d, Hits=%d, Misses=%d, HitRate=%.2f%%\n",
		cacheStats.Size, cacheStats.Hits, cacheStats.Misses, cacheStats.HitRate)

	fmt.Println()
}
