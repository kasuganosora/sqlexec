package main

import (
	"fmt"
	"mysql-proxy/mysql"
	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/pool"
	"time"
)

func main() {
	fmt.Println("=== 池系统和监控系统集成测试 ===\n")

	// 测试1: 创建服务器并验证组件
	testServerCreation()

	// 测试2: 验证 goroutine 池
	testGoroutinePool()

	// 测试3: 验证监控系统
	testMonitorSystem()

	// 测试4: 验证缓存系统
	testCacheSystem()

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

func testGoroutinePool() {
	fmt.Println("【测试2】验证 goroutine 池")

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
	time.Sleep(200 * time.Millisecond)

	if taskCount == 10 {
		fmt.Printf("  ✅ GoroutinePool 执行了 %d 个任务\n", taskCount)
	} else {
		fmt.Printf("  ⚠️  GoroutinePool 执行了 %d 个任务（期望10）\n", taskCount)
	}

	// 关闭池
	goroutinePool.Close()

	fmt.Println()
}

func testMonitorSystem() {
	fmt.Println("【测试3】验证监控系统")

	// 创建监控器
	metricsCollector := monitor.NewMetricsCollector()

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

	fmt.Println()
}

func testCacheSystem() {
	fmt.Println("【测试4】验证缓存系统")

	// 创建缓存管理器
	cacheManager := monitor.NewCacheManager(100, 100, 100)
	queryCache := cacheManager.GetQueryCache()

	// 测试缓存
	queryCache.Set("test_key", "test_value", time.Minute)

	value, found := queryCache.Get("test_key")
	if found && value == "test_value" {
		fmt.Println("  ✅ CacheManager 缓存功能正常")
	} else {
		fmt.Printf("  ❌ CacheManager 缓存功能异常: found=%v\n", found)
	}

	// 获取缓存统计
	cacheStats := queryCache.GetStats()
	fmt.Printf("  缓存统计: Size=%d, Hits=%d, Misses=%d, HitRate=%.2f%%\n",
		cacheStats.Size, cacheStats.Hits, cacheStats.Misses, cacheStats.HitRate)

	if cacheStats.HitRate > 0 {
		fmt.Println("  ✅ CacheManager 统计功能正常")
	} else {
		fmt.Println("  ⚠️  CacheManager 统计功能需要更多数据")
	}

	fmt.Println()
}
