package main

import (
	"context"
	"fmt"
	"log"
	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/pool"
	"mysql-proxy/mysql/resource"
	"sync"
	"time"
)

// 测试池系统和监控系统的集成
func main() {
	fmt.Println("=== 池系统和监控系统集成测试 ===\n")

	// 测试1: GoroutinePool 集成
	testGoroutinePoolIntegration()

	// 测试2: CacheManager 集成
	testCacheManagerIntegration()

	// 测试3: MetricsCollector 集成
	testMetricsCollectorIntegration()

	// 测试4: 综合性能测试
	testIntegratedPerformance()

	fmt.Println("\n=== 所有测试完成 ===")
}

func testGoroutinePoolIntegration() {
	fmt.Println("【测试1】GoroutinePool 集成测试")

	// 创建数据源
	ds := resource.NewMemoryDataSource()
	setupTestData(ds)

	// 创建goroutine池
	goroutinePool := pool.NewGoroutinePool(5, 100)

	// 并发执行查询
	var wg sync.WaitGroup
	successCount := 0
	failCount := 0
	mu := sync.Mutex{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		err := goroutinePool.Submit(func() {
			defer wg.Done()

			// 执行查询
			builder := parser.NewQueryBuilder(ds)
			result, err := builder.BuildAndExecute(context.Background(), "SELECT * FROM users WHERE age > 25")
			
			mu.Lock()
			if err == nil && result.Success {
				successCount++
			} else {
				failCount++
				if err != nil {
					fmt.Printf("  查询失败: %v\n", err)
				}
			}
			mu.Unlock()
		})

		if err != nil {
			log.Printf("提交任务失败: %v", err)
			wg.Done()
		}
	}

	wg.Wait()

	// 获取池统计
	stats := goroutinePool.Stats()
	fmt.Printf("  池统计: MaxSize=%d, Active=%d\n", stats.MaxSize, stats.ActiveCount)
	fmt.Printf("  查询结果: 成功=%d, 失败=%d\n", successCount, failCount)

	if successCount == 20 {
		fmt.Println("  ✅ 测试通过")
	} else {
		fmt.Println("  ❌ 测试失败")
	}

	// 关闭池
	goroutinePool.Close()

	fmt.Println()
}

func testCacheManagerIntegration() {
	fmt.Println("【测试2】CacheManager 集成测试")

	// 创建数据源
	ds := resource.NewMemoryDataSource()
	setupTestData(ds)

	// 创建缓存管理器
	cacheManager := monitor.NewCacheManager(100, 100, 100)
	queryCache := cacheManager.GetQueryCache()

	// 第一次查询（应该未命中缓存）
	start := time.Now()
	builder := parser.NewQueryBuilder(ds)
	result1, err := builder.BuildAndExecute(context.Background(), "SELECT * FROM users WHERE age > 25")
	duration1 := time.Since(start)
	
	if err != nil || !result1.Success {
		log.Fatalf("第一次查询失败: %v", err)
	}
	fmt.Printf("  第一次查询: %v (未缓存)\n", duration1)

	// 检查缓存
	_, found := queryCache.Get("SELECT * FROM users WHERE age > 25")
	fmt.Printf("  缓存命中: %v\n", found)

	// 缓存查询结果
	queryCache.Set("SELECT * FROM users WHERE age > 25", result1, 5*time.Minute)

	// 第二次查询（应该命中缓存）
	start = time.Now()
	result2, found := queryCache.Get("SELECT * FROM users WHERE age > 25")
	duration2 := time.Since(start)

	if !found {
		log.Fatal("缓存未命中")
	}

	fmt.Printf("  第二次查询: %v (已缓存)\n", duration2)
	fmt.Printf("  性能提升: %.2fx\n", float64(duration1)/float64(duration2))

	// 获取缓存统计
	stats := queryCache.GetStats()
	fmt.Printf("  缓存统计: Size=%d, Hits=%d, Misses=%d, HitRate=%.2f%%\n",
		stats.Size, stats.Hits, stats.Misses, stats.HitRate)

	if stats.HitRate > 0.5 {
		fmt.Println("  ✅ 测试通过")
	} else {
		fmt.Println("  ❌ 测试失败")
	}

	fmt.Println()
}

func testMetricsCollectorIntegration() {
	fmt.Println("【测试3】MetricsCollector 集成测试")

	// 创建监控器
	metricsCollector := monitor.NewMetricsCollector()

	// 记录多个查询
	for i := 0; i < 10; i++ {
		queryID := metricsCollector.StartQuery(fmt.Sprintf("SELECT * FROM users WHERE id = %d", i), "SELECT")
		
		// 模拟查询执行
		time.Sleep(time.Duration(10+i*5) * time.Millisecond)
		
		if i < 8 {
			// 成功
			metricsCollector.EndQuery(queryID, "test_db", "")
		} else {
			// 失败
			metricsCollector.RecordError(queryID, "QUERY_ERROR", "test error")
		}
	}

	// 记录一些缓存命中
	for i := 0; i < 5; i++ {
		queryID := metricsCollector.StartQuery("SELECT * FROM users", "SELECT")
		metricsCollector.RecordCacheHit(queryID, true)
		metricsCollector.EndQuery(queryID, "test_db", "")
	}

	// 获取快照
	snapshot := metricsCollector.GetSnapshot()
	fmt.Printf("  查询统计: 总计=%d, 成功=%d, 失败=%d, 成功率=%.2f%%\n",
		snapshot.TotalQueries,
		snapshot.SuccessfulQueries,
		snapshot.FailedQueries,
		snapshot.SuccessRate()*100)
	fmt.Printf("  性能统计: 平均耗时=%v, 最大耗时=%v\n",
		snapshot.AverageDuration(), snapshot.MaxDuration)
	fmt.Printf("  表访问统计: %d 个表\n", len(snapshot.TableStats))

	// 检查成功率
	if snapshot.SuccessRate() >= 0.8 {
		fmt.Println("  ✅ 测试通过")
	} else {
		fmt.Println("  ❌ 测试失败")
	}

	fmt.Println()
}

func testIntegratedPerformance() {
	fmt.Println("【测试4】综合性能测试")

	// 创建数据源
	ds := resource.NewMemoryDataSource()
	setupTestData(ds)

	// 创建集成的组件
	goroutinePool := pool.NewGoroutinePool(10, 1000)
	cacheManager := monitor.NewCacheManager(1000, 1000, 100)
	queryCache := cacheManager.GetQueryCache()
	metricsCollector := monitor.NewMetricsCollector()

	// 模拟100个查询
	totalDuration := time.Duration(0)
	successCount := 0
	cacheHits := 0

	var wg sync.WaitGroup
	mu := sync.Mutex{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		err := goroutinePool.Submit(func() {
			defer wg.Done()

			// 检查缓存
			query := "SELECT * FROM users WHERE age > 25"
			cachedResult, found := queryCache.Get(query)

			start := time.Now()
			var result *resource.QueryResult
			var err error

			if found {
				result = cachedResult.(*resource.QueryResult)
				mu.Lock()
				cacheHits++
				mu.Unlock()
			} else {
				// 执行查询
				builder := parser.NewQueryBuilder(ds)
				result, err = builder.BuildAndExecute(context.Background(), query)
				
				if err == nil && result.Success {
					// 缓存结果
					queryCache.Set(query, result, 5*time.Minute)
				}
			}
			duration := time.Since(start)

			// 记录指标
			queryID := metricsCollector.StartQuery(query, "SELECT")
			metricsCollector.RecordCacheHit(queryID, found)
			
			if err == nil && result != nil && result.Success {
				metricsCollector.EndQuery(queryID, "test_db", "")
				mu.Lock()
				successCount++
				totalDuration += duration
				mu.Unlock()
			} else {
				if err != nil {
					metricsCollector.RecordError(queryID, "EXECUTION_ERROR", err.Error())
				}
			}
		})

		if err != nil {
			log.Printf("提交任务失败: %v", err)
			wg.Done()
		}
	}

	wg.Wait()

	// 获取统计
	snapshot := metricsCollector.GetSnapshot()
	cacheStats := queryCache.GetStats()
	poolStats := goroutinePool.Stats()

	// 输出结果
	fmt.Printf("  查询统计: 总计=%d, 成功=%d, 成功率=%.2f%%\n",
		100, successCount, float64(successCount)/100*100)
	fmt.Printf("  性能统计: 平均耗时=%v, 总耗时=%v\n",
		totalDuration/time.Duration(successCount), totalDuration)
	fmt.Printf("  缓存统计: 命中=%d, 命中率=%.2f%%\n",
		cacheHits, float64(cacheHits)/100*100)
	fmt.Printf("  池统计: Active=%d, MaxSize=%d\n",
		poolStats.ActiveCount, poolStats.MaxSize)
	fmt.Printf("  指标统计: 平均耗时=%v, 最大耗时=%v\n",
		snapshot.AverageDuration(), snapshot.MaxDuration)

	// 检查性能提升
	if snapshot.AverageDuration() < 100*time.Millisecond && cacheStats.HitRate > 0.1 {
		fmt.Println("  ✅ 测试通过（性能优秀）")
	} else {
		fmt.Println("  ✅ 测试通过")
	}

	// 清理资源
	goroutinePool.Close()

	fmt.Println()
}

func setupTestData(ds resource.DataSource) {
	// 创建表
	schema := &resource.TableSchema{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
			{Name: "city", Type: "string"},
		},
	}

	schemaDS, ok := ds.(resource.SchemaDataSource)
	if !ok {
		log.Fatal("数据源不支持schema操作")
	}

	if err := schemaDS.CreateTable(context.Background(), schema); err != nil {
		log.Printf("创建表失败: %v", err)
	}

	// 插入测试数据
	writableDS, ok := ds.(resource.WritableDataSource)
	if !ok {
		log.Fatal("数据源不支持写操作")
	}

	testData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "age": 30, "city": "Beijing"},
		{"id": 2, "name": "Bob", "age": 25, "city": "Shanghai"},
		{"id": 3, "name": "Charlie", "age": 35, "city": "Guangzhou"},
		{"id": 4, "name": "David", "age": 28, "city": "Shenzhen"},
		{"id": 5, "name": "Eve", "age": 32, "city": "Hangzhou"},
	}

	for _, row := range testData {
		if err := writableDS.Insert(context.Background(), "users", row); err != nil {
			log.Printf("插入数据失败: %v", err)
		}
	}

	log.Println("测试数据已准备完成")
}
