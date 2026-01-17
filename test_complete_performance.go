package main

import (
	"context"
	"fmt"
	"time"

	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 完整性能测试 ===\n")

	// 创建监控组件
	metrics := monitor.NewMetricsCollector()
	slowQueryAnalyzer := monitor.NewSlowQueryAnalyzer(100*time.Millisecond, 1000)
	cacheManager := monitor.NewCacheManager(1000, 500, 100)

	// 创建数据源
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(context.Background())
	defer dataSource.Close(context.Background())

	// 创建测试表
	tableInfo := &resource.TableInfo{
		Name:   "performance_test",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "value", Type: "int64"},
			{Name: "category", Type: "string"},
		},
	}
	dataSource.CreateTable(context.Background(), tableInfo)

	// 插入测试数据
	testData := make([]resource.Row, 100000)
	for i := 0; i < 100000; i++ {
		testData[i] = resource.Row{
			"id":       int64(i),
			"name":     fmt.Sprintf("Item%d", i),
			"value":    int64(i * 10),
			"category": fmt.Sprintf("cat%d", i%10),
		}
	}
	dataSource.Insert(context.Background(), "performance_test", testData, nil)

	fmt.Println("【测试1】基本查询性能")
	testBasicQuery(dataSource, metrics, cacheManager)

	fmt.Println("【测试2】缓存性能影响")
	testCacheImpact(dataSource, metrics, cacheManager)

	fmt.Println("【测试3】慢查询检测")
	testSlowQueryDetection(dataSource, metrics, slowQueryAnalyzer)

	fmt.Println("【测试4】并发查询性能")
	testConcurrentQuery(dataSource, metrics)

	fmt.Println("【测试5】复杂查询性能")
	testComplexQuery(dataSource, metrics)

	// 打印总体统计
	printOverallMetrics(metrics, cacheManager, slowQueryAnalyzer)

	fmt.Println("\n=== 所有测试完成 ===")
}

func testBasicQuery(dataSource resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	ctx := context.Background()
	queryCache := cacheMgr.GetQueryCache()

	// 测试简单查询
	start := time.Now()
	for i := 0; i < 1000; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
		monitorCtx.TableName = "performance_test"
		monitorCtx.Start()

		cacheKey := fmt.Sprintf("query_simple_%d", i%100)
		if _, found := queryCache.Get(cacheKey); !found {
			options := &resource.QueryOptions{
				Limit: 100,
			}
			result, _ := dataSource.Query(ctx, "performance_test", options)
			queryCache.Set(cacheKey, result, time.Minute*5)
		}

		monitorCtx.End(true, 100, nil)
	}
	duration := time.Since(start)

	fmt.Printf("  1000次简单查询: %v\n", duration)
	fmt.Printf("  平均时长: %v\n", metrics.GetAvgDuration())
	fmt.Printf("  吞吐量: %.2f queries/sec\n", 1000.0/duration.Seconds())
}

func testCacheImpact(dataSource resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	ctx := context.Background()
	queryCache := cacheMgr.GetQueryCache()

	options := &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "category", Operator: "=", Value: "cat1"},
		},
		Limit: 100,
	}

	// 无缓存测试
	metrics.Reset()
	start := time.Now()
	for i := 0; i < 500; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
		monitorCtx.Start()
		dataSource.Query(ctx, "performance_test", options)
		monitorCtx.End(true, 100, nil)
	}
	noCacheDuration := time.Since(start)

	// 有缓存测试
	cacheKey := "cache_test_key"
	result, _ := dataSource.Query(ctx, "performance_test", options)
	queryCache.Set(cacheKey, result, time.Minute*10)

	metrics.Reset()
	start = time.Now()
	for i := 0; i < 500; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
		monitorCtx.Start()
		if _, found := queryCache.Get(cacheKey); found {
			monitorCtx.End(true, 100, nil)
		}
	}
	withCacheDuration := time.Since(start)

	stats := queryCache.GetStats()
	fmt.Printf("  无缓存: %v (%.2f queries/sec)\n", noCacheDuration, 500.0/noCacheDuration.Seconds())
	fmt.Printf("  有缓存: %v (%.2f queries/sec)\n", withCacheDuration, 500.0/withCacheDuration.Seconds())
	fmt.Printf("  缓存命中率: %.2f%%\n", stats.HitRate)
	fmt.Printf("  性能提升: %.2fx\n", float64(noCacheDuration)/float64(withCacheDuration))
}

func testSlowQueryDetection(dataSource resource.DataSource, metrics *monitor.MetricsCollector, slowQuery *monitor.SlowQueryAnalyzer) {
	ctx := context.Background()

	// 执行一些慢查询
	for i := 0; i < 20; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, slowQuery, "")
		monitorCtx.StartTime = time.Now()
		monitorCtx.TableName = "performance_test"

		// 模拟慢查询
		time.Sleep(time.Duration(150+i*10) * time.Millisecond)

		slowQuery.RecordSlowQuery(
			fmt.Sprintf("SELECT * FROM performance_test WHERE id > %d", i*1000),
			time.Since(monitorCtx.StartTime),
			"performance_test",
			int64(100000-i*2000),
		)

		metrics.RecordQuery(time.Since(monitorCtx.StartTime), true, "performance_test")
	}

	analysis := slowQuery.AnalyzeSlowQueries()
	fmt.Printf("  慢查询总数: %d\n", analysis.TotalQueries)
	fmt.Printf("  平均时长: %v\n", analysis.AvgDuration)
	fmt.Printf("  错误率: %.2f%%\n", float64(analysis.ErrorCount)/float64(analysis.TotalQueries)*100)

	recommendations := slowQuery.GetRecommendations()
	if len(recommendations) > 0 {
		fmt.Println("  优化建议:")
		for _, rec := range recommendations {
			fmt.Printf("    - %s\n", rec)
		}
	}
}

func testConcurrentQuery(dataSource resource.DataSource, metrics *monitor.MetricsCollector) {
	ctx := context.Background()

	concurrency := 10
	queriesPerWorker := 100

	start := time.Now()

	// 并发执行查询
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			for j := 0; j < queriesPerWorker; j++ {
				monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
				monitorCtx.TableName = "performance_test"
				monitorCtx.Start()

				options := &resource.QueryOptions{
					Filters: []resource.Filter{
						{Field: "value", Operator: ">", Value: int64(workerID*1000 + j*10)},
					},
					Limit: 100,
				}
				dataSource.Query(ctx, "performance_test", options)

				monitorCtx.End(true, 100, nil)
			}
		}(i)
	}

	// 等待所有查询完成
	time.Sleep(2 * time.Second)

	duration := time.Since(start)
	totalQueries := concurrency * queriesPerWorker

	fmt.Printf("  并发级别: %d\n", concurrency)
	fmt.Printf("  总查询数: %d\n", totalQueries)
	fmt.Printf("  耗时: %v\n", duration)
	fmt.Printf("  吞吐量: %.2f queries/sec\n", float64(totalQueries)/duration.Seconds())
}

func testComplexQuery(dataSource resource.DataSource, metrics *monitor.MetricsCollector) {
	ctx := context.Background()

	tests := []struct {
		name     string
		options  *resource.QueryOptions
		iterations int
	}{
		{
			name: "单条件过滤",
			options: &resource.QueryOptions{
				Filters: []resource.Filter{
					{Field: "value", Operator: ">", Value: int64(50000)},
				},
				Limit: 100,
			},
			iterations: 500,
		},
		{
			name: "多条件AND",
			options: &resource.QueryOptions{
				Filters: []resource.Filter{
					{Field: "value", Operator: ">", Value: int64(50000)},
					{Field: "category", Operator: "=", Value: "cat5"},
				},
				Limit: 100,
			},
			iterations: 500,
		},
		{
			name: "排序查询",
			options: &resource.QueryOptions{
				OrderBy: "value",
				Order:  "DESC",
				Limit:   100,
			},
			iterations: 500,
		},
		{
			name: "分页查询",
			options: &resource.QueryOptions{
				Limit:  100,
				Offset: 50000,
			},
			iterations: 500,
		},
	}

	for _, test := range tests {
		start := time.Now()
		for i := 0; i < test.iterations; i++ {
			monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
			monitorCtx.TableName = "performance_test"
			monitorCtx.Start()

			dataSource.Query(ctx, "performance_test", test.options)

			monitorCtx.End(true, 100, nil)
		}
		duration := time.Since(start)

		fmt.Printf("  %s: %v (%.2f queries/sec)\n",
			test.name, duration, float64(test.iterations)/duration.Seconds())
	}
}

func printOverallMetrics(metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("\n【总体统计】")

	// 查询指标
	snapshot := metrics.GetSnapshot()
	fmt.Println("  查询指标:")
	fmt.Printf("    总查询数: %d\n", snapshot.QueryCount)
	fmt.Printf("    成功查询: %d\n", snapshot.QuerySuccess)
	fmt.Printf("    失败查询: %d\n", snapshot.QueryError)
	fmt.Printf("    成功率: %.2f%%\n", snapshot.SuccessRate)
	fmt.Printf("    平均时长: %v\n", snapshot.AvgDuration)
	fmt.Printf("    慢查询数: %d\n", snapshot.SlowQueryCount)
	fmt.Printf("    运行时间: %v\n", snapshot.Uptime)

	// 错误统计
	errors := snapshot.ErrorCount
	if len(errors) > 0 {
		fmt.Println("\n  错误统计:")
		for errType, count := range errors {
			fmt.Printf("    %s: %d\n", errType, count)
		}
	}

	// 表访问统计
	tableAccess := snapshot.TableAccessCount
	if len(tableAccess) > 0 {
		fmt.Println("\n  表访问统计:")
		for tableName, count := range tableAccess {
			fmt.Printf("    %s: %d 次\n", tableName, count)
		}
	}

	// 缓存统计
	allStats := cacheMgr.GetStats()
	fmt.Println("\n  缓存统计:")
	for cacheName, stats := range allStats {
		fmt.Printf("    %s: 大小 %d/%d, 命中率 %.2f%%, 淘汰 %d\n",
			cacheName, stats.Size, stats.MaxSize, stats.HitRate, stats.Evictions)
	}

	// 慢查询统计
	slowAnalysis := slowQuery.AnalyzeSlowQueries()
	fmt.Println("\n  慢查询统计:")
	fmt.Printf("    总数: %d\n", slowAnalysis.TotalQueries)
	fmt.Printf("    平均时长: %v\n", slowAnalysis.AvgDuration)
	fmt.Printf("    最大时长: %v\n", slowAnalysis.MaxDuration)
	fmt.Printf("    错误数: %d\n", slowAnalysis.ErrorCount)

	if len(slowAnalysis.TableStats) > 0 {
		fmt.Println("\n  表级别慢查询:")
		for tableName, stats := range slowAnalysis.TableStats {
			fmt.Printf("    %s: %d 条, 平均 %v\n",
				tableName, stats.QueryCount, stats.AvgDuration)
		}
	}

	// 优化建议
	recommendations := slowQuery.GetRecommendations()
	if len(recommendations) > 0 {
		fmt.Println("\n  优化建议:")
		for _, rec := range recommendations {
			fmt.Printf("    - %s\n", rec)
		}
	}
}
