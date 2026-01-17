package main

import (
	"context"
	"fmt"
	"sync/atomic"
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
		Name:   "perf_test",
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
	testData := make([]resource.Row, 50000)
	for i := 0; i < 50000; i++ {
		testData[i] = resource.Row{
			"id":       int64(i),
			"name":     fmt.Sprintf("Item%d", i),
			"value":    int64(i * 10),
			"category": fmt.Sprintf("cat%d", i%10),
		}
	}
	dataSource.Insert(context.Background(), "perf_test", testData, nil)

	// 测试1: 基本查询性能
	testBasicQuery(context.Background(), dataSource, metrics, cacheManager)

	// 测试2: 缓存性能影响
	testCacheImpact(context.Background(), dataSource, metrics, cacheManager)

	// 测试3: 慢查询检测
	testSlowQueryDetection(context.Background(), metrics, slowQueryAnalyzer)

	// 测试4: 多种查询类型
	testQueryTypes(context.Background(), dataSource, metrics)

	// 打印总体统计
	printOverallMetrics(metrics, cacheManager, slowQueryAnalyzer)

	fmt.Println("\n=== 所有测试完成 ===")
}

func testBasicQuery(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试1】基本查询性能")

	queryCache := cacheMgr.GetQueryCache()

	var totalDuration time.Duration
	var queryCount int64

	// 执行1000次查询
	for i := 0; i < 1000; i++ {
		start := time.Now()

		cacheKey := fmt.Sprintf("query_%d", i%100)
		var result *resource.QueryResult
		if cached, found := queryCache.Get(cacheKey); found {
			result = cached.(*resource.QueryResult)
		} else {
			options := &resource.QueryOptions{
				Limit: 100,
			}
			result, _ = ds.Query(ctx, "perf_test", options)
			queryCache.Set(cacheKey, result, time.Minute*5)
		}

		duration := time.Since(start)
		totalDuration += duration
		atomic.AddInt64(&queryCount, 1)

		metrics.RecordQuery(duration, true, "perf_test")
	}

	avgDuration := totalDuration / time.Duration(queryCount)
	fmt.Printf("  总查询数: %d\n", queryCount)
	fmt.Printf("  平均时长: %v\n", avgDuration)
	fmt.Printf("  吞吐量: %.2f queries/sec\n", float64(queryCount)/totalDuration.Seconds())
}

func testCacheImpact(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试2】缓存性能影响")

	queryCache := cacheMgr.GetQueryCache()

	options := &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "category", Operator: "=", Value: "cat5"},
		},
		Limit: 100,
	}

	// 无缓存测试
	metrics.Reset()
	start := time.Now()
	for i := 0; i < 500; i++ {
		metrics.StartQuery()
		ds.Query(ctx, "perf_test", options)
		metrics.EndQuery()
	}
	noCacheDuration := time.Since(start)

	// 有缓存测试
	result, _ := ds.Query(ctx, "perf_test", options)
	cacheKey := "cache_test_key"
	queryCache.Set(cacheKey, result, time.Minute*10)

	metrics.Reset()
	start = time.Now()
	for i := 0; i < 500; i++ {
		metrics.StartQuery()
		if _, found := queryCache.Get(cacheKey); found {
			metrics.EndQuery()
			metrics.RecordQuery(time.Microsecond, true, "perf_test")
		}
	}
	withCacheDuration := time.Since(start)
	withCacheCount := metrics.GetQueryCount()

	stats := queryCache.GetStats()
	performanceGain := float64(noCacheDuration) / float64(withCacheDuration)

	fmt.Printf("  无缓存: %v (%.2f queries/sec)\n", noCacheDuration, 500.0/noCacheDuration.Seconds())
	fmt.Printf("  有缓存: %v (%.2f queries/sec)\n", withCacheDuration, 500.0/withCacheDuration.Seconds())
	fmt.Printf("  缓存命中率: %.2f%%\n", stats.HitRate)
	fmt.Printf("  性能提升: %.2fx\n", performanceGain)
}

func testSlowQueryDetection(ctx context.Context, metrics *monitor.MetricsCollector, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("【测试3】慢查询检测")

	// 执行一些慢查询
	for i := 0; i < 20; i++ {
		start := time.Now()

		// 模拟慢查询
		time.Sleep(time.Duration(150+i*20) * time.Millisecond)

		duration := time.Since(start)

		slowQuery.RecordSlowQuery(
			fmt.Sprintf("SELECT * FROM perf_test WHERE id > %d", i*100),
			duration,
			"perf_test",
			int64(50000-i*1000),
		)

		metrics.RecordQuery(duration, true, "perf_test")
	}

	analysis := slowQuery.AnalyzeSlowQueries()
	fmt.Printf("  慢查询总数: %d\n", analysis.TotalQueries)
	fmt.Printf("  平均时长: %v\n", analysis.AvgDuration)
	fmt.Printf("  最大时长: %v\n", analysis.MaxDuration)
	fmt.Printf("  错误率: %.2f%%\n",
		float64(analysis.ErrorCount)/float64(analysis.TotalQueries)*100)

	recommendations := slowQuery.GetRecommendations()
	if len(recommendations) > 0 {
		fmt.Println("  优化建议:")
		for _, rec := range recommendations {
			fmt.Printf("    - %s\n", rec)
		}
	}
}

func testQueryTypes(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector) {
	fmt.Println("【测试4】多种查询类型")

	tests := []struct {
		name     string
		options  *resource.QueryOptions
		iterate int
	}{
		{
			name: "简单查询",
			options: &resource.QueryOptions{
				Limit: 100,
			},
			iterate: 200,
		},
		{
			name: "条件过滤",
			options: &resource.QueryOptions{
				Filters: []resource.Filter{
					{Field: "value", Operator: ">", Value: int64(250000)},
				},
				Limit: 100,
			},
			iterate: 200,
		},
		{
			name: "排序查询",
			options: &resource.QueryOptions{
				OrderBy: "value",
				Order:   "DESC",
				Limit:   100,
			},
			iterate: 200,
		},
		{
			name: "分页查询",
			options: &resource.QueryOptions{
				Limit:  100,
				Offset: 25000,
			},
			iterate: 200,
		},
	}

	for _, test := range tests {
		start := time.Now()
		for i := 0; i < test.iterate; i++ {
			metrics.StartQuery()
			ds.Query(ctx, "perf_test", test.options)
			metrics.EndQuery()
			metrics.RecordQuery(time.Since(start), true, "perf_test")
		}
		duration := time.Since(start)

		fmt.Printf("  %s: %v (%.2f queries/sec)\n",
			test.name, duration, float64(test.iterate)/duration.Seconds())
	}
}

func printOverallMetrics(metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("\n【总体统计】")

	// 查询指标
	snapshot := metrics.GetSnapshot()
	fmt.Println("  查询指标:")
	fmt.Printf("    总查询数: %d\n", snapshot.QueryCount)
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
		fmt.Printf("    %s: 大小 %d/%d, 命中率 %.2f%%\n",
			cacheName, stats.Size, stats.MaxSize, stats.HitRate)
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
