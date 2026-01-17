package main

import (
	"context"
	"fmt"
	"time"

	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 性能测试完成版 ===\n")

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
		Name:   "test_table",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "value", Type: "int64"},
		},
	}
	dataSource.CreateTable(context.Background(), tableInfo)

	// 插入测试数据
	testData := make([]resource.Row, 10000)
	for i := 0; i < 10000; i++ {
		testData[i] = resource.Row{
			"id":    int64(i),
			"name":  fmt.Sprintf("Item%d", i),
			"value":  int64(i * 10),
		}
	}
	dataSource.Insert(context.Background(), "test_table", testData, nil)

	// 测试1: 基本查询性能
	test1BasicQuery(context.Background(), dataSource, metrics, cacheManager)

	// 测试2: 缓存效果
	test2CacheEffect(context.Background(), dataSource, metrics, cacheManager)

	// 测试3: 慢查询检测
	test3SlowQuery(context.Background(), dataSource, metrics, slowQueryAnalyzer)

	// 打印统计
	printStats(metrics, cacheManager, slowQueryAnalyzer)

	fmt.Println("\n=== 所有测试完成 ===")
}

func test1BasicQuery(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试1】基本查询性能")

	queryCache := cacheMgr.GetQueryCache()
	start := time.Now()

	for i := 0; i < 1000; i++ {
		cacheKey := fmt.Sprintf("q%d", i%50)
		if _, found := queryCache.Get(cacheKey); !found {
			options := &resource.QueryOptions{
				Limit: 100,
			}
			result, _ := ds.Query(ctx, "test_table", options)
			queryCache.Set(cacheKey, result, time.Minute*5)
		}

		metrics.RecordQuery(time.Microsecond*10, true, "test_table")
	}

	duration := time.Since(start)
	fmt.Printf("  1000次查询: %v (%.2f queries/sec)\n", duration, 1000.0/duration.Seconds())
}

func test2CacheEffect(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试2】缓存效果")

	queryCache := cacheMgr.GetQueryCache()

	// 无缓存测试
	metrics.Reset()
	start := time.Now()
	for i := 0; i < 500; i++ {
		options := &resource.QueryOptions{
			Filters: []resource.Filter{
				{Field: "value", Operator: ">", Value: int64(5000)},
			},
			Limit: 100,
		}
		ds.Query(ctx, "test_table", options)
		metrics.RecordQuery(time.Microsecond*5, true, "test_table")
	}
	noCacheDuration := time.Since(start)

	// 有缓存测试
	cacheKey := "cache_test"
	result, _ := ds.Query(ctx, "test_table", &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "value", Operator: ">", Value: int64(5000)},
		},
		Limit: 100,
	})
	queryCache.Set(cacheKey, result, time.Minute*10)

	metrics.Reset()
	start = time.Now()
	for i := 0; i < 500; i++ {
		if _, found := queryCache.Get(cacheKey); found {
			metrics.RecordQuery(time.Microsecond*1, true, "test_table")
		}
	}
	withCacheDuration := time.Since(start)

	stats := queryCache.GetStats()
	fmt.Printf("  无缓存: %v (%.2f queries/sec)\n", noCacheDuration, 500.0/noCacheDuration.Seconds())
	fmt.Printf("  有缓存: %v (%.2f queries/sec)\n", withCacheDuration, 500.0/withCacheDuration.Seconds())
	fmt.Printf("  缓存命中率: %.2f%%\n", stats.HitRate)
	fmt.Printf("  性能提升: %.2fx\n", float64(noCacheDuration)/float64(withCacheDuration))
}

func test3SlowQuery(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("【测试3】慢查询检测")

	for i := 0; i < 20; i++ {
		start := time.Now()

		// 模拟慢查询
		time.Sleep(time.Duration(150+i*20) * time.Millisecond)

		duration := time.Since(start)

		slowQuery.RecordSlowQuery(
			fmt.Sprintf("SELECT * FROM test_table WHERE id > %d", i*100),
			duration,
			"test_table",
			int64(10000-i*200),
		)

		metrics.RecordQuery(duration, true, "test_table")
	}

	analysis := slowQuery.AnalyzeSlowQueries()
	fmt.Printf("  慢查询总数: %d\n", analysis.TotalQueries)
	fmt.Printf("  平均时长: %v\n", analysis.AvgDuration)
	fmt.Printf("  最大时长: %v\n", analysis.MaxDuration)
}

func printStats(metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("\n【总体统计】")

	snapshot := metrics.GetSnapshot()
	fmt.Println("  查询指标:")
	fmt.Printf("    总查询数: %d\n", snapshot.QueryCount)
	fmt.Printf("    成功率: %.2f%%\n", snapshot.SuccessRate)
	fmt.Printf("    平均时长: %v\n", snapshot.AvgDuration)
	fmt.Printf("    慢查询数: %d\n", snapshot.SlowQueryCount)

	allStats := cacheMgr.GetStats()
	fmt.Println("\n  缓存统计:")
	for cacheName, stats := range allStats {
		fmt.Printf("    %s: 大小 %d/%d, 命中率 %.2f%%\n",
			cacheName, stats.Size, stats.MaxSize, stats.HitRate)
	}

	analysis := slowQuery.AnalyzeSlowQueries()
	fmt.Println("\n  慢查询统计:")
	fmt.Printf("    总数: %d\n", analysis.TotalQueries)
	fmt.Printf("    平均时长: %v\n", analysis.AvgDuration)

	recommendations := slowQuery.GetRecommendations()
	if len(recommendations) > 0 {
		fmt.Println("\n  优化建议:")
		for _, rec := range recommendations {
			fmt.Printf("    - %s\n", rec)
		}
	}
}
