package main

import (
	"context"
	"fmt"
	"time"

	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/pool"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 性能优化集成示例 ===\n")

	// 创建监控组件
	metrics := monitor.NewMetricsCollector()
	slowQueryAnalyzer := monitor.NewSlowQueryAnalyzer(100*time.Millisecond, 1000)
	cacheManager := monitor.NewCacheManager(1000, 500, 100)

	// 创建索引管理器
	indexManager := optimizer.NewIndexManager()

	// 创建性能优化器
	perfOptimizer := optimizer.NewPerformanceOptimizer()

	// 创建连接管理器
	connManager := pool.NewConnectionManager()

	// 创建数据源
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(context.Background())
	defer dataSource.Close(context.Background())

	// 创建测试表
	tableInfo := &resource.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
			{Name: "email", Type: "string"},
		},
	}
	dataSource.CreateTable(context.Background(), tableInfo)

	// 添加索引
	indexManager.AddIndex(&optimizer.Index{
		Name:       "idx_users_age",
		TableName:  "users",
		Columns:    []string{"age"},
		Unique:      false,
		Cardinality: 50,
	})

	indexManager.AddIndex(&optimizer.Index{
		Name:       "idx_users_name",
		TableName:  "users",
		Columns:    []string{"name"},
		Unique:      true,
		Cardinality: 1000,
	})

	// 插入测试数据
	testData := make([]resource.Row, 10000)
	for i := 0; i < 10000; i++ {
		testData[i] = resource.Row{
			"id":    int64(i),
			"name":  fmt.Sprintf("User%d", i),
			"age":   int64(20 + i%50),
			"email": fmt.Sprintf("user%d@example.com", i),
		}
	}
	dataSource.Insert(context.Background(), "users", testData, nil)

	// 测试1: 基本查询优化
	testBasicQueryOptimization(context.Background(), dataSource, metrics, slowQueryAnalyzer, cacheManager)

	// 测试2: 索引优化
	testIndexOptimization(context.Background(), dataSource, metrics, indexManager, perfOptimizer)

	// 测试3: 批量操作优化
	testBatchOptimization(context.Background(), dataSource, metrics)

	// 测试4: 缓存优化
	testCacheOptimization(context.Background(), dataSource, metrics, cacheManager)

	// 测试5: 慢查询分析
	testSlowQueryAnalysis(context.Background(), dataSource, metrics, slowQueryAnalyzer)

	// 打印总体统计
	printOverallMetrics(metrics, cacheManager, indexManager)

	fmt.Println("\n=== 所有测试完成 ===")
}

func testBasicQueryOptimization(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, slowQuery *monitor.SlowQueryAnalyzer, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试1】基本查询优化")

	queryCache := cacheMgr.GetQueryCache()

	// 执行多个查询
	for i := 0; i < 100; i++ {
		// 创建监控上下文
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, slowQuery, "")
		monitorCtx.TableName = "users"
		monitorCtx.Start()

		// 检查缓存
		cacheKey := fmt.Sprintf("query_age_%d", i%30)
		if _, found := queryCache.Get(cacheKey); found {
			monitorCtx.End(true, 100, nil)
			continue
		}

		// 执行查询
		options := &resource.QueryOptions{
			Filters: []resource.Filter{
				{Field: "age", Operator: ">", Value: int64(i % 60)},
			},
			Limit: 100,
		}

		result, err := ds.Query(ctx, "users", options)
		success := err == nil
		rowCount := int64(0)
		if result != nil {
			rowCount = int64(len(result.Rows))
			// 缓存结果
			queryCache.Set(cacheKey, result, time.Minute*5)
		}

		monitorCtx.End(success, rowCount, err)
	}

	fmt.Printf("  总查询数: %d\n", metrics.GetQueryCount())
	fmt.Printf("  成功率: %.2f%%\n", metrics.GetSuccessRate())
	fmt.Printf("  平均时长: %v\n", metrics.GetAvgDuration())
	fmt.Printf("  慢查询数: %d\n", slowQuery.GetSlowQueryCount())

	cacheStats := queryCache.GetStats()
	fmt.Printf("  缓存命中率: %.2f%%\n", cacheStats.HitRate)
	fmt.Println()
}

func testIndexOptimization(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, indexManager *optimizer.IndexManager, perfOptimizer *optimizer.PerformanceOptimizer) {
	fmt.Println("【测试2】索引优化")

	// 创建优化上下文
	optCtx := &optimizer.OptimizationContext{
		DataSource: ds,
		TableInfo:  make(map[string]*resource.TableInfo),
		Stats:      make(map[string]*optimizer.Statistics),
	}

	tableInfo, _ := ds.GetTableInfo(ctx, "users")
	optCtx.TableInfo["users"] = tableInfo

	// 测试不同查询的优化建议
	tests := []struct {
		name    string
		filters []resource.Filter
	}{
		{
			name: "按 age 过滤（有索引）",
			filters: []resource.Filter{
				{Field: "age", Operator: ">", Value: int64(30)},
			},
		},
		{
			name: "按 name 过滤（有索引）",
			filters: []resource.Filter{
				{Field: "name", Operator: "LIKE", Value: "User1%"},
			},
		},
		{
			name: "按 email 过滤（无索引）",
			filters: []resource.Filter{
				{Field: "email", Operator: "LIKE", Value: "%@example.com"},
			},
		},
	}

	for _, test := range tests {
		fmt.Printf("  %s:\n", test.name)

		// 获取优化建议
		optimization := perfOptimizer.OptimizeScan("users", test.filters, optCtx)
		fmt.Printf("    %s\n", optimization.Explain())

		// 执行查询
		start := time.Now()
		for i := 0; i < 50; i++ {
			ds.Query(ctx, "users", &resource.QueryOptions{
				Filters: test.filters,
				Limit:   100,
			})
		}
		duration := time.Since(start)
		fmt.Printf("    查询性能: %v (%.2f queries/sec)\n",
			duration, 50.0/duration.Seconds())

		// 记录索引访问
		if optimization.UseIndex {
			indexManager.RecordIndexAccess(optimization.IndexName, duration/50)
		}
	}

	// 显示索引统计
	fmt.Println("\n  索引统计:")
	indices := indexManager.GetIndices("users")
	for _, index := range indices {
		stats := indexManager.GetIndexStats(index.Name)
		if stats != nil {
			fmt.Printf("    %s: 命中 %d 次, 平均访问时间 %v\n",
				index.Name, stats.HitCount, stats.AvgAccessTime)
		}
	}
	fmt.Println()
}

func testBatchOptimization(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector) {
	fmt.Println("【测试3】批量操作优化")

	// 清空表
	ds.TruncateTable(ctx, "users")

	// 测试单条插入
	start := time.Now()
	for i := 0; i < 1000; i++ {
		row := resource.Row{
			"id":    int64(i),
			"name":  fmt.Sprintf("User%d", i),
			"age":   int64(20 + i%50),
			"email": fmt.Sprintf("user%d@example.com", i),
		}
		ds.Insert(ctx, "users", []resource.Row{row}, nil)
	}
	singleInsertDuration := time.Since(start)

	// 清空表
	ds.TruncateTable(ctx, "users")

	// 测试批量插入
	batchSize := 100
	batches := 1000 / batchSize
	start = time.Now()
	for i := 0; i < batches; i++ {
		batch := make([]resource.Row, batchSize)
		for j := 0; j < batchSize; j++ {
			idx := i*batchSize + j
			batch[j] = resource.Row{
				"id":    int64(idx),
				"name":  fmt.Sprintf("User%d", idx),
				"age":   int64(20 + idx%50),
				"email": fmt.Sprintf("user%d@example.com", idx),
			}
		}
		ds.Insert(ctx, "users", batch, nil)
	}
	batchInsertDuration := time.Since(start)

	fmt.Printf("  单条插入 1000 条: %v (%.2f rows/sec)\n",
		singleInsertDuration, 1000.0/singleInsertDuration.Seconds())
	fmt.Printf("  批量插入 %d*%d 条: %v (%.2f rows/sec)\n",
		batchSize, batches, batchInsertDuration, 1000.0/batchInsertDuration.Seconds())
	fmt.Printf("  性能提升: %.2fx\n", float64(singleInsertDuration)/float64(batchInsertDuration))
	fmt.Println()
}

func testCacheOptimization(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试4】缓存优化")

	queryCache := cacheMgr.GetQueryCache()
	resultCache := cacheMgr.GetResultCache()

	options := &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "age", Operator: ">", Value: int64(30)},
		},
		Limit: 100,
	}

	// 第一次查询（无缓存）
	monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
	monitorCtx.Start()
	result, _ := ds.Query(ctx, "users", options)
	monitorCtx.End(true, int64(len(result.Rows)), nil)

	// 缓存结果
	resultCache.Set("cache_key_1", result, time.Minute*10)

	// 后续查询（使用缓存）
	for i := 0; i < 100; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
		monitorCtx.Start()

		if _, found := resultCache.Get("cache_key_1"); found {
			monitorCtx.End(true, 100, nil)
			continue
		}

		monitorCtx.End(true, 100, nil)
	}

	resultStats := resultCache.GetStats()
	fmt.Printf("  结果缓存命中率: %.2f%%\n", resultStats.HitRate)
	fmt.Printf("  缓存大小: %d/%d\n", resultStats.Size, resultStats.MaxSize)
	fmt.Printf("  总查询数: %d\n", metrics.GetQueryCount())
	fmt.Printf("  平均时长: %v\n", metrics.GetAvgDuration())
	fmt.Println()
}

func testSlowQueryAnalysis(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("【测试5】慢查询分析")

	// 执行一些慢查询
	for i := 0; i < 20; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, slowQuery, "")
		monitorCtx.StartTime = time.Now()
		monitorCtx.TableName = "users"

		// 模拟慢查询
		time.Sleep(time.Duration(150+i*20) * time.Millisecond)

		slowQuery.RecordSlowQuery(
			fmt.Sprintf("SELECT * FROM users WHERE age > %d", i*2),
			time.Since(monitorCtx.StartTime),
			"users",
			int64(10000-i*200),
		)

		metrics.RecordQuery(time.Since(monitorCtx.StartTime), true, "users")
	}

	// 获取慢查询分析
	analysis := slowQuery.AnalyzeSlowQueries()
	fmt.Printf("  慢查询总数: %d\n", analysis.TotalQueries)
	fmt.Printf("  平均时长: %v\n", analysis.AvgDuration)
	fmt.Printf("  最大时长: %v\n", analysis.MaxDuration)
	fmt.Printf("  错误率: %.2f%%\n",
		float64(analysis.ErrorCount)/float64(analysis.TotalQueries)*100)

	// 表级别统计
	fmt.Println("\n  表级别统计:")
	for tableName, stats := range analysis.TableStats {
		fmt.Printf("    %s: %d 条慢查询, 平均时长 %v\n",
			tableName, stats.QueryCount, stats.AvgDuration)
	}

	// 优化建议
	recommendations := slowQuery.GetRecommendations()
	fmt.Println("\n  优化建议:")
	for _, rec := range recommendations {
		fmt.Printf("    - %s\n", rec)
	}
	fmt.Println()
}

func printOverallMetrics(metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager, indexManager *optimizer.IndexManager) {
	fmt.Println("【总体统计】")

	// 查询指标
	fmt.Println("  查询指标:")
	snapshot := metrics.GetSnapshot()
	fmt.Printf("    总查询数: %d\n", snapshot.QueryCount)
	fmt.Printf("    成功率: %.2f%%\n", snapshot.SuccessRate)
	fmt.Printf("    平均时长: %v\n", snapshot.AvgDuration)
	fmt.Printf("    慢查询数: %d\n", snapshot.SlowQueryCount)

	// 缓存统计
	fmt.Println("\n  缓存统计:")
	allStats := cacheMgr.GetStats()
	for cacheName, stats := range allStats {
		fmt.Printf("    %s: 大小 %d/%d, 命中率 %.2f%%\n",
			cacheName, stats.Size, stats.MaxSize, stats.HitRate)
	}

	// 索引统计
	fmt.Println("\n  索引统计:")
	for tableName, indices := range map[string][]*optimizer.Index{
		"users": indexManager.GetIndices(tableName),
	} {
		for _, index := range indices {
			stats := indexManager.GetIndexStats(index.Name)
			if stats != nil && stats.HitCount > 0 {
				fmt.Printf("    %s: 命中 %d 次, 平均访问时间 %v\n",
					index.Name, stats.HitCount, stats.AvgAccessTime)
			}
		}
	}
}
