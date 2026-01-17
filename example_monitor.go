package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	// 创建监控组件
	metrics := monitor.NewMetricsCollector()
	slowQueryAnalyzer := monitor.NewSlowQueryAnalyzer(100*time.Millisecond, 1000)
	cacheManager := monitor.NewCacheManager(1000, 500, 100)

	// 创建性能优化器
	perfOptimizer := optimizer.NewPerformanceOptimizer()

	// 创建数据源
	factory := resource.NewMemoryFactory()
	dataSource, err := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 连接数据源
	ctx := context.Background()
	if err := dataSource.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer dataSource.Close(ctx)

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
	if err := dataSource.CreateTable(ctx, tableInfo); err != nil {
		log.Fatal(err)
	}

	// 插入测试数据
	testData := make([]resource.Row, 100)
	for i := 0; i < 100; i++ {
		testData[i] = resource.Row{
			"id":    int64(i + 1),
			"name":  fmt.Sprintf("User%d", i+1),
			"age":   int64(20 + i%50),
			"email": fmt.Sprintf("user%d@example.com", i+1),
		}
	}
	if _, err := dataSource.Insert(ctx, "users", testData, nil); err != nil {
		log.Fatal(err)
	}

	// 添加测试索引
	indexManager := optimizer.NewIndexManager()
	indexManager.AddIndex(&optimizer.Index{
		Name:       "idx_users_age",
		TableName:  "users",
		Columns:    []string{"age"},
		Unique:      false,
		Cardinality: 50,
	})

	fmt.Println("=== 性能监控和优化测试 ===\n")

	// 测试1: 基本查询监控
	testBasicQuery(ctx, dataSource, metrics, slowQueryAnalyzer, cacheManager)

	// 测试2: 缓存效果测试
	testCacheEffect(ctx, dataSource, metrics, cacheManager)

	// 测试3: 慢查询分析
	testSlowQueryAnalysis(ctx, dataSource, slowQueryAnalyzer)

	// 测试4: 索引优化建议
	testIndexOptimization(ctx, dataSource, indexManager, perfOptimizer)

	// 测试5: 性能指标统计
	printPerformanceMetrics(metrics, cacheManager)

	fmt.Println("\n=== 测试完成 ===")
}

// testBasicQuery 测试基本查询监控
func testBasicQuery(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, slowQuery *monitor.SlowQueryAnalyzer, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试1】基本查询监控")

	// 执行多个查询
	for i := 0; i < 10; i++ {
		// 创建监控上下文
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, slowQuery, "")
		monitorCtx.TableName = "users"
		monitorCtx.Start()

		// 执行查询
		options := &resource.QueryOptions{
			Filters: []resource.Filter{
				{Field: "age", Operator: ">", Value: int64(30)},
			},
			Limit: 10,
		}

		result, err := ds.Query(ctx, "users", options)
		success := err == nil
		rowCount := int64(0)
		if result != nil {
			rowCount = int64(len(result.Rows))
		}

		// 结束监控
		monitorCtx.End(success, rowCount, err)

		// 模拟一些慢查询
		if i%3 == 0 {
			time.Sleep(150 * time.Millisecond)
		}
	}

	fmt.Printf("  查询总数: %d\n", metrics.GetQueryCount())
	fmt.Printf("  成功率: %.2f%%\n", metrics.GetSuccessRate())
	fmt.Printf("  平均时长: %v\n", metrics.GetAvgDuration())
	fmt.Printf("  慢查询数: %d\n", slowQuery.GetSlowQueryCount())
	fmt.Println()
}

// testCacheEffect 测试缓存效果
func testCacheEffect(ctx context.Context, ds resource.DataSource, metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试2】缓存效果测试")

	queryCache := cacheMgr.GetQueryCache()

	// 第一次查询（缓存未命中）
	monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
	monitorCtx.TableName = "users"
	monitorCtx.Start()

	cacheKey := monitor.GenerateKey(&monitor.CacheKey{
		SQL: "SELECT * FROM users WHERE age > 30 LIMIT 10",
	})

	if _, found := queryCache.Get(cacheKey); !found {
		options := &resource.QueryOptions{
			Filters: []resource.Filter{
				{Field: "age", Operator: ">", Value: int64(30)},
			},
			Limit: 10,
		}
		result, _ := ds.Query(ctx, "users", options)
		queryCache.Set(cacheKey, result, time.Minute)
	}

	monitorCtx.End(true, 10, nil)

	// 第二次查询（缓存命中）
	monitorCtx2 := monitor.NewMonitorContext(ctx, metrics, nil, "")
	monitorCtx2.TableName = "users"
	monitorCtx2.Start()

	if _, found := queryCache.Get(cacheKey); found {
		// 缓存命中
	}

	monitorCtx2.End(true, 10, nil)

	// 执行多次以测试缓存命中率
	for i := 0; i < 10; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, metrics, nil, "")
		monitorCtx.TableName = "users"
		monitorCtx.Start()

		if _, found := queryCache.Get(cacheKey); found {
			// 缓存命中
		}

		monitorCtx.End(true, 10, nil)
	}

	stats := queryCache.GetStats()
	fmt.Printf("  缓存大小: %d/%d\n", stats.Size, stats.MaxSize)
	fmt.Printf("  缓存命中: %d\n", stats.Hits)
	fmt.Printf("  缓存未命中: %d\n", stats.Misses)
	fmt.Printf("  命中率: %.2f%%\n", stats.HitRate)
	fmt.Printf("  淘汰次数: %d\n", stats.Evictions)
	fmt.Println()
}

// testSlowQueryAnalysis 测试慢查询分析
func testSlowQueryAnalysis(ctx context.Context, ds resource.DataSource, slowQuery *monitor.SlowQueryAnalyzer) {
	fmt.Println("【测试3】慢查询分析")

	// 执行一些慢查询
	for i := 0; i < 5; i++ {
		monitorCtx := monitor.NewMonitorContext(ctx, nil, slowQuery, "")
		monitorCtx.StartTime = time.Now()
		monitorCtx.TableName = "users"

		// 模拟慢查询
		time.Sleep(200 * time.Millisecond + time.Duration(i*50)*time.Millisecond)

		// 记录慢查询
		slowQuery.RecordSlowQuery(
			fmt.Sprintf("SELECT * FROM users WHERE age > %d", 10+i*10),
			time.Since(monitorCtx.StartTime),
			"users",
			int64(100-i*10),
		)
	}

	// 获取慢查询分析
	analysis := slowQuery.AnalyzeSlowQueries()
	fmt.Printf("  慢查询总数: %d\n", analysis.TotalQueries)
	fmt.Printf("  平均时长: %v\n", analysis.AvgDuration)
	fmt.Printf("  最大时长: %v\n", analysis.MaxDuration)
	fmt.Printf("  最小时长: %v\n", analysis.MinDuration)
	fmt.Printf("  总扫描行数: %d\n", analysis.TotalRowCount)
	fmt.Printf("  错误数: %d\n", analysis.ErrorCount)

	// 表级别统计
	fmt.Println("\n  表级别统计:")
	for tableName, stats := range analysis.TableStats {
		fmt.Printf("    表 %s:\n", tableName)
		fmt.Printf("      查询数: %d\n", stats.QueryCount)
		fmt.Printf("      平均时长: %v\n", stats.AvgDuration)
		fmt.Printf("      最大时长: %v\n", stats.MaxDuration)
		fmt.Printf("      总行数: %d\n", stats.TotalRowCount)
	}

	// 优化建议
	recommendations := slowQuery.GetRecommendations()
	fmt.Println("\n  优化建议:")
	for _, rec := range recommendations {
		fmt.Printf("    - %s\n", rec)
	}
	fmt.Println()
}

// testIndexOptimization 测试索引优化
func testIndexOptimization(ctx context.Context, ds resource.DataSource, indexManager *optimizer.IndexManager, perfOptimizer *optimizer.PerformanceOptimizer) {
	fmt.Println("【测试4】索引优化建议")

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
			name: "age 列过滤",
			filters: []resource.Filter{
				{Field: "age", Operator: ">", Value: int64(30)},
			},
		},
		{
			name: "name 列过滤",
			filters: []resource.Filter{
				{Field: "name", Operator: "LIKE", Value: "User%"},
			},
		},
		{
			name: "age + email 组合过滤",
			filters: []resource.Filter{
				{Field: "age", Operator: ">=", Value: int64(25)},
				{Field: "email", Operator: "LIKE", Value: "%@example.com"},
			},
		},
	}

	for _, test := range tests {
		fmt.Printf("  查询: %s\n", test.name)

		// 优化扫描操作
		optimization := perfOptimizer.OptimizeScan("users", test.filters, optCtx)
		fmt.Printf("    %s\n", optimization.Explain())

		// 显示索引信息
		if optimization.UseIndex {
			indexStats := indexManager.GetIndexStats(optimization.IndexName)
			if indexStats != nil {
				fmt.Printf("    索引统计: 命中 %d 次, 平均访问时间 %v\n",
					indexStats.HitCount, indexStats.AvgAccessTime)
			}
		}
	}
	fmt.Println()
}

// printPerformanceMetrics 打印性能指标
func printPerformanceMetrics(metrics *monitor.MetricsCollector, cacheMgr *monitor.CacheManager) {
	fmt.Println("【测试5】性能指标统计")

	// 查询指标
	fmt.Println("  查询指标:")
	fmt.Printf("    总查询数: %d\n", metrics.GetQueryCount())
	fmt.Printf("    成功查询: %d\n", metrics.GetQuerySuccess())
	fmt.Printf("    失败查询: %d\n", metrics.GetQueryError())
	fmt.Printf("    成功率: %.2f%%\n", metrics.GetSuccessRate())
	fmt.Printf("    平均时长: %v\n", metrics.GetAvgDuration())
	fmt.Printf("    当前活跃: %d\n", metrics.GetActiveQueries())
	fmt.Printf("    运行时间: %v\n", metrics.GetUptime())

	// 错误统计
	errors := metrics.GetAllErrors()
	if len(errors) > 0 {
		fmt.Println("\n  错误统计:")
		for errType, count := range errors {
			fmt.Printf("    %s: %d\n", errType, count)
		}
	}

	// 表访问统计
	tableAccess := metrics.GetAllTableAccessCount()
	if len(tableAccess) > 0 {
		fmt.Println("\n  表访问统计:")
		for tableName, count := range tableAccess {
			fmt.Printf("    %s: %d 次\n", tableName, count)
		}
	}

	// 缓存统计
	fmt.Println("\n  缓存统计:")
	allStats := cacheMgr.GetStats()
	for cacheName, stats := range allStats {
		fmt.Printf("    %s 缓存:\n", cacheName)
		fmt.Printf("      大小: %d/%d\n", stats.Size, stats.MaxSize)
		fmt.Printf("      命中率: %.2f%%\n", stats.HitRate)
		fmt.Printf("      淘汰次数: %d\n", stats.Evictions)
	}
	fmt.Println()
}
