package main

import (
	"fmt"
	"time"

	"mysql-proxy/mysql/monitor"
)

func main() {
	fmt.Println("=== 监控模块测试 ===\n")

	// 测试1: MetricsCollector
	testMetricsCollector()

	// 测试2: SlowQueryAnalyzer
	testSlowQueryAnalyzer()

	// 测试3: QueryCache
	testQueryCache()

	fmt.Println("\n=== 所有测试完成 ===")
}

func testMetricsCollector() {
	fmt.Println("【测试1】MetricsCollector")

	metrics := monitor.NewMetricsCollector()

	// 模拟查询
	for i := 0; i < 100; i++ {
		duration := time.Duration(10+i%50) * time.Millisecond
		success := i%10 != 0 // 90% 成功率
		metrics.RecordQuery(duration, success, "test_table")
	}

	// 添加一些错误
	metrics.RecordError("connection_error")
	metrics.RecordError("timeout_error")
	metrics.RecordError("connection_error")

	fmt.Printf("  总查询数: %d\n", metrics.GetQueryCount())
	fmt.Printf("  成功查询: %d\n", metrics.GetQuerySuccess())
	fmt.Printf("  失败查询: %d\n", metrics.GetQueryError())
	fmt.Printf("  成功率: %.2f%%\n", metrics.GetSuccessRate())
	fmt.Printf("  平均时长: %v\n", metrics.GetAvgDuration())
	fmt.Printf("  运行时间: %v\n", metrics.GetUptime())

	// 错误统计
	errors := metrics.GetAllErrors()
	fmt.Printf("  错误统计: %d 种错误\n", len(errors))
	for errType, count := range errors {
		fmt.Printf("    %s: %d\n", errType, count)
	}

	// 表访问统计
	tableAccess := metrics.GetAllTableAccessCount()
	fmt.Printf("  表访问统计: %d 个表\n", len(tableAccess))
	for tableName, count := range tableAccess {
		fmt.Printf("    %s: %d 次\n", tableName, count)
	}

	// 获取快照
	snapshot := metrics.GetSnapshot()
	fmt.Printf("\n  快照统计:\n")
	fmt.Printf("    查询总数: %d\n", snapshot.QueryCount)
	fmt.Printf("    成功率: %.2f%%\n", snapshot.SuccessRate)
	fmt.Println()
}

func testSlowQueryAnalyzer() {
	fmt.Println("【测试2】SlowQueryAnalyzer")

	analyzer := monitor.NewSlowQueryAnalyzer(100*time.Millisecond, 100)

	// 模拟慢查询
	for i := 0; i < 20; i++ {
		duration := time.Duration(150+i*20) * time.Millisecond
		rowCount := int64(1000 - i*20)
		sql := fmt.Sprintf("SELECT * FROM table%d WHERE id > %d", i%3, i*10)

		if i%5 == 4 {
			analyzer.RecordSlowQueryWithError(sql, duration, fmt.Sprintf("table%d", i%3), rowCount, "timeout")
		} else {
			analyzer.RecordSlowQuery(sql, duration, fmt.Sprintf("table%d", i%3), rowCount)
		}
	}

	fmt.Printf("  慢查询总数: %d\n", analyzer.GetSlowQueryCount())
	fmt.Printf("  慢查询阈值: %v\n", analyzer.GetThreshold())

	// 获取所有慢查询
	allQueries := analyzer.GetAllSlowQueries()
	fmt.Printf("  慢查询列表: %d 条\n", len(allQueries))

	if len(allQueries) > 0 {
		fmt.Printf("  最新慢查询:\n")
		for i, q := range allQueries {
			if i >= 3 {
				break
			}
			fmt.Printf("    [%d] SQL: %s\n", q.ID, q.SQL)
			fmt.Printf("        时长: %v, 行数: %d, 表: %s\n", q.Duration, q.RowCount, q.TableName)
			if q.Error != "" {
				fmt.Printf("        错误: %s\n", q.Error)
			}
		}
	}

	// 按表分组
	fmt.Printf("\n  按表统计:\n")
	for i := 0; i < 3; i++ {
		tableName := fmt.Sprintf("table%d", i)
		queries := analyzer.GetSlowQueriesByTable(tableName)
		fmt.Printf("    %s: %d 条慢查询\n", tableName, len(queries))
	}

	// 分析慢查询
	analysis := analyzer.AnalyzeSlowQueries()
	fmt.Printf("\n  慢查询分析:\n")
	fmt.Printf("    总查询数: %d\n", analysis.TotalQueries)
	fmt.Printf("    平均时长: %v\n", analysis.AvgDuration)
	fmt.Printf("    最大时长: %v\n", analysis.MaxDuration)
	fmt.Printf("    最小时长: %v\n", analysis.MinDuration)
	fmt.Printf("    总行数: %d\n", analysis.TotalRowCount)
	fmt.Printf("    错误数: %d\n", analysis.ErrorCount)

	// 获取优化建议
	recommendations := analyzer.GetRecommendations()
	fmt.Printf("\n  优化建议:\n")
	for _, rec := range recommendations {
		fmt.Printf("    - %s\n", rec)
	}

	fmt.Println()
}

func testQueryCache() {
	fmt.Println("【测试3】QueryCache")

	cache := monitor.NewQueryCache(100, time.Minute)

	// 设置缓存
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		cache.Set(key, value, time.Second*10)
	}

	// 读取缓存
	hits := 0
	misses := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i%60)
		if _, found := cache.Get(key); found {
			hits++
		} else {
			misses++
		}
	}

	stats := cache.GetStats()
	fmt.Printf("  缓存大小: %d/%d\n", stats.Size, stats.MaxSize)
	fmt.Printf("  命中次数: %d\n", stats.Hits)
	fmt.Printf("  未命中次数: %d\n", stats.Misses)
	fmt.Printf("  命中率: %.2f%%\n", stats.HitRate)
	fmt.Printf("  淘汰次数: %d\n", stats.Evictions)
	fmt.Printf("  最大TTL: %v\n", stats.MaxTTL)

	// 测试缓存管理器
	fmt.Printf("\n  缓存管理器测试:\n")
	cacheManager := monitor.NewCacheManager(100, 50, 20)

	queryCache := cacheManager.GetQueryCache()
	queryCache.Set("test_query", "test_result", time.Second*30)

	resultCache := cacheManager.GetResultCache()
	resultCache.Set("test_result", "result_data", time.Second*60)

	schemaCache := cacheManager.GetSchemaCache()
	schemaCache.Set("test_schema", "schema_info", time.Minute*5)

	allStats := cacheManager.GetStats()
	fmt.Printf("    查询缓存: %d/%d\n", allStats["query"].Size, allStats["query"].MaxSize)
	fmt.Printf("    结果缓存: %d/%d\n", allStats["result"].Size, allStats["result"].MaxSize)
	fmt.Printf("    Schema缓存: %d/%d\n", allStats["schema"].Size, allStats["schema"].MaxSize)

	fmt.Println()
}
