package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 性能优化基准测试 ===\n")

	// 测试1: 批量操作性能
	testBatchPerformance()

	// 测试2: 缓存性能
	testCachePerformance()

	// 测试3: 并发查询性能
	testConcurrentQueryPerformance()

	// 测试4: 索引性能
	testIndexPerformance()

	// 测试5: 内存使用优化
	testMemoryOptimization()

	fmt.Println("\n=== 所有测试完成 ===")
}

// testBatchPerformance 测试批量操作性能
func testBatchPerformance() {
	fmt.Println("【测试1】批量操作性能")

	ctx := context.Background()
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(ctx)
	defer dataSource.Close(ctx)

	// 创建测试表
	tableInfo := &resource.TableInfo{
		Name:   "batch_test",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "value", Type: "int64"},
		},
	}
	dataSource.CreateTable(ctx, tableInfo)

	// 测试单条插入
	start := time.Now()
	for i := 0; i < 1000; i++ {
		row := resource.Row{
			"id":    int64(i),
			"name":  fmt.Sprintf("Item%d", i),
			"value": int64(i * 10),
		}
		dataSource.Insert(ctx, "batch_test", []resource.Row{row}, nil)
	}
	singleInsertDuration := time.Since(start)

	// 清空表
	dataSource.TruncateTable(ctx, "batch_test")

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
				"name":  fmt.Sprintf("Item%d", idx),
				"value": int64(idx * 10),
			}
		}
		dataSource.Insert(ctx, "batch_test", batch, nil)
	}
	batchInsertDuration := time.Since(start)

	fmt.Printf("  单条插入 1000 条: %v (%.2f ops/sec)\n",
		singleInsertDuration, 1000.0/singleInsertDuration.Seconds())
	fmt.Printf("  批量插入 %d*%d 条: %v (%.2f ops/sec)\n",
		batchSize, batches, batchInsertDuration, 1000.0/batchInsertDuration.Seconds())
	fmt.Printf("  性能提升: %.2fx\n", float64(singleInsertDuration)/float64(batchInsertDuration))
	fmt.Println()
}

// testCachePerformance 测试缓存性能
func testCachePerformance() {
	fmt.Println("【测试2】缓存性能")

	ctx := context.Background()
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(ctx)
	defer dataSource.Close(ctx)

	// 创建测试表并插入数据
	tableInfo := &resource.TableInfo{
		Name:   "cache_test",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
		},
	}
	dataSource.CreateTable(ctx, tableInfo)

	data := make([]resource.Row, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = resource.Row{
			"id":   int64(i),
			"name": fmt.Sprintf("User%d", i),
		}
	}
	dataSource.Insert(ctx, "cache_test", data, nil)

	// 创建缓存
	cacheManager := monitor.NewCacheManager(1000, 1000, 100)
	queryCache := cacheManager.GetQueryCache()

	options := &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "id", Operator: ">", Value: int64(5000)},
		},
		Limit: 100,
	}

	// 无缓存查询
	start := time.Now()
	for i := 0; i < 100; i++ {
		dataSource.Query(ctx, "cache_test", options)
	}
	noCacheDuration := time.Since(start)

	// 有缓存查询
	cacheKey := "test_query"
	dataSource.Query(ctx, "cache_test", options)
	result, _ := dataSource.Query(ctx, "cache_test", options)
	queryCache.Set(cacheKey, result, time.Minute)

	start = time.Now()
	for i := 0; i < 100; i++ {
		queryCache.Get(cacheKey)
	}
	withCacheDuration := time.Since(start)

	stats := queryCache.GetStats()
	fmt.Printf("  无缓存查询 100 次: %v (%.2f queries/sec)\n",
		noCacheDuration, 100.0/noCacheDuration.Seconds())
	fmt.Printf("  有缓存查询 100 次: %v (%.2f queries/sec)\n",
		withCacheDuration, 100.0/withCacheDuration.Seconds())
	fmt.Printf("  缓存命中率: %.2f%%\n", stats.HitRate)
	fmt.Printf("  性能提升: %.2fx\n", float64(noCacheDuration)/float64(withCacheDuration))
	fmt.Println()
}

// testConcurrentQueryPerformance 测试并发查询性能
func testConcurrentQueryPerformance() {
	fmt.Println("【测试3】并发查询性能")

	ctx := context.Background()
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(ctx)
	defer dataSource.Close(ctx)

	// 创建测试表并插入数据
	tableInfo := &resource.TableInfo{
		Name:   "concurrent_test",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
		},
	}
	dataSource.CreateTable(ctx, tableInfo)

	data := make([]resource.Row, 50000)
	for i := 0; i < 50000; i++ {
		data[i] = resource.Row{
			"id":   int64(i),
			"name": fmt.Sprintf("User%d", i),
			"age":  int64(20 + i%60),
		}
	}
	dataSource.Insert(ctx, "concurrent_test", data, nil)

	// 测试不同并发级别
	concurrencyLevels := []int{1, 4, 8, 16, 32}

	for _, concurrency := range concurrencyLevels {
		var wg sync.WaitGroup
		wg.Add(concurrency)

		start := time.Now()

		for i := 0; i < concurrency; i++ {
			go func(workerID int) {
				defer wg.Done()

				options := &resource.QueryOptions{
					Filters: []resource.Filter{
						{Field: "age", Operator: ">", Value: int64(workerID % 60)},
					},
					Limit: 100,
				}

				for j := 0; j < 100; j++ {
					dataSource.Query(ctx, "concurrent_test", options)
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)
		totalQueries := concurrency * 100

		fmt.Printf("  并发级别 %d: %v (%.2f queries/sec, %.2f queries/sec/thread)\n",
			concurrency, duration,
			float64(totalQueries)/duration.Seconds(),
			float64(100)/duration.Seconds())
	}
	fmt.Println()
}

// testIndexPerformance 测试索引性能
func testIndexPerformance() {
	fmt.Println("【测试4】索引性能")

	ctx := context.Background()
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(ctx)
	defer dataSource.Close(ctx)

	// 创建索引管理器
	indexManager := optimizer.NewIndexManager()

	// 创建测试表并插入数据
	tableInfo := &resource.TableInfo{
		Name:   "index_test",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
			{Name: "email", Type: "string"},
		},
	}
	dataSource.CreateTable(ctx, tableInfo)

	data := make([]resource.Row, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = resource.Row{
			"id":    int64(i),
			"name":  fmt.Sprintf("User%d", i),
			"age":   int64(20 + i%50),
			"email": fmt.Sprintf("user%d@example.com", i),
		}
	}
	dataSource.Insert(ctx, "index_test", data)

	// 添加索引
	indexManager.AddIndex(&optimizer.Index{
		Name:       "idx_age",
		TableName:  "index_test",
		Columns:    []string{"age"},
		Unique:      false,
		Cardinality: 50,
	})

	optCtx := &optimizer.OptimizationContext{
		DataSource: dataSource,
		TableInfo:  map[string]*resource.TableInfo{"index_test": tableInfo},
		Stats:      make(map[string]*optimizer.Statistics),
	}

	perfOptimizer := optimizer.NewPerformanceOptimizer()

	// 测试不同查询的优化建议
	testCases := []struct {
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
			name: "按 name 过滤（无索引）",
			filters: []resource.Filter{
				{Field: "name", Operator: "LIKE", Value: "User1%"},
			},
		},
	}

	for _, tc := range testCases {
		fmt.Printf("  %s:\n", tc.name)

		optimization := perfOptimizer.OptimizeScan("index_test", tc.filters, optCtx)
		fmt.Printf("    优化建议: %s\n", optimization.Explain())

		// 执行查询测量性能
		start := time.Now()
		for i := 0; i < 100; i++ {
			dataSource.Query(ctx, "index_test", &resource.QueryOptions{
				Filters: tc.filters,
				Limit:   100,
			})
		}
		duration := time.Since(start)
		fmt.Printf("    查询性能: %v (%.2f queries/sec)\n",
			duration, 100.0/duration.Seconds())

		// 记录索引访问
		if optimization.UseIndex {
			indexManager.RecordIndexAccess(optimization.IndexName, duration/100)
		}
	}

	// 显示索引统计
	fmt.Println("\n  索引统计:")
	for tableName, indices := range map[string][]*optimizer.Index{
		"index_test": indexManager.GetIndices(tableName),
	} {
		for _, index := range indices {
			stats := indexManager.GetIndexStats(index.Name)
			if stats != nil {
				fmt.Printf("    %s: 命中 %d 次, 平均访问时间 %v\n",
					index.Name, stats.HitCount, stats.AvgAccessTime)
			}
		}
	}
	fmt.Println()
}

// testMemoryOptimization 测试内存使用优化
func testMemoryOptimization() {
	fmt.Println("【测试5】内存使用优化")

	ctx := context.Background()
	factory := resource.NewMemoryFactory()
	dataSource, _ := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
	})
	dataSource.Connect(ctx)
	defer dataSource.Close(ctx)

	// 创建测试表
	tableInfo := &resource.TableInfo{
		Name:   "memory_test",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "value", Type: "int64"},
		},
	}
	dataSource.CreateTable(ctx, tableInfo)

	// 测试大批量数据插入
	testSizes := []int{1000, 10000, 100000}

	for _, size := range testSizes {
		// 插入数据
		data := make([]resource.Row, size)
		for i := 0; i < size; i++ {
			data[i] = resource.Row{
				"id":    int64(i),
				"name":  fmt.Sprintf("Item%d", i),
				"value": int64(i),
			}
		}

		start := time.Now()
		dataSource.Insert(ctx, "memory_test", data, nil)
		insertDuration := time.Since(start)

		// 查询数据
		start = time.Now()
		for i := 0; i < 10; i++ {
			dataSource.Query(ctx, "memory_test", &resource.QueryOptions{
				Limit: 1000,
			})
		}
		queryDuration := time.Since(start)

		fmt.Printf("  数据量 %d:\n", size)
		fmt.Printf("    插入耗时: %v (%.2f ops/sec)\n",
			insertDuration, float64(size)/insertDuration.Seconds())
		fmt.Printf("    查询耗时: %v (%.2f queries/sec)\n",
			queryDuration, 10.0/queryDuration.Seconds())
		fmt.Printf("    每条记录插入: %.2f μs\n",
			float64(insertDuration.Microseconds())/float64(size))

		// 清空表
		dataSource.TruncateTable(ctx, "memory_test")
	}
	fmt.Println()
}

// 简单的内存统计函数
func getMemoryStats() {
	// TODO: 实现 runtime.MemStats 统计
	// 这里只是占位符，实际实现需要使用 runtime.ReadMemStats()
}
