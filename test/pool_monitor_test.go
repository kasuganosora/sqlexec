package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"mysql-proxy/mysql"
	"mysql-proxy/mysql/monitor"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/pool"
	"mysql-proxy/mysql/resource"
)

// TestGoroutinePool 测试GoroutinePool
func TestGoroutinePool(t *testing.T) {
	goroutinePool := pool.NewGoroutinePool(5, 100)

	taskCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		err := goroutinePool.Submit(func() {
			mu.Lock()
			taskCount++
			mu.Unlock()
		})
		if err != nil {
			t.Errorf("提交任务失败: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	if taskCount != 10 {
		t.Errorf("期望10个任务执行, 实际 %d", taskCount)
	}

	stats := goroutinePool.Stats()
	if stats.MaxSize != 5 {
		t.Errorf("期望MaxSize=5, 实际 %d", stats.MaxSize)
	}

	goroutinePool.Close()
}

// TestObjectPool 测试ObjectPool
func TestObjectPool(t *testing.T) {
	objectPool := pool.NewObjectPool(10)

	// 获取对象
	obj, err := objectPool.Get()
	if err != nil {
		t.Fatalf("获取对象失败: %v", err)
	}

	if obj == nil {
		t.Error("对象不应该为空")
	}

	// 释放对象
	err = objectPool.Put(obj)
	if err != nil {
		t.Errorf("释放对象失败: %v", err)
	}

	stats := objectPool.Stats()
	if stats.TotalAcquired < 1 {
		t.Error("应该至少获取过1个对象")
	}
}

// TestMetricsCollector 测试MetricsCollector
func TestMetricsCollector(t *testing.T) {
	metricsCollector := monitor.NewMetricsCollector()

	for i := 0; i < 10; i++ {
		start := time.Now()
		time.Sleep(time.Duration(i) * time.Millisecond)
		metricsCollector.RecordQuery(time.Since(start), true, "test_table")
	}

	snapshot := metricsCollector.GetSnapshot()

	if snapshot.QueryCount != 10 {
		t.Errorf("期望10次查询, 实际 %d", snapshot.QueryCount)
	}

	if snapshot.QuerySuccess != 10 {
		t.Errorf("期望10次成功, 实际 %d", snapshot.QuerySuccess)
	}

	if snapshot.SuccessRate < 0.99 {
		t.Errorf("期望成功率>=99%%, 实际 %.2f", snapshot.SuccessRate)
	}
}

// TestCacheManager 测试CacheManager
func TestCacheManager(t *testing.T) {
	cacheManager := monitor.NewCacheManager(100, 100, 100)
	queryCache := cacheManager.GetQueryCache()

	queryCache.Set("test_key", "test_value", time.Minute)

	value, found := queryCache.Get("test_key")
	if !found {
		t.Error("缓存未命中")
	}

	if value != "test_value" {
		t.Errorf("期望 'test_value', 实际 %v", value)
	}

	cacheStats := queryCache.GetStats()
	if cacheStats.Size < 1 {
		t.Error("缓存大小应该>=1")
	}
}

// TestGoroutinePoolIntegration 测试GoroutinePool集成
func TestGoroutinePoolIntegration(t *testing.T) {
	ds := resource.NewMemoryDataSource()
	setupTestData(t, ds)

	goroutinePool := pool.NewGoroutinePool(5, 100)

	var wg sync.WaitGroup
	successCount := 0
	failCount := 0
	var mu sync.Mutex

	for i := 0; i < 20; i++ {
		wg.Add(1)
		err := goroutinePool.Submit(func() {
			defer wg.Done()

			builder := parser.NewQueryBuilder(ds)
			result, err := builder.BuildAndExecute(context.Background(), "SELECT * FROM users WHERE age > 25")

			mu.Lock()
			if err == nil && result.Success {
				successCount++
			} else {
				failCount++
			}
			mu.Unlock()
		})

		if err != nil {
			t.Errorf("提交任务失败: %v", err)
			wg.Done()
		}
	}

	wg.Wait()

	if successCount != 20 {
		t.Errorf("期望20个成功, 实际 %d", successCount)
	}

	goroutinePool.Close()
}

// TestCacheManagerIntegration 测试CacheManager集成
func TestCacheManagerIntegration(t *testing.T) {
	ds := resource.NewMemoryDataSource()
	setupTestData(t, ds)

	cacheManager := monitor.NewCacheManager(100, 100, 100)
	queryCache := cacheManager.GetQueryCache()

	builder := parser.NewQueryBuilder(ds)
	result1, err := builder.BuildAndExecute(context.Background(), "SELECT * FROM users WHERE age > 25")
	if err != nil || !result1.Success {
		t.Fatalf("第一次查询失败: %v", err)
	}

	cacheKey := "SELECT * FROM users WHERE age > 25"
	queryCache.Set(cacheKey, result1, 5*time.Minute)

	result2, found := queryCache.Get(cacheKey)
	if !found {
		t.Fatal("缓存未命中")
	}

	if result2.(*resource.QueryResult).Success != true {
		t.Error("缓存结果应该是成功的")
	}

	cacheStats := queryCache.GetStats()
	if cacheStats.HitRate < 0.5 {
		t.Errorf("期望命中率>=50%%, 实际 %.2f%%", cacheStats.HitRate*100)
	}
}

// TestIntegratedPerformance 测试综合性能
func TestIntegratedPerformance(t *testing.T) {
	ds := resource.NewMemoryDataSource()
	setupTestData(t, ds)

	goroutinePool := pool.NewGoroutinePool(10, 1000)
	cacheManager := monitor.NewCacheManager(1000, 1000, 100)
	queryCache := cacheManager.GetQueryCache()
	metricsCollector := monitor.NewMetricsCollector()

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 100; i++ {
		wg.Add(1)
		err := goroutinePool.Submit(func() {
			defer wg.Done()

			query := "SELECT * FROM users WHERE age > 25"
			cachedResult, found := queryCache.Get(query)

			var result *resource.QueryResult
			var err error

			if found {
				result = cachedResult.(*resource.QueryResult)
			} else {
				builder := parser.NewQueryBuilder(ds)
				result, err = builder.BuildAndExecute(context.Background(), query)

				if err == nil && result.Success {
					queryCache.Set(query, result, 5*time.Minute)
				}
			}

			queryID := metricsCollector.StartQuery(query, "SELECT")
			metricsCollector.RecordCacheHit(queryID, found)

			if err == nil && result != nil && result.Success {
				metricsCollector.EndQuery(queryID, "test_db", "")
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		})

		if err != nil {
			t.Errorf("提交任务失败: %v", err)
			wg.Done()
		}
	}

	wg.Wait()

	if successCount < 90 {
		t.Errorf("期望至少90个成功, 实际 %d", successCount)
	}

	goroutinePool.Close()
}

// TestServerCreation 测试Server创建
func TestServerCreation(t *testing.T) {
	server := mysql.NewServer()

	stats := server.GetGoroutinePoolStats()
	if stats.MaxSize <= 0 {
		t.Error("GoroutinePool应该已创建")
	}

	objStats := server.GetObjectPoolStats()
	if objStats.MaxSize <= 0 {
		t.Error("ObjectPool应该已创建")
	}

	if server.GetMetricsCollector() == nil {
		t.Error("MetricsCollector应该已创建")
	}

	if server.GetCacheManager() == nil {
		t.Error("CacheManager应该已创建")
	}
}

// TestQueryCache 测试QueryCache
func TestQueryCache(t *testing.T) {
	cache := monitor.NewQueryCache(1000)

	result := &resource.QueryResult{
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
		Rows: []resource.Row{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
		Total: 2,
	}

	cache.Set("test_query", result, time.Minute)

	cached, found := cache.Get("test_query")
	if !found {
		t.Error("缓存未命中")
	}

	if len(cached.Rows) != 2 {
		t.Errorf("期望2行, 实际 %d", len(cached.Rows))
	}

	cache.Invalidate("users")

	stats := cache.Stats()
	if stats.Size < 1 {
		t.Error("缓存大小应该>=1")
	}
}

// TestSlowQueryLogger 测试SlowQueryLogger
func TestSlowQueryLogger(t *testing.T) {
	logger := monitor.NewSlowQueryLogger(100)

	logger.Log("SELECT * FROM users", 50*time.Millisecond)
	logger.Log("SELECT * FROM orders WHERE user_id = 1", 120*time.Millisecond)
	logger.Log("SELECT * FROM products WHERE category = 'electronics'", 200*time.Millisecond)

	stats := logger.Stats()
	if stats["count"].(int) != 3 {
		t.Errorf("期望3条慢查询, 实际 %v", stats["count"])
	}

	logs := logger.GetLogs()
	if len(logs) < 2 {
		t.Errorf("期望至少2条日志, 实际 %d", len(logs))
	}
}

// TestConnectionPool 测试ConnectionPool
func TestConnectionPool(t *testing.T) {
	connPool := pool.NewConnectionPool()

	connPool.SetMaxOpenConns(5)
	connPool.SetMaxIdleConns(2)

	// 测试连接获取
	conn, err := connPool.Get()
	if err != nil {
		t.Errorf("获取连接失败: %v", err)
	}

	if conn != nil {
		connPool.Release(conn)
	}

	stats := connPool.Stats()
	if stats["max_open"] != 5 {
		t.Errorf("期望max_open=5, 实际 %v", stats["max_open"])
	}

	connPool.Close()
}

// Helper function

func setupTestData(t *testing.T, ds resource.DataSource) {
	t.Helper()

	schemaDS, ok := ds.(resource.SchemaDataSource)
	if !ok {
		t.Fatal("数据源不支持schema操作")
	}

	schema := &resource.TableSchema{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
			{Name: "city", Type: "string"},
		},
	}

	err := schemaDS.CreateTable(context.Background(), schema)
	if err != nil {
		t.Fatalf("创建表失败: %v", err)
	}

	writableDS, ok := ds.(resource.WritableDataSource)
	if !ok {
		t.Fatal("数据源不支持写操作")
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
			t.Fatalf("插入数据失败: %v", err)
		}
	}
}
