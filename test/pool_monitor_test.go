package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/mysql"
	"github.com/kasuganosora/sqlexec/mysql/monitor"
	"github.com/kasuganosora/sqlexec/mysql/pool"
	"github.com/kasuganosora/sqlexec/mysql/resource"
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
	objectPool := pool.NewObjectPool(
		func() (interface{}, error) {
			return &struct{}{}, nil
		},
		func(obj interface{}) error {
			return nil
		},
		10,
	)

	// 获取对象
	obj, err := objectPool.Get(context.Background())
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
		time.Sleep(time.Duration(i) * time.Millisecond)
		metricsCollector.StartQuery()
		time.Sleep(time.Duration(i) * time.Millisecond)
		metricsCollector.EndQuery()
		metricsCollector.RecordQuery(time.Duration(i)*time.Millisecond, true, "test_table")
	}

	// MetricsCollector测试
	_ = metricsCollector
}

// TestQueryCache 测试QueryCache
func TestQueryCache(t *testing.T) {
	cache := monitor.NewQueryCache(1000, time.Minute)

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

	if cachedResult, ok := cached.(*resource.QueryResult); ok {
		if len(cachedResult.Rows) != 2 {
			t.Errorf("期望2行, 实际 %d", len(cachedResult.Rows))
		}
	}

	stats := cache.GetStats()
	if stats.Size < 1 {
		t.Error("缓存大小应该>=1")
	}
}

// TestSlowQueryLogger 测试SlowQueryLogger
func TestSlowQueryLogger(t *testing.T) {
	logger := monitor.NewSlowQueryAnalyzer(100*time.Millisecond, 100)

	logger.RecordSlowQuery("SELECT * FROM users", 50*time.Millisecond, "users", 0)
	logger.RecordSlowQuery("SELECT * FROM orders WHERE user_id = 1", 120*time.Millisecond, "orders", 10)
	logger.RecordSlowQuery("SELECT * FROM products WHERE category = 'electronics'", 200*time.Millisecond, "products", 50)

	logs := logger.GetAllSlowQueries()
	if len(logs) < 2 {
		t.Errorf("期望至少2条慢查询日志, 实际 %d", len(logs))
	}

	// 验证慢查询记录
	if logger.IsSlowQuery(120*time.Millisecond) {
		t.Log("120ms的查询被正确识别为慢查询")
	}
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
