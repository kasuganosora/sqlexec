package cache

import (
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewQueryCache 测试创建查询缓存
func TestNewQueryCache(t *testing.T) {
	cache := NewQueryCache()

	if cache == nil {
		t.Errorf("NewQueryCache() returned nil")
	}

	if cache.maxSize != 100 {
		t.Errorf("Expected maxSize 100, got %d", cache.maxSize)
	}

	if cache.ttl != 5*time.Minute {
		t.Errorf("Expected ttl 5m, got %v", cache.ttl)
	}
}

// TestNewQueryCacheWithConfig 测试使用配置创建查询缓存
func TestNewQueryCacheWithConfig(t *testing.T) {
	maxSize := 200
	ttl := 10 * time.Minute

	cache := NewQueryCacheWithConfig(maxSize, ttl)

	if cache == nil {
		t.Errorf("NewQueryCacheWithConfig() returned nil")
	}

	if cache.maxSize != maxSize {
		t.Errorf("Expected maxSize %d, got %d", maxSize, cache.maxSize)
	}

	if cache.ttl != ttl {
		t.Errorf("Expected ttl %v, got %v", ttl, cache.ttl)
	}
}

// TestQueryCache_SetAndGet 测试设置和获取缓存
func TestQueryCache_SetAndGet(t *testing.T) {
	cache := NewQueryCache()

	query := "SELECT * FROM users"
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
		Rows:    []domain.Row{{"id": int64(1)}},
		Total:   1,
	}

	// 设置缓存
	cache.Set(query, result)

	// 获取缓存
	cachedResult, exists := cache.Get(query)
	if !exists {
		t.Errorf("Expected cache to exist")
	}

	if cachedResult == nil {
		t.Errorf("Expected cached result, got nil")
	}

	if len(cachedResult.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(cachedResult.Rows))
	}
}

// TestQueryCache_Get_NotExists 测试获取不存在的缓存
func TestQueryCache_Get_NotExists(t *testing.T) {
	cache := NewQueryCache()

	query := "SELECT * FROM users"
	_, exists := cache.Get(query)

	if exists {
		t.Errorf("Expected cache to not exist")
	}
}

// TestQueryCache_Get_Expired 测试获取过期缓存
func TestQueryCache_Get_Expired(t *testing.T) {
	maxSize := 10
	ttl := 10 * time.Millisecond

	cache := NewQueryCacheWithConfig(maxSize, ttl)

	query := "SELECT * FROM users"
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
		Rows:    []domain.Row{{"id": int64(1)}},
		Total:   1,
	}

	// 设置缓存
	cache.Set(query, result)

	// 等待过期
	time.Sleep(20 * time.Millisecond)

	// 获取缓存
	_, exists := cache.Get(query)

	if exists {
		t.Errorf("Expected expired cache to not exist")
	}
}

// TestQueryCache_Overwrite 测试覆盖缓存
func TestQueryCache_Overwrite(t *testing.T) {
	cache := NewQueryCache()

	query := "SELECT * FROM users"
	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
		Rows:    []domain.Row{{"id": int64(1)}},
		Total:   1,
	}

	result2 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
		Rows:    []domain.Row{{"id": int64(2)}},
		Total:   1,
	}

	// 设置缓存
	cache.Set(query, result1)

	// 覆盖缓存
	cache.Set(query, result2)

	// 获取缓存
	cachedResult, _ := cache.Get(query)

	if len(cachedResult.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(cachedResult.Rows))
	}

	id, ok := cachedResult.Rows[0]["id"].(int64)
	if !ok || id != 2 {
		t.Errorf("Expected id to be int64(2), got %v (type: %T)", cachedResult.Rows[0]["id"], cachedResult.Rows[0]["id"])
	}
}

// TestQueryCache_Invalidate 测试使缓存失效
func TestQueryCache_Invalidate(t *testing.T) {
	cache := NewQueryCache()

	queries := []string{
		"SELECT * FROM users",
		"SELECT * FROM products",
		"SELECT * FROM users WHERE id = 1",
	}

	// 设置缓存
	for _, query := range queries {
		cache.Set(query, &domain.QueryResult{
			Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
			Rows:    []domain.Row{{"id": 1}},
			Total:   1,
		})
	}

	// 使users表的缓存失效
	cache.Invalidate("users")

	// 验证users表缓存已失效
	_, exists1 := cache.Get("SELECT * FROM users")
	if exists1 {
		t.Errorf("Expected users cache to be invalidated")
	}

	_, exists2 := cache.Get("SELECT * FROM users WHERE id = 1")
	if exists2 {
		t.Errorf("Expected users WHERE cache to be invalidated")
	}

	// 验证products表缓存仍然存在
	_, exists3 := cache.Get("SELECT * FROM products")
	if !exists3 {
		t.Errorf("Expected products cache to still exist")
	}
}

// TestQueryCache_Clear 测试清空缓存
func TestQueryCache_Clear(t *testing.T) {
	cache := NewQueryCache()

	queries := []string{
		"SELECT * FROM users",
		"SELECT * FROM products",
	}

	// 设置缓存
	for _, query := range queries {
		cache.Set(query, &domain.QueryResult{
			Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
			Rows:    []domain.Row{{"id": 1}},
			Total:   1,
		})
	}

	// 清空缓存
	cache.Clear()

	// 验证所有缓存已清空
	for _, query := range queries {
		_, exists := cache.Get(query)
		if exists {
			t.Errorf("Expected cache to be cleared for query: %s", query)
		}
	}
}

// TestQueryCache_Stats 测试获取统计信息
func TestQueryCache_Stats(t *testing.T) {
	cache := NewQueryCache()

	// 设置缓存
	for i := 0; i < 3; i++ {
		query := "SELECT * FROM table" + string(rune('0'+i))
		cache.Set(query, &domain.QueryResult{
			Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
			Rows:    []domain.Row{{"id": i}},
			Total:   1,
		})
	}

	// 获取统计信息
	stats := cache.Stats()

	if stats == nil {
		t.Errorf("Stats() returned nil")
	}

	size, ok := stats["size"].(int)
	if !ok || size != 3 {
		t.Errorf("Expected size 3, got %v", stats["size"])
	}

	maxSize, ok := stats["max_size"].(int)
	if !ok || maxSize != 100 {
		t.Errorf("Expected max_size 100, got %v", stats["max_size"])
	}

	ttl, ok := stats["ttl"].(time.Duration)
	if !ok || ttl != 5*time.Minute {
		t.Errorf("Expected ttl 5m, got %v", stats["ttl"])
	}
}

// TestQueryCache_SetMaxSize 测试设置最大缓存大小
func TestQueryCache_SetMaxSize(t *testing.T) {
	cache := NewQueryCache()

	cache.SetMaxSize(50)

	if cache.maxSize != 50 {
		t.Errorf("Expected maxSize 50, got %d", cache.maxSize)
	}
}

// TestQueryCache_SetTTL 测试设置TTL
func TestQueryCache_SetTTL(t *testing.T) {
	cache := NewQueryCache()

	newTTL := 10 * time.Minute
	cache.SetTTL(newTTL)

	if cache.ttl != newTTL {
		t.Errorf("Expected ttl %v, got %v", newTTL, cache.ttl)
	}
}

// TestQueryCache_Eviction 测试缓存淘汰
func TestQueryCache_Eviction(t *testing.T) {
	maxSize := 3
	cache := NewQueryCacheWithConfig(maxSize, 5*time.Minute)

	// 添加超过maxSize的条目
	for i := 0; i < 5; i++ {
		query := "SELECT * FROM table" + string(rune('0'+i))
		cache.Set(query, &domain.QueryResult{
			Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
			Rows:    []domain.Row{{"id": i}},
			Total:   1,
		})
	}

	// 验证缓存大小不超过maxSize
	stats := cache.Stats()
	size, ok := stats["size"].(int)
	if !ok {
		t.Errorf("stats[\"size\"] is not int")
	} else if size > maxSize {
		t.Errorf("Expected cache size <= %d, got %d", maxSize, size)
	}
}

// TestQueryCache_ConcurrentOperations 测试并发操作
func TestQueryCache_ConcurrentOperations(t *testing.T) {
	cache := NewQueryCache()

	done := make(chan bool)
	ops := 50

	// 并发设置
	for i := 0; i < ops; i++ {
		go func(idx int) {
			defer func() { done <- true }()
			query := "SELECT * FROM table" + string(rune('0'+idx%10))
			cache.Set(query, &domain.QueryResult{
				Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
				Rows:    []domain.Row{{"id": idx}},
				Total:   1,
			})
		}(i)
	}

	// 并发获取
	for i := 0; i < ops; i++ {
		go func(idx int) {
			defer func() { done <- true }()
			query := "SELECT * FROM table" + string(rune('0'+idx%10))
			cache.Get(query)
		}(i)
	}

	// 等待所有操作完成
	for i := 0; i < ops*2; i++ {
		<-done
	}

	// 验证缓存仍然可用
	stats := cache.Stats()
	size, ok := stats["size"].(int)
	if !ok || size == 0 {
		t.Errorf("Expected cache to have entries after concurrent operations")
	}
}

// TestQueryCache_AccessCount 测试访问计数
func TestQueryCache_AccessCount(t *testing.T) {
	cache := NewQueryCache()

	query := "SELECT * FROM users"
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
		Rows:    []domain.Row{{"id": int64(1)}},
		Total:   1,
	}

	// 设置缓存
	cache.Set(query, result)

	// 多次获取
	for i := 0; i < 5; i++ {
		cache.Get(query)
	}

	// 验证访问计数
	stats := cache.Stats()
	totalAccess, ok := stats["total_access"].(int64)
	if !ok || totalAccess == 0 {
		t.Errorf("Expected total_access > 0")
	}
}

// TestCacheEntry_Expiration 测试缓存条目过期
func TestCacheEntry_Expiration(t *testing.T) {
	cache := NewQueryCacheWithConfig(10, 10*time.Millisecond)

	query := "SELECT * FROM users"
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int64"}},
		Rows:    []domain.Row{{"id": int64(1)}},
		Total:   1,
	}

	// 设置缓存
	cache.Set(query, result)

	// 立即获取，应该存在
	_, exists1 := cache.Get(query)
	if !exists1 {
		t.Errorf("Expected cache to exist immediately after set")
	}

	// 等待过期
	time.Sleep(20 * time.Millisecond)

	// 再次获取，应该不存在
	_, exists2 := cache.Get(query)
	if exists2 {
		t.Errorf("Expected cache to be expired")
	}
}
