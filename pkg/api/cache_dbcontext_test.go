package api

import (
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// TestQueryCacheCurrentDB 测试缓存键包含数据库上下文
func TestQueryCacheCurrentDB(t *testing.T) {
	config := DefaultCacheConfig
	config.TTL = 1 * time.Hour
	cache := NewQueryCache(config)

	// 测试 generateKey 包含 currentDB
	key1 := cache.generateKey("SELECT * FROM users", nil)
	cache.currentDB = "default"
	key2 := cache.generateKey("SELECT * FROM users", nil)
	cache.currentDB = "test_db"
	key3 := cache.generateKey("SELECT * FROM users", nil)

	// 不同的数据库应该生成不同的键
	assert.NotEqual(t, key1, key2)
	assert.NotEqual(t, key2, key3)

	// 相同的数据库应该生成相同的键
	cache.currentDB = "default"
	key4 := cache.generateKey("SELECT * FROM users", nil)
	assert.Equal(t, key2, key4)
}

// TestSetCurrentDB 测试设置当前数据库
func TestSetCurrentDB(t *testing.T) {
	config := DefaultCacheConfig
	cache := NewQueryCache(config)

	// 初始状态
	assert.Equal(t, "", cache.currentDB)

	// 设置数据库
	cache.SetCurrentDB("information_schema")
	assert.Equal(t, "information_schema", cache.currentDB)

	// 切换数据库
	cache.SetCurrentDB("test_db")
	assert.Equal(t, "test_db", cache.currentDB)
}

// TestQueryCacheDifferentDBsNoCacheHit 测试不同数据库的查询不会互相干扰
func TestQueryCacheDifferentDBsNoCacheHit(t *testing.T) {
	config := DefaultCacheConfig
	cache := NewQueryCache(config)

	// 在 default 数据库下缓存查询
	cache.currentDB = "default"
	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id"}},
		Rows:    []domain.Row{{"id": "1"}},
	}
	cache.Set("SELECT * FROM users", nil, result1)

	// 切换到 test_db 数据库
	cache.SetCurrentDB("test_db")

	// 应该获取不到缓存
	result2, found := cache.Get("SELECT * FROM users", nil)
	assert.False(t, found)
	assert.Nil(t, result2)

	// 重新设置回 default
	cache.SetCurrentDB("default")
	result3, found := cache.Get("SELECT * FROM users", nil)
	assert.True(t, found)
	assert.NotNil(t, result3)
}
