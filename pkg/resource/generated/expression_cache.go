package generated

import (
	"sync"
	"sync/atomic"
	"time"
)

// ExpressionCache 表达式缓存管理器
// 缓存解析后的表达式，避免重复解析和编译
type ExpressionCache struct {
	cache sync.Map // key: "tableName:columnName", value: *CachedExpression
}

// CachedExpression 缓存的表达式条目
type CachedExpression struct {
	ExprString  string    // 表达式字符串
	ParsedAt    time.Time // 解析时间
	AccessCount  int64     // 访问计数（atomic）
}

// NewExpressionCache 创建表达式缓存
func NewExpressionCache() *ExpressionCache {
	return &ExpressionCache{
		cache: sync.Map{},
	}
}

// Get 获取缓存的表达式
// 返回表达式字符串和是否缓存命中
func (c *ExpressionCache) Get(tableName, columnName string) (string, bool) {
	key := c.buildKey(tableName, columnName)
	if value, ok := c.cache.Load(key); ok {
		if cached, ok := value.(*CachedExpression); ok {
			atomic.AddInt64(&cached.AccessCount, 1)
			return cached.ExprString, true
		}
	}
	return "", false
}

// Set 设置缓存的表达式
func (c *ExpressionCache) Set(tableName, columnName, exprString string) {
	key := c.buildKey(tableName, columnName)
	cached := &CachedExpression{
		ExprString:  exprString,
		ParsedAt:    time.Now(),
		AccessCount:  int64(0),
	}
	c.cache.Store(key, cached)
}

// Clear 清除指定表的所有缓存
func (c *ExpressionCache) Clear(tableName string) {
	prefix := tableName + ":"
	c.cache.Range(func(key, value interface{}) bool {
		if keyStr, ok := key.(string); ok {
			if len(keyStr) > len(prefix) && keyStr[:len(prefix)] == prefix {
				c.cache.Delete(key)
			}
		}
		return true
	})
}

// ClearAll 清除所有缓存
func (c *ExpressionCache) ClearAll() {
	c.cache = sync.Map{}
}

// GetStats 获取缓存统计信息
func (c *ExpressionCache) GetStats() CacheStats {
	stats := CacheStats{
		TotalEntries: 0,
		TotalAccess:  0,
	}

	c.cache.Range(func(key, value interface{}) bool {
		stats.TotalEntries++
		if cached, ok := value.(*CachedExpression); ok {
			stats.TotalAccess += int(atomic.LoadInt64(&cached.AccessCount))
		}
		return true
	})

	return stats
}

// CacheStats 缓存统计信息
type CacheStats struct {
	TotalEntries int // 缓存条目总数
	TotalAccess  int // 总访问次数
}

// buildKey 构建缓存键
func (c *ExpressionCache) buildKey(tableName, columnName string) string {
	return tableName + ":" + columnName
}
