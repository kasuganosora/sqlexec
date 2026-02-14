package cache

import (
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== 查询缓存 ====================

// CacheEntry 缓存条目
type CacheEntry struct {
	result      *domain.QueryResult
	createdAt   time.Time
	expiresAt   time.Time
	accessCount int64
	lastAccess  time.Time
	query       string
}

// QueryCache 查询缓存
type QueryCache struct {
	cache   map[string]*CacheEntry
	maxSize int
	ttl     time.Duration
	mu      sync.RWMutex
}

// NewQueryCache 创建查询缓存
func NewQueryCache() *QueryCache {
	return &QueryCache{
		cache:   make(map[string]*CacheEntry),
		maxSize: 100,
		ttl:     5 * time.Minute,
	}
}

// NewQueryCacheWithConfig 使用配置创建查询缓存
func NewQueryCacheWithConfig(maxSize int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		cache:   make(map[string]*CacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// Get 获取缓存
func (c *QueryCache) Get(query string) (*domain.QueryResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[query]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if time.Now().After(entry.expiresAt) {
		delete(c.cache, query)
		return nil, false
	}

	// 更新访问信息
	entry.lastAccess = time.Now()
	entry.accessCount++

	return entry.result, true
}

// Set 设置缓存
func (c *QueryCache) Set(query string, result *domain.QueryResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查缓存大小
	if len(c.cache) >= c.maxSize {
		c.evict()
	}

	now := time.Now()
	c.cache[query] = &CacheEntry{
		result:      result,
		createdAt:   now,
		expiresAt:   now.Add(c.ttl),
		accessCount: 1,
		lastAccess:  now,
		query:       query,
	}
}

// Invalidate 使表的缓存失效
func (c *QueryCache) Invalidate(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for query := range c.cache {
		if util.ContainsTable(query, tableName) {
			delete(c.cache, query)
		}
	}
}

// Clear 清空缓存
func (c *QueryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*CacheEntry)
}

// Stats 获取统计信息
func (c *QueryCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalAccess := int64(0)
	hitCount := int64(0)

	for _, entry := range c.cache {
		totalAccess += entry.accessCount
		if entry.accessCount > 1 {
			hitCount++
		}
	}

	hitRate := float64(0)
	if totalAccess > 0 {
		hitRate = float64(hitCount) / float64(totalAccess)
	}

	return map[string]interface{}{
		"size":         len(c.cache),
		"max_size":     c.maxSize,
		"ttl":          c.ttl,
		"total_access": totalAccess,
		"hit_count":    hitCount,
		"hit_rate":     hitRate,
	}
}

// evict 淘汰最少使用的条目
func (c *QueryCache) evict() {
	var lruQuery string
	var lruAccess int64

	for query, entry := range c.cache {
		if lruAccess == 0 || entry.accessCount < lruAccess {
			lruAccess = entry.accessCount
			lruQuery = query
		}
	}

	if lruQuery != "" {
		delete(c.cache, lruQuery)
	}
}

// SetMaxSize 设置最大缓存大小
func (c *QueryCache) SetMaxSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxSize = size
}

// SetTTL 设置缓存过期时间
func (c *QueryCache) SetTTL(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ttl = ttl
}
