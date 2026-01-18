package monitor

import (
	"sync"
	"time"
)

// CacheEntry 缓存条目
type CacheEntry struct {
	Key        string
	Value      interface{}
	Expiration time.Time
	CreatedAt  time.Time
	AccessCount int64
	LastAccess time.Time
}

// IsExpired 检查是否过�?
func (e *CacheEntry) IsExpired() bool {
	if e.Expiration.IsZero() {
		return false
	}
	return time.Now().After(e.Expiration)
}

// QueryCache 查询缓存
type QueryCache struct {
	mu         sync.RWMutex
	entries    map[string]*CacheEntry
	maxSize    int
	maxTTL     time.Duration
	hits       int64
	misses     int64
	evictions  int64
}

// NewQueryCache 创建查询缓存
func NewQueryCache(maxSize int, maxTTL time.Duration) *QueryCache {
	return &QueryCache{
		entries: make(map[string]*CacheEntry),
		maxSize: maxSize,
		maxTTL:  maxTTL,
	}
}

// Get 获取缓存
func (c *QueryCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		c.misses++
		return nil, false
	}

	// 检查是否过�?
	if entry.IsExpired() {
		delete(c.entries, key)
		c.misses++
		return nil, false
	}

	entry.AccessCount++
	entry.LastAccess = time.Now()
	c.hits++
	return entry.Value, true
}

// Set 设置缓存
func (c *QueryCache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果已存在，删除旧条�?
	if _, ok := c.entries[key]; ok {
		delete(c.entries, key)
	}

	// 如果缓存已满，淘汰最久未使用的条�?
	if len(c.entries) >= c.maxSize {
		c.evictLRU()
	}

	// 计算过期时间
	var expiration time.Time
	if ttl > 0 && ttl <= c.maxTTL {
		expiration = time.Now().Add(ttl)
	} else if c.maxTTL > 0 {
		expiration = time.Now().Add(c.maxTTL)
	}

	c.entries[key] = &CacheEntry{
		Key:        key,
		Value:      value,
		Expiration: expiration,
		CreatedAt:  time.Now(),
		AccessCount: 1,
		LastAccess: time.Now(),
	}
}

// Delete 删除缓存
func (c *QueryCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.entries[key]; ok {
		delete(c.entries, key)
		return true
	}
	return false
}

// Clear 清空缓存
func (c *QueryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*CacheEntry)
	c.hits = 0
	c.misses = 0
	c.evictions = 0
}

// evictLRU 淘汰最久未使用的条�?
func (c *QueryCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.entries {
		if oldestKey == "" || entry.LastAccess.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.LastAccess
		}
	}

	if oldestKey != "" {
		delete(c.entries, oldestKey)
		c.evictions++
	}
}

// GetSize 获取缓存大小
func (c *QueryCache) GetSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// GetStats 获取缓存统计
func (c *QueryCache) GetStats() *CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hitRate := 0.0
	total := c.hits + c.misses
	if total > 0 {
		hitRate = float64(c.hits) / float64(total) * 100
	}

	return &CacheStats{
		Size:     len(c.entries),
		Hits:     c.hits,
		Misses:   c.misses,
		HitRate:  hitRate,
		Evictions: c.evictions,
		MaxSize:  c.maxSize,
		MaxTTL:   c.maxTTL,
	}
}

// CacheStats 缓存统计
type CacheStats struct {
	Size      int
	Hits      int64
	Misses    int64
	HitRate   float64
	Evictions int64
	MaxSize   int
	MaxTTL    time.Duration
}

// CacheManager 缓存管理�?
type CacheManager struct {
	queryCache   *QueryCache
	resultCache  *QueryCache
	schemaCache  *QueryCache
}

// NewCacheManager 创建缓存管理�?
func NewCacheManager(queryCacheSize int, resultCacheSize int, schemaCacheSize int) *CacheManager {
	return &CacheManager{
		queryCache:  NewQueryCache(queryCacheSize, time.Minute*5),
		resultCache: NewQueryCache(resultCacheSize, time.Minute*10),
		schemaCache: NewQueryCache(schemaCacheSize, time.Hour),
	}
}

// GetQueryCache 获取查询缓存
func (cm *CacheManager) GetQueryCache() *QueryCache {
	return cm.queryCache
}

// GetResultCache 获取结果缓存
func (cm *CacheManager) GetResultCache() *QueryCache {
	return cm.resultCache
}

// GetSchemaCache 获取schema缓存
func (cm *CacheManager) GetSchemaCache() *QueryCache {
	return cm.schemaCache
}

// GetStats 获取所有缓存统�?
func (cm *CacheManager) GetStats() map[string]*CacheStats {
	return map[string]*CacheStats{
		"query":  cm.queryCache.GetStats(),
		"result": cm.resultCache.GetStats(),
		"schema": cm.schemaCache.GetStats(),
	}
}

// CacheKey 生成缓存�?
type CacheKey struct {
	SQL        string
	Params     []interface{}
	Database   string
	User       string
}

// GenerateKey 生成缓存�?
func GenerateKey(cacheKey *CacheKey) string {
	return string(cacheKey.SQL)
}

// CachedResult 缓存的查询结�?
type CachedResult struct {
	Columns    []string
	Rows       []interface{}
	Total      int64
	ExecutedAt time.Time
	Duration   time.Duration
}
