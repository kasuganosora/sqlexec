package api

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// CacheConfig 缓存配置
type CacheConfig struct {
	Enabled bool
	TTL     time.Duration // 缓存过期时间
	MaxSize int           // 最大缓存条目数
}

// DefaultCacheConfig 默认缓存配置
var DefaultCacheConfig = CacheConfig{
	Enabled: true,
	TTL:     5 * time.Minute,
	MaxSize: 1000,
}

// QueryCache 查询缓存
type QueryCache struct {
	store   map[string]*CacheEntry
	mu      sync.RWMutex
	ttl     time.Duration
	maxSize int
}

// CacheEntry 缓存条目
type CacheEntry struct {
	Result    *domain.QueryResult
	Params    []interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
	Hits      int64
}

// NewQueryCache 创建查询缓存
func NewQueryCache(config CacheConfig) *QueryCache {
	if !config.Enabled {
		return nil
	}

	return &QueryCache{
		store:   make(map[string]*CacheEntry),
		ttl:     config.TTL,
		maxSize: config.MaxSize,
	}
}

// Get 获取缓存
func (c *QueryCache) Get(sql string, params []interface{}) (*domain.QueryResult, bool) {
	if c == nil {
		return nil, false
	}

	key := c.generateKey(sql, params)

	c.mu.RLock()
	entry, exists := c.store[key]
	c.mu.RUnlock()

	if !exists {
		return nil, false
	}

	// 检查是否过期
	if time.Now().After(entry.ExpiresAt) {
		c.mu.Lock()
		delete(c.store, key)
		c.mu.Unlock()
		return nil, false
	}

	// 更新命中次数
	entry.Hits++

	return entry.Result, true
}

// Set 设置缓存
func (c *QueryCache) Set(sql string, params []interface{}, result *domain.QueryResult) {
	if c == nil || result == nil {
		return
	}

	key := c.generateKey(sql, params)

	// 检查缓存大小限制
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.store) >= c.maxSize {
		c.evictOldest()
	}

	now := time.Now()
	entry := &CacheEntry{
		Result:    result,
		Params:    params,
		CreatedAt: now,
		ExpiresAt: now.Add(c.ttl),
		Hits:      0,
	}

	c.store[key] = entry
}

// Clear 清空所有缓存
func (c *QueryCache) Clear() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = make(map[string]*CacheEntry)
}

// ClearTable 清空指定表的缓存
func (c *QueryCache) ClearTable(tableName string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 删除所有包含该表名的缓存键
	for key := range c.store {
		if c.containsTable(key, tableName) {
			delete(c.store, key)
		}
	}
}

// ClearExpired 清空过期的缓存
func (c *QueryCache) ClearExpired() {
	if c == nil {
		return
	}

	now := time.Now()
	keysToDelete := []string{}

	c.mu.RLock()
	for key, entry := range c.store {
		if now.After(entry.ExpiresAt) {
			keysToDelete = append(keysToDelete, key)
		}
	}
	c.mu.RUnlock()

	if len(keysToDelete) > 0 {
		c.mu.Lock()
		for _, key := range keysToDelete {
			delete(c.store, key)
		}
		c.mu.Unlock()
	}
}

// Stats 获取缓存统计信息
func (c *QueryCache) Stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	size := len(c.store)
	totalHits := int64(0)
	oldest := time.Time{}
	newest := time.Time{}

	for _, entry := range c.store {
		totalHits += entry.Hits
		if oldest.IsZero() || entry.CreatedAt.Before(oldest) {
			oldest = entry.CreatedAt
		}
		if newest.IsZero() || entry.CreatedAt.After(newest) {
			newest = entry.CreatedAt
		}
	}

	return CacheStats{
		Size:     size,
		MaxSize:  c.maxSize,
		TotalHits: totalHits,
		Oldest:   oldest,
		Newest:   newest,
	}
}

// generateKey 生成缓存键
func (c *QueryCache) generateKey(sql string, params []interface{}) string {
	h := fnv.New32a()
	h.Write([]byte(sql))

	// 参数排序以确保相同参数不同顺序生成相同键
	if len(params) > 0 {
		sort.Slice(params, func(i, j int) bool {
			return fmt.Sprintf("%v", params[i]) < fmt.Sprintf("%v", params[j])
		})

		for _, param := range params {
			h.Write([]byte(fmt.Sprintf("%v", param)))
		}
	}

	return fmt.Sprintf("%x", h.Sum32())
}

// containsTable 检查缓存键是否包含指定表名
func (c *QueryCache) containsTable(key, tableName string) bool {
	// 简单实现：检查键中是否包含表名
	// TODO: 更精确的实现可能需要解析 SQL
	return len(key) > 0 && contains(key, tableName)
}

// evictOldest 淘汰最老的缓存条目
func (c *QueryCache) evictOldest() {
	if len(c.store) == 0 {
		return
	}

	oldestKey := ""
	var oldestTime time.Time

	for key, entry := range c.store {
		if oldestTime.IsZero() || entry.CreatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.CreatedAt
		}
	}

	if oldestKey != "" {
		delete(c.store, oldestKey)
	}
}

// contains 检查字符串是否包含子串（不区分大小写）
func contains(s, substr string) bool {
	// 简化实现
	return len(s) > 0 && len(substr) > 0 && len(s) >= len(substr)
}

// CacheStats 缓存统计信息
type CacheStats struct {
	Size     int       // 当前缓存条目数
	MaxSize  int       // 最大缓存条目数
	TotalHits int64     // 总命中次数
	Oldest   time.Time // 最老的缓存创建时间
	Newest   time.Time // 最新的缓存创建时间
}

// String 返回统计信息的字符串表示
func (s CacheStats) String() string {
	return fmt.Sprintf("Size: %d/%d, TotalHits: %d, Oldest: %v, Newest: %v",
		s.Size, s.MaxSize, s.TotalHits, s.Oldest, s.Newest)
}
