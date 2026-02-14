package api

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
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
	store         map[string]*CacheEntry
	explainStore  map[string]*ExplainEntry
	mu            sync.RWMutex
	ttl           time.Duration
	maxSize       int
	currentDB     string // 当前数据库上下文
}

// CacheEntry 缓存条目
type CacheEntry struct {
	SQL       string // original SQL for table-level invalidation
	Result    *domain.QueryResult
	Params    []interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
	Hits      int64
}

// ExplainEntry Explain 缓存条目
type ExplainEntry struct {
	Explain   string
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
		store:        make(map[string]*CacheEntry),
		explainStore: make(map[string]*ExplainEntry),
		ttl:          config.TTL,
		maxSize:      config.MaxSize,
	}
}

// Get 获取缓存
func (c *QueryCache) Get(sql string, params []interface{}) (*domain.QueryResult, bool) {
	if c == nil {
		return nil, false
	}

	key := c.generateKey(sql, params)

	c.mu.Lock()
	entry, exists := c.store[key]
	if !exists {
		c.mu.Unlock()
		return nil, false
	}

	// 检查是否过期
	if time.Now().After(entry.ExpiresAt) {
		delete(c.store, key)
		c.mu.Unlock()
		return nil, false
	}

	// 更新命中次数
	entry.Hits++
	result := entry.Result
	c.mu.Unlock()

	return result, true
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
		SQL:       sql,
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

// SetCurrentDB 设置当前数据库上下文
func (c *QueryCache) SetCurrentDB(dbName string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.currentDB = dbName
}

// ClearTable 清空指定表的缓存
func (c *QueryCache) ClearTable(tableName string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 删除所有SQL中包含该表名的缓存条目
	for key, entry := range c.store {
		if strings.Contains(entry.SQL, tableName) {
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
	// 在缓存键中包含当前数据库上下文
	c.mu.RLock()
	h.Write([]byte(c.currentDB))
	c.mu.RUnlock()

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

// GetExplain 获取 Explain 缓存
func (c *QueryCache) GetExplain(sql string) (string, bool) {
	if c == nil {
		return "", false
	}

	key := c.generateKey(sql, nil)

	c.mu.Lock()
	entry, exists := c.explainStore[key]
	if !exists {
		c.mu.Unlock()
		return "", false
	}

	// 检查是否过期
	if time.Now().After(entry.ExpiresAt) {
		delete(c.explainStore, key)
		c.mu.Unlock()
		return "", false
	}

	// 更新命中次数
	entry.Hits++
	explain := entry.Explain
	c.mu.Unlock()

	return explain, true
}

// SetExplain 设置 Explain 缓存
func (c *QueryCache) SetExplain(sql string, explain string) {
	if c == nil || explain == "" {
		return
	}

	key := c.generateKey(sql, nil)

	// 检查缓存大小限制
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.store)+len(c.explainStore) >= c.maxSize {
		c.evictOldestExplain()
	}

	now := time.Now()
	entry := &ExplainEntry{
		Explain:   explain,
		CreatedAt: now,
		ExpiresAt: now.Add(c.ttl),
		Hits:      0,
	}

	c.explainStore[key] = entry
}

// evictOldestExplain 淘汰最老的 Explain 缓存条目
func (c *QueryCache) evictOldestExplain() {
	if len(c.explainStore) == 0 {
		return
	}

	oldestKey := ""
	var oldestTime time.Time

	for key, entry := range c.explainStore {
		if oldestTime.IsZero() || entry.CreatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.CreatedAt
		}
	}

	if oldestKey != "" {
		delete(c.explainStore, oldestKey)
	}
}

