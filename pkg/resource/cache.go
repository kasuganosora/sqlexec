package resource

import (
	"database/sql"
	"sync"
	"time"
)

// ==================== 语句缓存 ====================

// CachedStatement 缓存的语句
type CachedStatement struct {
	stmt       *sql.Stmt
	createdAt  time.Time
	lastUsed   time.Time
	useCount   int64
	query      string
}

// StatementCache 语句缓存
type StatementCache struct {
	cache     map[string]*CachedStatement
	tables    map[string]bool // 表名 -> 是否有修改
	maxSize   int
	mu        sync.RWMutex
}

// NewStatementCache 创建语句缓存
func NewStatementCache() *StatementCache {
	return &StatementCache{
		cache:  make(map[string]*CachedStatement),
		tables: make(map[string]bool),
		maxSize: 100,
	}
}

// Get 获取或创建语句
func (c *StatementCache) Get(conn *sql.DB, query string) (*sql.Stmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 检查缓存
	if stmt, exists := c.cache[query]; exists {
		// 检查语句是否仍然有效
		if stmt.stmt != nil {
			// 更新使用信息
			stmt.lastUsed = time.Now()
			stmt.useCount++
			c.mu.RUnlock()
			return stmt.stmt, nil
		}
	}

	c.mu.RUnlock()

	// 准备新语句
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	// 添加到缓存
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查缓存大小
	if len(c.cache) >= c.maxSize {
		c.evict()
	}

	c.cache[query] = &CachedStatement{
		stmt:      stmt,
		createdAt:  time.Now(),
		lastUsed:   time.Now(),
		useCount:   1,
		query:     query,
	}

	return stmt, nil
}

// InvalidateTable 使表的缓存失效
func (c *StatementCache) InvalidateTable(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 标记表已修改
	c.tables[tableName] = true

	// 清除相关缓存
	for query, stmt := range c.cache {
		if c.isTableRelated(query, tableName) {
			stmt.stmt.Close()
			delete(c.cache, query)
		}
	}
}

// IsTableValid 检查表是否有效（未修改）
func (c *StatementCache) IsTableValid(tableName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.tables[tableName]
}

// Clear 清空缓存
func (c *StatementCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭所有语句
	for _, stmt := range c.cache {
		if stmt.stmt != nil {
			stmt.stmt.Close()
		}
	}

	c.cache = make(map[string]*CachedStatement)
	c.tables = make(map[string]bool)
}

// Stats 获取统计信息
func (c *StatementCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalUseCount := int64(0)
	hitCount := int64(0)

	for _, stmt := range c.cache {
		totalUseCount += stmt.useCount
		if stmt.useCount > 1 {
			hitCount++
		}
	}

	return map[string]interface{}{
		"size":          len(c.cache),
		"max_size":      c.maxSize,
		"total_uses":    totalUseCount,
		"hit_count":     hitCount,
		"hit_rate":      float64(hitCount) / float64(totalUseCount),
		"modified_tables": c.getModifiedTables(),
	}
}

// evict 淘汰最久未使用的语句
func (c *StatementCache) evict() {
	var oldestQuery string
	var oldestTime time.Time

	for query, stmt := range c.cache {
		if oldestTime.IsZero() || stmt.lastUsed.Before(oldestTime) {
			oldestTime = stmt.lastUsed
			oldestQuery = query
		}
	}

	if oldestQuery != "" {
		stmt := c.cache[oldestQuery]
		stmt.stmt.Close()
		delete(c.cache, oldestQuery)
	}
}

// isTableRelated 检查查询是否与表相关
func (c *StatementCache) isTableRelated(query, tableName string) bool {
	// 简单实现：检查查询中是否包含表名
	return containsTable(query, tableName)
}

// getModifiedTables 获取修改的表列表
func (c *StatementCache) getModifiedTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]string, 0)
	for table, modified := range c.tables {
		if modified {
			tables = append(tables, table)
		}
	}
	return tables
}

// ==================== 查询缓存 ====================

// CacheEntry 缓存条目
type CacheEntry struct {
	result     *QueryResult
	createdAt  time.Time
	expiresAt  time.Time
	accessCount int64
	lastAccess time.Time
	query      string
}

// QueryCache 查询缓存
type QueryCache struct {
	cache    map[string]*CacheEntry
	maxSize  int
	ttl      time.Duration
	mu        sync.RWMutex
}

// NewQueryCache 创建查询缓存
func NewQueryCache() *QueryCache {
	return &QueryCache{
		cache:   make(map[string]*CacheEntry),
		maxSize: 100,
		ttl:     5 * time.Minute,
	}
}

// Get 获取缓存
func (c *QueryCache) Get(query string) (*QueryResult, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

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
func (c *QueryCache) Set(query string, result *QueryResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查缓存大小
	if len(c.cache) >= c.maxSize {
		c.evict()
	}

	now := time.Now()
	c.cache[query] = &CacheEntry{
		result:     result,
		createdAt:  now,
		expiresAt:  now.Add(c.ttl),
		accessCount: 1,
		lastAccess:  now,
		query:      query,
	}
}

// Invalidate 使表的缓存失效
func (c *QueryCache) Invalidate(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for query := range c.cache {
		if c.isTableRelated(query, tableName) {
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
		"size":      len(c.cache),
		"max_size":  c.maxSize,
		"ttl":       c.ttl,
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

// isTableRelated 检查查询是否与表相关
func (c *QueryCache) isTableRelated(query, tableName string) bool {
	return containsTable(query, tableName)
}

// ==================== 慢查询日志 ====================

// SlowQueryLog 慢查询日志条目
type SlowQueryLog struct {
	Query     string
	Duration  time.Duration
	Timestamp time.Time
}

// SlowQueryLogger 慢查询日志器
type SlowQueryLogger struct {
	logs    []*SlowQueryLog
	threshold time.Duration
	mu       sync.RWMutex
	maxSize  int
}

// NewSlowQueryLogger 创建慢查询日志器
func NewSlowQueryLogger() *SlowQueryLogger {
	return &SlowQueryLogger{
		logs:     make([]*SlowQueryLog, 0),
		threshold: 100 * time.Millisecond,
		maxSize:   1000,
	}
}

// SetThreshold 设置慢查询阈值
func (l *SlowQueryLogger) SetThreshold(threshold time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.threshold = threshold
}

// Log 记录查询
func (l *SlowQueryLogger) Log(query string, duration time.Duration) {
	if duration < l.threshold {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.logs = append(l.logs, &SlowQueryLog{
		Query:     query,
		Duration:  duration,
		Timestamp: time.Now(),
	})

	// 限制日志大小
	if len(l.logs) > l.maxSize {
		l.logs = l.logs[1:]
	}
}

// GetLogs 获取所有慢查询
func (l *SlowQueryLogger) GetLogs() []*SlowQueryLog {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.logs
}

// Clear 清空日志
func (l *SlowQueryLogger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = make([]*SlowQueryLog, 0)
}

// Stats 获取统计信息
func (l *SlowQueryLogger) Stats() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.logs) == 0 {
		return map[string]interface{}{
			"count":    0,
			"threshold": l.threshold,
		}
	}

	totalDuration := time.Duration(0)
	for _, log := range l.logs {
		totalDuration += log.Duration
	}

	avgDuration := totalDuration / time.Duration(len(l.logs))

	maxDuration := time.Duration(0)
	for _, log := range l.logs {
		if log.Duration > maxDuration {
			maxDuration = log.Duration
		}
	}

	return map[string]interface{}{
		"count":       len(l.logs),
		"threshold":   l.threshold,
		"avg_duration": avgDuration,
		"max_duration": maxDuration,
		"total_duration": totalDuration,
	}
}
