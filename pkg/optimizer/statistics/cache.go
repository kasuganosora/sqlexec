package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// StatisticsCache 统计信息缓存
// 避免频繁收集统计信息，提升性能
type StatisticsCache struct {
	mu     sync.RWMutex
	cache  map[string]*CachedStatistics
	ttl    time.Duration // 缓存过期时间
	hits   int64         // 缓存命中次数
	misses int64         // 缓存未命中次数
}

// CachedStatistics 缓存的统计信息
type CachedStatistics struct {
	Statistics   *TableStatistics
	CollectTime  time.Time
	LastAccessed time.Time
	HitCount     int64
}

// NewStatisticsCache 创建统计信息缓存
func NewStatisticsCache(ttl time.Duration) *StatisticsCache {
	return &StatisticsCache{
		cache: make(map[string]*CachedStatistics),
		ttl:   ttl,
	}
}

// Get 获取缓存的统计信息
func (sc *StatisticsCache) Get(tableName string) (*TableStatistics, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	cached, exists := sc.cache[tableName]
	if !exists {
		sc.misses++
		return nil, false
	}

	// 检查是否过期
	if time.Since(cached.CollectTime) > sc.ttl {
		delete(sc.cache, tableName)
		sc.misses++
		return nil, false
	}

	cached.LastAccessed = time.Now()
	cached.HitCount++
	sc.hits++

	return cached.Statistics, true
}

// Set 设置缓存的统计信息
func (sc *StatisticsCache) Set(tableName string, stats *TableStatistics) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.cache[tableName] = &CachedStatistics{
		Statistics:   stats,
		CollectTime:  time.Now(),
		LastAccessed: time.Now(),
		HitCount:     0,
	}
}

// Invalidate 使指定表的缓存失效
func (sc *StatisticsCache) Invalidate(tableName string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.cache, tableName)
}

// InvalidateAll 使所有缓存失效
func (sc *StatisticsCache) InvalidateAll() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.cache = make(map[string]*CachedStatistics)
	sc.hits = 0
	sc.misses = 0
}

// Stats 返回缓存统计信息
func (sc *StatisticsCache) Stats() CacheStats {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	totalRequests := sc.hits + sc.misses
	hitRate := 0.0
	if totalRequests > 0 {
		hitRate = float64(sc.hits) / float64(totalRequests)
	}

	return CacheStats{
		Size:    len(sc.cache),
		Hits:    sc.hits,
		Misses:  sc.misses,
		HitRate: hitRate,
		TTL:     sc.ttl,
	}
}

// CacheStats 缓存统计信息
type CacheStats struct {
	Size    int           // 缓存中的表数量
	Hits    int64         // 命中次数
	Misses  int64         // 未命中次数
	HitRate float64       // 命中率
	TTL     time.Duration // 过期时间
}

// TableStatistics 增强的表统计信息
type TableStatistics struct {
	Name              string
	RowCount          int64
	SampleCount       int64   // 采样行数
	SampleRatio       float64 // 采样比例
	ColumnStats       map[string]*ColumnStatistics
	Histograms        map[string]*Histogram // 列直方图
	CollectTimestamp  time.Time             // 收集时间
	EstimatedRowCount int64                 // 估计的行数（可能不同于实际RowCount）
}

// ColumnStatistics 增强的列统计信息
type ColumnStatistics struct {
	Name          string
	DataType      string
	DistinctCount int64 // NDV (Number of Distinct Values)
	NullCount     int64
	MinValue      interface{}
	MaxValue      interface{}
	NullFraction  float64
	AvgWidth      float64     // 平均字符串长度
	MedianValue   interface{} // 中位数（可选）
	StdDev        float64     // 标准差（可选）
}

// AutoRefreshStatisticsCache 自动刷新的统计信息缓存
type AutoRefreshStatisticsCache struct {
	cache      *StatisticsCache
	collector  *SamplingCollector
	dataSource domain.DataSource
	refreshOn  map[string]time.Time // 下次刷新时间
	mu         sync.RWMutex
}

// NewAutoRefreshStatisticsCache 创建自动刷新缓存
func NewAutoRefreshStatisticsCache(
	collector *SamplingCollector,
	dataSource domain.DataSource,
	ttl time.Duration,
) *AutoRefreshStatisticsCache {
	return &AutoRefreshStatisticsCache{
		cache:      NewStatisticsCache(ttl),
		collector:  collector,
		dataSource: dataSource,
		refreshOn:  make(map[string]time.Time),
	}
}

// Get 获取统计信息（自动刷新过期数据）
func (arc *AutoRefreshStatisticsCache) Get(tableName string) (*TableStatistics, error) {
	arc.mu.RLock()
	nextRefresh, exists := arc.refreshOn[tableName]
	arc.mu.RUnlock()

	// 检查是否需要刷新
	needRefresh := !exists || time.Now().After(nextRefresh)

	if needRefresh {
		return arc.refresh(tableName)
	}

	// 从缓存获取
	stats, ok := arc.cache.Get(tableName)
	if !ok {
		return arc.refresh(tableName)
	}

	return stats, nil
}

// refresh 刷新表的统计信息
func (arc *AutoRefreshStatisticsCache) refresh(tableName string) (*TableStatistics, error) {
	// 收集新的统计信息
	stats, err := arc.collector.CollectStatistics(nil, tableName)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	arc.cache.Set(tableName, stats)

	// 更新下次刷新时间
	arc.mu.Lock()
	arc.refreshOn[tableName] = time.Now().Add(arc.cache.ttl)
	arc.mu.Unlock()

	return stats, nil
}

// Invalidate 使缓存失效并清除刷新计划
func (arc *AutoRefreshStatisticsCache) Invalidate(tableName string) {
	arc.cache.Invalidate(tableName)

	arc.mu.Lock()
	delete(arc.refreshOn, tableName)
	arc.mu.Unlock()
}

// InvalidateAll 使所有缓存失效
func (arc *AutoRefreshStatisticsCache) InvalidateAll() {
	arc.cache.InvalidateAll()

	arc.mu.Lock()
	arc.refreshOn = make(map[string]time.Time)
	arc.mu.Unlock()
}

// Stats 返回缓存统计信息
func (arc *AutoRefreshStatisticsCache) Stats() CacheStats {
	return arc.cache.Stats()
}

// Preload 预加载多个表的统计信息
func (arc *AutoRefreshStatisticsCache) Preload(tableNames []string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(tableNames))

	for _, tableName := range tableNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			_, err := arc.refresh(name)
			if err != nil {
				errChan <- err
			}
		}(tableName)
	}

	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// StartAutoRefresh 启动后台自动刷新goroutine
func (arc *AutoRefreshStatisticsCache) StartAutoRefresh(ctx context.Context, interval time.Duration) {
	go arc.autoRefreshLoop(ctx, interval)
}

// StopAutoRefresh 停止自动刷新goroutine（通过取消context）
func (arc *AutoRefreshStatisticsCache) StopAutoRefresh() {
	// 自动刷新循环会检查context取消状态，无需额外处理
}

// autoRefreshLoop 自动刷新循环
func (arc *AutoRefreshStatisticsCache) autoRefreshLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			debugln("  [STATS] Auto refresh stopped")
			return
		case <-ticker.C:
			arc.refreshExpiredStats()
		}
	}
}

// refreshExpiredStats 刷新所有过期的统计信息
func (arc *AutoRefreshStatisticsCache) refreshExpiredStats() {
	arc.mu.RLock()
	expiredTables := make([]string, 0)
	now := time.Now()

	for tableName, nextRefresh := range arc.refreshOn {
		if now.After(nextRefresh) {
			expiredTables = append(expiredTables, tableName)
		}
	}
	arc.mu.RUnlock()

	if len(expiredTables) > 0 {
		debugf("  [STATS] Refreshing %d expired tables\n", len(expiredTables))

		// 并发刷新过期表
		for _, tableName := range expiredTables {
			stats, err := arc.refresh(tableName)
			if err != nil {
				debugf("  [STATS] Failed to refresh %s: %v\n", tableName, err)
			} else {
				debugf("  [STATS] Refreshed %s: %d rows\n", tableName, stats.RowCount)
			}
		}
	}
}
