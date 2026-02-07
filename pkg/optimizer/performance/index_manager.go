package performance

import (
	"sync"
	"time"
)

// Index 索引定义
type Index struct {
	Name       string
	TableName  string
	Columns    []string
	Unique     bool
	Primary    bool
	Cardinality int64 // 基数（唯一值数量）
}

// IndexManager 索引管理器
type IndexManager struct {
	mu      sync.RWMutex
	indices map[string][]*Index // table_name -> indices
	stats   map[string]*IndexStats // index_name -> stats
}

// IndexStats 索引统计信息
type IndexStats struct {
	Name         string
	HitCount     int64
	MissCount    int64
	AvgAccessTime time.Duration
	LastAccessed time.Time
}

// NewIndexManager 创建索引管理器
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indices: make(map[string][]*Index),
		stats:   make(map[string]*IndexStats),
	}
}

// AddIndex 添加索引
func (im *IndexManager) AddIndex(index *Index) {
	im.mu.Lock()
	defer im.mu.Unlock()

	im.indices[index.TableName] = append(im.indices[index.TableName], index)
	im.stats[index.Name] = &IndexStats{
		Name:         index.Name,
		LastAccessed: time.Now(),
	}
}

// GetIndices 获取表的所有索引
func (im *IndexManager) GetIndices(tableName string) []*Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if indices, ok := im.indices[tableName]; ok {
		result := make([]*Index, len(indices))
		copy(result, indices)
		return result
	}
	return nil
}

// FindBestIndex 查找最佳索引
func (im *IndexManager) FindBestIndex(tableName string, columns []string) *Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	indices, ok := im.indices[tableName]
	if !ok {
		return nil
	}

	// 寻找列数匹配且基数最高的索引
	var bestIndex *Index
	maxCardinality := int64(0)

	for _, index := range indices {
		if len(index.Columns) >= len(columns) {
			// 检查前几列是否匹配
			match := true
			for i, col := range columns {
				if i >= len(index.Columns) || index.Columns[i] != col {
					match = false
					break
				}
			}

			if match && index.Cardinality > maxCardinality {
				bestIndex = index
				maxCardinality = index.Cardinality
			}
		}
	}

	return bestIndex
}

// RecordIndexAccess 记录索引访问
func (im *IndexManager) RecordIndexAccess(indexName string, duration time.Duration) {
	im.mu.Lock()
	defer im.mu.Unlock()

	if stats, ok := im.stats[indexName]; ok {
		stats.HitCount++
		stats.LastAccessed = time.Now()

		// 更新平均访问时间
		if stats.AvgAccessTime == 0 {
			stats.AvgAccessTime = duration
		} else {
			stats.AvgAccessTime = (stats.AvgAccessTime*time.Duration(stats.HitCount) + duration) / time.Duration(stats.HitCount+1)
		}
	}
}

// GetIndexStats 获取索引统计
func (im *IndexManager) GetIndexStats(indexName string) *IndexStats {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if stats, ok := im.stats[indexName]; ok {
		// 返回副本
		return &IndexStats{
			Name:         stats.Name,
			HitCount:     stats.HitCount,
			MissCount:    stats.MissCount,
			AvgAccessTime: stats.AvgAccessTime,
			LastAccessed: stats.LastAccessed,
		}
	}
	return nil
}
