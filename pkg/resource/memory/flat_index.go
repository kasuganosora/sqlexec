package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// FlatIndex 暴力搜索索引（精确搜索）
type FlatIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc
	vectors    map[int64][]float32
	mu         sync.RWMutex
}

// NewFlatIndex 创建Flat索引
func NewFlatIndex(columnName string, config *VectorIndexConfig) (*FlatIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}
	return &FlatIndex{
		columnName: columnName,
		config:     config,
		distFunc:   distFunc,
		vectors:    make(map[int64][]float32),
	}, nil
}

// Build 构建索引
func (f *FlatIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	if loader == nil {
		return fmt.Errorf("loader cannot be nil")
	}
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		f.vectors[rec.ID] = vec
	}
	return nil
}

// Search 暴力搜索
func (f *FlatIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != f.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", f.config.Dimension, len(query))
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	type idDist struct {
		id   int64
		dist float32
	}

	candidates := make([]idDist, 0, len(f.vectors))
	for id, vec := range f.vectors {
		// filter is not nil means we need to filter by IDs
		if filter != nil {
			// empty IDs list means no matches
			if len(filter.IDs) == 0 {
				continue
			}
			found := false
			for _, fid := range filter.IDs {
				if fid == id {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		dist := f.distFunc.Compute(query, vec)
		candidates = append(candidates, idDist{id, dist})
	}

	// 排序取TopK
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	if len(candidates) > k {
		candidates = candidates[:k]
	}

	result := &VectorSearchResult{
		IDs:       make([]int64, len(candidates)),
		Distances: make([]float32, len(candidates)),
	}
	for i, c := range candidates {
		result.IDs[i] = c.id
		result.Distances[i] = c.dist
	}
	return result, nil
}

// Insert 插入向量
func (f *FlatIndex) Insert(id int64, vector []float32) error {
	if len(vector) != f.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", f.config.Dimension, len(vector))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	vec := make([]float32, len(vector))
	copy(vec, vector)
	f.vectors[id] = vec
	return nil
}

// Delete 删除向量
func (f *FlatIndex) Delete(id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.vectors, id)
	return nil
}

// GetConfig 获取索引配置
func (f *FlatIndex) GetConfig() *VectorIndexConfig {
	return f.config
}

// Stats 获取索引统计信息
func (f *FlatIndex) Stats() VectorIndexStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var memorySize int64
	for _, vec := range f.vectors {
		memorySize += int64(len(vec) * 4) // float32 = 4 bytes
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorFlat,
		Metric:     f.config.MetricType,
		Dimension:  f.config.Dimension,
		Count:      int64(len(f.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (f *FlatIndex) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vectors = make(map[int64][]float32)
	return nil
}
