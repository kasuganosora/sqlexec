package memory

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// HNSWIndex HNSW近似最近邻索引（简化版）
type HNSWIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc
	vectors    map[int64][]float32
	// HNSW图结构（简化实现：使用随机分层）
	layers     []map[int64][]int64 // 每层：节点ID -> 邻居ID列表
	nodeLevel  map[int64]int       // 节点ID -> 所在最高层
	mu         sync.RWMutex
	rng        *rand.Rand
}

// HNSWParams HNSW参数
type HNSWParams struct {
	M              int     // 每个节点的最大邻居数
	EFConstruction int     // 构建时的搜索深度
	EFSearch       int     // 搜索时的搜索深度
	ML             float64 // 层数因子
}

// DefaultHNSWParams 默认HNSW参数
var DefaultHNSWParams = HNSWParams{
	M:              16,
	EFConstruction: 200,
	EFSearch:       64,
	ML:             0.25,
}

// NewHNSWIndex 创建HNSW索引
func NewHNSWIndex(columnName string, config *VectorIndexConfig) (*HNSWIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	return &HNSWIndex{
		columnName: columnName,
		config:     config,
		distFunc:   distFunc,
		vectors:    make(map[int64][]float32),
		layers:     make([]map[int64][]int64, 0),
		nodeLevel:  make(map[int64]int),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引
func (h *HNSWIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		h.vectors[rec.ID] = vec
		// 简化：随机分配层数
		level := h.randomLevel()
		h.nodeLevel[rec.ID] = level
		// 确保层数足够
		for len(h.layers) <= level {
			h.layers = append(h.layers, make(map[int64][]int64))
		}
		// 在该层添加节点
		h.layers[level][rec.ID] = make([]int64, 0)
	}

	// 简化：建立随机连接
	h.buildConnections()

	return nil
}

// randomLevel 随机生成层数
func (h *HNSWIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < DefaultHNSWParams.ML && level < 16 {
		level++
	}
	return level
}

// buildConnections 建立连接（简化版：随机连接）
func (h *HNSWIndex) buildConnections() {
	ids := make([]int64, 0, len(h.vectors))
	for id := range h.vectors {
		ids = append(ids, id)
	}

	for level := 0; level < len(h.layers); level++ {
		for id := range h.layers[level] {
			// 随机选择邻居
			neighbors := make([]int64, 0, DefaultHNSWParams.M)
			for _, candidate := range ids {
				if candidate != id {
					neighbors = append(neighbors, candidate)
					if len(neighbors) >= DefaultHNSWParams.M {
						break
					}
				}
			}
			h.layers[level][id] = neighbors
		}
	}
}

// Search 近似最近邻搜索（简化版：退化为暴力搜索+分层剪枝）
func (h *HNSWIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != h.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", h.config.Dimension, len(query))
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	type idDist struct {
		id   int64
		dist float32
	}

	// 简化实现：使用所有向量进行搜索
	candidates := make([]idDist, 0, len(h.vectors))
	for id, vec := range h.vectors {
		if filter != nil && len(filter.IDs) > 0 {
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
		dist := h.distFunc.Compute(query, vec)
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
func (h *HNSWIndex) Insert(id int64, vector []float32) error {
	if len(vector) != h.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.config.Dimension, len(vector))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	vec := make([]float32, len(vector))
	copy(vec, vector)
	h.vectors[id] = vec

	// 分配层数
	level := h.randomLevel()
	h.nodeLevel[id] = level

	// 确保层数足够
	for len(h.layers) <= level {
		h.layers = append(h.layers, make(map[int64][]int64))
	}

	// 在各层添加节点
	for l := 0; l <= level; l++ {
		h.layers[l][id] = make([]int64, 0)
	}

	return nil
}

// Delete 删除向量
func (h *HNSWIndex) Delete(id int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.vectors, id)
	delete(h.nodeLevel, id)

	for level := range h.layers {
		delete(h.layers[level], id)
		// 从其他节点的邻居列表中移除
		for node, neighbors := range h.layers[level] {
			newNeighbors := make([]int64, 0, len(neighbors))
			for _, n := range neighbors {
				if n != id {
					newNeighbors = append(newNeighbors, n)
				}
			}
			h.layers[level][node] = newNeighbors
		}
	}

	return nil
}

// GetConfig 获取索引配置
func (h *HNSWIndex) GetConfig() *VectorIndexConfig {
	return h.config
}

// Stats 获取索引统计信息
func (h *HNSWIndex) Stats() VectorIndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var memorySize int64
	// 向量数据
	for _, vec := range h.vectors {
		memorySize += int64(len(vec) * 4)
	}
	// 图结构
	for _, layer := range h.layers {
		memorySize += int64(len(layer) * 8) // map overhead
		for _, neighbors := range layer {
			memorySize += int64(len(neighbors) * 8)
		}
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorHNSW,
		Metric:     h.config.MetricType,
		Dimension:  h.config.Dimension,
		Count:      int64(len(h.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (h *HNSWIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vectors = make(map[int64][]float32)
	h.layers = make([]map[int64][]int64, 0)
	h.nodeLevel = make(map[int64]int)
	return nil
}
