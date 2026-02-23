package memory

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// HNSWIndexImproved 改进的 HNSW 近似最近邻索引（参考 Milvus）
// 使用真正的 HNSW 算法，包括动态邻居更新和贪心搜索
type HNSWIndexImproved struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 向量存储
	vectors map[int64][]float32

	// HNSW 图结构
	layers    []map[int64]*hnswNode // 每层的节点
	nodeLevel map[int64]int         // 节点所在的最高层

	// 动态连接策略
	mu  sync.RWMutex
	rng *rand.Rand
}

// hnswNode HNSW 节点
type hnswNode struct {
	id        int64
	vector    []float32
	level     int
	neighbors map[int64]*hnswNode // 每层的邻居（使用指针避免重复计算）
}

// HNSWParamsImproved 改进的 HNSW 参数
type HNSWParamsImproved struct {
	M              int     // 每个节点的最大邻居数
	EFConstruction int     // 构建时的搜索深度
	EFSearch       int     // 搜索时的探索宽度
	ML             float64 // 层数因子
}

// DefaultHNSWParamsImproved 默认参数（优化召回率）
var DefaultHNSWParamsImproved = HNSWParamsImproved{
	M:              32, // 增加邻居数提高召回率
	EFConstruction: 200,
	EFSearch:       128,  // 增加搜索宽度
	ML:             0.25, // 标准层数因子
}

// NewHNSWIndexImproved 创建改进的 HNSW 索引
func NewHNSWIndexImproved(columnName string, config *VectorIndexConfig) (*HNSWIndexImproved, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	return &HNSWIndexImproved{
		columnName: columnName,
		config:     config,
		distFunc:   distFunc,
		vectors:    make(map[int64][]float32),
		layers:     make([]map[int64]*hnswNode, 0),
		nodeLevel:  make(map[int64]int),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（改进版：动态邻居选择）
func (h *HNSWIndexImproved) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// 插入所有向量
	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		h.vectors[rec.ID] = vec
		h.insertNode(rec.ID, vec)
	}

	return nil
}

// insertNode 插入节点到 HNSW 图（动态邻居选择）
func (h *HNSWIndexImproved) insertNode(id int64, vector []float32) {
	// 计算节点层数（几何分布）
	level := h.randomLevel()
	h.nodeLevel[id] = level

	// 确保层数足够
	for len(h.layers) <= level {
		h.layers = append(h.layers, make(map[int64]*hnswNode))
	}

	// 创建节点
	node := &hnswNode{
		id:        id,
		vector:    vector,
		level:     level,
		neighbors: make(map[int64]*hnswNode),
	}

	// 从顶层到底层，动态选择最优邻居
	entryPoint := h.findEntryPoint(level)

	for l := level; l >= 0; l-- {
		// 在当前层搜索最近的 M 个邻居
		candidates := h.searchLayerGreedy(vector, l, entryPoint, DefaultHNSWParamsImproved.EFConstruction)

		// 选择最近的 M 个作为邻居
		if len(candidates) > DefaultHNSWParamsImproved.M {
			candidates = candidates[:DefaultHNSWParamsImproved.M]
		}

		// 双向连接
		for _, candidate := range candidates {
			node.neighbors[candidate.id] = candidate
			candidate.neighbors[id] = node
		}

		// 更新入口点为最近邻
		if len(candidates) > 0 {
			entryPoint = candidates[0]
		}
	}

	// 0 层需要连接到所有邻居
	if level > 0 {
		entryPoint = h.findEntryPoint(0)
		candidates := h.searchLayerGreedy(vector, 0, entryPoint, DefaultHNSWParamsImproved.EFConstruction)
		if len(candidates) > DefaultHNSWParamsImproved.M {
			candidates = candidates[:DefaultHNSWParamsImproved.M]
		}
		for _, candidate := range candidates {
			node.neighbors[candidate.id] = candidate
			candidate.neighbors[id] = node
		}
	}

	// 存储节点
	h.layers[level][id] = node
}

// findEntryPoint 找到入口点（最高层的某个节点）
func (h *HNSWIndexImproved) findEntryPoint(level int) *hnswNode {
	// 随机选择一个在 >= level 层的节点
	candidates := make([]*hnswNode, 0)
	for id, nodeLevel := range h.nodeLevel {
		if nodeLevel >= level && nodeLevel < len(h.layers) {
			node := h.getOrCreateNode(id, nodeLevel)
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// 选择一个作为入口点
	return candidates[h.rng.Intn(len(candidates))]
}

// getOrCreateNode 获取或创建节点
func (h *HNSWIndexImproved) getOrCreateNode(id int64, level int) *hnswNode {
	if level >= len(h.layers) {
		return nil
	}

	layer := h.layers[level]
	if node, exists := layer[id]; exists {
		return node
	}

	// 创建空节点
	vec, exists := h.vectors[id]
	if !exists {
		return nil
	}

	node := &hnswNode{
		id:        id,
		vector:    vec,
		level:     0, // 简化
		neighbors: make(map[int64]*hnswNode),
	}
	layer[id] = node
	return node
}

// searchLayerGreedy 在指定层进行贪心搜索
func (h *HNSWIndexImproved) searchLayerGreedy(query []float32, level int, entryPoint *hnswNode, ef int) []*hnswNode {
	if entryPoint == nil || entryPoint.level < level {
		return nil
	}

	// 使用优先队列（最小堆）
	pq := &minHeap{}
	heap.Init(pq)

	// 初始化访问集合
	visited := make(map[int64]bool)

	// 从入口点开始
	heap.Push(pq, &heapNode{
		node:     entryPoint,
		distance: h.distFunc.Compute(query, entryPoint.vector),
	})
	visited[entryPoint.id] = true

	// 贪心搜索
	for pq.Len() > 0 {
		current := heap.Pop(pq).(*heapNode)

		// 探索邻居
		for _, neighbor := range current.node.neighbors {
			if !visited[neighbor.id] {
				dist := h.distFunc.Compute(query, neighbor.vector)
				heap.Push(pq, &heapNode{
					node:     neighbor,
					distance: dist,
				})
				visited[neighbor.id] = true
			}
		}

		// 收集结果
		if pq.Len() >= ef {
			break
		}
	}

	// 返回按距离排序的节点
	result := make([]*hnswNode, 0)
	for pq.Len() > 0 {
		result = append(result, heap.Pop(pq).(*heapNode).node)
	}

	return result
}

// Search 近似最近邻搜索（改进版：真正的 HNSW 贪心搜索）
func (h *HNSWIndexImproved) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != h.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", h.config.Dimension, len(query))
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.vectors) == 0 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// 应用过滤器
	validIDs := make(map[int64]bool)
	for id := range h.vectors {
		if filter == nil || len(filter.IDs) == 0 {
			validIDs[id] = true
		} else {
			for _, fid := range filter.IDs {
				if fid == id {
					validIDs[id] = true
					break
				}
			}
		}
	}

	if len(validIDs) == 0 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// HNSW 搜索：从顶层到底层
	topLevel := len(h.layers) - 1
	if topLevel < 0 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// 找到入口点（最高层的一个节点）
	entryPoint := h.findEntryPoint(topLevel)
	if entryPoint == nil {
		// 如果没有入口点，返回所有有效ID的前k个
		return h.searchAll(query, k, validIDs), nil
	}

	// ef: 搜索时的探索宽度
	ef := DefaultHNSWParamsImproved.EFSearch
	if k > ef {
		ef = k
	}

	// 阶段1: 从顶层到底层贪心搜索，找到入口点到第0层的最近邻
	nearestInLevel0 := h.searchTopToBottom(query, entryPoint, validIDs)

	// 阶段2: 在第0层扩展搜索 ef 个候选
	candidates := h.searchLayerGreedy(query, 0, nearestInLevel0, ef)

	// 过滤候选
	filteredCandidates := make([]*heapNode, 0, len(candidates))
	for _, node := range candidates {
		if validIDs[node.id] {
			filteredCandidates = append(filteredCandidates, &heapNode{
				node:     node,
				distance: h.distFunc.Compute(query, node.vector),
			})
		}
	}

	// 排序并返回前 k 个
	sort.Slice(filteredCandidates, func(i, j int) bool {
		return filteredCandidates[i].distance < filteredCandidates[j].distance
	})

	if len(filteredCandidates) > k {
		filteredCandidates = filteredCandidates[:k]
	}

	result := &VectorSearchResult{
		IDs:       make([]int64, len(filteredCandidates)),
		Distances: make([]float32, len(filteredCandidates)),
	}

	for i, c := range filteredCandidates {
		result.IDs[i] = c.node.id
		result.Distances[i] = c.distance
	}

	return result, nil
}

// searchTopToBottom 从顶层到底层进行贪心搜索
func (h *HNSWIndexImproved) searchTopToBottom(query []float32, entryPoint *hnswNode, validIDs map[int64]bool) *hnswNode {
	current := entryPoint
	topLevel := len(h.layers) - 1

	// 从顶层到底层
	for level := topLevel; level >= 0; level-- {
		if level >= len(h.layers) {
			continue
		}

		// 在当前层进行贪心搜索，找到距离查询最近的节点
		improved := true
		for improved {
			improved = false
			minDist := h.distFunc.Compute(query, current.vector)

			// 检查当前节点的邻居
			for _, neighbor := range current.neighbors {
				if !validIDs[neighbor.id] {
					continue
				}

				dist := h.distFunc.Compute(query, neighbor.vector)
				if dist < minDist {
					current = neighbor
					minDist = dist
					improved = true
				}
			}
		}
	}

	return current
}

// searchAll 在所有向量中搜索（fallback）
func (h *HNSWIndexImproved) searchAll(query []float32, k int, validIDs map[int64]bool) *VectorSearchResult {
	candidates := make([]*candidateNode, 0, len(validIDs))

	for id, vec := range h.vectors {
		if !validIDs[id] {
			continue
		}

		dist := h.distFunc.Compute(query, vec)
		candidates = append(candidates, &candidateNode{
			id:   id,
			dist: dist,
			node: h.getNode(id),
		})
	}

	// 排序
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

	return result
}

// getNode 获取节点（如果不存在则创建空节点）
func (h *HNSWIndexImproved) getNode(id int64) *hnswNode {
	vec, exists := h.vectors[id]
	if !exists {
		return &hnswNode{
			id:        id,
			vector:    nil,
			level:     0,
			neighbors: make(map[int64]*hnswNode),
		}
	}

	level, exists := h.nodeLevel[id]
	if !exists {
		return &hnswNode{
			id:        id,
			vector:    vec,
			level:     0,
			neighbors: make(map[int64]*hnswNode),
		}
	}

	return &hnswNode{
		id:        id,
		vector:    vec,
		level:     level,
		neighbors: make(map[int64]*hnswNode),
	}
}

// Insert 插入向量
func (h *HNSWIndexImproved) Insert(id int64, vector []float32) error {
	if len(vector) != h.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.config.Dimension, len(vector))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	vec := make([]float32, len(vector))
	copy(vec, vector)
	h.vectors[id] = vec

	h.insertNode(id, vec)

	return nil
}

// Delete 删除向量
func (h *HNSWIndexImproved) Delete(id int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.vectors, id)
	delete(h.nodeLevel, id)

	for level := range h.layers {
		delete(h.layers[level], id)

		// 从邻居中移除
		for _, node := range h.layers[level] {
			delete(node.neighbors, id)
		}
	}

	return nil
}

// GetConfig 获取索引配置
func (h *HNSWIndexImproved) GetConfig() *VectorIndexConfig {
	return h.config
}

// Stats 获取索引统计信息
func (h *HNSWIndexImproved) Stats() VectorIndexStats {
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
		for _, node := range layer {
			memorySize += int64(8 + len(node.vector)*4 + len(node.neighbors)*8)
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
func (h *HNSWIndexImproved) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.vectors = make(map[int64][]float32)
	h.layers = make([]map[int64]*hnswNode, 0)
	h.nodeLevel = make(map[int64]int)

	return nil
}

// randomLevel 随机生成层数（使用 ML 参数）
func (h *HNSWIndexImproved) randomLevel() int {
	level := 0
	ml := DefaultHNSWParamsImproved.ML

	// 使用几何分布：每层有 ml 的概率继续向上
	// 标准HNSW实现：P(level) = 1/ln(M)，这里简化为 0.25
	for h.rng.Float64() < ml {
		level++
	}

	return level
}

// 辅助类型

type candidateNode struct {
	id   int64
	dist float32
	node *hnswNode
}

type heapNode struct {
	node     *hnswNode
	distance float32
}

// minHeap 最小堆实现
type minHeap []*heapNode

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].distance < h[j].distance }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(*heapNode)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
