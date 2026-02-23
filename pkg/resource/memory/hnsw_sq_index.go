package memory

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// HNSWSQIndex HNSW-SQ 索引（HNSW with Scalar Quantization）
// 使用 HNSW 图结构 + float32->int8 标量量化
type HNSWSQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 原始向量（用于训练量化器）
	vectors map[int64][]float32

	// 量化后的向量存储
	quantizedVectors map[int64][]int8

	// 量化器参数
	scale []float32
	shift []float32

	// HNSW 图结构（第0层使用量化向量）
	layers []map[int64]*hnswNodeSQ

	// 参数
	maxLevel       int
	ml             float64
	efConstruction int // 构建时的探索宽度
	ef             int // 搜索时的探索宽度

	mu  sync.RWMutex
	rng *rand.Rand
}

// hnswNodeSQ HNSW-SQ 节点
type hnswNodeSQ struct {
	id        int64
	vector    []int8    // 量化后的向量
	neighbors [][]int64 // 每一层的邻居
}

// heapNode 最小堆节点
type heapNodeSQ struct {
	id       int64
	distance float32
}

// minHeap 最小堆实现
type minHeapSQ []*heapNodeSQ

func (h minHeapSQ) Len() int            { return len(h) }
func (h minHeapSQ) Less(i, j int) bool  { return h[i].distance < h[j].distance }
func (h minHeapSQ) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeapSQ) Push(x interface{}) { *h = append(*h, x.(*heapNodeSQ)) }
func (h *minHeapSQ) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// HNSWSQParams HNSW-SQ 参数
type HNSWSQParams struct {
	M              int     // 每层连接数（默认：8）
	MaxLevel       int     // 最大层数（默认：16）
	ML             float64 // level generation factor（默认：1/ln(M)）
	EFConstruction int     // 构建时的搜索宽度（默认：40）
	EF             int     // 搜索时的探索宽度（默认：96）
}

// DefaultHNSWSQParams 默认参数（参考 Milvus 最佳实践）
var DefaultHNSWSQParams = HNSWSQParams{
	M:              8,
	MaxLevel:       16,
	ML:             float64(1.0 / math.Log(8.0)),
	EFConstruction: 40,
	EF:             96,
}

// NewHNSWSQIndex 创建 HNSW-SQ 索引
func NewHNSWSQIndex(columnName string, config *VectorIndexConfig) (*HNSWSQIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	maxLevel := DefaultHNSWSQParams.MaxLevel
	if val, ok := config.Params["max_level"].(int); ok {
		maxLevel = val
	}

	ml := DefaultHNSWSQParams.ML
	if val, ok := config.Params["ml"].(float64); ok {
		ml = val
	}

	efConstruction := DefaultHNSWSQParams.EFConstruction
	if val, ok := config.Params["efConstruction"].(int); ok {
		efConstruction = val
	}

	ef := DefaultHNSWSQParams.EF
	if val, ok := config.Params["ef"].(int); ok {
		ef = val
	}

	return &HNSWSQIndex{
		columnName:       columnName,
		config:           config,
		distFunc:         distFunc,
		vectors:          make(map[int64][]float32),
		quantizedVectors: make(map[int64][]int8),
		scale:            make([]float32, config.Dimension),
		shift:            make([]float32, config.Dimension),
		layers:           make([]map[int64]*hnswNodeSQ, maxLevel),
		maxLevel:         maxLevel,
		ml:               ml,
		efConstruction:   efConstruction,
		ef:               ef,
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（训练量化器 + 构建 HNSW）
func (h *HNSWSQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// 存储原始向量
	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		h.vectors[rec.ID] = vec
	}

	// 训练标量量化器
	err = h.trainScalarQuantizer(records)
	if err != nil {
		return err
	}

	// 初始化所有层的 map
	for l := 0; l < h.maxLevel; l++ {
		h.layers[l] = make(map[int64]*hnswNodeSQ)
	}

	// 批量插入向量
	for _, rec := range records {
		h.insertNoLock(rec.ID, rec.Vector)
	}

	return nil
}

// trainScalarQuantizer 训练标量量化器
func (h *HNSWSQIndex) trainScalarQuantizer(records []VectorRecord) error {
	dimension := h.config.Dimension

	// 计算每个维度的最大最小值
	mins := make([]float32, dimension)
	maxs := make([]float32, dimension)
	for d := 0; d < dimension; d++ {
		mins[d] = math.MaxFloat32
		maxs[d] = -math.MaxFloat32
	}

	for _, rec := range records {
		for d := 0; d < dimension; d++ {
			val := rec.Vector[d]
			if val < mins[d] {
				mins[d] = val
			}
			if val > maxs[d] {
				maxs[d] = val
			}
		}
	}

	// 计算缩放和偏移
	for d := 0; d < dimension; d++ {
		if maxs[d] == mins[d] {
			h.scale[d] = 1.0
			h.shift[d] = 0.0
		} else {
			h.scale[d] = 127.0 / (maxs[d] - mins[d])
			h.shift[d] = (maxs[d] + mins[d]) / 2.0
		}
	}

	return nil
}

// quantize 将 float32 向量量化为 int8
func (h *HNSWSQIndex) quantize(vec []float32) []int8 {
	quantized := make([]int8, len(vec))
	for d := 0; d < len(vec); d++ {
		val := (vec[d] - h.shift[d]) * h.scale[d]
		if val > 127 {
			val = 127
		}
		if val < -128 {
			val = -128
		}
		quantized[d] = int8(val)
	}
	return quantized
}

// dequantize 将 int8 向量反量化为 float32
func (h *HNSWSQIndex) dequantize(vec []int8) []float32 {
	dequantized := make([]float32, len(vec))
	for d := 0; d < len(vec); d++ {
		dequantized[d] = float32(vec[d])/h.scale[d] + h.shift[d]
	}
	return dequantized
}

// computeQuantizedDistance 计算量化向量的近似距离
func (h *HNSWSQIndex) computeQuantizedDistance(query []float32, quantized []int8) float32 {
	dequantized := h.dequantize(quantized)
	return h.distFunc.Compute(query, dequantized)
}

// randomLevel 生成随机层数
func (h *HNSWSQIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < h.ml {
		level++
	}
	return level
}

// insertNoLock 插入向量（无锁版本）
func (h *HNSWSQIndex) insertNoLock(id int64, vector []float32) {
	// 量化向量
	quantized := h.quantize(vector)
	h.vectors[id] = vector
	h.quantizedVectors[id] = quantized

	// 计算插入层数
	level := h.randomLevel()
	if level >= h.maxLevel {
		level = h.maxLevel - 1
	}

	// 创建节点
	node := &hnswNodeSQ{
		id:     id,
		vector: quantized,
	}

	// 初始化邻居列表
	node.neighbors = make([][]int64, level+1)

	// 从顶层开始逐层插入
	// 从最高层到当前层，使用贪心搜索
	enterPoint := int64(-1)
	if len(h.layers[0]) > 0 {
		// 找到入口点（第0层的第一个节点）
		for nid := range h.layers[0] {
			enterPoint = nid
			break
		}
	}

	// 从最高层到当前层+1，只做搜索更新
	for l := h.maxLevel - 1; l > level; l-- {
		if enterPoint == -1 || len(h.layers[l]) == 0 {
			continue
		}
		// 在第 l 层搜索
		candidates := []int64{enterPoint}
		visited := make(map[int64]bool)

		// 使用优先队列进行贪心搜索
		pq := make(minHeapSQ, 0)
		heap.Init(&pq)

		// 计算入口点的距离
		enterDist := h.computeQuantizedDistance(vector, h.layers[l][enterPoint].vector)
		heap.Push(&pq, &heapNodeSQ{id: enterPoint, distance: enterDist})

		for len(pq) > 0 && len(candidates) < h.efConstruction {
			current := heap.Pop(&pq).(*heapNodeSQ)
			if visited[current.id] {
				continue
			}
			visited[current.id] = true
			candidates = append(candidates, current.id)

			nodeL := h.layers[l][current.id]
			for _, neighborID := range nodeL.neighbors[l] {
				if !visited[neighborID] {
					neighborDist := h.computeQuantizedDistance(vector, h.layers[l][neighborID].vector)
					heap.Push(&pq, &heapNodeSQ{id: neighborID, distance: neighborDist})
				}
			}
		}

		// 更新入口点
		if len(candidates) > 0 {
			// 找到距离最近的节点
			bestDist := float32(math.MaxFloat32)
			for _, cid := range candidates {
				dist := h.computeQuantizedDistance(vector, h.layers[l][cid].vector)
				if dist < bestDist {
					bestDist = dist
					enterPoint = cid
				}
			}
		}
	}

	// 从当前层到第0层，进行插入和连接
	for l := level; l >= 0; l-- {
		if len(h.layers[l]) == 0 {
			// 空层，直接添加节点
			h.layers[l][id] = node
			continue
		}

		// 在第 l 层搜索最近的 efConstruction 个节点
		nearest := h.searchLayerSQ(vector, l, h.efConstruction, enterPoint)

		// 连接到最近的节点
		maxNeighbors := DefaultHNSWSQParams.M
		if l == 0 {
			maxNeighbors = DefaultHNSWSQParams.M * 2 // 第0层连接更多
		}

		// 选择最近的 maxNeighbors 个邻居
		if len(nearest) > maxNeighbors {
			nearest = nearest[:maxNeighbors]
		}

		// 添加边（双向）
		node.neighbors[l] = make([]int64, 0, len(nearest))
		for _, nid := range nearest {
			node.neighbors[l] = append(node.neighbors[l], nid)
			h.layers[l][nid].neighbors[l] = append(h.layers[l][nid].neighbors[l], id)

			// 裁剪邻居节点（如果超过最大连接数）
			if len(h.layers[l][nid].neighbors[l]) > maxNeighbors {
				// 按距离排序并保留最近的
				h.pruneNeighbors(l, nid, maxNeighbors)
			}
		}

		// 添加到第 l 层
		h.layers[l][id] = node

		// 更新入口点为最近节点
		if len(nearest) > 0 {
			enterPoint = nearest[0]
		}
	}
}

// searchLayerSQ 在指定层搜索最近的节点
func (h *HNSWSQIndex) searchLayerSQ(query []float32, level, ef int, enterPoint int64) []int64 {
	if enterPoint == -1 || len(h.layers[level]) == 0 {
		return nil
	}

	// 贪心搜索
	nearest := make([]int64, 0, ef)
	visited := make(map[int64]bool)

	pq := make(minHeapSQ, 0)
	heap.Init(&pq)

	// 添加入口点
	enterDist := h.computeQuantizedDistance(query, h.layers[level][enterPoint].vector)
	heap.Push(&pq, &heapNodeSQ{id: enterPoint, distance: enterDist})

	for len(pq) > 0 {
		current := heap.Pop(&pq).(*heapNodeSQ)

		if visited[current.id] {
			continue
		}

		visited[current.id] = true

		// 如果已找到足够的最近邻，且当前距离比最远的还远，停止
		if len(nearest) >= ef {
			// 找到最远的距离
			maxDist := float32(-1)
			for _, nid := range nearest {
				dist := h.computeQuantizedDistance(query, h.layers[level][nid].vector)
				if dist > maxDist {
					maxDist = dist
				}
			}
			if current.distance >= maxDist {
				break
			}
		}

		nearest = append(nearest, current.id)

		// 扩展邻居
		node := h.layers[level][current.id]
		for _, neighborID := range node.neighbors[level] {
			if !visited[neighborID] {
				neighborDist := h.computeQuantizedDistance(query, h.layers[level][neighborID].vector)
				heap.Push(&pq, &heapNodeSQ{id: neighborID, distance: neighborDist})
			}
		}
	}

	return nearest
}

// pruneNeighbors 裁剪邻居节点
func (h *HNSWSQIndex) pruneNeighbors(level int, id int64, maxNeighbors int) {
	node := h.layers[level][id]
	if len(node.neighbors[level]) <= maxNeighbors {
		return
	}

	// 计算所有邻居到 id 的距离
	type neighborDist struct {
		id   int64
		dist float32
	}

	neighbors := make([]neighborDist, len(node.neighbors[level]))
	for i, nid := range node.neighbors[level] {
		neighbors[i] = neighborDist{
			id:   nid,
			dist: h.distFunc.Compute(h.vectors[id], h.vectors[nid]),
		}
	}

	// 按距离排序
	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i].dist < neighbors[j].dist
	})

	// 保留最近的 maxNeighbors 个
	node.neighbors[level] = make([]int64, 0, maxNeighbors)
	for i := 0; i < maxNeighbors; i++ {
		node.neighbors[level] = append(node.neighbors[level], neighbors[i].id)
	}
}

// Search 搜索最近邻（从顶层到底层）
func (h *HNSWSQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
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

	// 获取 ef 搜索参数（可动态配置）
	ef := h.ef
	if val, ok := h.config.Params["ef"].(int); ok {
		ef = val
	}

	// 从顶层开始搜索
	enterPoint := int64(-1)
	if len(h.layers[0]) > 0 {
		for nid := range h.layers[0] {
			enterPoint = nid
			break
		}
	}

	if enterPoint == -1 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// 从顶层到第1层，只做搜索更新
	for l := h.maxLevel - 1; l > 0; l-- {
		if len(h.layers[l]) == 0 {
			continue
		}
		// 在第 l 层搜索
		candidates := []int64{enterPoint}
		visited := make(map[int64]bool)

		pq := make(minHeapSQ, 0)
		heap.Init(&pq)

		enterDist := h.computeQuantizedDistance(query, h.layers[l][enterPoint].vector)
		heap.Push(&pq, &heapNodeSQ{id: enterPoint, distance: enterDist})

		for len(pq) > 0 && len(candidates) < h.ef {
			current := heap.Pop(&pq).(*heapNodeSQ)
			if visited[current.id] {
				continue
			}
			visited[current.id] = true
			candidates = append(candidates, current.id)

			nodeL := h.layers[l][current.id]
			for _, neighborID := range nodeL.neighbors[l] {
				if !visited[neighborID] {
					neighborDist := h.computeQuantizedDistance(query, h.layers[l][neighborID].vector)
					heap.Push(&pq, &heapNodeSQ{id: neighborID, distance: neighborDist})
				}
			}
		}

		// 更新入口点
		if len(candidates) > 0 {
			bestDist := float32(math.MaxFloat32)
			for _, cid := range candidates {
				dist := h.computeQuantizedDistance(query, h.layers[l][cid].vector)
				if dist < bestDist {
					bestDist = dist
					enterPoint = cid
				}
			}
		}
	}

	// 在第0层进行优先队列扩展搜索
	candidates := []int64{enterPoint}
	visited := make(map[int64]bool)

	pq := make(minHeapSQ, 0)
	heap.Init(&pq)

	enterDist := h.computeQuantizedDistance(query, h.layers[0][enterPoint].vector)
	heap.Push(&pq, &heapNodeSQ{id: enterPoint, distance: enterDist})

	for len(pq) > 0 && len(candidates) < ef {
		current := heap.Pop(&pq).(*heapNodeSQ)

		if visited[current.id] {
			continue
		}

		visited[current.id] = true
		candidates = append(candidates, current.id)

		node := h.layers[0][current.id]
		for _, neighborID := range node.neighbors[0] {
			if !visited[neighborID] {
				neighborDist := h.computeQuantizedDistance(query, h.layers[0][neighborID].vector)
				heap.Push(&pq, &heapNodeSQ{id: neighborID, distance: neighborDist})
			}
		}
	}

	// 应用过滤器
	var filtered []int64
	if filter != nil && len(filter.IDs) > 0 {
		filterSet := make(map[int64]bool)
		for _, fid := range filter.IDs {
			filterSet[fid] = true
		}
		for _, cid := range candidates {
			if filterSet[cid] {
				filtered = append(filtered, cid)
			}
		}
		candidates = filtered
	}

	// 按距离排序
	type resultItem struct {
		id   int64
		dist float32
	}
	results := make([]resultItem, len(candidates))
	for i, cid := range candidates {
		results[i] = resultItem{
			id:   cid,
			dist: h.computeQuantizedDistance(query, h.layers[0][cid].vector),
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

	// 返回前 k 个
	if len(results) > k {
		results = results[:k]
	}

	output := &VectorSearchResult{
		IDs:       make([]int64, len(results)),
		Distances: make([]float32, len(results)),
	}

	for i, r := range results {
		output.IDs[i] = r.id
		output.Distances[i] = r.dist
	}

	return output, nil
}

// Insert 插入向量
func (h *HNSWSQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != h.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.config.Dimension, len(vector))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.insertNoLock(id, vector)
	return nil
}

// Delete 删除向量
func (h *HNSWSQIndex) Delete(id int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 从所有层中删除节点
	for l := 0; l < h.maxLevel; l++ {
		if node, exists := h.layers[l][id]; exists {
			// 从邻居的列表中移除该节点
			for _, neighborID := range node.neighbors[l] {
				if neighborNode, exists := h.layers[l][neighborID]; exists {
					// 删除反向边
					newNeighbors := make([]int64, 0)
					for _, nid := range neighborNode.neighbors[l] {
						if nid != id {
							newNeighbors = append(newNeighbors, nid)
						}
					}
					neighborNode.neighbors[l] = newNeighbors
				}
			}
			delete(h.layers[l], id)
		}
	}

	delete(h.vectors, id)
	delete(h.quantizedVectors, id)

	return nil
}

// GetConfig 获取索引配置
func (h *HNSWSQIndex) GetConfig() *VectorIndexConfig {
	return h.config
}

// Stats 返回索引统计信息
func (h *HNSWSQIndex) Stats() VectorIndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var memorySize int64

	// 量化后的向量数据（int8）
	for _, vec := range h.quantizedVectors {
		memorySize += int64(len(vec)) * 1
	}

	// 量化器参数
	memorySize += int64(len(h.scale)) * 4
	memorySize += int64(len(h.shift)) * 4

	// 图结构
	for _, layer := range h.layers {
		memorySize += int64(len(layer) * 8) // map overhead
		for _, node := range layer {
			memorySize += int64(8 + len(node.vector)*1 + len(node.neighbors)*8)
		}
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorHNSWSQ,
		Metric:     h.config.MetricType,
		Dimension:  h.config.Dimension,
		Count:      int64(len(h.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (h *HNSWSQIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.vectors = make(map[int64][]float32)
	h.quantizedVectors = make(map[int64][]int8)
	h.scale = make([]float32, 0)
	h.shift = make([]float32, 0)
	for l := 0; l < h.maxLevel; l++ {
		h.layers[l] = make(map[int64]*hnswNodeSQ)
	}

	return nil
}
