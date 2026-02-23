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

// HNSWPQIndex HNSW-PQ 索引（HNSW with Product Quantization）
// 使用 HNSW 图结构 + 乘积量化
type HNSWPQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 原始向量（用于训练量化器）
	vectors map[int64][]float32

	// PQ 编码
	codes map[int64][]int8 // 向量ID -> PQ编码

	// PQ 码本
	codebooks [][][]float32 // codebooks[subq][centroid_id][subvec_dim]

	// PQ 参数
	nsubq int // 子向量数量
	ksubq int // 每个子量化器的质心数量

	// HNSW 图结构（第0层使用 PQ 编码）
	layers []map[int64]*hnswNodePQ

	// 参数
	maxLevel       int
	ml             float64
	efConstruction int // 构建时的探索宽度
	ef             int // 搜索时的探索宽度

	mu  sync.RWMutex
	rng *rand.Rand
}

// hnswNodePQ HNSW-PQ 节点
type hnswNodePQ struct {
	id        int64
	code      []int8    // PQ 编码
	neighbors [][]int64 // 每一层的邻居
}

// HNSWPQParams HNSW-PQ 参数
type HNSWPQParams struct {
	M              int     // 每层连接数（默认：8）
	MaxLevel       int     // 最大层数（默认：16）
	ML             float64 // level generation factor（默认：1/ln(M)）
	EFConstruction int     // 构建时的探索宽度（默认：40）
	EF             int     // 搜索时的探索宽度（默认：96）
	Nbits          int     // 每个子量化器的编码位数（默认：8）
}

// DefaultHNSWPQParams 默认参数（参考 Milvus 最佳实践）
var DefaultHNSWPQParams = HNSWPQParams{
	M:              8,
	MaxLevel:       16,
	ML:             float64(1.0 / math.Log(8.0)),
	EFConstruction: 40,
	EF:             96,
	Nbits:          8,
}

// NewHNSWPQIndex 创建 HNSW-PQ 索引
func NewHNSWPQIndex(columnName string, config *VectorIndexConfig) (*HNSWPQIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	maxLevel := DefaultHNSWPQParams.MaxLevel
	if val, ok := config.Params["max_level"].(int); ok {
		maxLevel = val
	}

	ml := DefaultHNSWPQParams.ML
	if val, ok := config.Params["ml"].(float64); ok {
		ml = val
	}

	efConstruction := DefaultHNSWPQParams.EFConstruction
	if val, ok := config.Params["efConstruction"].(int); ok {
		efConstruction = val
	}

	ef := DefaultHNSWPQParams.EF
	if val, ok := config.Params["ef"].(int); ok {
		ef = val
	}

	nbits := DefaultHNSWPQParams.Nbits
	if val, ok := config.Params["nbits"].(int); ok {
		nbits = val
	}

	m := DefaultHNSWPQParams.M
	if val, ok := config.Params["m"].(int); ok {
		m = val
	}

	if m == 0 || config.Dimension%m != 0 {
		return nil, fmt.Errorf("M (%d) must divide dimension (%d)", m, config.Dimension)
	}

	ksubq := 1 << uint(nbits)
	nsubq := m

	return &HNSWPQIndex{
		columnName:     columnName,
		config:         config,
		distFunc:       distFunc,
		vectors:        make(map[int64][]float32),
		codes:          make(map[int64][]int8),
		codebooks:      make([][][]float32, nsubq),
		layers:         make([]map[int64]*hnswNodePQ, maxLevel),
		nsubq:          nsubq,
		ksubq:          ksubq,
		maxLevel:       maxLevel,
		ml:             ml,
		efConstruction: efConstruction,
		ef:             ef,
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（训练 PQ + 构建 HNSW）
func (h *HNSWPQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
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

	// 训练乘积量化器
	err = h.trainProductQuantizer(records)
	if err != nil {
		return err
	}

	// 初始化所有层的 map
	for l := 0; l < h.maxLevel; l++ {
		h.layers[l] = make(map[int64]*hnswNodePQ)
	}

	// 批量插入向量
	for _, rec := range records {
		h.insertNoLock(rec.ID, rec.Vector)
	}

	return nil
}

// trainProductQuantizer 训练乘积量化器
func (h *HNSWPQIndex) trainProductQuantizer(records []VectorRecord) error {
	nsubq := h.nsubq
	ksubq := h.ksubq
	subDim := h.config.Dimension / nsubq

	// 初始化每个子量化器的码本
	for subq := 0; subq < nsubq; subq++ {
		h.codebooks[subq] = make([][]float32, ksubq)

		// 随机初始化质心
		for c := 0; c < ksubq; c++ {
			h.codebooks[subq][c] = make([]float32, subDim)
			idx := h.rng.Intn(len(records))
			start := subq * subDim
			copy(h.codebooks[subq][c], records[idx].Vector[start:start+subDim])
		}

		// 对每个子向量执行 K-Means
		err := h.subqKmeans(records, subq, subDim, ksubq)
		if err != nil {
			return err
		}
	}

	return nil
}

// subqKmeans 对子量化器执行 K-Means
func (h *HNSWPQIndex) subqKmeans(records []VectorRecord, subq, subDim, ksubq int) error {
	maxIterations := 20
	tolerance := float32(1e-4)

	for iter := 0; iter < maxIterations; iter++ {
		// 统计每个质心的子向量
		sums := make([][]float32, ksubq)
		counts := make([]int, ksubq)
		for c := 0; c < ksubq; c++ {
			sums[c] = make([]float32, subDim)
		}

		// 分配子向量到最近的质心
		for _, rec := range records {
			start := subq * subDim
			subvec := rec.Vector[start : start+subDim]

			bestCentroid := 0
			minDist := float32(math.MaxFloat32)

			for c := 0; c < ksubq; c++ {
				dist := float32(0)
				for d := 0; d < subDim; d++ {
					diff := subvec[d] - h.codebooks[subq][c][d]
					dist += diff * diff
				}
				if dist < minDist {
					minDist = dist
					bestCentroid = c
				}
			}

			for d := 0; d < subDim; d++ {
				sums[bestCentroid][d] += subvec[d]
			}
			counts[bestCentroid]++
		}

		// 更新质心
		converged := true
		for c := 0; c < ksubq; c++ {
			if counts[c] == 0 {
				continue
			}

			newCentroid := make([]float32, subDim)
			for d := 0; d < subDim; d++ {
				newCentroid[d] = sums[c][d] / float32(counts[c])
			}

			// 检查变化
			if len(h.codebooks[subq][c]) > 0 {
				shift := float32(0)
				for d := 0; d < subDim; d++ {
					diff := newCentroid[d] - h.codebooks[subq][c][d]
					shift += diff * diff
				}
				if shift > tolerance {
					converged = false
				}
			}

			h.codebooks[subq][c] = newCentroid
		}

		if converged {
			break
		}
	}

	return nil
}

// encode 编码向量为 PQ 码
func (h *HNSWPQIndex) encode(vec []float32) []int8 {
	nsubq := h.nsubq
	subDim := h.config.Dimension / nsubq
	code := make([]int8, nsubq)

	for subq := 0; subq < nsubq; subq++ {
		start := subq * subDim
		subvec := vec[start : start+subDim]

		// 找到最近的质心
		bestCentroid := 0
		minDist := float32(math.MaxFloat32)

		for c := 0; c < h.ksubq; c++ {
			dist := float32(0)
			for d := 0; d < subDim; d++ {
				diff := subvec[d] - h.codebooks[subq][c][d]
				dist += diff * diff
			}
			if dist < minDist {
				minDist = dist
				bestCentroid = c
			}
		}

		code[subq] = int8(bestCentroid)
	}

	return code
}

// computeApproxDistance 计算 PQ 编码向量的近似距离
func (h *HNSWPQIndex) computeApproxDistance(query []float32, code []int8) float32 {
	nsubq := h.nsubq
	subDim := h.config.Dimension / nsubq

	distance := float32(0)

	for subq := 0; subq < nsubq; subq++ {
		start := subq * subDim
		subvec := query[start : start+subDim]
		centroidIdx := int(code[subq])

		// 计算 L2 距离
		for d := 0; d < subDim; d++ {
			diff := subvec[d] - h.codebooks[subq][centroidIdx][d]
			distance += diff * diff
		}
	}

	return distance
}

// randomLevel 生成随机层数
func (h *HNSWPQIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < h.ml {
		level++
	}
	return level
}

// heapNode 最小堆节点
type heapNodePQ struct {
	id       int64
	distance float32
}

// minHeap 最小堆实现
type minHeapPQ []*heapNodePQ

func (h minHeapPQ) Len() int            { return len(h) }
func (h minHeapPQ) Less(i, j int) bool  { return h[i].distance < h[j].distance }
func (h minHeapPQ) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeapPQ) Push(x interface{}) { *h = append(*h, x.(*heapNodePQ)) }
func (h *minHeapPQ) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// insertNoLock 插入向量（无锁版本）
func (h *HNSWPQIndex) insertNoLock(id int64, vector []float32) {
	// 编码向量
	code := h.encode(vector)
	h.vectors[id] = vector
	h.codes[id] = code

	// 计算插入层数
	level := h.randomLevel()
	if level >= h.maxLevel {
		level = h.maxLevel - 1
	}

	// 创建节点
	node := &hnswNodePQ{
		id:   id,
		code: code,
	}

	// 初始化邻居列表
	node.neighbors = make([][]int64, level+1)

	// 从顶层开始逐层插入
	enterPoint := int64(-1)
	if len(h.layers[0]) > 0 {
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

		pq := make(minHeapPQ, 0)
		heap.Init(&pq)

		enterDist := h.computeApproxDistance(vector, h.layers[l][enterPoint].code)
		heap.Push(&pq, &heapNodePQ{id: enterPoint, distance: enterDist})

		for len(pq) > 0 && len(candidates) < h.efConstruction {
			current := heap.Pop(&pq).(*heapNodePQ)
			if visited[current.id] {
				continue
			}
			visited[current.id] = true
			candidates = append(candidates, current.id)

			nodeL := h.layers[l][current.id]
			for _, neighborID := range nodeL.neighbors[l] {
				if !visited[neighborID] {
					neighborDist := h.computeApproxDistance(vector, h.layers[l][neighborID].code)
					heap.Push(&pq, &heapNodePQ{id: neighborID, distance: neighborDist})
				}
			}
		}

		// 更新入口点
		if len(candidates) > 0 {
			bestDist := float32(math.MaxFloat32)
			for _, cid := range candidates {
				dist := h.computeApproxDistance(vector, h.layers[l][cid].code)
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
			h.layers[l][id] = node
			continue
		}

		// 在第 l 层搜索最近的 h.ef 个节点
		nearest := h.searchLayerPQ(vector, l, h.ef, enterPoint)

		// 连接到最近的节点
		maxNeighbors := DefaultHNSWPQParams.M
		if l == 0 {
			maxNeighbors = DefaultHNSWPQParams.M * 2
		}

		if len(nearest) > maxNeighbors {
			nearest = nearest[:maxNeighbors]
		}

		// 添加边（双向）
		node.neighbors[l] = make([]int64, 0, len(nearest))
		for _, nid := range nearest {
			node.neighbors[l] = append(node.neighbors[l], nid)
			h.layers[l][nid].neighbors[l] = append(h.layers[l][nid].neighbors[l], id)

			// 裁剪邻居节点
			if len(h.layers[l][nid].neighbors[l]) > maxNeighbors {
				h.pruneNeighborsPQ(l, nid, maxNeighbors)
			}
		}

		// 添加到第 l 层
		h.layers[l][id] = node

		// 更新入口点
		if len(nearest) > 0 {
			enterPoint = nearest[0]
		}
	}
}

// searchLayerPQ 在指定层搜索最近的节点
func (h *HNSWPQIndex) searchLayerPQ(query []float32, level, ef int, enterPoint int64) []int64 {
	if enterPoint == -1 || len(h.layers[level]) == 0 {
		return nil
	}

	nearest := make([]int64, 0, ef)
	visited := make(map[int64]bool)

	pq := make(minHeapPQ, 0)
	heap.Init(&pq)

	enterDist := h.computeApproxDistance(query, h.layers[level][enterPoint].code)
	heap.Push(&pq, &heapNodePQ{id: enterPoint, distance: enterDist})

	for len(pq) > 0 {
		current := heap.Pop(&pq).(*heapNodePQ)

		if visited[current.id] {
			continue
		}

		visited[current.id] = true

		if len(nearest) >= ef {
			maxDist := float32(-1)
			for _, nid := range nearest {
				dist := h.computeApproxDistance(query, h.layers[level][nid].code)
				if dist > maxDist {
					maxDist = dist
				}
			}
			if current.distance >= maxDist {
				break
			}
		}

		nearest = append(nearest, current.id)

		node := h.layers[level][current.id]
		for _, neighborID := range node.neighbors[level] {
			if !visited[neighborID] {
				neighborDist := h.computeApproxDistance(query, h.layers[level][neighborID].code)
				heap.Push(&pq, &heapNodePQ{id: neighborID, distance: neighborDist})
			}
		}
	}

	return nearest
}

// pruneNeighborsPQ 裁剪邻居节点
func (h *HNSWPQIndex) pruneNeighborsPQ(level int, id int64, maxNeighbors int) {
	node := h.layers[level][id]
	if len(node.neighbors[level]) <= maxNeighbors {
		return
	}

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

	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i].dist < neighbors[j].dist
	})

	node.neighbors[level] = make([]int64, 0, maxNeighbors)
	for i := 0; i < maxNeighbors; i++ {
		node.neighbors[level] = append(node.neighbors[level], neighbors[i].id)
	}
}

// Search 搜索最近邻
func (h *HNSWPQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
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

	// 从顶层到第1层
	for l := h.maxLevel - 1; l > 0; l-- {
		if len(h.layers[l]) == 0 {
			continue
		}
		candidates := []int64{enterPoint}
		visited := make(map[int64]bool)

		pq := make(minHeapPQ, 0)
		heap.Init(&pq)

		enterDist := h.computeApproxDistance(query, h.layers[l][enterPoint].code)
		heap.Push(&pq, &heapNodePQ{id: enterPoint, distance: enterDist})

		for len(pq) > 0 && len(candidates) < ef {
			current := heap.Pop(&pq).(*heapNodePQ)
			if visited[current.id] {
				continue
			}
			visited[current.id] = true
			candidates = append(candidates, current.id)

			nodeL := h.layers[l][current.id]
			for _, neighborID := range nodeL.neighbors[l] {
				if !visited[neighborID] {
					neighborDist := h.computeApproxDistance(query, h.layers[l][neighborID].code)
					heap.Push(&pq, &heapNodePQ{id: neighborID, distance: neighborDist})
				}
			}
		}

		if len(candidates) > 0 {
			bestDist := float32(math.MaxFloat32)
			for _, cid := range candidates {
				dist := h.computeApproxDistance(query, h.layers[l][cid].code)
				if dist < bestDist {
					bestDist = dist
					enterPoint = cid
				}
			}
		}
	}

	// 第0层优先队列扩展搜索
	candidates := []int64{enterPoint}
	visited := make(map[int64]bool)

	pq := make(minHeapPQ, 0)
	heap.Init(&pq)

	enterDist := h.computeApproxDistance(query, h.layers[0][enterPoint].code)
	heap.Push(&pq, &heapNodePQ{id: enterPoint, distance: enterDist})

	for len(pq) > 0 && len(candidates) < ef {
		current := heap.Pop(&pq).(*heapNodePQ)

		if visited[current.id] {
			continue
		}

		visited[current.id] = true
		candidates = append(candidates, current.id)

		node := h.layers[0][current.id]
		for _, neighborID := range node.neighbors[0] {
			if !visited[neighborID] {
				neighborDist := h.computeApproxDistance(query, h.layers[0][neighborID].code)
				heap.Push(&pq, &heapNodePQ{id: neighborID, distance: neighborDist})
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
			dist: h.computeApproxDistance(query, h.layers[0][cid].code),
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

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
func (h *HNSWPQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != h.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.config.Dimension, len(vector))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.insertNoLock(id, vector)
	return nil
}

// Delete 删除向量
func (h *HNSWPQIndex) Delete(id int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for l := 0; l < h.maxLevel; l++ {
		if node, exists := h.layers[l][id]; exists {
			for _, neighborID := range node.neighbors[l] {
				if neighborNode, exists := h.layers[l][neighborID]; exists {
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
	delete(h.codes, id)

	return nil
}

// GetConfig 获取索引配置
func (h *HNSWPQIndex) GetConfig() *VectorIndexConfig {
	return h.config
}

// Stats 返回索引统计信息
func (h *HNSWPQIndex) Stats() VectorIndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var memorySize int64

	// PQ 编码
	for _, code := range h.codes {
		memorySize += int64(len(code)) * 1
	}

	// 码本
	for subq := 0; subq < h.nsubq; subq++ {
		for c := 0; c < h.ksubq; c++ {
			memorySize += int64(len(h.codebooks[subq][c])) * 4
		}
	}

	// 图结构
	for _, layer := range h.layers {
		memorySize += int64(len(layer) * 8)
		for _, node := range layer {
			memorySize += int64(8 + len(node.code)*1 + len(node.neighbors)*8)
		}
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorHNSWPQ,
		Metric:     h.config.MetricType,
		Dimension:  h.config.Dimension,
		Count:      int64(len(h.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (h *HNSWPQIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.vectors = make(map[int64][]float32)
	h.codes = make(map[int64][]int8)
	for l := 0; l < h.maxLevel; l++ {
		h.layers[l] = make(map[int64]*hnswNodePQ)
	}

	return nil
}
