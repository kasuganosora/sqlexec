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

// HNSWPRQIndex HNSW-PRQ 索引（HNSW with Product Residual Quantization）
// 残差乘积量化：先粗量化，再对残差进行精细的 PQ 量化
type HNSWPRQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 原始向量
	vectors map[int64][]float32

	// 粗量化编码（每个向量一个粗码本索引）
	coarseCodes map[int64]int8

	// 残差 PQ 编码（每个向量一个 PQ 编码）
	residualCodes map[int64][]int8

	// 粗量化码本（较小的码本）
	coarseCodebook [][]float32

	// 残差 PQ 码本
	residualCodebooks [][][]float32

	// HNSW 图结构（使用粗量化编码）
	layers []map[int64]*hnswNodePRQ

	// 参数
	maxLevel     int
	ml           float64
	ef           int
	nsubq        int   // 子量化器数量
	ksubq        int   // 每个子量化器的质心数量
	kcoarse      int   // 粗量化码本大小

	mu   sync.RWMutex
	rng  *rand.Rand
}

// hnswNodePRQ HNSW-PRQ 节点
type hnswNodePRQ struct {
	id         int64
	coarseCode int8       // 粗量化编码
	residual   []int8     // 残差 PQ 编码
	neighbors  [][]int64  // 每一层的邻居
}

// HNSWPRQParams HNSW-PRQ 参数
type HNSWPRQParams struct {
	M         int     // 每层连接数
	MaxLevel  int     // 最大层数
	ML        float64 // level generation factor
	EF        int     // 构建时的搜索宽度
	Nbits     int     // 每个子量化器的编码位数
	Kcoarse   int     // 粗量化码本大小
}

// DefaultHNSWPRQParams 默认参数
var DefaultHNSWPRQParams = HNSWPRQParams{
	M:        16,
	MaxLevel: 16,
	ML:       float64(1.0 / math.Log(16.0)),
	EF:       200,
	Nbits:    8,
	Kcoarse:  64,
}

// NewHNSWPRQIndex 创建 HNSW-PRQ 索引
func NewHNSWPRQIndex(columnName string, config *VectorIndexConfig) (*HNSWPRQIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	maxLevel := DefaultHNSWPRQParams.MaxLevel
	if val, ok := config.Params["max_level"].(int); ok {
		maxLevel = val
	}

	ml := DefaultHNSWPRQParams.ML
	if val, ok := config.Params["ml"].(float64); ok {
		ml = val
	}

	ef := DefaultHNSWPRQParams.EF
	if val, ok := config.Params["ef"].(int); ok {
		ef = val
	}

	nbits := DefaultHNSWPRQParams.Nbits
	if val, ok := config.Params["nbits"].(int); ok {
		nbits = val
	}

	m := DefaultHNSWPRQParams.M
	if val, ok := config.Params["m"].(int); ok {
		m = val
	}

	if m == 0 || config.Dimension%m != 0 {
		return nil, fmt.Errorf("M (%d) must divide dimension (%d)", m, config.Dimension)
	}

	ksubq := 1 << uint(nbits)
	nsubq := m
	kcoarse := DefaultHNSWPRQParams.Kcoarse
	if val, ok := config.Params["kcoarse"].(int); ok {
		kcoarse = val
	}

	return &HNSWPRQIndex{
		columnName:        columnName,
		config:            config,
		distFunc:          distFunc,
		vectors:           make(map[int64][]float32),
		coarseCodes:      make(map[int64]int8),
		residualCodes:     make(map[int64][]int8),
		coarseCodebook:    make([][]float32, kcoarse),
		residualCodebooks: make([][][]float32, nsubq),
		layers:            make([]map[int64]*hnswNodePRQ, maxLevel),
		maxLevel:          maxLevel,
		ml:                ml,
		ef:                ef,
		nsubq:             nsubq,
		ksubq:             ksubq,
		kcoarse:           kcoarse,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（粗量化 + 残差 PQ + HNSW）
func (h *HNSWPRQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
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

	// 训练粗量化码本
	err = h.trainCoarseQuantizer(records)
	if err != nil {
		return err
	}

	// 训练残差 PQ 码本
	err = h.trainResidualPQ(records)
	if err != nil {
		return err
	}

	// 初始化所有层的 map
	for l := 0; l < h.maxLevel; l++ {
		h.layers[l] = make(map[int64]*hnswNodePRQ)
	}

	// 批量插入向量
	for _, rec := range records {
		h.insertNoLock(rec.ID, rec.Vector)
	}

	return nil
}

// trainCoarseQuantizer 训练粗量化码本（K-Means）
func (h *HNSWPRQIndex) trainCoarseQuantizer(records []VectorRecord) error {
	dimension := h.config.Dimension
	kcoarse := h.kcoarse

	// 随机初始化质心
	for c := 0; c < kcoarse; c++ {
		h.coarseCodebook[c] = make([]float32, dimension)
		idx := h.rng.Intn(len(records))
		copy(h.coarseCodebook[c], records[idx].Vector)
	}

	// K-Means 迭代
	maxIterations := 20
	tolerance := float32(1e-4)

	for iter := 0; iter < maxIterations; iter++ {
		assigns := make([]int, len(records))
		sums := make([][]float32, kcoarse)
		counts := make([]int, kcoarse)

		for c := 0; c < kcoarse; c++ {
			sums[c] = make([]float32, dimension)
		}

		// 分配向量到最近的质心
		for i, rec := range records {
			bestCentroid := 0
			minDist := float32(math.MaxFloat32)

			for c := 0; c < kcoarse; c++ {
				dist := h.distFunc.Compute(rec.Vector, h.coarseCodebook[c])
				if dist < minDist {
					minDist = dist
					bestCentroid = c
				}
			}

			assigns[i] = bestCentroid
			for d := 0; d < dimension; d++ {
				sums[bestCentroid][d] += rec.Vector[d]
			}
			counts[bestCentroid]++
		}

		// 更新质心
		converged := true
		for c := 0; c < kcoarse; c++ {
			if counts[c] == 0 {
				continue
			}

			newCentroid := make([]float32, dimension)
			for d := 0; d < dimension; d++ {
				newCentroid[d] = sums[c][d] / float32(counts[c])
			}

			if len(h.coarseCodebook[c]) > 0 {
				shift := h.distFunc.Compute(h.coarseCodebook[c], newCentroid)
				if shift > tolerance {
					converged = false
				}
			}
			h.coarseCodebook[c] = newCentroid
		}

		if converged {
			break
		}
	}

	return nil
}

// trainResidualPQ 训练残差 PQ 码本
func (h *HNSWPRQIndex) trainResidualPQ(records []VectorRecord) error {
	nsubq := h.nsubq
	ksubq := h.ksubq
	subDim := h.config.Dimension / nsubq

	// 计算残差向量
	residuals := make([][]float32, len(records))
	for i, rec := range records {
		// 找到粗量化码
		bestCentroid := 0
		minDist := float32(math.MaxFloat32)

		for c := 0; c < h.kcoarse; c++ {
			dist := h.distFunc.Compute(rec.Vector, h.coarseCodebook[c])
			if dist < minDist {
				minDist = dist
				bestCentroid = c
			}
		}

		// 计算残差
		residual := make([]float32, len(rec.Vector))
		coarseVec := h.coarseCodebook[bestCentroid]
		for d := 0; d < len(rec.Vector); d++ {
			residual[d] = rec.Vector[d] - coarseVec[d]
		}

		h.coarseCodes[rec.ID] = int8(bestCentroid)
		residuals[i] = residual
	}

	// 对残差训练 PQ 码本
	for subq := 0; subq < nsubq; subq++ {
		h.residualCodebooks[subq] = make([][]float32, ksubq)

		// 随机初始化质心
		for c := 0; c < ksubq; c++ {
			h.residualCodebooks[subq][c] = make([]float32, subDim)
			idx := h.rng.Intn(len(records))
			start := subq * subDim
			copy(h.residualCodebooks[subq][c], residuals[idx][start:start+subDim])
		}

		// K-Means 迭代
		err := h.subqKmeansOnResiduals(residuals, subq, subDim, ksubq)
		if err != nil {
			return err
		}
	}

	// 编码所有向量
	for id, vec := range h.vectors {
		coarseCode := h.coarseCodes[id]
		coarseVec := h.coarseCodebook[int(coarseCode)]

		// 计算残差
		residual := make([]float32, len(vec))
		for d := 0; d < len(vec); d++ {
			residual[d] = vec[d] - coarseVec[d]
		}

		// PQ 编码残差
		residualCode := h.encodeResidualPQ(residual)
		h.residualCodes[id] = residualCode
	}

	return nil
}

// subqKmeansOnResiduals 对残差执行 K-Means
func (h *HNSWPRQIndex) subqKmeansOnResiduals(residuals [][]float32, subq, subDim, ksubq int) error {
	maxIterations := 20
	tolerance := float32(1e-4)

	for iter := 0; iter < maxIterations; iter++ {
		sums := make([][]float32, ksubq)
		counts := make([]int, ksubq)

		for c := 0; c < ksubq; c++ {
			sums[c] = make([]float32, subDim)
		}

		assigns := make([]int, len(residuals))

		// 分配残差子向量到最近的质心
		for i, residual := range residuals {
			start := subq * subDim
			end := start + subDim
			subvec := make([]float32, subDim)
			copy(subvec, residual[start:end])

			bestCentroid := 0
			minDist := float32(math.MaxFloat32)

			for c := 0; c < ksubq; c++ {
				dist := float32(0)
				for d := 0; d < subDim; d++ {
					diff := subvec[d] - h.residualCodebooks[subq][c][d]
					dist += diff * diff
				}
				if dist < minDist {
					minDist = dist
					bestCentroid = c
				}
			}

			assigns[i] = bestCentroid
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

			if len(h.residualCodebooks[subq][c]) > 0 {
				shift := float32(0)
				for d := 0; d < subDim; d++ {
					diff := newCentroid[d] - h.residualCodebooks[subq][c][d]
					shift += diff * diff
				}
				if shift > tolerance {
					converged = false
				}
			}
			h.residualCodebooks[subq][c] = newCentroid
		}

		if converged {
			break
		}
	}

	return nil
}

// encodeResidualPQ 对残差进行 PQ 编码
func (h *HNSWPRQIndex) encodeResidualPQ(residual []float32) []int8 {
	nsubq := h.nsubq
	subDim := h.config.Dimension / nsubq
	code := make([]int8, nsubq)

	for subq := 0; subq < nsubq; subq++ {
		start := subq * subDim
		end := start + subDim
		subvec := make([]float32, subDim)
		copy(subvec, residual[start:end])

		// 找到最近的质心
		bestCentroid := 0
		minDist := float32(math.MaxFloat32)

		for c := 0; c < h.ksubq; c++ {
			dist := float32(0)
			for d := 0; d < subDim; d++ {
				diff := subvec[d] - h.residualCodebooks[subq][c][d]
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

// decodeResidualPQ 解码残差 PQ 码
func (h *HNSWPRQIndex) decodeResidualPQ(code []int8) []float32 {
	nsubq := h.nsubq
	subDim := h.config.Dimension / nsubq

	decoded := make([]float32, h.config.Dimension)

	for subq := 0; subq < nsubq; subq++ {
		start := subq * subDim
		end := start + subDim
		centroidIdx := int(code[subq])
		copy(decoded[start:end], h.residualCodebooks[subq][centroidIdx])
	}

	return decoded
}

// computeApproxDistance 计算近似距离
func (h *HNSWPRQIndex) computeApproxDistance(query []float32, coarseCode int8, residualCode []int8) float32 {
	// 反量化向量
	coarseVec := h.coarseCodebook[int(coarseCode)]
	residual := h.decodeResidualPQ(residualCode)

	// 重构向量
	reconstructed := make([]float32, len(query))
	for d := 0; d < len(query); d++ {
		reconstructed[d] = coarseVec[d] + residual[d]
	}

	// 计算距离
	return h.distFunc.Compute(query, reconstructed)
}

// randomLevel 生成随机层数
func (h *HNSWPRQIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < h.ml {
		level++
	}
	return level
}

// heapNode 最小堆节点
type heapNodePRQ struct {
	id       int64
	distance float32
}

// minHeap 最小堆实现
type minHeapPRQ []*heapNodePRQ

func (h minHeapPRQ) Len() int           { return len(h) }
func (h minHeapPRQ) Less(i, j int) bool { return h[i].distance < h[j].distance }
func (h minHeapPRQ) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeapPRQ) Push(x interface{}) { *h = append(*h, x.(*heapNodePRQ)) }
func (h *minHeapPRQ) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// insertNoLock 插入向量（无锁版本）
func (h *HNSWPRQIndex) insertNoLock(id int64, vector []float32) {
	// 粗量化 + 残差 PQ 编码
	coarseCode := h.coarseCodes[id]
	residualCode := h.residualCodes[id]
	h.vectors[id] = vector

	// 计算插入层数
	level := h.randomLevel()
	if level >= h.maxLevel {
		level = h.maxLevel - 1
	}

	// 创建节点
	node := &hnswNodePRQ{
		id:         id,
		coarseCode: coarseCode,
		residual:   residualCode,
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

		candidates := []int64{enterPoint}
		visited := make(map[int64]bool)

		pq := make(minHeapPRQ, 0)
		heap.Init(&pq)

		enterDist := h.computeApproxDistance(vector, h.layers[l][enterPoint].coarseCode, h.layers[l][enterPoint].residual)
		heap.Push(&pq, &heapNodePRQ{id: enterPoint, distance: enterDist})

		for len(pq) > 0 && len(candidates) < h.ef {
			current := heap.Pop(&pq).(*heapNodePRQ)
			if visited[current.id] {
				continue
			}
			visited[current.id] = true
			candidates = append(candidates, current.id)

			nodeL := h.layers[l][current.id]
			for _, neighborID := range nodeL.neighbors[l] {
				if !visited[neighborID] {
					neighborDist := h.computeApproxDistance(vector, h.layers[l][neighborID].coarseCode, h.layers[l][neighborID].residual)
					heap.Push(&pq, &heapNodePRQ{id: neighborID, distance: neighborDist})
				}
			}
		}

		if len(candidates) > 0 {
			bestDist := float32(math.MaxFloat32)
			for _, cid := range candidates {
				dist := h.computeApproxDistance(vector, h.layers[l][cid].coarseCode, h.layers[l][cid].residual)
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

		nearest := h.searchLayerPRQ(vector, l, h.ef, enterPoint)

		maxNeighbors := DefaultHNSWPRQParams.M
		if l == 0 {
			maxNeighbors = DefaultHNSWPRQParams.M * 2
		}

		if len(nearest) > maxNeighbors {
			nearest = nearest[:maxNeighbors]
		}

		node.neighbors[l] = make([]int64, 0, len(nearest))
		for _, nid := range nearest {
			node.neighbors[l] = append(node.neighbors[l], nid)
			h.layers[l][nid].neighbors[l] = append(h.layers[l][nid].neighbors[l], id)

			if len(h.layers[l][nid].neighbors[l]) > maxNeighbors {
				h.pruneNeighborsPRQ(l, nid, maxNeighbors)
			}
		}

		h.layers[l][id] = node

		if len(nearest) > 0 {
			enterPoint = nearest[0]
		}
	}
}

// searchLayerPRQ 在指定层搜索最近的节点
func (h *HNSWPRQIndex) searchLayerPRQ(query []float32, level, ef int, enterPoint int64) []int64 {
	if enterPoint == -1 || len(h.layers[level]) == 0 {
		return nil
	}

	nearest := make([]int64, 0, ef)
	visited := make(map[int64]bool)

	pq := make(minHeapPRQ, 0)
	heap.Init(&pq)

	enterDist := h.computeApproxDistance(query, h.layers[level][enterPoint].coarseCode, h.layers[level][enterPoint].residual)
	heap.Push(&pq, &heapNodePRQ{id: enterPoint, distance: enterDist})

	for len(pq) > 0 {
		current := heap.Pop(&pq).(*heapNodePRQ)

		if visited[current.id] {
			continue
		}

		visited[current.id] = true

		if len(nearest) >= ef {
			maxDist := float32(-1)
			for _, nid := range nearest {
				dist := h.computeApproxDistance(query, h.layers[level][nid].coarseCode, h.layers[level][nid].residual)
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
				neighborDist := h.computeApproxDistance(query, h.layers[level][neighborID].coarseCode, h.layers[level][neighborID].residual)
				heap.Push(&pq, &heapNodePRQ{id: neighborID, distance: neighborDist})
			}
		}
	}

	return nearest
}

// pruneNeighborsPRQ 裁剪邻居节点
func (h *HNSWPRQIndex) pruneNeighborsPRQ(level int, id int64, maxNeighbors int) {
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
func (h *HNSWPRQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
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

		pq := make(minHeapPRQ, 0)
		heap.Init(&pq)

		enterDist := h.computeApproxDistance(query, h.layers[l][enterPoint].coarseCode, h.layers[l][enterPoint].residual)
		heap.Push(&pq, &heapNodePRQ{id: enterPoint, distance: enterDist})

		for len(pq) > 0 && len(candidates) < h.ef {
			current := heap.Pop(&pq).(*heapNodePRQ)
			if visited[current.id] {
				continue
			}
			visited[current.id] = true
			candidates = append(candidates, current.id)

			nodeL := h.layers[l][current.id]
			for _, neighborID := range nodeL.neighbors[l] {
				if !visited[neighborID] {
					neighborDist := h.computeApproxDistance(query, h.layers[l][neighborID].coarseCode, h.layers[l][neighborID].residual)
					heap.Push(&pq, &heapNodePRQ{id: neighborID, distance: neighborDist})
				}
			}
		}

		if len(candidates) > 0 {
			bestDist := float32(math.MaxFloat32)
			for _, cid := range candidates {
				dist := h.computeApproxDistance(query, h.layers[l][cid].coarseCode, h.layers[l][cid].residual)
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

	pq := make(minHeapPRQ, 0)
	heap.Init(&pq)

	enterDist := h.computeApproxDistance(query, h.layers[0][enterPoint].coarseCode, h.layers[0][enterPoint].residual)
	heap.Push(&pq, &heapNodePRQ{id: enterPoint, distance: enterDist})

	for len(pq) > 0 && len(candidates) < h.ef {
		current := heap.Pop(&pq).(*heapNodePRQ)

		if visited[current.id] {
			continue
		}

		visited[current.id] = true
		candidates = append(candidates, current.id)

		node := h.layers[0][current.id]
		for _, neighborID := range node.neighbors[0] {
			if !visited[neighborID] {
				neighborDist := h.computeApproxDistance(query, h.layers[0][neighborID].coarseCode, h.layers[0][neighborID].residual)
				heap.Push(&pq, &heapNodePRQ{id: neighborID, distance: neighborDist})
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
			dist: h.computeApproxDistance(query, h.layers[0][cid].coarseCode, h.layers[0][cid].residual),
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
func (h *HNSWPRQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != h.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.config.Dimension, len(vector))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// 先编码
	bestCentroid := 0
	minDist := float32(math.MaxFloat32)
	for c := 0; c < h.kcoarse; c++ {
		dist := h.distFunc.Compute(vector, h.coarseCodebook[c])
		if dist < minDist {
			minDist = dist
			bestCentroid = c
		}
	}

	coarseCode := int8(bestCentroid)
	coarseVec := h.coarseCodebook[bestCentroid]

	residual := make([]float32, len(vector))
	for d := 0; d < len(vector); d++ {
		residual[d] = vector[d] - coarseVec[d]
	}

	residualCode := h.encodeResidualPQ(residual)

	h.vectors[id] = vector
	h.coarseCodes[id] = coarseCode
	h.residualCodes[id] = residualCode

	h.insertNoLock(id, vector)
	return nil
}

// Delete 删除向量
func (h *HNSWPRQIndex) Delete(id int64) error {
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
	delete(h.coarseCodes, id)
	delete(h.residualCodes, id)

	return nil
}

// GetConfig 获取索引配置
func (h *HNSWPRQIndex) GetConfig() *VectorIndexConfig {
	return h.config
}

// Stats 返回索引统计信息
func (h *HNSWPRQIndex) Stats() VectorIndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var memorySize int64

	// 粗量化编码
	memorySize += int64(len(h.coarseCodes)) * 1

	// 残差 PQ 编码
	for _, code := range h.residualCodes {
		memorySize += int64(len(code)) * 1
	}

	// 粗量化码本
	for _, centroid := range h.coarseCodebook {
		memorySize += int64(len(centroid)) * 4
	}

	// 残差 PQ 码本
	for subq := 0; subq < h.nsubq; subq++ {
		for c := 0; c < h.ksubq; c++ {
			memorySize += int64(len(h.residualCodebooks[subq][c])) * 4
		}
	}

	// 图结构
	for _, layer := range h.layers {
		memorySize += int64(len(layer) * 8)
		for _, node := range layer {
			memorySize += int64(8 + 1 + len(node.residual)*1 + len(node.neighbors)*8)
		}
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorHNSWPRQ,
		Metric:     h.config.MetricType,
		Dimension:  h.config.Dimension,
		Count:      int64(len(h.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (h *HNSWPRQIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.vectors = make(map[int64][]float32)
	h.coarseCodes = make(map[int64]int8)
	h.residualCodes = make(map[int64][]int8)
	for l := 0; l < h.maxLevel; l++ {
		h.layers[l] = make(map[int64]*hnswNodePRQ)
	}

	return nil
}
