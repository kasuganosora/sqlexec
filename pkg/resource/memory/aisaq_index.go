package memory

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// AISAQIndex AISAQ 索引
// 自适应索引标量量化，基于 Vamana 图 + 自适应标量量化
type AISAQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 原始向量
	vectors map[int64][]float32

	// 自适应量化编码
	quantizedVectors map[int64][]int8

	// 自适应量化参数
	scale []float32
	shift []float32

	// Vamana 图结构
	graph map[int64]*vamanaNode

	// 参数
	maxDegree      int
	searchListSize int

	mu  sync.RWMutex
	rng *rand.Rand
}

// vamanaNode Vamana 图节点
type vamanaNode struct {
	id        int64
	vector    []int8
	neighbors []int64
}

// AISAQParams AISAQ 参数
type AISAQParams struct {
	MaxDegree      int
	SearchListSize int
}

// DefaultAISAQParams 默认参数
var DefaultAISAQParams = AISAQParams{
	MaxDegree:      56,
	SearchListSize: 100,
}

// NewAISAQIndex 创建 AISAQ 索引
func NewAISAQIndex(columnName string, config *VectorIndexConfig) (*AISAQIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	maxDegree := DefaultAISAQParams.MaxDegree
	if val, ok := config.Params["max_degree"].(int); ok {
		maxDegree = val
	}

	searchListSize := DefaultAISAQParams.SearchListSize
	if val, ok := config.Params["search_list_size"].(int); ok {
		searchListSize = val
	}

	return &AISAQIndex{
		columnName:       columnName,
		config:           config,
		distFunc:         distFunc,
		vectors:          make(map[int64][]float32),
		quantizedVectors: make(map[int64][]int8),
		scale:            make([]float32, config.Dimension),
		shift:            make([]float32, config.Dimension),
		graph:            make(map[int64]*vamanaNode),
		maxDegree:        maxDegree,
		searchListSize:   searchListSize,
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引
func (a *AISAQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// 存储原始向量
	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		a.vectors[rec.ID] = vec
	}

	// 训练自适应标量量化器
	err = a.trainAdaptiveScalarQuantizer(records)
	if err != nil {
		return err
	}

	// 量化所有向量
	for id, vec := range a.vectors {
		quantized := a.adaptiveQuantize(vec)
		a.quantizedVectors[id] = quantized
	}

	// 构建 Vamana 图
	err = a.buildVamanaGraph(records)
	if err != nil {
		return err
	}

	return nil
}

// trainAdaptiveScalarQuantizer 训练自适应标量量化器
func (a *AISAQIndex) trainAdaptiveScalarQuantizer(records []VectorRecord) error {
	dimension := a.config.Dimension

	// 计算每个维度的统计量
	mins := make([]float32, dimension)
	maxs := make([]float32, dimension)
	mean := make([]float32, dimension)
	std := make([]float32, dimension)

	for d := 0; d < dimension; d++ {
		mins[d] = math.MaxFloat32
		maxs[d] = -math.MaxFloat32
		mean[d] = 0
		std[d] = 0
	}

	// 第一遍：计算最小最大值和均值
	for _, rec := range records {
		for d := 0; d < len(rec.Vector); d++ {
			val := rec.Vector[d]
			if val < mins[d] {
				mins[d] = val
			}
			if val > maxs[d] {
				maxs[d] = val
			}
			mean[d] += val
		}
	}

	count := float32(len(records))
	for d := 0; d < dimension; d++ {
		mean[d] /= count
	}

	// 第二遍：计算标准差
	for _, rec := range records {
		for d := 0; d < len(rec.Vector); d++ {
			diff := rec.Vector[d] - mean[d]
			std[d] += diff * diff
		}
	}

	for d := 0; d < dimension; d++ {
		std[d] = float32(math.Sqrt(float64(std[d] / count)))
	}

	// 计算缩放和偏移
	for d := 0; d < dimension; d++ {
		if maxs[d] == mins[d] {
			a.scale[d] = 1.0
			a.shift[d] = 0.0
		} else {
			if std[d] > 0 {
				a.scale[d] = 127.0 / std[d]
			} else {
				a.scale[d] = 1.0
			}
			a.shift[d] = mean[d]
		}
	}

	return nil
}

// adaptiveQuantize 自适应量化
func (a *AISAQIndex) adaptiveQuantize(vec []float32) []int8 {
	quantized := make([]int8, len(vec))
	for d := 0; d < len(vec); d++ {
		val := (vec[d] - a.shift[d]) * a.scale[d]
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

// adaptiveDequantize 自适应反量化
func (a *AISAQIndex) adaptiveDequantize(quantized []int8) []float32 {
	dequantized := make([]float32, len(quantized))
	for d := 0; d < len(quantized); d++ {
		dequantized[d] = float32(quantized[d])/a.scale[d] + a.shift[d]
	}
	return dequantized
}

// buildVamanaGraph 构建 Vamana 图
func (a *AISAQIndex) buildVamanaGraph(records []VectorRecord) error {
	// 按随机顺序插入节点
	indices := a.rng.Perm(len(records))

	// 第一个节点作为起点
	if len(records) > 0 {
		startIdx := indices[0]
		startID := records[startIdx].ID
		quantized := a.quantizedVectors[startID]

		a.graph[startID] = &vamanaNode{
			id:        startID,
			vector:    quantized,
			neighbors: []int64{},
		}
	}

	// 依次插入其他节点
	for i := 1; i < len(indices); i++ {
		idx := indices[i]
		rec := records[idx]
		quantized := a.quantizedVectors[rec.ID]

		// 使用 beam search查找最近的邻居
		neighbors := a.beamSearch(quantized, a.searchListSize)

		// 裁剪邻居到 maxDegree
		if len(neighbors) > a.maxDegree {
			neighbors = a.pruneFurthest(rec.ID, neighbors, a.maxDegree)
		}

		// 创建节点并添加边（双向）
		node := &vamanaNode{
			id:        rec.ID,
			vector:    quantized,
			neighbors: make([]int64, len(neighbors)),
		}
		copy(node.neighbors, neighbors)

		a.graph[rec.ID] = node

		// 添加反向边
		for _, nid := range neighbors {
			if neighborNode, exists := a.graph[nid]; exists {
				neighborNode.neighbors = append(neighborNode.neighbors, rec.ID)
				// 裁剪邻居节点的边
				if len(neighborNode.neighbors) > a.maxDegree {
					// 裁剪最远的边
					neighborNode.neighbors = a.pruneFurthest(nid, neighborNode.neighbors, a.maxDegree)
				}
			}
		}
	}

	return nil
}

// beamSearch 使用 beam search查找最近的节点
func (a *AISAQIndex) beamSearch(query []int8, beamWidth int) []int64 {
	if len(a.graph) == 0 {
		return nil
	}

	// 任意选择一个节点作为起点
	var startID int64
	for id := range a.graph {
		startID = id
		break
	}

	// beam search
	type candidate struct {
		id       int64
		distance float32
	}

	beam := make([]candidate, 0, beamWidth)
	visited := make(map[int64]bool)

	// 添加起点
	startDist := a.computeAdaptiveDistance(query, a.graph[startID].vector)
	beam = append(beam, candidate{id: startID, distance: startDist})
	visited[startID] = true

	for len(beam) < beamWidth {
		if len(beam) == 0 {
			break
		}

		// 找到 beam 中距离最近的节点
		sort.Slice(beam, func(i, j int) bool {
			return beam[i].distance < beam[j].distance
		})

		// 扩展最近的节点
		current := beam[0]
		beam = beam[1:]

		// 添加未访问的邻居
		for _, neighborID := range a.graph[current.id].neighbors {
			if !visited[neighborID] {
				visited[neighborID] = true
				dist := a.computeAdaptiveDistance(query, a.graph[neighborID].vector)
				beam = append(beam, candidate{id: neighborID, distance: dist})
			}
		}
	}

	// 返回 beam 中的所有节点
	result := make([]int64, 0, len(beam))
	for _, c := range beam {
		result = append(result, c.id)
	}

	return result
}

// computeAdaptiveDistance 计算自适应量化向量的距离
func (a *AISAQIndex) computeAdaptiveDistance(q, v []int8) float32 {
	dequantizedQ := a.adaptiveDequantize(q)
	dequantizedV := a.adaptiveDequantize(v)
	return a.distFunc.Compute(dequantizedQ, dequantizedV)
}

// pruneFurthest 裁剪最远的邻居
func (a *AISAQIndex) pruneFurthest(id int64, neighbors []int64, maxDegree int) []int64 {
	if len(neighbors) <= maxDegree {
		return neighbors
	}

	type neighborDist struct {
		id   int64
		dist float32
	}

	neighborsWithDist := make([]neighborDist, len(neighbors))
	baseVec := a.vectors[id]

	for i, nid := range neighbors {
		nodeVec := a.vectors[nid]
		neighborsWithDist[i] = neighborDist{
			id:   nid,
			dist: a.distFunc.Compute(baseVec, nodeVec),
		}
	}

	sort.Slice(neighborsWithDist, func(i, j int) bool {
		return neighborsWithDist[i].dist < neighborsWithDist[j].dist
	})

	result := make([]int64, maxDegree)
	for i := 0; i < maxDegree; i++ {
		result[i] = neighborsWithDist[i].id
	}

	return result
}

// Search 搜索最近邻
func (a *AISAQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != a.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", a.config.Dimension, len(query))
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.vectors) == 0 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// 量化查询向量
	quantizedQuery := a.adaptiveQuantize(query)

	// 使用 beam search查找最近的节点
	candidates := a.beamSearch(quantizedQuery, a.maxDegree*k)

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

	// 计算实际距离并排序
	type resultItem struct {
		id   int64
		dist float32
	}

	results := make([]resultItem, 0, len(candidates))
	for _, cid := range candidates {
		dist := a.distFunc.Compute(query, a.vectors[cid])
		results = append(results, resultItem{id: cid, dist: dist})
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
func (a *AISAQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != a.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", a.config.Dimension, len(vector))
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	vec := make([]float32, len(vector))
	copy(vec, vector)
	a.vectors[id] = vec

	// 自适应量化
	quantized := a.adaptiveQuantize(vec)
	a.quantizedVectors[id] = quantized

	// 查找最近的邻居
	neighbors := a.beamSearch(quantized, a.searchListSize)

	// 裁剪邻居
	if len(neighbors) > a.maxDegree {
		neighbors = a.pruneFurthest(id, neighbors, a.maxDegree)
	}

	// 创建节点
	node := &vamanaNode{
		id:        id,
		vector:    quantized,
		neighbors: neighbors,
	}

	a.graph[id] = node

	// 添加反向边
	for _, nid := range neighbors {
		if neighborNode, exists := a.graph[nid]; exists {
			neighborNode.neighbors = append(neighborNode.neighbors, id)
			if len(neighborNode.neighbors) > a.maxDegree {
				neighborNode.neighbors = a.pruneFurthest(nid, neighborNode.neighbors, a.maxDegree)
			}
		}
	}

	return nil
}

// Delete 删除向量
func (a *AISAQIndex) Delete(id int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	node, exists := a.graph[id]
	if !exists {
		return nil
	}

	// 从邻居的列表中移除
	for _, nid := range node.neighbors {
		if neighborNode, exists := a.graph[nid]; exists {
			newNeighbors := make([]int64, 0)
			for _, nnid := range neighborNode.neighbors {
				if nnid != id {
					newNeighbors = append(newNeighbors, nnid)
				}
			}
			neighborNode.neighbors = newNeighbors
		}
	}

	delete(a.graph, id)
	delete(a.vectors, id)
	delete(a.quantizedVectors, id)

	return nil
}

// GetConfig 获取索引配置
func (a *AISAQIndex) GetConfig() *VectorIndexConfig {
	return a.config
}

// Stats 返回索引统计信息
func (a *AISAQIndex) Stats() VectorIndexStats {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var memorySize int64

	// 量化后的向量
	for _, vec := range a.quantizedVectors {
		memorySize += int64(len(vec)) * 1
	}

	// 量化器参数
	memorySize += int64(len(a.scale)) * 4
	memorySize += int64(len(a.shift)) * 4

	// 图结构
	memorySize += int64(len(a.graph) * 8)
	for _, node := range a.graph {
		memorySize += int64(8 + len(node.vector)*1 + len(node.neighbors)*8)
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorAISAQ,
		Metric:     a.config.MetricType,
		Dimension:  a.config.Dimension,
		Count:      int64(len(a.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (a *AISAQIndex) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.vectors = make(map[int64][]float32)
	a.quantizedVectors = make(map[int64][]int8)
	a.scale = make([]float32, 0)
	a.shift = make([]float32, 0)
	a.graph = make(map[int64]*vamanaNode)

	return nil
}
