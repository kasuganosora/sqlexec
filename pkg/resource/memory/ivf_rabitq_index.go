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

// IVFRabitQIndex IVF-RaBitQ 索引
// 基于 SIGMOD 2024 论文 "RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound"
type IVFRabitQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 原始向量
	vectors map[int64][]float32

	// RaBitQ 量化编码（D 维向量 -> D 比特二进制串）
	quantizedVectors map[int64][]uint64 // 每个 uint64 表示 64 个维度的二进制编码

	// 随机投影矩阵（Johnson-Lindenstrauss 变换）
	projectionMatrix [][]float32

	// IVF 结构
	centroids        [][]float32
	vectorsByCluster map[int][]VectorRecord
	assignments      map[int64]int
	clusterCounts    []int

	// 参数
	nlist int

	mu   sync.RWMutex
	rng  *rand.Rand
}

// IVFRabitQParams IVF-RaBitQ 参数
type IVFRabitQParams struct {
	Nlist  int // 聚类数量
	Nprobe int // 搜索时检查的聚类数量
}

// DefaultIVFRabitQParams 默认参数
var DefaultIVFRabitQParams = IVFRabitQParams{
	Nlist:  100,
	Nprobe: 10,
}

// NewIVFRabitQIndex 创建 IVF-RaBitQ 索引
func NewIVFRabitQIndex(columnName string, config *VectorIndexConfig) (*IVFRabitQIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	nlist := DefaultIVFRabitQParams.Nlist
	if val, ok := config.Params["nlist"].(int); ok {
		nlist = val
	}

	// 生成随机投影矩阵
	dimension := config.Dimension
	projectionMatrix := make([][]float32, dimension)
	for i := 0; i < dimension; i++ {
		projectionMatrix[i] = make([]float32, dimension)
		// 使用正态分布随机初始化
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
		for j := 0; j < dimension; j++ {
			projectionMatrix[i][j] = float32(rng.NormFloat64() / math.Sqrt(float64(dimension)))
		}
	}

	return &IVFRabitQIndex{
		columnName:        columnName,
		config:            config,
		distFunc:          distFunc,
		vectors:           make(map[int64][]float32),
		quantizedVectors:  make(map[int64][]uint64),
		projectionMatrix:  projectionMatrix,
		centroids:         make([][]float32, nlist),
		vectorsByCluster:  make(map[int][]VectorRecord, nlist),
		assignments:       make(map[int64]int),
		clusterCounts:     make([]int, nlist),
		nlist:             nlist,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（IVF 聚类 + RaBitQ 量化）
func (i *IVFRabitQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// 存储原始向量
	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		i.vectors[rec.ID] = vec
	}

	// 执行 IVF K-Means 聚类
	err = i.kmeans(records)
	if err != nil {
		return err
	}

	// 对所有向量执行 RaBitQ 量化
	for id, vec := range i.vectors {
		quantized := i.rabitqQuantize(vec)
		i.quantizedVectors[id] = quantized
	}

	return nil
}

// kmeans K-Means 聚类
func (i *IVFRabitQIndex) kmeans(records []VectorRecord) error {
	dimension := i.config.Dimension
	nlist := i.nlist

	// 初始化聚类中心
	i.centroids = make([][]float32, nlist)
	for j := 0; j < nlist; j++ {
		center := make([]float32, dimension)
		idx := i.rng.Intn(len(records))
		copy(center, records[idx].Vector)
		i.centroids[j] = center
	}

	// K-Means 迭代
	maxIterations := 20
	tolerance := float32(1e-4)

	for iter := 0; iter < maxIterations; iter++ {
		for j := 0; j < nlist; j++ {
			i.vectorsByCluster[j] = make([]VectorRecord, 0)
			i.clusterCounts[j] = 0
		}

		assignments := make(map[int64]int)
		for _, rec := range records {
			bestCluster := 0
			minDist := float32(math.MaxFloat32)

			for j, center := range i.centroids {
				dist := i.distFunc.Compute(rec.Vector, center)
				if dist < minDist {
					minDist = dist
					bestCluster = j
				}
			}

			assignments[rec.ID] = bestCluster
			i.vectorsByCluster[bestCluster] = append(i.vectorsByCluster[bestCluster], rec)
			i.clusterCounts[bestCluster]++
		}

		converged := true
		if iter > 0 {
			for id, newCluster := range assignments {
				if oldCluster, exists := i.assignments[id]; !exists || oldCluster != newCluster {
					converged = false
					break
				}
			}
		}

		for j := 0; j < nlist; j++ {
			if len(i.vectorsByCluster[j]) == 0 {
				continue
			}

			newCenter := make([]float32, dimension)
			for _, rec := range i.vectorsByCluster[j] {
				for d := 0; d < dimension; d++ {
					newCenter[d] += rec.Vector[d]
				}
			}

			count := float32(len(i.vectorsByCluster[j]))
			for d := 0; d < dimension; d++ {
				newCenter[d] /= count
			}

			if len(i.centroids[j]) > 0 {
				shift := i.distFunc.Compute(i.centroids[j], newCenter)
				if shift > tolerance {
					converged = false
				}
			}
			i.centroids[j] = newCenter
		}

		i.assignments = assignments

		if converged {
			break
		}
	}

	return nil
}

// rabitqQuantize RaBitQ 量化：D 维向量 -> D 比特二进制编码
func (i *IVFRabitQIndex) rabitqQuantize(vec []float32) []uint64 {
	dimension := i.config.Dimension

	// 步骤1：随机投影（Johnson-Lindenstrauss 变换）
	projected := make([]float32, dimension)
	for d := 0; d < dimension; d++ {
		dot := float32(0)
		for j := 0; j < dimension; j++ {
			dot += i.projectionMatrix[d][j] * vec[j]
		}
		projected[d] = dot
	}

	// 步骤2：二值化（每个维度量化为 1 比特）
	// 使用 0 作为阈值（假设投影后的向量分布以 0 为中心）
	binWords := make([]uint64, (dimension+63)/64) // 向上取整

	for d := 0; d < dimension; d++ {
		wordIdx := d / 64
		bitIdx := uint(d % 64)

		if projected[d] >= 0 {
			binWords[wordIdx] |= (1 << bitIdx)
		}
	}

	return binWords
}

// computeHammingDistance 计算汉明距离（RaBitQ 的距离估计）
func (i *IVFRabitQIndex) computeHammingDistance(a, b []uint64) int {
	distance := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		// XOR 后统计 1 的个数
		xor := a[i] ^ b[i]
		distance += popcount(xor)
	}
	return distance
}

// popcount 统计 uint64 中 1 的个数（用于汉明距离）
func popcount(x uint64) int {
	// Brian Kernighan 算法
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

// Search 搜索最近邻（IVF + RaBitQ）
func (i *IVFRabitQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != i.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", i.config.Dimension, len(query))
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	if len(i.vectors) == 0 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// 获取 nprobe 参数
	nprobe := DefaultIVFRabitQParams.Nprobe
	if val, ok := i.config.Params["nprobe"].(int); ok {
		nprobe = val
	}
	if nprobe > i.nlist {
		nprobe = i.nlist
	}

	// 对查询向量进行 RaBitQ 量化
	quantizedQuery := i.rabitqQuantize(query)

	// 找到 nprobe 个最近的聚类中心
	type clusterDist struct {
		clusterID int
		distance  float32
	}

	clusterDists := make([]clusterDist, i.nlist)
	for j, center := range i.centroids {
		if i.clusterCounts[j] == 0 {
			continue
		}
		dist := i.distFunc.Compute(query, center)
		clusterDists[j] = clusterDist{clusterID: j, distance: dist}
	}

	sort.Slice(clusterDists, func(a, b int) bool {
		return clusterDists[a].distance < clusterDists[b].distance
	})

	// 收集候选向量
	type candidate struct {
		id       int64
		distance int // 汉明距离
	}
	candidates := make([]candidate, 0, len(i.vectors))

	// 在选中的聚类中搜索
	for j := 0; j < nprobe; j++ {
		clusterID := clusterDists[j].clusterID

		for _, rec := range i.vectorsByCluster[clusterID] {
			if filter != nil && len(filter.IDs) > 0 {
				found := false
				for _, fid := range filter.IDs {
					if fid == rec.ID {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			quantizedVec, exists := i.quantizedVectors[rec.ID]
			if !exists {
				continue
			}

			// 使用汉明距离作为距离估计
			dist := i.computeHammingDistance(quantizedQuery, quantizedVec)
			candidates = append(candidates, candidate{
				id:       rec.ID,
				distance: dist,
			})
		}
	}

	// 按汉明距离排序
	sort.Slice(candidates, func(a, b int) bool {
		return candidates[a].distance < candidates[b].distance
	})

	// 返回前 k 个
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	result := &VectorSearchResult{
		IDs:       make([]int64, len(candidates)),
		Distances: make([]float32, len(candidates)),
	}

	for j, c := range candidates {
		result.IDs[j] = c.id
		// 汉明距离作为近似距离
		result.Distances[j] = float32(c.distance)
	}

	return result, nil
}

// Insert 插入向量
func (i *IVFRabitQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != i.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", i.config.Dimension, len(vector))
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	vec := make([]float32, len(vector))
	copy(vec, vector)
	i.vectors[id] = vec

	// RaBitQ 量化
	quantized := i.rabitqQuantize(vec)
	i.quantizedVectors[id] = quantized

	// 找到最近的聚类
	bestCluster := 0
	minDist := float32(math.MaxFloat32)
	for j, center := range i.centroids {
		dist := i.distFunc.Compute(vector, center)
		if dist < minDist {
			minDist = dist
			bestCluster = j
		}
	}

	// 添加到聚类
	i.vectorsByCluster[bestCluster] = append(i.vectorsByCluster[bestCluster], VectorRecord{
		ID:     id,
		Vector: vec,
	})
	i.clusterCounts[bestCluster]++
	i.assignments[id] = bestCluster

	return nil
}

// Delete 删除向量
func (i *IVFRabitQIndex) Delete(id int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	cluster, exists := i.assignments[id]
	if !exists {
		return nil
	}

	vecs := i.vectorsByCluster[cluster]
	for j, rec := range vecs {
		if rec.ID == id {
			i.vectorsByCluster[cluster] = append(vecs[:j], vecs[j+1:]...)
			break
		}
	}

	delete(i.vectors, id)
	delete(i.quantizedVectors, id)
	delete(i.assignments, id)
	i.clusterCounts[cluster]--

	return nil
}

// GetConfig 获取索引配置
func (i *IVFRabitQIndex) GetConfig() *VectorIndexConfig {
	return i.config
}

// Stats 返回索引统计信息
func (i *IVFRabitQIndex) Stats() VectorIndexStats {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var memorySize int64

	// 量化后的向量（每个维度 1 比特）
	for _, vec := range i.quantizedVectors {
		memorySize += int64(len(vec)) * 8 // 每个 uint64 是 8 字节
	}

	// 投影矩阵
	memorySize += int64(len(i.projectionMatrix) * len(i.projectionMatrix[0])) * 4

	// 聚类中心
	for _, centroid := range i.centroids {
		memorySize += int64(len(centroid)) * 4
	}

	// 聚类分配
	memorySize += int64(len(i.assignments) * 8)
	for _, recs := range i.vectorsByCluster {
		memorySize += int64(len(recs) * (8 + 4*i.config.Dimension))
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorIVFRabitQ,
		Metric:     i.config.MetricType,
		Dimension:  i.config.Dimension,
		Count:      int64(len(i.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (i *IVFRabitQIndex) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.vectors = make(map[int64][]float32)
	i.quantizedVectors = make(map[int64][]uint64)
	i.centroids = make([][]float32, 0)
	i.vectorsByCluster = make(map[int][]VectorRecord, 0)
	i.assignments = make(map[int64]int)
	i.clusterCounts = make([]int, 0)

	return nil
}
