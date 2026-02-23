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

// IVFPQIndex IVF-PQ 索引（IVF with Product Quantization）
// 使用 K-Means 聚类 + 乘积量化
type IVFPQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 原始向量（用于训练量化器）
	vectors map[int64][]float32

	// PQ 参数
	nsubq     int              // 子向量数量（维度分片）
	centroids [][][]float32    // centroids[subq][centroid_id][subvec_dim]
	codebooks [][][]float32    // 每个子量化器的码本
	codes     map[int64][]int8 // 向量ID -> PQ编码

	// IVF 结构
	ivfCentroids     [][]float32            // IVF 聚类中心
	vectorsByCluster map[int][]VectorRecord // 每个聚类的向量
	assignments      map[int64]int          // 向量ID -> 聚类ID
	clusterCounts    []int                  // 每个聚类的向量数量

	// 参数
	nlist int // IVF 聚类数量
	m     int // 子量化器数量
	nbits int // 每个子量化器的编码位数
	ksubq int // 每个子量化器的质心数量 (2^nbits)

	mu  sync.RWMutex
	rng *rand.Rand
}

// IVFPQParams IVF-PQ 参数
type IVFPQParams struct {
	Nlist  int // IVF 聚类数量（默认：sqrt(n)）
	Nprobe int // 搜索时检查的聚类数量（默认：10）
	M      int // 子量化器数量（默认：dimension/2）
	Nbits  int // 每个子量化器的编码位数（默认：8）
}

// DefaultIVFPQParams 默认参数（参考 Milvus 最佳实践）
var DefaultIVFPQParams = IVFPQParams{
	Nlist:  128,
	Nprobe: 32,
	M:      16,
	Nbits:  8,
}

// NewIVFPQIndex 创建 IVF-PQ 索引
func NewIVFPQIndex(columnName string, config *VectorIndexConfig) (*IVFPQIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	nlist := DefaultIVFPQParams.Nlist
	if val, ok := config.Params["nlist"].(int); ok {
		nlist = val
	}

	m := DefaultIVFPQParams.M
	if val, ok := config.Params["m"].(int); ok {
		m = val
	}
	if m == 0 || config.Dimension%m != 0 {
		return nil, fmt.Errorf("M (%d) must divide dimension (%d)", m, config.Dimension)
	}

	nbits := DefaultIVFPQParams.Nbits
	if val, ok := config.Params["nbits"].(int); ok {
		nbits = val
	}

	ksubq := 1 << uint(nbits)

	return &IVFPQIndex{
		columnName:       columnName,
		config:           config,
		distFunc:         distFunc,
		vectors:          make(map[int64][]float32),
		codes:            make(map[int64][]int8),
		ivfCentroids:     make([][]float32, nlist),
		vectorsByCluster: make(map[int][]VectorRecord, nlist),
		assignments:      make(map[int64]int),
		clusterCounts:    make([]int, nlist),
		nlist:            nlist,
		m:                m,
		nbits:            nbits,
		ksubq:            ksubq,
		nsubq:            m,
		centroids:        make([][][]float32, m),
		codebooks:        make([][][]float32, m),
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（IVF + PQ）
func (i *IVFPQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
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
	err = i.ivfKmeans(records)
	if err != nil {
		return err
	}

	// 训练乘积量化器
	err = i.trainProductQuantizer(records)
	if err != nil {
		return err
	}

	// 编码所有向量
	for id, vec := range i.vectors {
		code := i.encode(vec)
		i.codes[id] = code
	}

	return nil
}

// ivfKmeans IVF K-Means 聚类
func (i *IVFPQIndex) ivfKmeans(records []VectorRecord) error {
	dimension := i.config.Dimension
	nlist := i.nlist

	// 初始化聚类中心（随机选择）
	i.ivfCentroids = make([][]float32, nlist)
	for j := 0; j < nlist; j++ {
		center := make([]float32, dimension)
		idx := i.rng.Intn(len(records))
		copy(center, records[idx].Vector)
		i.ivfCentroids[j] = center
	}

	// K-Means 迭代
	maxIterations := 20
	tolerance := float32(1e-4)

	for iter := 0; iter < maxIterations; iter++ {
		// 清空聚类
		for j := 0; j < nlist; j++ {
			i.vectorsByCluster[j] = make([]VectorRecord, 0)
			i.clusterCounts[j] = 0
		}

		// 分配每个向量到最近的聚类中心
		assignments := make(map[int64]int)
		for _, rec := range records {
			bestCluster := 0
			minDist := float32(math.MaxFloat32)

			for j, center := range i.ivfCentroids {
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

		// 检查收敛
		converged := true
		if iter > 0 {
			for id, newCluster := range assignments {
				if oldCluster, exists := i.assignments[id]; !exists || oldCluster != newCluster {
					converged = false
					break
				}
			}
		}

		// 更新聚类中心
		for j := 0; j < nlist; j++ {
			if len(i.vectorsByCluster[j]) == 0 {
				continue
			}

			// 计算新的中心（均值）
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

			// 检查中心变化
			if len(i.ivfCentroids[j]) > 0 {
				shift := i.distFunc.Compute(i.ivfCentroids[j], newCenter)
				if shift > tolerance {
					converged = false
				}
			}
			i.ivfCentroids[j] = newCenter
		}

		// 更新分配
		i.assignments = assignments

		// 如果收敛，提前结束
		if converged {
			break
		}
	}

	return nil
}

// trainProductQuantizer 训练乘积量化器
func (i *IVFPQIndex) trainProductQuantizer(records []VectorRecord) error {
	m := i.m
	ksubq := i.ksubq
	subDim := i.config.Dimension / m

	// 初始化每个子量化器的码本
	for subq := 0; subq < m; subq++ {
		i.codebooks[subq] = make([][]float32, ksubq)
		i.centroids[subq] = make([][]float32, ksubq)

		// 随机初始化质心
		for c := 0; c < ksubq; c++ {
			i.centroids[subq][c] = make([]float32, subDim)
			idx := i.rng.Intn(len(records))
			start := subq * subDim
			copy(i.centroids[subq][c], records[idx].Vector[start:start+subDim])
		}

		// 对每个子向量执行 K-Means
		err := i.subqKmeans(records, subq, subDim, ksubq)
		if err != nil {
			return err
		}

		// 将质心复制到码本
		for c := 0; c < ksubq; c++ {
			i.codebooks[subq][c] = make([]float32, subDim)
			copy(i.codebooks[subq][c], i.centroids[subq][c])
		}
	}

	return nil
}

// subqKmeans 对子量化器执行 K-Means
func (i *IVFPQIndex) subqKmeans(records []VectorRecord, subq, subDim, ksubq int) error {
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
					diff := subvec[d] - i.centroids[subq][c][d]
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
			if len(i.centroids[subq][c]) > 0 {
				shift := float32(0)
				for d := 0; d < subDim; d++ {
					diff := newCentroid[d] - i.centroids[subq][c][d]
					shift += diff * diff
				}
				if shift > tolerance {
					converged = false
				}
			}

			i.centroids[subq][c] = newCentroid
		}

		if converged {
			break
		}
	}

	return nil
}

// encode 编码向量为 PQ 码
func (i *IVFPQIndex) encode(vec []float32) []int8 {
	m := i.m
	subDim := i.config.Dimension / m
	code := make([]int8, m)

	for subq := 0; subq < m; subq++ {
		start := subq * subDim
		subvec := vec[start : start+subDim]

		// 找到最近的质心
		bestCentroid := 0
		minDist := float32(math.MaxFloat32)

		for c := 0; c < i.ksubq; c++ {
			dist := float32(0)
			for d := 0; d < subDim; d++ {
				diff := subvec[d] - i.codebooks[subq][c][d]
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
func (i *IVFPQIndex) computeApproxDistance(query []float32, code []int8) float32 {
	m := i.m
	subDim := i.config.Dimension / m

	// 使用对称距离计算（预计算查询向量到所有质心的距离）
	distance := float32(0)

	for subq := 0; subq < m; subq++ {
		start := subq * subDim
		subvec := query[start : start+subDim]
		centroidIdx := int(code[subq])

		// 计算 L2 距离
		for d := 0; d < subDim; d++ {
			diff := subvec[d] - i.codebooks[subq][centroidIdx][d]
			distance += diff * diff
		}
	}

	return distance
}

// Search 搜索最近邻（IVF + PQ）
func (i *IVFPQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
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
	nprobe := DefaultIVFPQParams.Nprobe
	if val, ok := i.config.Params["nprobe"].(int); ok {
		nprobe = val
	}
	if nprobe > i.nlist {
		nprobe = i.nlist
	}

	// 阶段1: 找到 nprobe 个最近的聚类中心
	type clusterDist struct {
		clusterID int
		distance  float32
	}

	clusterDists := make([]clusterDist, i.nlist)
	for j, center := range i.ivfCentroids {
		if i.clusterCounts[j] == 0 {
			continue
		}
		dist := i.distFunc.Compute(query, center)
		clusterDists[j] = clusterDist{clusterID: j, distance: dist}
	}

	// 按距离排序
	sort.Slice(clusterDists, func(a, b int) bool {
		return clusterDists[a].distance < clusterDists[b].distance
	})

	// 收集候选向量
	candidates := make([]candidateNode, 0, len(i.vectors))

	// 阶段2: 在选中的聚类中搜索
	for j := 0; j < nprobe; j++ {
		clusterID := clusterDists[j].clusterID

		// 应用过滤器
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

			code, exists := i.codes[rec.ID]
			if !exists {
				continue
			}

			dist := i.computeApproxDistance(query, code)
			candidates = append(candidates, candidateNode{
				id:   rec.ID,
				dist: dist,
			})
		}
	}

	// 排序候选
	sort.Slice(candidates, func(a, b int) bool {
		return candidates[a].dist < candidates[b].dist
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
		result.Distances[j] = c.dist
	}

	return result, nil
}

// Insert 插入向量
func (i *IVFPQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != i.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", i.config.Dimension, len(vector))
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	vec := make([]float32, len(vector))
	copy(vec, vector)
	i.vectors[id] = vec

	// 编码向量
	code := i.encode(vec)
	i.codes[id] = code

	// 找到最近的聚类
	bestCluster := 0
	minDist := float32(math.MaxFloat32)
	for j, center := range i.ivfCentroids {
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
func (i *IVFPQIndex) Delete(id int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	cluster, exists := i.assignments[id]
	if !exists {
		return nil
	}

	// 从聚类中移除
	vecs := i.vectorsByCluster[cluster]
	for j, rec := range vecs {
		if rec.ID == id {
			i.vectorsByCluster[cluster] = append(vecs[:j], vecs[j+1:]...)
			break
		}
	}

	delete(i.vectors, id)
	delete(i.codes, id)
	delete(i.assignments, id)
	i.clusterCounts[cluster]--

	return nil
}

// GetConfig 获取索引配置
func (i *IVFPQIndex) GetConfig() *VectorIndexConfig {
	return i.config
}

// Stats 返回索引统计信息
func (i *IVFPQIndex) Stats() VectorIndexStats {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var memorySize int64

	// PQ 编码
	for _, code := range i.codes {
		memorySize += int64(len(code)) * 1
	}

	// 码本
	for subq := 0; subq < i.m; subq++ {
		for c := 0; c < i.ksubq; c++ {
			memorySize += int64(len(i.codebooks[subq][c])) * 4
		}
	}

	// IVF 聚类中心
	for _, centroid := range i.ivfCentroids {
		memorySize += int64(len(centroid)) * 4
	}

	// 聚类分配
	memorySize += int64(len(i.assignments) * 8)
	for _, recs := range i.vectorsByCluster {
		memorySize += int64(len(recs) * (8 + 4*i.config.Dimension))
	}

	return VectorIndexStats{
		Type:       IndexTypeVectorIVFPQ,
		Metric:     i.config.MetricType,
		Dimension:  i.config.Dimension,
		Count:      int64(len(i.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (i *IVFPQIndex) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.vectors = make(map[int64][]float32)
	i.codes = make(map[int64][]int8)
	i.ivfCentroids = make([][]float32, 0)
	i.vectorsByCluster = make(map[int][]VectorRecord, 0)
	i.assignments = make(map[int64]int)
	i.clusterCounts = make([]int, 0)

	return nil
}
