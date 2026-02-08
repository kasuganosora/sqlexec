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

// IVFFlatIndex IVF-Flat 索引（Inverted File with Flat）
// 使用 K-Means 聚类 + 每个聚类中的 Flat 搜索
type IVFFlatIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	// 向量存储
	vectors map[int64][]float32

	// IVF 结构
	centroids        [][]float32           // 聚类中心
	vectorsByCluster map[int][]VectorRecord  // 每个聚类的向量
	assignments      map[int64]int          // 向量ID -> 聚类ID
	clusterCounts    []int                  // 每个聚类的向量数量

	// 参数
	nlist int // 聚类数量

	mu   sync.RWMutex
	rng  *rand.Rand
}

// IVFFlatParams IVF-Flat 参数
type IVFFlatParams struct {
	Nlist  int // 聚类数量（默认：sqrt(n)）
	Nprobe int // 搜索时检查的聚类数量（默认：10）
}

// DefaultIVFFlatParams 默认参数（参考 Milvus 最佳实践）
var DefaultIVFFlatParams = IVFFlatParams{
	Nlist:  128,
	Nprobe: 32,
}

// NewIVFFlatIndex 创建 IVF-Flat 索引
func NewIVFFlatIndex(columnName string, config *VectorIndexConfig) (*IVFFlatIndex, error) {
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	nlist := DefaultIVFFlatParams.Nlist
	if val, ok := config.Params["nlist"].(int); ok {
		nlist = val
	}

	return &IVFFlatIndex{
		columnName:        columnName,
		config:            config,
		distFunc:          distFunc,
		vectors:           make(map[int64][]float32),
		centroids:         make([][]float32, nlist),
		vectorsByCluster: make(map[int][]VectorRecord, nlist),
		assignments:       make(map[int64]int),
		clusterCounts:     make([]int, nlist),
		nlist:             nlist,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Build 构建索引（K-Means 聚类）
func (i *IVFFlatIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// 存储所有向量
	for _, rec := range records {
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		i.vectors[rec.ID] = vec
	}

	// 执行 K-Means 聚类
	err = i.kmeans(records)
	if err != nil {
		return err
	}

	return nil
}

// kmeans K-Means 聚类算法
func (i *IVFFlatIndex) kmeans(records []VectorRecord) error {
	dimension := i.config.Dimension
	nlist := i.nlist

	// 初始化聚类中心（随机选择）
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
			if len(i.centroids[j]) > 0 {
				shift := i.distFunc.Compute(i.centroids[j], newCenter)
				if shift > tolerance {
					converged = false
				}
			}
			i.centroids[j] = newCenter
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

// Search 搜索最近邻（IVF + Flat）
func (i *IVFFlatIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
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
	nprobe := DefaultIVFFlatParams.Nprobe
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
	for j, center := range i.centroids {
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

			dist := i.distFunc.Compute(query, rec.Vector)
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
func (i *IVFFlatIndex) Insert(id int64, vector []float32) error {
	if len(vector) != i.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", i.config.Dimension, len(vector))
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	vec := make([]float32, len(vector))
	copy(vec, vector)
	i.vectors[id] = vec

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
func (i *IVFFlatIndex) Delete(id int64) error {
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
	delete(i.assignments, id)
	i.clusterCounts[cluster]--

	return nil
}

// GetConfig 获取索引配置
func (i *IVFFlatIndex) GetConfig() *VectorIndexConfig {
	return i.config
}

// Stats 返回索引统计信息
func (i *IVFFlatIndex) Stats() VectorIndexStats {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var memorySize int64

	// 向量数据
	for _, vec := range i.vectors {
		memorySize += int64(len(vec)) * 4
	}

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
		Type:       IndexTypeVectorIVFFlat,
		Metric:     i.config.MetricType,
		Dimension:  i.config.Dimension,
		Count:      int64(len(i.vectors)),
		MemorySize: memorySize,
	}
}

// Close 关闭索引
func (i *IVFFlatIndex) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.vectors = make(map[int64][]float32)
	i.centroids = make([][]float32, 0)
	i.vectorsByCluster = make(map[int][]VectorRecord, 0)
	i.assignments = make(map[int64]int)
	i.clusterCounts = make([]int, 0)

	return nil
}

