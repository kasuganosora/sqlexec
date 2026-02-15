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

// HNSWIndex HNSW近似最近邻索引
// Proper implementation following the original HNSW paper (Malkov & Yashunin, 2016)
type HNSWIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc
	vectors    map[int64][]float32

	// HNSW graph: per-layer adjacency lists
	// layers[level][nodeID] = list of neighbor IDs at that level
	layers    []map[int64][]int64
	nodeLevel map[int64]int // nodeID -> maximum level assigned

	// Global entry point: the node with the highest level
	entryPoint int64
	entryLevel int
	hasEntry   bool

	mu  sync.RWMutex
	rng *rand.Rand
}

// HNSWParams HNSW参数
type HNSWParams struct {
	M              int     // max neighbors per node per layer
	EFConstruction int     // beam width during construction
	EFSearch       int     // beam width during search
	ML             float64 // level generation factor: 1/ln(M)
}

// DefaultHNSWParams 默认HNSW参数
var DefaultHNSWParams = HNSWParams{
	M:              16,
	EFConstruction: 200,
	EFSearch:       256,
	ML:             0.25, // ~1/ln(16) ≈ 0.36, using 0.25 for fewer layers
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
		h.insertInternal(rec.ID, vec)
	}

	return nil
}

// randomLevel 随机生成层数 (geometric distribution)
func (h *HNSWIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < DefaultHNSWParams.ML && level < 16 {
		level++
	}
	return level
}

// ensureLayers ensures layers slice is large enough
func (h *HNSWIndex) ensureLayers(level int) {
	for len(h.layers) <= level {
		h.layers = append(h.layers, make(map[int64][]int64))
	}
}

// insertInternal inserts a node into the HNSW graph (must hold write lock)
func (h *HNSWIndex) insertInternal(id int64, vector []float32) {
	level := h.randomLevel()
	h.nodeLevel[id] = level
	h.ensureLayers(level)

	// Add node to all layers 0..level with empty neighbor lists
	for l := 0; l <= level; l++ {
		cap := DefaultHNSWParams.M * 2 // Mmax0 for level 0
		if l > 0 {
			cap = DefaultHNSWParams.M
		}
		h.layers[l][id] = make([]int64, 0, cap)
	}

	if !h.hasEntry {
		// First node: set as entry point, no connections needed
		h.entryPoint = id
		h.entryLevel = level
		h.hasEntry = true
		return
	}

	ep := h.entryPoint
	epLevel := h.entryLevel

	// Phase 1: Greedy descent from top level to level+1
	// Navigate to the closest node at each level above the new node's level
	for l := epLevel; l > level; l-- {
		if l >= len(h.layers) {
			continue
		}
		ep = h.greedyClosest(vector, ep, l)
	}

	// Phase 2: Insert at each level from min(level, epLevel) down to 0
	// At each level, find efConstruction nearest neighbors, then connect
	topInsertLevel := level
	if epLevel < topInsertLevel {
		topInsertLevel = epLevel
	}

	for l := topInsertLevel; l >= 0; l-- {
		// Search for nearest neighbors at this level using beam search
		candidates := h.searchLevel(vector, ep, DefaultHNSWParams.EFConstruction, l)

		// Select neighbors: level 0 uses 2*M (Mmax0), upper levels use M
		maxConn := DefaultHNSWParams.M * 2 // Mmax0 = 2*M for level 0
		if l > 0 {
			maxConn = DefaultHNSWParams.M // M for upper layers
		}

		neighbors := h.selectNeighbors(vector, candidates, maxConn)

		// Connect: add bidirectional edges
		h.layers[l][id] = neighbors
		for _, neighborID := range neighbors {
			nNeighbors := h.layers[l][neighborID]
			nNeighbors = append(nNeighbors, id)

			// Prune neighbor's connections if exceeding max
			if len(nNeighbors) > maxConn {
				nNeighbors = h.pruneNeighbors(neighborID, nNeighbors, maxConn)
			}
			h.layers[l][neighborID] = nNeighbors
		}

		// Use the closest found node as entry point for the next level down
		if len(candidates) > 0 {
			ep = candidates[0].id
		}
	}

	// Update global entry point if new node has higher level
	if level > h.entryLevel {
		h.entryPoint = id
		h.entryLevel = level
	}
}

// greedyClosest finds the closest node to query starting from ep at the given level
// Simple greedy walk: follow the neighbor with smallest distance until no improvement
func (h *HNSWIndex) greedyClosest(query []float32, ep int64, level int) int64 {
	if level >= len(h.layers) {
		return ep
	}

	current := ep
	currentDist := h.dist(query, current)

	for {
		improved := false
		neighbors := h.layers[level][current]
		for _, nid := range neighbors {
			d := h.dist(query, nid)
			if d < currentDist {
				current = nid
				currentDist = d
				improved = true
			}
		}
		if !improved {
			break
		}
	}
	return current
}

type hnswCandidate struct {
	id   int64
	dist float32
}

// searchLevel performs beam search at a specific level, returning up to ef nearest candidates
func (h *HNSWIndex) searchLevel(query []float32, ep int64, ef int, level int) []hnswCandidate {
	if level >= len(h.layers) {
		return nil
	}

	visited := make(map[int64]bool)
	visited[ep] = true

	epDist := h.dist(query, ep)

	// candidates: min-heap by distance (closest first for expansion)
	candidates := []hnswCandidate{{id: ep, dist: epDist}}
	// results: all explored nodes sorted by distance
	results := []hnswCandidate{{id: ep, dist: epDist}}

	for len(candidates) > 0 {
		// Pop the closest candidate
		closest := candidates[0]
		candidates = candidates[1:]

		// If the closest candidate is farther than the farthest result, stop
		if len(results) >= ef && closest.dist > results[ef-1].dist {
			break
		}

		// Explore neighbors
		neighbors := h.layers[level][closest.id]
		for _, nid := range neighbors {
			if visited[nid] {
				continue
			}
			visited[nid] = true

			d := h.dist(query, nid)

			// Add to results if better than current worst, or results not full
			if len(results) < ef || d < results[len(results)-1].dist {
				c := hnswCandidate{id: nid, dist: d}

				// Insert into results maintaining sorted order
				results = insertSorted(results, c)
				if len(results) > ef {
					results = results[:ef]
				}

				// Add to candidates for further exploration
				candidates = insertSorted(candidates, c)
			}
		}
	}

	return results
}

// insertSorted inserts a candidate into a sorted slice maintaining sort order
func insertSorted(slice []hnswCandidate, c hnswCandidate) []hnswCandidate {
	i := sort.Search(len(slice), func(i int) bool {
		return slice[i].dist > c.dist
	})
	slice = append(slice, hnswCandidate{})
	copy(slice[i+1:], slice[i:])
	slice[i] = c
	return slice
}

// selectNeighbors uses the HNSW heuristic neighbor selection (Algorithm 4 from the paper).
// Instead of just picking the M closest, it selects neighbors that provide diverse graph paths.
func (h *HNSWIndex) selectNeighbors(query []float32, candidates []hnswCandidate, m int) []int64 {
	if len(candidates) <= m {
		result := make([]int64, len(candidates))
		for i, c := range candidates {
			result[i] = c.id
		}
		return result
	}

	// Heuristic selection: prefer neighbors that are closer to query than to already-selected neighbors
	selected := make([]int64, 0, m)
	selectedVecs := make([][]float32, 0, m)

	for _, c := range candidates {
		if len(selected) >= m {
			break
		}

		cVec := h.vectors[c.id]
		if cVec == nil {
			continue
		}

		// Check if this candidate is closer to query than to any already-selected neighbor
		good := true
		for _, sVec := range selectedVecs {
			if h.distFunc.Compute(cVec, sVec) < c.dist {
				good = false
				break
			}
		}

		if good {
			selected = append(selected, c.id)
			selectedVecs = append(selectedVecs, cVec)
		}
	}

	// Fill remaining slots with closest unselected candidates if heuristic didn't fill
	if len(selected) < m {
		selectedSet := make(map[int64]bool, len(selected))
		for _, id := range selected {
			selectedSet[id] = true
		}
		for _, c := range candidates {
			if len(selected) >= m {
				break
			}
			if !selectedSet[c.id] {
				selected = append(selected, c.id)
				selectedSet[c.id] = true
			}
		}
	}

	return selected
}

// pruneNeighbors prunes a node's neighbor list to maxConn, keeping closest
func (h *HNSWIndex) pruneNeighbors(nodeID int64, neighbors []int64, maxConn int) []int64 {
	nodeVec := h.vectors[nodeID]
	if nodeVec == nil {
		if len(neighbors) > maxConn {
			return neighbors[:maxConn]
		}
		return neighbors
	}

	type nd struct {
		id   int64
		dist float32
	}
	scored := make([]nd, len(neighbors))
	for i, nid := range neighbors {
		scored[i] = nd{id: nid, dist: h.distFunc.Compute(nodeVec, h.vectors[nid])}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].dist < scored[j].dist
	})
	if len(scored) > maxConn {
		scored = scored[:maxConn]
	}
	result := make([]int64, len(scored))
	for i, s := range scored {
		result[i] = s.id
	}
	return result
}

// dist computes distance between query and a stored vector by ID
func (h *HNSWIndex) dist(query []float32, id int64) float32 {
	vec := h.vectors[id]
	if vec == nil {
		return float32(math.MaxFloat32)
	}
	return h.distFunc.Compute(query, vec)
}

// Search 近似最近邻搜索
func (h *HNSWIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != h.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", h.config.Dimension, len(query))
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.hasEntry || len(h.vectors) == 0 {
		return &VectorSearchResult{
			IDs:       make([]int64, 0),
			Distances: make([]float32, 0),
		}, nil
	}

	// Build filter set for O(1) lookup
	var filterSet map[int64]bool
	if filter != nil && len(filter.IDs) > 0 {
		filterSet = make(map[int64]bool, len(filter.IDs))
		for _, fid := range filter.IDs {
			filterSet[fid] = true
		}
	}

	// ef must be at least k
	ef := DefaultHNSWParams.EFSearch
	if k > ef {
		ef = k
	}
	// When filtering, increase ef to compensate for filtered-out candidates
	if filterSet != nil {
		ratio := float64(len(h.vectors)) / float64(len(filterSet))
		adjustedEf := int(float64(ef) * ratio)
		if adjustedEf > len(h.vectors) {
			adjustedEf = len(h.vectors)
		}
		if adjustedEf > ef {
			ef = adjustedEf
		}
	}

	ep := h.entryPoint

	// Phase 1: Greedy descent from top level to level 1
	for l := h.entryLevel; l >= 1; l-- {
		ep = h.greedyClosest(query, ep, l)
	}

	// Phase 2: Beam search at level 0 with ef candidates
	candidates := h.searchLevel(query, ep, ef, 0)

	// Apply filter and collect top-k results
	type result struct {
		id   int64
		dist float32
	}
	filtered := make([]result, 0, k)
	for _, c := range candidates {
		if filterSet != nil && !filterSet[c.id] {
			continue
		}
		filtered = append(filtered, result{id: c.id, dist: c.dist})
		if len(filtered) >= k && filterSet == nil {
			break
		}
	}

	// If we don't have enough results (due to filtering), do a broader search
	if len(filtered) < k && filterSet != nil {
		// Fallback: scan all valid IDs
		for id := range h.vectors {
			if !filterSet[id] {
				continue
			}
			// Check if already in results
			found := false
			for _, r := range filtered {
				if r.id == id {
					found = true
					break
				}
			}
			if found {
				continue
			}
			d := h.distFunc.Compute(query, h.vectors[id])
			filtered = append(filtered, result{id: id, dist: d})
		}
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].dist < filtered[j].dist
		})
	}

	if len(filtered) > k {
		filtered = filtered[:k]
	}

	res := &VectorSearchResult{
		IDs:       make([]int64, len(filtered)),
		Distances: make([]float32, len(filtered)),
	}
	for i, r := range filtered {
		res.IDs[i] = r.id
		res.Distances[i] = r.dist
	}
	return res, nil
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

	h.insertInternal(id, vec)

	return nil
}

// Delete 删除向量
func (h *HNSWIndex) Delete(id int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.vectors, id)

	level, exists := h.nodeLevel[id]
	if !exists {
		return nil
	}
	delete(h.nodeLevel, id)

	// Remove from all layers and from neighbors' adjacency lists
	for l := 0; l <= level && l < len(h.layers); l++ {
		delete(h.layers[l], id)
		for nid, neighbors := range h.layers[l] {
			for i, n := range neighbors {
				if n == id {
					h.layers[l][nid] = append(neighbors[:i], neighbors[i+1:]...)
					break
				}
			}
		}
	}

	// Update entry point if deleted node was the entry point
	if h.hasEntry && h.entryPoint == id {
		h.hasEntry = false
		// Find new entry point: node with highest level
		for nid, nLevel := range h.nodeLevel {
			if !h.hasEntry || nLevel > h.entryLevel {
				h.entryPoint = nid
				h.entryLevel = nLevel
				h.hasEntry = true
			}
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
	for _, vec := range h.vectors {
		memorySize += int64(len(vec) * 4)
	}
	for _, layer := range h.layers {
		memorySize += int64(len(layer) * 8)
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
	h.hasEntry = false
	return nil
}
