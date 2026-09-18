package memory

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

// DiskBBQIndex is a disk-oriented IVF alternative to HNSW.
// Hierarchical K-means (two centroid layers) + Better Binary Quantization (BBQ)
// stores one compressed block per leaf cluster so search only scores probed blocks.
type DiskBBQIndex struct {
	columnName string
	config     *VectorIndexConfig
	distFunc   DistanceFunc

	coarseCentroids [][]float32
	fineCentroids   [][]float32
	fineParent      []int
	clusterCounts   []int

	blocks      map[int][]diskBBQEntry
	assignments map[int64]int

	rotSigns []float32
	rotPerm  []int
	invSqrtD float32

	ncoarse      int
	nleaf        int
	nprobe       int
	nprobeCoarse int

	mu  sync.RWMutex
	rng *rand.Rand
}

type diskBBQEntry struct {
	ID      int64
	Bits    []uint64
	ResNorm float32
	VecNorm float32
}

type diskBBQCand struct {
	id   int64
	dist float32
}

// DiskBBQParams holds DiskBBQ build/search knobs.
type DiskBBQParams struct {
	Ncoarse      int
	Nlist        int
	Nprobe       int
	NprobeCoarse int
}

// DefaultDiskBBQParams are the default DiskBBQ parameters.
var DefaultDiskBBQParams = DiskBBQParams{
	Ncoarse:      16,
	Nlist:        64,
	Nprobe:       8,
	NprobeCoarse: 4,
}

// NewDiskBBQIndex creates a DiskBBQ index.
func NewDiskBBQIndex(columnName string, config *VectorIndexConfig) (*DiskBBQIndex, error) {
	if config == nil {
		return nil, fmt.Errorf("vector index config is required")
	}
	distFunc, err := GetDistance(string(config.MetricType))
	if err != nil {
		return nil, err
	}

	nlist := vectorParamInt(config.Params, "nlist", DefaultDiskBBQParams.Nlist)
	if nlist < 1 {
		nlist = 1
	}
	ncoarse := vectorParamInt(config.Params, "ncoarse", 0)
	if ncoarse < 1 {
		ncoarse = int(math.Sqrt(float64(nlist)))
		if ncoarse < 2 && nlist >= 2 {
			ncoarse = 2
		}
		if ncoarse < 1 {
			ncoarse = 1
		}
	}
	if ncoarse > nlist {
		ncoarse = nlist
	}
	nprobe := vectorParamInt(config.Params, "nprobe", DefaultDiskBBQParams.Nprobe)
	if nprobe < 1 {
		nprobe = 1
	}
	nprobeCoarse := vectorParamInt(config.Params, "nprobe_coarse", 0)
	if nprobeCoarse < 1 {
		nprobeCoarse = nprobe / 2
		if nprobeCoarse < 2 && ncoarse >= 2 {
			nprobeCoarse = 2
		}
		if nprobeCoarse < 1 {
			nprobeCoarse = 1
		}
	}

	invSqrtD := float32(1)
	if config.Dimension > 0 {
		invSqrtD = 1 / float32(math.Sqrt(float64(config.Dimension)))
	}

	return &DiskBBQIndex{
		columnName:   columnName,
		config:       config,
		distFunc:     distFunc,
		blocks:       make(map[int][]diskBBQEntry),
		assignments:  make(map[int64]int),
		invSqrtD:     invSqrtD,
		ncoarse:      ncoarse,
		nleaf:        nlist,
		nprobe:       nprobe,
		nprobeCoarse: nprobeCoarse,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func vectorParamInt(params map[string]interface{}, key string, def int) int {
	if params == nil {
		return def
	}
	val, ok := params[key]
	if !ok {
		return def
	}
	switch n := val.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float32:
		return int(n)
	case float64:
		return int(n)
	default:
		return def
	}
}

// Build trains hierarchical K-means, a BBQ rotation, and writes leaf blocks.
func (d *DiskBBQIndex) Build(ctx context.Context, loader VectorDataLoader) error {
	records, err := loader.Load(ctx)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.initRotation()
	leafOf := d.buildHierarchy(records)

	d.blocks = make(map[int][]diskBBQEntry, d.nleaf)
	d.assignments = make(map[int64]int, len(records))
	d.clusterCounts = make([]int, d.nleaf)

	dim := d.config.Dimension
	residual := make([]float32, dim)
	projected := make([]float32, dim)
	for i, rec := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		leaf := 0
		if i < len(leafOf) {
			leaf = leafOf[i]
		}
		if leaf < 0 {
			leaf = 0
		}
		entry := d.encodeBBQReuse(rec.ID, rec.Vector, d.centroidForLeaf(leaf, rec.Vector), residual, projected)
		d.blocks[leaf] = append(d.blocks[leaf], entry)
		d.assignments[rec.ID] = leaf
		if leaf >= len(d.clusterCounts) {
			next := make([]int, leaf+1)
			copy(next, d.clusterCounts)
			d.clusterCounts = next
		}
		d.clusterCounts[leaf]++
	}
	return nil
}

func (d *DiskBBQIndex) initRotation() {
	dim := d.config.Dimension
	d.rotPerm = d.rng.Perm(dim)
	d.rotSigns = make([]float32, dim)
	for i := range d.rotSigns {
		if d.rng.Intn(2) == 0 {
			d.rotSigns[i] = 1
		} else {
			d.rotSigns[i] = -1
		}
	}
}

func (d *DiskBBQIndex) buildHierarchy(records []VectorRecord) []int {
	nleaf := d.nleaf
	if nleaf > len(records) {
		nleaf = len(records)
	}
	if nleaf < 1 {
		nleaf = 1
	}
	ncoarse := d.ncoarse
	if ncoarse > nleaf {
		ncoarse = nleaf
	}
	if ncoarse < 1 {
		ncoarse = 1
	}
	d.nleaf = nleaf
	d.ncoarse = ncoarse

	leafOf := make([]int, len(records))
	coarse, coarseAssign := d.kmeans(records, ncoarse)
	d.coarseCentroids = coarse
	groups := make([][]int, ncoarse)
	for i, c := range coarseAssign {
		if c < 0 || c >= ncoarse {
			c = 0
		}
		groups[c] = append(groups[c], i)
	}

	d.fineCentroids = make([][]float32, nleaf)
	d.fineParent = make([]int, nleaf)
	per := (nleaf + ncoarse - 1) / ncoarse
	fineID := 0
	for c := 0; c < ncoarse && fineID < nleaf; c++ {
		take := per
		if fineID+take > nleaf {
			take = nleaf - fineID
		}
		idxs := groups[c]
		if len(idxs) == 0 {
			for t := 0; t < take; t++ {
				d.fineParent[fineID] = c
				fineID++
			}
			continue
		}
		k := take
		if k > len(idxs) {
			k = len(idxs)
		}
		groupRecs := make([]VectorRecord, len(idxs))
		for i, ri := range idxs {
			groupRecs[i] = records[ri]
		}
		fineBase := fineID
		cents, assigns := d.kmeans(groupRecs, k)
		for t := 0; t < take; t++ {
			d.fineParent[fineID] = c
			if t < len(cents) {
				d.fineCentroids[fineID] = cents[t]
			}
			fineID++
		}
		for i, ri := range idxs {
			fa := 0
			if i < len(assigns) {
				fa = assigns[i]
			}
			if fa < 0 || fa >= take {
				fa = 0
			}
			leafOf[ri] = fineBase + fa
		}
	}
	return leafOf
}

func (d *DiskBBQIndex) kmeans(records []VectorRecord, k int) ([][]float32, []int) {
	if len(records) == 0 || k < 1 {
		return nil, nil
	}
	if k > len(records) {
		k = len(records)
	}
	dim := d.config.Dimension
	centroids := make([][]float32, k)
	used := make(map[int]struct{}, k)
	for j := 0; j < k; j++ {
		idx := d.rng.Intn(len(records))
		for {
			if _, ok := used[idx]; !ok || len(used) >= len(records) {
				break
			}
			idx = d.rng.Intn(len(records))
		}
		used[idx] = struct{}{}
		center := make([]float32, dim)
		copy(center, records[idx].Vector)
		centroids[j] = center
	}

	assignments := make([]int, len(records))
	sums := make([][]float32, k)
	for j := 0; j < k; j++ {
		sums[j] = make([]float32, dim)
	}
	counts := make([]int, k)
	useL2 := d.config.MetricType == VectorMetricL2
	maxIterations := 8
	if len(records) < 500 {
		maxIterations = 20
	} else if len(records) < 2000 {
		maxIterations = 12
	}
	tolerance := float32(1e-4)

	for iter := 0; iter < maxIterations; iter++ {
		changed := d.assignClusters(records, centroids, assignments, useL2)
		for j := 0; j < k; j++ {
			counts[j] = 0
			clear(sums[j])
		}
		for i, rec := range records {
			c := assignments[i]
			if c < 0 || c >= k {
				continue
			}
			counts[c]++
			vec := rec.Vector
			dst := sums[c]
			n := dim
			if len(vec) < n {
				n = len(vec)
			}
			for dlt := 0; dlt < n; dlt++ {
				dst[dlt] += vec[dlt]
			}
		}

		shift := float32(0)
		for j := 0; j < k; j++ {
			if counts[j] == 0 {
				continue
			}
			inv := 1 / float32(counts[j])
			center := centroids[j]
			for dlt := 0; dlt < dim; dlt++ {
				nv := sums[j][dlt] * inv
				diff := nv - center[dlt]
				shift += diff * diff
				center[dlt] = nv
			}
		}
		if !changed && shift <= tolerance {
			break
		}
	}
	return centroids, assignments
}

func (d *DiskBBQIndex) assignClusters(records []VectorRecord, centroids [][]float32, assignments []int, useL2 bool) bool {
	n := len(records)
	if n < 1500 {
		changed := false
		for i, rec := range records {
			best := d.nearestCenter(centroids, rec.Vector, useL2)
			if assignments[i] != best {
				assignments[i] = best
				changed = true
			}
		}
		return changed
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > 8 {
		workers = 8
	}
	if workers < 2 {
		workers = 2
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		changed bool
	)
	chunk := (n + workers - 1) / workers
	for w := 0; w < workers; w++ {
		lo := w * chunk
		hi := lo + chunk
		if hi > n {
			hi = n
		}
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			localChanged := false
			for i := lo; i < hi; i++ {
				best := d.nearestCenter(centroids, records[i].Vector, useL2)
				if assignments[i] != best {
					assignments[i] = best
					localChanged = true
				}
			}
			if localChanged {
				mu.Lock()
				changed = true
				mu.Unlock()
			}
		}(lo, hi)
	}
	wg.Wait()
	return changed
}

func (d *DiskBBQIndex) nearestCenter(centroids [][]float32, vec []float32, useL2 bool) int {
	if useL2 {
		return nearestCentroidL2sq(centroids, vec)
	}
	return nearestCentroid(centroids, vec, d.distFunc)
}

func nearestCentroidL2sq(centroids [][]float32, vec []float32) int {
	best := 0
	minD := float32(math.MaxFloat32)
	found := false
	for j, center := range centroids {
		if len(center) != len(vec) {
			continue
		}
		dist := l2Squared(vec, center)
		if !found || dist < minD {
			minD = dist
			best = j
			found = true
		}
	}
	return best
}

func l2Squared(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var sum float32
	i := 0
	for ; i+3 < n; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

func (d *DiskBBQIndex) rotateInto(dst, vec []float32) {
	n := len(vec)
	if len(dst) < n {
		return
	}
	if len(d.rotPerm) != n || len(d.rotSigns) != n {
		copy(dst, vec)
		return
	}
	for i, p := range d.rotPerm {
		if p >= 0 && p < n {
			dst[i] = vec[p] * d.rotSigns[i]
		}
	}
}

func packBBQBits(projected []float32) []uint64 {
	words := make([]uint64, (len(projected)+63)/64)
	for i, v := range projected {
		if v >= 0 {
			words[i/64] |= 1 << uint(i%64)
		}
	}
	return words
}

func signedDot(projected []float32, bits []uint64) float32 {
	var ip float32
	n := len(projected)
	i := 0
	for ; i+3 < n; i += 4 {
		ip += signedTerm(projected[i], bits, i)
		ip += signedTerm(projected[i+1], bits, i+1)
		ip += signedTerm(projected[i+2], bits, i+2)
		ip += signedTerm(projected[i+3], bits, i+3)
	}
	for ; i < n; i++ {
		ip += signedTerm(projected[i], bits, i)
	}
	return ip
}

func signedTerm(v float32, bits []uint64, i int) float32 {
	word := i >> 6
	if word >= len(bits) || bits[word]&(1<<uint(i&63)) == 0 {
		return -v
	}
	return v
}

func (d *DiskBBQIndex) encodeBBQ(id int64, vec, centroid []float32) diskBBQEntry {
	dim := d.config.Dimension
	return d.encodeBBQReuse(id, vec, centroid, make([]float32, dim), make([]float32, dim))
}

func (d *DiskBBQIndex) encodeBBQReuse(id int64, vec, centroid, residual, projected []float32) diskBBQEntry {
	dim := d.config.Dimension
	var resNorm, vecNorm float32
	for i := 0; i < dim; i++ {
		qv := float32(0)
		if i < len(vec) {
			qv = vec[i]
			vecNorm += qv * qv
		}
		cv := float32(0)
		if i < len(centroid) {
			cv = centroid[i]
		}
		r := qv - cv
		residual[i] = r
		resNorm += r * r
	}
	d.rotateInto(projected, residual)
	return diskBBQEntry{
		ID:      id,
		Bits:    packBBQBits(projected),
		ResNorm: float32(math.Sqrt(float64(resNorm))),
		VecNorm: float32(math.Sqrt(float64(vecNorm))),
	}
}

func bbqDistPrecomputed(metric VectorMetricType, e diskBBQEntry, qresNorm, qNorm, qc, invSqrtD float32, qproj []float32) float32 {
	approxResIP := e.ResNorm * invSqrtD * signedDot(qproj, e.Bits)
	switch metric {
	case VectorMetricL2:
		l2sq := qresNorm + e.ResNorm*e.ResNorm - 2*approxResIP
		if l2sq < 0 {
			l2sq = 0
		}
		return float32(math.Sqrt(float64(l2sq)))
	case VectorMetricIP:
		return -(qc + approxResIP)
	default:
		if qNorm == 0 || e.VecNorm == 0 {
			return 1
		}
		return 1 - (qc+approxResIP)/(float32(math.Sqrt(float64(qNorm)))*e.VecNorm)
	}
}

func (d *DiskBBQIndex) nearestLeaf(vec []float32) int {
	if len(d.fineCentroids) == 0 {
		return 0
	}
	useL2 := d.config.MetricType == VectorMetricL2
	coarse := d.nearestCenter(d.coarseCentroids, vec, useL2)
	best := -1
	bestDist := float32(math.MaxFloat32)
	for f, parent := range d.fineParent {
		if parent != coarse || f >= len(d.fineCentroids) || len(d.fineCentroids[f]) != len(vec) {
			continue
		}
		var dist float32
		if useL2 {
			dist = l2Squared(vec, d.fineCentroids[f])
		} else {
			dist = d.distFunc.Compute(vec, d.fineCentroids[f])
		}
		if best < 0 || dist < bestDist {
			best = f
			bestDist = dist
		}
	}
	if best >= 0 {
		return best
	}
	return d.nearestCenter(d.fineCentroids, vec, useL2)
}

func (d *DiskBBQIndex) centroidForLeaf(leaf int, vec []float32) []float32 {
	if leaf >= 0 && leaf < len(d.fineCentroids) && len(d.fineCentroids[leaf]) == len(vec) {
		return d.fineCentroids[leaf]
	}
	if leaf >= 0 && leaf < len(d.fineParent) {
		c := d.fineParent[leaf]
		if c >= 0 && c < len(d.coarseCentroids) && len(d.coarseCentroids[c]) == len(vec) {
			return d.coarseCentroids[c]
		}
	}
	if len(d.coarseCentroids) > 0 && len(d.coarseCentroids[0]) == len(vec) {
		return d.coarseCentroids[0]
	}
	return vec
}

func (d *DiskBBQIndex) seedFromVector(vec []float32) {
	center := make([]float32, len(vec))
	copy(center, vec)
	d.ncoarse = 1
	d.nleaf = 1
	d.coarseCentroids = [][]float32{center}
	d.fineCentroids = [][]float32{append([]float32(nil), center...)}
	d.fineParent = []int{0}
	d.clusterCounts = []int{0}
	d.blocks = make(map[int][]diskBBQEntry)
	d.assignments = make(map[int64]int)
	if len(d.rotPerm) != d.config.Dimension {
		d.initRotation()
	}
}

func (d *DiskBBQIndex) upsertLocked(id int64, vector []float32) {
	if d.assignments != nil {
		if old, ok := d.assignments[id]; ok {
			d.removeFromLeaf(old, id)
		}
	}
	if len(d.fineCentroids) == 0 {
		d.seedFromVector(vector)
	}
	leaf := d.nearestLeaf(vector)
	if leaf < 0 {
		leaf = 0
	}
	entry := d.encodeBBQ(id, vector, d.centroidForLeaf(leaf, vector))
	d.blocks[leaf] = append(d.blocks[leaf], entry)
	if d.assignments == nil {
		d.assignments = make(map[int64]int)
	}
	d.assignments[id] = leaf
	if leaf >= len(d.clusterCounts) {
		next := make([]int, leaf+1)
		copy(next, d.clusterCounts)
		d.clusterCounts = next
	}
	d.clusterCounts[leaf]++
}

func (d *DiskBBQIndex) removeFromLeaf(leaf int, id int64) {
	entries := d.blocks[leaf]
	for i, e := range entries {
		if e.ID == id {
			d.blocks[leaf] = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	if leaf >= 0 && leaf < len(d.clusterCounts) && d.clusterCounts[leaf] > 0 {
		d.clusterCounts[leaf]--
	}
	delete(d.assignments, id)
}

// Search scores BBQ codes in the nearest hierarchical clusters.
func (d *DiskBBQIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
	if len(query) != d.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", d.config.Dimension, len(query))
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.assignments) == 0 {
		return &VectorSearchResult{IDs: []int64{}, Distances: []float32{}}, nil
	}

	probed := d.probeLeaves(query)
	dim := d.config.Dimension
	qres := make([]float32, dim)
	qproj := make([]float32, dim)
	var qNorm float32
	for _, v := range query {
		qNorm += v * v
	}
	capHint := 0
	for _, leaf := range probed {
		capHint += len(d.blocks[leaf])
	}
	candidates := make([]diskBBQCand, 0, capHint)
	metric := d.config.MetricType
	invSqrtD := d.invSqrtD
	if invSqrtD == 0 && dim > 0 {
		invSqrtD = 1 / float32(math.Sqrt(float64(dim)))
	}

	for _, leaf := range probed {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		centroid := d.centroidForLeaf(leaf, query)
		var qresNorm, qc float32
		for i := 0; i < dim; i++ {
			cv := float32(0)
			if i < len(centroid) {
				cv = centroid[i]
			}
			r := query[i] - cv
			qres[i] = r
			qresNorm += r * r
			qc += query[i] * cv
		}
		d.rotateInto(qproj, qres)
		for _, e := range d.blocks[leaf] {
			if !vectorIDAllowed(filter, e.ID) {
				continue
			}
			candidates = append(candidates, diskBBQCand{
				id:   e.ID,
				dist: bbqDistPrecomputed(metric, e, qresNorm, qNorm, qc, invSqrtD, qproj),
			})
		}
	}

	if k < 0 {
		k = 0
	}
	if k > 0 && len(candidates) > k {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].dist < candidates[j].dist
		})
		candidates = candidates[:k]
	} else {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].dist < candidates[j].dist
		})
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

func (d *DiskBBQIndex) probeLeaves(query []float32) []int {
	if len(d.fineCentroids) == 0 {
		return nil
	}
	ncoarse := len(d.coarseCentroids)
	coarseCounts := make([]int, ncoarse)
	for f, parent := range d.fineParent {
		if parent >= 0 && parent < ncoarse && f < len(d.clusterCounts) {
			coarseCounts[parent] += d.clusterCounts[f]
		}
	}
	useL2 := d.config.MetricType == VectorMetricL2
	coarseIDs := selectProbedClusters(ncoarse, d.nprobeCoarse, coarseCounts, func(j int) float32 {
		if j >= len(d.coarseCentroids) || len(d.coarseCentroids[j]) != len(query) {
			return float32(math.MaxFloat32)
		}
		if useL2 {
			return l2Squared(query, d.coarseCentroids[j])
		}
		return d.distFunc.Compute(query, d.coarseCentroids[j])
	})
	allowed := make([]bool, ncoarse)
	for _, c := range coarseIDs {
		if c >= 0 && c < ncoarse {
			allowed[c] = true
		}
	}

	nleaf := len(d.fineCentroids)
	childCounts := make([]int, nleaf)
	for f := 0; f < nleaf; f++ {
		if f < len(d.fineParent) {
			p := d.fineParent[f]
			if p >= 0 && p < ncoarse && allowed[p] && f < len(d.clusterCounts) {
				childCounts[f] = d.clusterCounts[f]
			}
		}
	}
	nprobe := d.nprobe
	if val, ok := d.config.Params["nprobe"].(int); ok && val > 0 {
		nprobe = val
	}
	return selectProbedClusters(nleaf, nprobe, childCounts, func(j int) float32 {
		if j >= len(d.fineCentroids) || len(d.fineCentroids[j]) != len(query) {
			return float32(math.MaxFloat32)
		}
		if useL2 {
			return l2Squared(query, d.fineCentroids[j])
		}
		return d.distFunc.Compute(query, d.fineCentroids[j])
	})
}

func vectorIDAllowed(filter *VectorFilter, id int64) bool {
	if filter == nil || len(filter.IDs) == 0 {
		return true
	}
	for _, fid := range filter.IDs {
		if fid == id {
			return true
		}
	}
	return false
}

// Insert adds or replaces a vector in the nearest leaf block.
func (d *DiskBBQIndex) Insert(id int64, vector []float32) error {
	if len(vector) != d.config.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", d.config.Dimension, len(vector))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.upsertLocked(id, vector)
	return nil
}

// Delete removes a vector from its leaf block.
func (d *DiskBBQIndex) Delete(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	leaf, ok := d.assignments[id]
	if !ok {
		return nil
	}
	d.removeFromLeaf(leaf, id)
	return nil
}

// GetConfig returns the index config.
func (d *DiskBBQIndex) GetConfig() *VectorIndexConfig {
	return d.config
}

// Stats reports DiskBBQ statistics. MemorySize counts centroids and BBQ codes, not raw float32 vectors.
func (d *DiskBBQIndex) Stats() VectorIndexStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var memorySize int64
	for _, block := range d.blocks {
		for _, e := range block {
			memorySize += int64(len(e.Bits))*8 + 16
		}
	}
	for _, c := range d.coarseCentroids {
		memorySize += int64(len(c)) * 4
	}
	for _, c := range d.fineCentroids {
		memorySize += int64(len(c)) * 4
	}
	memorySize += int64(len(d.rotSigns))*4 + int64(len(d.rotPerm))*4
	memorySize += int64(len(d.assignments) * 12)

	return VectorIndexStats{
		Type:       IndexTypeVectorDiskBBQ,
		Metric:     d.config.MetricType,
		Dimension:  d.config.Dimension,
		Count:      int64(len(d.assignments)),
		MemorySize: memorySize,
	}
}

// Close releases in-memory blocks.
func (d *DiskBBQIndex) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.coarseCentroids = nil
	d.fineCentroids = nil
	d.fineParent = nil
	d.clusterCounts = nil
	d.blocks = make(map[int][]diskBBQEntry)
	d.assignments = make(map[int64]int)
	d.rotSigns = nil
	d.rotPerm = nil
	return nil
}
