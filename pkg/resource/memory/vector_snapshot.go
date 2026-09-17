package memory

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const vectorSnapshotMagic = "SQLEXECVEC1"

// VectorSnapshotStore persists built vector indexes to durable storage.
type VectorSnapshotStore interface {
	SaveVectorSnapshot(table, column string, data []byte) error
	LoadVectorSnapshot(table, column string) ([]byte, error)
	RemoveVectorSnapshot(table, column string) error
}

// DirVectorSnapshotStore stores one gob file per table.column under Dir.
type DirVectorSnapshotStore struct {
	Dir string
}

func NewDirVectorSnapshotStore(dir string) *DirVectorSnapshotStore {
	return &DirVectorSnapshotStore{Dir: dir}
}

func (s *DirVectorSnapshotStore) fileName(table, column string) string {
	safe := func(v string) string {
		v = strings.ReplaceAll(v, string(os.PathSeparator), "_")
		v = strings.ReplaceAll(v, "/", "_")
		return v
	}
	return filepath.Join(s.Dir, safe(table)+"."+safe(column)+".vecidx")
}

func (s *DirVectorSnapshotStore) SaveVectorSnapshot(table, column string, data []byte) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.fileName(table, column), data, 0o644)
}

func (s *DirVectorSnapshotStore) LoadVectorSnapshot(table, column string) ([]byte, error) {
	return os.ReadFile(s.fileName(table, column))
}

func (s *DirVectorSnapshotStore) RemoveVectorSnapshot(table, column string) error {
	err := os.Remove(s.fileName(table, column))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

type vectorSnapshotHeader struct {
	Magic     string
	Type      IndexType
	Metric    VectorMetricType
	Dimension int
	Count     int64
	Payload   []byte
}

type hnswDump struct {
	Vectors    map[int64][]float32
	Layers     []map[int64][]int64
	NodeLevel  map[int64]int
	EntryPoint int64
	EntryLevel int
	HasEntry   bool
}

type flatDump struct {
	Vectors map[int64][]float32
}

type ivfFlatDump struct {
	Vectors          map[int64][]float32
	Centroids        [][]float32
	VectorsByCluster map[int][]VectorRecord
	Assignments      map[int64]int
	ClusterCounts    []int
	Nlist            int
}

type ivfSQ8Dump struct {
	Vectors          map[int64][]float32
	Centroids        [][]float32
	VectorsByCluster map[int][]VectorRecord
	Assignments      map[int64]int
	ClusterCounts    []int
	Nlist            int
	Quantized        map[int64][]int8
	Scale            []float32
	Shift            []float32
}

type ivfPQDump struct {
	Vectors          map[int64][]float32
	Centroids        [][]float32
	VectorsByCluster map[int][]VectorRecord
	Assignments      map[int64]int
	ClusterCounts    []int
	Nlist            int
	Nsubq            int
	PQSubCentroids   [][][]float32
	Codebooks        [][][]float32
	Codes            map[int64][]int8
	M                int
	Nbits            int
	Ksubq            int
}

type ivfRabitQDump struct {
	Vectors          map[int64][]float32
	Centroids        [][]float32
	VectorsByCluster map[int][]VectorRecord
	Assignments      map[int64]int
	ClusterCounts    []int
	Nlist            int
	Quantized        map[int64][]uint64
	ProjectionMatrix [][]float32
}

type hnswNodeDump struct {
	ID        int64
	Vector    []int8
	Code      []int8
	Coarse    int8
	Residual  []int8
	Neighbors [][]int64
}

type hnswQuantDump struct {
	Vectors        map[int64][]float32
	Quantized      map[int64][]int8
	Codes          map[int64][]int8
	CoarseCodes    map[int64]int8
	ResidualCodes  map[int64][]int8
	Scale          []float32
	Shift          []float32
	Codebooks      [][][]float32
	CoarseCodebook [][]float32
	ResidualBooks  [][][]float32
	Layers         []map[int64]hnswNodeDump
	MaxLevel       int
	ML             float64
	EFConstruction int
	EF             int
	Nsubq          int
	Ksubq          int
	Kcoarse        int
}

type aisaqDump struct {
	Vectors    map[int64][]float32
	Quantized  map[int64][]int8
	Scale      []float32
	Shift      []float32
	Graph      map[int64]hnswNodeDump
	MaxDegree  int
	SearchList int
}

// EncodeVectorSnapshot serializes a built vector index.
func EncodeVectorSnapshot(idx VectorIndex) ([]byte, error) {
	if idx == nil {
		return nil, fmt.Errorf("nil vector index")
	}
	stats := idx.Stats()
	cfg := idx.GetConfig()
	payload, err := encodeVectorPayload(idx)
	if err != nil {
		return nil, err
	}
	hdr := vectorSnapshotHeader{
		Magic:     vectorSnapshotMagic,
		Type:      stats.Type,
		Metric:    cfg.MetricType,
		Dimension: cfg.Dimension,
		Count:     stats.Count,
		Payload:   payload,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(hdr); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeVectorPayload(idx VectorIndex) ([]byte, error) {
	var payload interface{}
	switch v := idx.(type) {
	case *FlatIndex:
		v.mu.RLock()
		payload = flatDump{Vectors: cloneFloat32Map(v.vectors)}
		v.mu.RUnlock()
	case *HNSWIndex:
		v.mu.RLock()
		payload = hnswDump{
			Vectors:    cloneFloat32Map(v.vectors),
			Layers:     cloneHNSWLayers(v.layers),
			NodeLevel:  cloneIntMap(v.nodeLevel),
			EntryPoint: v.entryPoint,
			EntryLevel: v.entryLevel,
			HasEntry:   v.hasEntry,
		}
		v.mu.RUnlock()
	case *IVFFlatIndex:
		v.mu.RLock()
		payload = dumpIVFFlat(v.vectors, v.centroids, v.vectorsByCluster, v.assignments, v.clusterCounts, v.nlist)
		v.mu.RUnlock()
	case *IVFSQ8Index:
		v.mu.RLock()
		base := dumpIVFFlat(v.vectors, v.centroids, v.vectorsByCluster, v.assignments, v.clusterCounts, v.nlist)
		payload = ivfSQ8Dump{
			Vectors:          base.Vectors,
			Centroids:        base.Centroids,
			VectorsByCluster: base.VectorsByCluster,
			Assignments:      base.Assignments,
			ClusterCounts:    base.ClusterCounts,
			Nlist:            base.Nlist,
			Quantized:        cloneInt8Map(v.quantizedVectors),
			Scale:            append([]float32(nil), v.scale...),
			Shift:            append([]float32(nil), v.shift...),
		}
		v.mu.RUnlock()
	case *IVFPQIndex:
		v.mu.RLock()
		base := dumpIVFFlat(v.vectors, v.ivfCentroids, v.vectorsByCluster, v.assignments, v.clusterCounts, v.nlist)
		payload = ivfPQDump{
			Vectors:          base.Vectors,
			Centroids:        base.Centroids,
			VectorsByCluster: base.VectorsByCluster,
			Assignments:      base.Assignments,
			ClusterCounts:    base.ClusterCounts,
			Nlist:            base.Nlist,
			Nsubq:            v.nsubq,
			PQSubCentroids:   v.centroids,
			Codebooks:        v.codebooks,
			Codes:            cloneInt8Map(v.codes),
			M:                v.m,
			Nbits:            v.nbits,
			Ksubq:            v.ksubq,
		}
		v.mu.RUnlock()
	case *IVFRabitQIndex:
		v.mu.RLock()
		base := dumpIVFFlat(v.vectors, v.centroids, v.vectorsByCluster, v.assignments, v.clusterCounts, v.nlist)
		payload = ivfRabitQDump{
			Vectors:          base.Vectors,
			Centroids:        base.Centroids,
			VectorsByCluster: base.VectorsByCluster,
			Assignments:      base.Assignments,
			ClusterCounts:    base.ClusterCounts,
			Nlist:            base.Nlist,
			Quantized:        cloneUint64Map(v.quantizedVectors),
			ProjectionMatrix: v.projectionMatrix,
		}
		v.mu.RUnlock()
	case *HNSWSQIndex:
		v.mu.RLock()
		payload = dumpHNSWSQ(v)
		v.mu.RUnlock()
	case *HNSWPQIndex:
		v.mu.RLock()
		payload = dumpHNSWPQ(v)
		v.mu.RUnlock()
	case *HNSWPRQIndex:
		v.mu.RLock()
		payload = dumpHNSWPRQ(v)
		v.mu.RUnlock()
	case *AISAQIndex:
		v.mu.RLock()
		payload = dumpAISAQ(v)
		v.mu.RUnlock()
	default:
		return nil, fmt.Errorf("unsupported vector index type %T", idx)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func dumpIVFFlat(vectors map[int64][]float32, centroids [][]float32, byCluster map[int][]VectorRecord, assignments map[int64]int, counts []int, nlist int) ivfFlatDump {
	return ivfFlatDump{
		Vectors:          cloneFloat32Map(vectors),
		Centroids:        centroids,
		VectorsByCluster: byCluster,
		Assignments:      assignments,
		ClusterCounts:    append([]int(nil), counts...),
		Nlist:            nlist,
	}
}

func dumpHNSWSQ(h *HNSWSQIndex) hnswQuantDump {
	layers := make([]map[int64]hnswNodeDump, len(h.layers))
	for i, layer := range h.layers {
		m := make(map[int64]hnswNodeDump, len(layer))
		for id, n := range layer {
			if n == nil {
				continue
			}
			m[id] = hnswNodeDump{ID: n.id, Vector: n.vector, Neighbors: n.neighbors}
		}
		layers[i] = m
	}
	return hnswQuantDump{
		Vectors:        cloneFloat32Map(h.vectors),
		Quantized:      cloneInt8Map(h.quantizedVectors),
		Scale:          append([]float32(nil), h.scale...),
		Shift:          append([]float32(nil), h.shift...),
		Layers:         layers,
		MaxLevel:       h.maxLevel,
		ML:             h.ml,
		EFConstruction: h.efConstruction,
		EF:             h.ef,
	}
}

func dumpHNSWPQ(h *HNSWPQIndex) hnswQuantDump {
	layers := make([]map[int64]hnswNodeDump, len(h.layers))
	for i, layer := range h.layers {
		m := make(map[int64]hnswNodeDump, len(layer))
		for id, n := range layer {
			if n == nil {
				continue
			}
			m[id] = hnswNodeDump{ID: n.id, Code: n.code, Neighbors: n.neighbors}
		}
		layers[i] = m
	}
	return hnswQuantDump{
		Vectors:        cloneFloat32Map(h.vectors),
		Codes:          cloneInt8Map(h.codes),
		Codebooks:      h.codebooks,
		Layers:         layers,
		MaxLevel:       h.maxLevel,
		ML:             h.ml,
		EFConstruction: h.efConstruction,
		EF:             h.ef,
		Nsubq:          h.nsubq,
		Ksubq:          h.ksubq,
	}
}

func dumpHNSWPRQ(h *HNSWPRQIndex) hnswQuantDump {
	layers := make([]map[int64]hnswNodeDump, len(h.layers))
	for i, layer := range h.layers {
		m := make(map[int64]hnswNodeDump, len(layer))
		for id, n := range layer {
			if n == nil {
				continue
			}
			m[id] = hnswNodeDump{ID: n.id, Coarse: n.coarseCode, Residual: n.residual, Neighbors: n.neighbors}
		}
		layers[i] = m
	}
	return hnswQuantDump{
		Vectors:        cloneFloat32Map(h.vectors),
		CoarseCodes:    h.coarseCodes,
		ResidualCodes:  cloneInt8Map(h.residualCodes),
		CoarseCodebook: h.coarseCodebook,
		ResidualBooks:  h.residualCodebooks,
		Layers:         layers,
		MaxLevel:       h.maxLevel,
		ML:             h.ml,
		EF:             h.ef,
		Nsubq:          h.nsubq,
		Ksubq:          h.ksubq,
		Kcoarse:        h.kcoarse,
	}
}

func dumpAISAQ(a *AISAQIndex) aisaqDump {
	graph := make(map[int64]hnswNodeDump, len(a.graph))
	for id, n := range a.graph {
		if n == nil {
			continue
		}
		graph[id] = hnswNodeDump{ID: n.id, Vector: n.vector, Neighbors: [][]int64{n.neighbors}}
	}
	return aisaqDump{
		Vectors:    cloneFloat32Map(a.vectors),
		Quantized:  cloneInt8Map(a.quantizedVectors),
		Scale:      append([]float32(nil), a.scale...),
		Shift:      append([]float32(nil), a.shift...),
		Graph:      graph,
		MaxDegree:  a.maxDegree,
		SearchList: a.searchListSize,
	}
}

// ApplyVectorSnapshot restores a previously encoded snapshot onto idx.
func ApplyVectorSnapshot(idx VectorIndex, data []byte, expectCount int64) error {
	var hdr vectorSnapshotHeader
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&hdr); err != nil {
		return err
	}
	if hdr.Magic != vectorSnapshotMagic {
		return fmt.Errorf("invalid vector snapshot magic")
	}
	cfg := idx.GetConfig()
	if cfg == nil || hdr.Type != vectorIndexConcreteType(idx) || hdr.Metric != cfg.MetricType || hdr.Dimension != cfg.Dimension {
		return fmt.Errorf("vector snapshot does not match index config")
	}
	if expectCount >= 0 && hdr.Count != expectCount {
		return fmt.Errorf("vector snapshot count %d does not match table %d", hdr.Count, expectCount)
	}
	dec := gob.NewDecoder(bytes.NewReader(hdr.Payload))
	switch v := idx.(type) {
	case *FlatIndex:
		var d flatDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		v.mu.Lock()
		v.vectors = d.Vectors
		v.mu.Unlock()
	case *HNSWIndex:
		var d hnswDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		v.mu.Lock()
		v.vectors = d.Vectors
		v.layers = d.Layers
		v.nodeLevel = d.NodeLevel
		v.entryPoint = d.EntryPoint
		v.entryLevel = d.EntryLevel
		v.hasEntry = d.HasEntry
		v.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		v.mu.Unlock()
	case *IVFFlatIndex:
		var d ivfFlatDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		applyIVFFlat(&v.mu, &v.vectors, &v.centroids, &v.vectorsByCluster, &v.assignments, &v.clusterCounts, &v.nlist, d)
	case *IVFSQ8Index:
		var d ivfSQ8Dump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		applyIVFFlat(&v.mu, &v.vectors, &v.centroids, &v.vectorsByCluster, &v.assignments, &v.clusterCounts, &v.nlist, ivfFlatDump{
			Vectors: d.Vectors, Centroids: d.Centroids, VectorsByCluster: d.VectorsByCluster,
			Assignments: d.Assignments, ClusterCounts: d.ClusterCounts, Nlist: d.Nlist,
		})
		v.mu.Lock()
		v.quantizedVectors = d.Quantized
		v.scale = d.Scale
		v.shift = d.Shift
		v.mu.Unlock()
	case *IVFPQIndex:
		var d ivfPQDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		applyIVFFlat(&v.mu, &v.vectors, &v.ivfCentroids, &v.vectorsByCluster, &v.assignments, &v.clusterCounts, &v.nlist, ivfFlatDump{
			Vectors: d.Vectors, Centroids: d.Centroids, VectorsByCluster: d.VectorsByCluster,
			Assignments: d.Assignments, ClusterCounts: d.ClusterCounts, Nlist: d.Nlist,
		})
		v.mu.Lock()
		v.nsubq = d.Nsubq
		v.centroids = d.PQSubCentroids
		v.codebooks = d.Codebooks
		v.codes = d.Codes
		v.m = d.M
		v.nbits = d.Nbits
		v.ksubq = d.Ksubq
		v.mu.Unlock()
	case *IVFRabitQIndex:
		var d ivfRabitQDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		applyIVFFlat(&v.mu, &v.vectors, &v.centroids, &v.vectorsByCluster, &v.assignments, &v.clusterCounts, &v.nlist, ivfFlatDump{
			Vectors: d.Vectors, Centroids: d.Centroids, VectorsByCluster: d.VectorsByCluster,
			Assignments: d.Assignments, ClusterCounts: d.ClusterCounts, Nlist: d.Nlist,
		})
		v.mu.Lock()
		v.quantizedVectors = d.Quantized
		v.projectionMatrix = d.ProjectionMatrix
		v.mu.Unlock()
	case *HNSWSQIndex:
		var d hnswQuantDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		v.mu.Lock()
		v.vectors = d.Vectors
		v.quantizedVectors = d.Quantized
		v.scale = d.Scale
		v.shift = d.Shift
		v.maxLevel = d.MaxLevel
		v.ml = d.ML
		v.efConstruction = d.EFConstruction
		v.ef = d.EF
		v.layers = make([]map[int64]*hnswNodeSQ, len(d.Layers))
		for i, layer := range d.Layers {
			m := make(map[int64]*hnswNodeSQ, len(layer))
			for id, n := range layer {
				m[id] = &hnswNodeSQ{id: n.ID, vector: n.Vector, neighbors: n.Neighbors}
			}
			v.layers[i] = m
		}
		v.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		v.mu.Unlock()
	case *HNSWPQIndex:
		var d hnswQuantDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		v.mu.Lock()
		v.vectors = d.Vectors
		v.codes = d.Codes
		v.codebooks = d.Codebooks
		v.maxLevel = d.MaxLevel
		v.ml = d.ML
		v.efConstruction = d.EFConstruction
		v.ef = d.EF
		v.nsubq = d.Nsubq
		v.ksubq = d.Ksubq
		v.layers = make([]map[int64]*hnswNodePQ, len(d.Layers))
		for i, layer := range d.Layers {
			m := make(map[int64]*hnswNodePQ, len(layer))
			for id, n := range layer {
				m[id] = &hnswNodePQ{id: n.ID, code: n.Code, neighbors: n.Neighbors}
			}
			v.layers[i] = m
		}
		v.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		v.mu.Unlock()
	case *HNSWPRQIndex:
		var d hnswQuantDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		v.mu.Lock()
		v.vectors = d.Vectors
		v.coarseCodes = d.CoarseCodes
		v.residualCodes = d.ResidualCodes
		v.coarseCodebook = d.CoarseCodebook
		v.residualCodebooks = d.ResidualBooks
		v.maxLevel = d.MaxLevel
		v.ml = d.ML
		v.ef = d.EF
		v.nsubq = d.Nsubq
		v.ksubq = d.Ksubq
		v.kcoarse = d.Kcoarse
		v.layers = make([]map[int64]*hnswNodePRQ, len(d.Layers))
		for i, layer := range d.Layers {
			m := make(map[int64]*hnswNodePRQ, len(layer))
			for id, n := range layer {
				m[id] = &hnswNodePRQ{id: n.ID, coarseCode: n.Coarse, residual: n.Residual, neighbors: n.Neighbors}
			}
			v.layers[i] = m
		}
		v.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		v.mu.Unlock()
	case *AISAQIndex:
		var d aisaqDump
		if err := dec.Decode(&d); err != nil {
			return err
		}
		v.mu.Lock()
		v.vectors = d.Vectors
		v.quantizedVectors = d.Quantized
		v.scale = d.Scale
		v.shift = d.Shift
		v.maxDegree = d.MaxDegree
		v.searchListSize = d.SearchList
		v.graph = make(map[int64]*vamanaNode, len(d.Graph))
		for id, n := range d.Graph {
			neighbors := n.Neighbors
			var list []int64
			if len(neighbors) > 0 {
				list = neighbors[0]
			}
			v.graph[id] = &vamanaNode{id: n.ID, vector: n.Vector, neighbors: list}
		}
		v.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		v.mu.Unlock()
	default:
		return fmt.Errorf("unsupported vector index type %T", idx)
	}
	return nil
}

func applyIVFFlat(mu *sync.RWMutex, vectors *map[int64][]float32, centroids *[][]float32, byCluster *map[int][]VectorRecord, assignments *map[int64]int, counts *[]int, nlist *int, d ivfFlatDump) {
	if d.Vectors == nil {
		d.Vectors = make(map[int64][]float32)
	}
	if d.VectorsByCluster == nil {
		d.VectorsByCluster = make(map[int][]VectorRecord)
	}
	if d.Assignments == nil {
		d.Assignments = make(map[int64]int)
	}
	mu.Lock()
	*vectors = d.Vectors
	*centroids = d.Centroids
	*byCluster = d.VectorsByCluster
	*assignments = d.Assignments
	*counts = d.ClusterCounts
	*nlist = d.Nlist
	mu.Unlock()
}

func vectorIndexConcreteType(idx VectorIndex) IndexType {
	switch idx.(type) {
	case *FlatIndex:
		return IndexTypeVectorFlat
	case *HNSWIndex:
		return IndexTypeVectorHNSW
	case *IVFFlatIndex:
		return IndexTypeVectorIVFFlat
	case *IVFSQ8Index:
		return IndexTypeVectorIVFSQ8
	case *IVFPQIndex:
		return IndexTypeVectorIVFPQ
	case *IVFRabitQIndex:
		return IndexTypeVectorIVFRabitQ
	case *HNSWSQIndex:
		return IndexTypeVectorHNSWSQ
	case *HNSWPQIndex:
		return IndexTypeVectorHNSWPQ
	case *HNSWPRQIndex:
		return IndexTypeVectorHNSWPRQ
	case *AISAQIndex:
		return IndexTypeVectorAISAQ
	default:
		return ""
	}
}

func cloneFloat32Map(in map[int64][]float32) map[int64][]float32 {
	out := make(map[int64][]float32, len(in))
	for k, v := range in {
		cp := make([]float32, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func cloneInt8Map(in map[int64][]int8) map[int64][]int8 {
	out := make(map[int64][]int8, len(in))
	for k, v := range in {
		cp := make([]int8, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func cloneUint64Map(in map[int64][]uint64) map[int64][]uint64 {
	out := make(map[int64][]uint64, len(in))
	for k, v := range in {
		cp := make([]uint64, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func cloneIntMap(in map[int64]int) map[int64]int {
	out := make(map[int64]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneHNSWLayers(in []map[int64][]int64) []map[int64][]int64 {
	out := make([]map[int64][]int64, len(in))
	for i, layer := range in {
		m := make(map[int64][]int64, len(layer))
		for id, ns := range layer {
			cp := make([]int64, len(ns))
			copy(cp, ns)
			m[id] = cp
		}
		out[i] = m
	}
	return out
}
