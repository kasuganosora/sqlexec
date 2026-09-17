package memory

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func smallVectorParams() map[string]interface{} {
	return map[string]interface{}{
		"nlist":            4,
		"nprobe":           4,
		"m":                8,
		"nbits":            4,
		"kcoarse":          8,
		"max_level":        4,
		"ef":               32,
		"efConstruction":   32,
		"max_degree":       8,
		"search_list_size": 16,
	}
}

func makeVectorRecords(rng *rand.Rand, n, dim int) []VectorRecord {
	records := make([]VectorRecord, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = rng.Float32()
		}
		records[i] = VectorRecord{ID: int64(i), Vector: vec}
	}
	return records
}

func containsID(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func TestVectorIndexVariants_BuildSearchDelete(t *testing.T) {
	ctx := context.Background()
	const dim = 32
	const n = 64
	rng := rand.New(rand.NewSource(42))
	records := makeVectorRecords(rng, n, dim)
	loader := &testVectorDataLoader{records: records}
	query := records[0].Vector

	types := []struct {
		name       string
		indexType  IndexType
		exactSelf  bool
		minResults int
	}{
		{"flat", IndexTypeVectorFlat, true, 10},
		{"hnsw", IndexTypeVectorHNSW, true, 10},
		{"ivf_flat", IndexTypeVectorIVFFlat, true, 10},
		{"ivf_sq8", IndexTypeVectorIVFSQ8, false, 1},
		{"ivf_pq", IndexTypeVectorIVFPQ, false, 1},
		{"hnsw_sq", IndexTypeVectorHNSWSQ, false, 1},
		{"hnsw_pq", IndexTypeVectorHNSWPQ, false, 1},
		{"ivf_rabitq", IndexTypeVectorIVFRabitQ, false, 1},
		{"hnsw_prq", IndexTypeVectorHNSWPRQ, false, 1},
		{"aisaq", IndexTypeVectorAISAQ, false, 1},
	}

	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			mgr := NewIndexManager()
			idx, err := mgr.CreateVectorIndex("docs", "embedding", VectorMetricL2, tc.indexType, dim, smallVectorParams())
			require.NoError(t, err)
			require.NotNil(t, idx)

			cfg := idx.GetConfig()
			require.NotNil(t, cfg)
			assert.Equal(t, dim, cfg.Dimension)
			assert.Equal(t, VectorMetricL2, cfg.MetricType)

			require.NoError(t, idx.Build(ctx, loader))
			assert.Equal(t, int64(n), idx.Stats().Count)

			empty, err := idx.Search(ctx, query, 10, nil)
			require.NoError(t, err)
			require.NotEmpty(t, empty.IDs)
			assert.Equal(t, len(empty.IDs), len(empty.Distances))
			assert.LessOrEqual(t, len(empty.IDs), 10)
			assert.GreaterOrEqual(t, len(empty.IDs), tc.minResults)

			if tc.exactSelf {
				assert.True(t, containsID(empty.IDs, 0), "%s should recall the query vector itself, got %v", tc.name, empty.IDs)
			}

			filtered, err := idx.Search(ctx, query, 5, &VectorFilter{IDs: []int64{0, 1, 2}})
			require.NoError(t, err)
			for _, id := range filtered.IDs {
				assert.True(t, id == 0 || id == 1 || id == 2, "filter leaked id %d", id)
			}

			require.NoError(t, idx.Delete(0))
			afterDelete, err := idx.Search(ctx, query, 10, nil)
			require.NoError(t, err)
			assert.False(t, containsID(afterDelete.IDs, 0), "%s still returned deleted id 0", tc.name)

			_, err = idx.Search(ctx, make([]float32, dim/2), 5, nil)
			assert.Error(t, err)

			require.NoError(t, idx.Close())
		})
	}
}

func TestIVFFlat_EmptyClustersDoNotHideResults(t *testing.T) {
	ctx := context.Background()
	const dim = 32
	rng := rand.New(rand.NewSource(7))
	records := makeVectorRecords(rng, 80, dim)

	idx, err := NewIVFFlatIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  dim,
		Params: map[string]interface{}{
			"nlist":  16,
			"nprobe": 4,
		},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[0].Vector, 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.True(t, containsID(result.IDs, 0), "IVF-Flat must not skip populated clusters; got %v", result.IDs)
}

func TestIVFFlat_InsertWithoutBuildAndDuplicateID(t *testing.T) {
	ctx := context.Background()
	idx, err := NewIVFFlatIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  8,
		Params:     map[string]interface{}{"nlist": 8, "nprobe": 8},
	})
	require.NoError(t, err)

	vec := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	require.NoError(t, idx.Insert(1, vec))
	require.NoError(t, idx.Insert(1, vec))
	assert.Equal(t, int64(1), idx.Stats().Count)

	result, err := idx.Search(ctx, vec, 3, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.IDs))
	assert.Equal(t, int64(1), result.IDs[0])
}

func TestQuantizedIndexes_InsertRequiresBuild(t *testing.T) {
	cfg := &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  32,
		Params:     smallVectorParams(),
	}
	vec := make([]float32, 32)

	pq, err := NewHNSWPQIndex("embedding", cfg)
	require.NoError(t, err)
	assert.Error(t, pq.Insert(1, vec))

	ivfpq, err := NewIVFPQIndex("embedding", cfg)
	require.NoError(t, err)
	assert.Error(t, ivfpq.Insert(1, vec))

	prq, err := NewHNSWPRQIndex("embedding", cfg)
	require.NoError(t, err)
	assert.Error(t, prq.Insert(1, vec))
}

func TestHNSWPQ_MMustDivideDimension(t *testing.T) {
	_, err := NewHNSWPQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  30,
		Params:     map[string]interface{}{"m": 8},
	})
	assert.Error(t, err)

	_, err = NewIVFPQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  30,
		Params:     map[string]interface{}{"m": 8},
	})
	assert.Error(t, err)
}

func TestVectorIndex_EmptySearch(t *testing.T) {
	ctx := context.Background()
	cfg := &VectorIndexConfig{MetricType: VectorMetricCosine, Dimension: 8}

	idx, err := NewFlatIndex("embedding", cfg)
	require.NoError(t, err)

	result, err := idx.Search(ctx, make([]float32, 8), 5, nil)
	require.NoError(t, err)
	assert.Empty(t, result.IDs)

	hnsw, err := NewHNSWIndex("embedding", cfg)
	require.NoError(t, err)
	result, err = hnsw.Search(ctx, make([]float32, 8), 5, nil)
	require.NoError(t, err)
	assert.Empty(t, result.IDs)
}

func TestIndexManager_AllVectorTypes(t *testing.T) {
	mgr := NewIndexManager()
	params := smallVectorParams()
	types := []IndexType{
		IndexTypeVectorFlat,
		IndexTypeVectorHNSW,
		IndexTypeVectorIVFFlat,
		IndexTypeVectorIVFSQ8,
		IndexTypeVectorIVFPQ,
		IndexTypeVectorHNSWSQ,
		IndexTypeVectorHNSWPQ,
		IndexTypeVectorIVFRabitQ,
		IndexTypeVectorHNSWPRQ,
		IndexTypeVectorAISAQ,
	}
	for i, typ := range types {
		idx, err := mgr.CreateVectorIndex("t", string(typ)+"_col", VectorMetricCosine, typ, 32, params)
		require.NoError(t, err, "type %s", typ)
		require.NotNil(t, idx)
		got, err := mgr.GetVectorIndex("t", string(typ)+"_col")
		require.NoError(t, err)
		require.Equal(t, idx, got)
		require.NoError(t, mgr.DropVectorIndex("t", string(typ)+"_col"))
		_, err = mgr.GetVectorIndex("t", string(typ)+"_col")
		assert.Error(t, err, "index %d %s should be dropped", i, typ)
	}
}

func TestSelectProbedClustersSkipsEmpty(t *testing.T) {
	counts := []int{0, 5, 0, 3, 0}
	dists := []float32{0, 2.0, 0, 1.0, 0}
	got := selectProbedClusters(5, 4, counts, func(id int) float32 { return dists[id] })
	require.Equal(t, []int{3, 1}, got)
}

func TestIVFFlat_IncrementalInsertAfterBuild(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(99))
	records := makeVectorRecords(rng, 40, 16)
	idx, err := NewIVFFlatIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  16,
		Params:     map[string]interface{}{"nlist": 4, "nprobe": 4},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	extra := make([]float32, 16)
	for i := range extra {
		extra[i] = 0.5
	}
	require.NoError(t, idx.Insert(999, extra))
	assert.Equal(t, int64(41), idx.Stats().Count)

	result, err := idx.Search(ctx, extra, 5, nil)
	require.NoError(t, err)
	assert.True(t, containsID(result.IDs, 999), "inserted vector should be searchable, got %v", result.IDs)
}

func TestHNSW_IncrementalInsertSearchAndDelete(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(3))
	records := makeVectorRecords(rng, 48, 16)
	idx, err := NewHNSWIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  16,
		Params:     smallVectorParams(),
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	extra := make([]float32, 16)
	copy(extra, records[0].Vector)
	extra[0] += 0.01
	require.NoError(t, idx.Insert(999, extra))
	assert.Equal(t, int64(49), idx.Stats().Count)

	result, err := idx.Search(ctx, extra, 5, nil)
	require.NoError(t, err)
	assert.True(t, containsID(result.IDs, 999), "HNSW insert after build should be searchable, got %v", result.IDs)

	require.NoError(t, idx.Delete(999))
	after, err := idx.Search(ctx, extra, 5, nil)
	require.NoError(t, err)
	assert.False(t, containsID(after.IDs, 999))
}

func TestQuantizedIndexes_InsertAfterBuild(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(11))
	const dim = 32
	records := makeVectorRecords(rng, 48, dim)
	loader := &testVectorDataLoader{records: records}
	extra := make([]float32, dim)
	copy(extra, records[0].Vector)

	cases := []struct {
		name string
		idx  VectorIndex
	}{}

	sq, err := NewHNSWSQIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim, Params: smallVectorParams()})
	require.NoError(t, err)
	pq, err := NewHNSWPQIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim, Params: smallVectorParams()})
	require.NoError(t, err)
	prq, err := NewHNSWPRQIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim, Params: smallVectorParams()})
	require.NoError(t, err)
	ivfpq, err := NewIVFPQIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim, Params: smallVectorParams()})
	require.NoError(t, err)
	aisaq, err := NewAISAQIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim, Params: smallVectorParams()})
	require.NoError(t, err)
	cases = []struct {
		name string
		idx  VectorIndex
	}{
		{"hnsw_sq", sq},
		{"hnsw_pq", pq},
		{"hnsw_prq", prq},
		{"ivf_pq", ivfpq},
		{"aisaq", aisaq},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.idx.Build(ctx, loader))
			require.NoError(t, tc.idx.Insert(999, extra))
			result, err := tc.idx.Search(ctx, extra, 10, nil)
			require.NoError(t, err)
			require.NotEmpty(t, result.IDs)
			assert.Equal(t, len(result.IDs), len(result.Distances))
		})
	}
}

func TestIVFSQ8_EmptyClustersDoNotHideResults(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(13))
	records := makeVectorRecords(rng, 80, 32)
	idx, err := NewIVFSQ8Index("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  32,
		Params:     map[string]interface{}{"nlist": 16, "nprobe": 4},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[0].Vector, 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
}

func TestVectorIndex_SearchKLargerThanN(t *testing.T) {
	ctx := context.Background()
	records := []VectorRecord{
		{ID: 1, Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}},
	}
	idx, err := NewFlatIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: 8})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[0].Vector, 10, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.IDs))
	assert.Equal(t, int64(1), result.IDs[0])
}

func TestVectorIndex_CosineMetric(t *testing.T) {
	ctx := context.Background()
	records := []VectorRecord{
		{ID: 1, Vector: []float32{1, 0, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0, 0}},
		{ID: 3, Vector: []float32{0.9, 0.1, 0, 0}},
	}
	idx, err := NewHNSWIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricCosine, Dimension: 4})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[0].Vector, 2, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, int64(1), result.IDs[0])
}

func TestSelectProbedClustersAllEmpty(t *testing.T) {
	got := selectProbedClusters(3, 2, []int{0, 0, 0}, func(int) float32 { return 0 })
	assert.Empty(t, got)
}
