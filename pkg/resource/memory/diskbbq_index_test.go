package memory

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVectorIndexType_DiskBBQ(t *testing.T) {
	aliases := []string{"diskbbq", "DISKBBQ", "disk_bbq", "vector_diskbbq", "bbq_disk", "bbqdisk"}
	for _, name := range aliases {
		assert.Equal(t, IndexTypeVectorDiskBBQ, ParseVectorIndexType(name), name)
	}
	assert.True(t, IndexTypeVectorDiskBBQ.IsVectorIndex())
}

func TestDiskBBQ_EmptySearchAndInsertWithoutBuild(t *testing.T) {
	ctx := context.Background()
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  8,
		Params:     map[string]interface{}{"nlist": 4, "nprobe": 4},
	})
	require.NoError(t, err)

	empty, err := idx.Search(ctx, make([]float32, 8), 5, nil)
	require.NoError(t, err)
	assert.Empty(t, empty.IDs)

	vec := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	require.NoError(t, idx.Insert(1, vec))
	require.NoError(t, idx.Insert(1, vec))
	assert.Equal(t, int64(1), idx.Stats().Count)
	assert.Equal(t, IndexTypeVectorDiskBBQ, idx.Stats().Type)

	result, err := idx.Search(ctx, vec, 3, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.IDs))
	assert.Equal(t, int64(1), result.IDs[0])
}

func TestDiskBBQ_BuildSearchFilterDelete(t *testing.T) {
	ctx := context.Background()
	const dim = 32
	rng := rand.New(rand.NewSource(21))
	records := makeVectorRecords(rng, 80, dim)
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  dim,
		Params:     map[string]interface{}{"nlist": 8, "ncoarse": 4, "nprobe": 8, "nprobe_coarse": 4},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))
	assert.Equal(t, int64(80), idx.Stats().Count)
	assert.Greater(t, idx.Stats().MemorySize, int64(0))

	result, err := idx.Search(ctx, records[0].Vector, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, len(result.IDs), len(result.Distances))

	filtered, err := idx.Search(ctx, records[0].Vector, 5, &VectorFilter{IDs: []int64{0, 1, 2}})
	require.NoError(t, err)
	for _, id := range filtered.IDs {
		assert.True(t, id == 0 || id == 1 || id == 2)
	}

	require.NoError(t, idx.Delete(0))
	after, err := idx.Search(ctx, records[0].Vector, 10, nil)
	require.NoError(t, err)
	assert.False(t, containsID(after.IDs, 0))

	_, err = idx.Search(ctx, make([]float32, dim/2), 5, nil)
	assert.Error(t, err)
	require.NoError(t, idx.Close())
}

func TestDiskBBQ_CosineMetric(t *testing.T) {
	ctx := context.Background()
	records := []VectorRecord{
		{ID: 1, Vector: []float32{1, 0, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0, 0}},
		{ID: 3, Vector: []float32{0.9, 0.1, 0, 0}},
	}
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  4,
		Params:     map[string]interface{}{"nlist": 2, "nprobe": 2},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[0].Vector, 2, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
}

func TestDiskBBQ_NewIndexValidation(t *testing.T) {
	_, err := NewDiskBBQIndex("embedding", nil)
	assert.Error(t, err)

	_, err = NewDiskBBQIndex("embedding", &VectorIndexConfig{MetricType: "unknown", Dimension: 8})
	assert.Error(t, err)
}

func TestDiskBBQ_InsertDimensionMismatch(t *testing.T) {
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: 8})
	require.NoError(t, err)
	assert.Error(t, idx.Insert(1, []float32{1, 2, 3}))
	require.NoError(t, idx.Delete(99))
}

func TestDiskBBQ_SearchKLargerThanNAndMonotonic(t *testing.T) {
	ctx := context.Background()
	records := []VectorRecord{
		{ID: 1, Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}},
		{ID: 3, Vector: []float32{0, 0, 1, 0, 0, 0, 0, 0}},
	}
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  8,
		Params:     map[string]interface{}{"nlist": 2, "nprobe": 2},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[0].Vector, 20, nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(result.IDs))
	for i := 1; i < len(result.Distances); i++ {
		assert.GreaterOrEqual(t, result.Distances[i], result.Distances[i-1])
	}
}

func TestDiskBBQ_InnerProductAndSelfRecall(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(3))
	records := makeVectorRecords(rng, 48, 16)
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricIP,
		Dimension:  16,
		Params:     map[string]interface{}{"nlist": 8, "nprobe": 8},
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	result, err := idx.Search(ctx, records[7].Vector, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.True(t, containsID(result.IDs, 7), "self vector should be recalled, got %v", result.IDs)
}

func TestDiskBBQ_IndexManagerWorkflow(t *testing.T) {
	ctx := context.Background()
	mgr := NewIndexManager()
	idx, err := mgr.CreateVectorIndex("docs", "embedding", VectorMetricL2, IndexTypeVectorDiskBBQ, 16, smallVectorParams())
	require.NoError(t, err)

	for i := 0; i < 40; i++ {
		require.NoError(t, idx.Insert(int64(i), randomVector(16)))
	}
	got, err := mgr.GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	result, err := got.Search(ctx, randomVector(16), 5, nil)
	require.NoError(t, err)
	require.Len(t, result.IDs, 5)
	require.Equal(t, int64(40), got.Stats().Count)

	require.NoError(t, mgr.DropVectorIndex("docs", "embedding"))
	_, err = mgr.GetVectorIndex("docs", "embedding")
	assert.Error(t, err)
}

func TestDiskBBQ_SnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(9))
	records := makeVectorRecords(rng, 64, 32)
	src, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  32,
		Params:     smallVectorParams(),
	})
	require.NoError(t, err)
	require.NoError(t, src.Build(ctx, &testVectorDataLoader{records: records}))

	data, err := EncodeVectorSnapshot(src)
	require.NoError(t, err)
	dst, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  32,
		Params:     smallVectorParams(),
	})
	require.NoError(t, err)
	require.NoError(t, ApplyVectorSnapshot(dst, data, 64))
	assert.Equal(t, int64(64), dst.Stats().Count)

	got, err := dst.Search(ctx, records[3].Vector, 8, nil)
	require.NoError(t, err)
	require.NotEmpty(t, got.IDs)
}

func TestDiskBBQ_MemorySmallerThanFlat(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(5))
	records := makeVectorRecords(rng, 200, 64)
	loader := &testVectorDataLoader{records: records}

	flat, err := NewFlatIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: 64})
	require.NoError(t, err)
	require.NoError(t, flat.Build(ctx, loader))

	bbq, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  64,
		Params:     map[string]interface{}{"nlist": 16, "nprobe": 8},
	})
	require.NoError(t, err)
	require.NoError(t, bbq.Build(ctx, loader))
	assert.Less(t, bbq.Stats().MemorySize, flat.Stats().MemorySize)
}
