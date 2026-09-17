package memory

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeApplyVectorSnapshot_AllTypes(t *testing.T) {
	ctx := context.Background()
	const dim = 32
	const n = 48
	rng := rand.New(rand.NewSource(7))
	records := makeVectorRecords(rng, n, dim)
	query := records[3].Vector

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

	for _, typ := range types {
		t.Run(string(typ), func(t *testing.T) {
			mgr := NewIndexManager()
			src, err := mgr.CreateVectorIndex("docs", "embedding", VectorMetricL2, typ, dim, smallVectorParams())
			require.NoError(t, err)
			require.NoError(t, src.Build(ctx, &testVectorDataLoader{records: records}))

			data, err := EncodeVectorSnapshot(src)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			mgr2 := NewIndexManager()
			dst, err := mgr2.CreateVectorIndex("docs", "embedding", VectorMetricL2, typ, dim, smallVectorParams())
			require.NoError(t, err)
			require.NoError(t, ApplyVectorSnapshot(dst, data, int64(n)))
			assert.Equal(t, int64(n), dst.Stats().Count)

			got, err := dst.Search(ctx, query, 5, nil)
			require.NoError(t, err)
			require.NotEmpty(t, got.IDs)

			if typ == IndexTypeVectorFlat || typ == IndexTypeVectorHNSW || typ == IndexTypeVectorIVFFlat {
				assert.Equal(t, int64(3), got.IDs[0])
			}
		})
	}
}

func TestApplyVectorSnapshot_RejectsStaleCount(t *testing.T) {
	ctx := context.Background()
	const dim = 8
	records := []VectorRecord{
		{ID: 1, Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}},
	}
	idx, err := NewFlatIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim})
	require.NoError(t, err)
	require.NoError(t, idx.Build(ctx, &testVectorDataLoader{records: records}))

	data, err := EncodeVectorSnapshot(idx)
	require.NoError(t, err)

	fresh, err := NewFlatIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim})
	require.NoError(t, err)
	err = ApplyVectorSnapshot(fresh, data, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "count")
}

func TestDirVectorSnapshotStore_CreateVectorIndexRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewDirVectorSnapshotStore(dir)

	ds := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	ds.SetVectorSnapshotStore(store)
	require.NoError(t, ds.Connect(ctx))
	require.NoError(t, ds.CreateTable(ctx, vectorTable("docs", 8)))

	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	_, err := ds.Insert(ctx, "docs", []domain.Row{
		{"id": int64(1), "title": "a", "embedding": query},
		{"id": int64(2), "title": "b", "embedding": []float32{0, 1, 0, 0, 0, 0, 0, 0}},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, ds.CreateVectorIndex("docs", "embedding", "l2", "flat", 8, nil))
	require.NoError(t, ds.Close(ctx))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	assert.Equal(t, "docs.embedding.vecidx", entries[0].Name())

	ds2 := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	ds2.SetVectorSnapshotStore(store)
	require.NoError(t, ds2.Connect(ctx))
	require.NoError(t, ds2.CreateTable(ctx, vectorTable("docs", 8)))
	_, err = ds2.Insert(ctx, "docs", []domain.Row{
		{"id": int64(1), "title": "a", "embedding": query},
		{"id": int64(2), "title": "b", "embedding": []float32{0, 1, 0, 0, 0, 0, 0, 0}},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, ds2.CreateVectorIndex("docs", "embedding", "l2", "flat", 8, nil))

	idx, err := ds2.GetIndexManager().GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	assert.Equal(t, int64(2), idx.Stats().Count)
	result, err := idx.Search(ctx, query, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, int64(1), result.IDs[0])
}

func TestDirVectorSnapshotStore_StaleSnapshotRebuilds(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewDirVectorSnapshotStore(dir)

	ds := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	ds.SetVectorSnapshotStore(store)
	require.NoError(t, ds.Connect(ctx))
	require.NoError(t, ds.CreateTable(ctx, vectorTable("docs", 8)))
	_, err := ds.Insert(ctx, "docs", []domain.Row{
		{"id": int64(1), "title": "a", "embedding": []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{"id": int64(2), "title": "b", "embedding": []float32{0, 1, 0, 0, 0, 0, 0, 0}},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, ds.CreateVectorIndex("docs", "embedding", "l2", "flat", 8, nil))
	require.NoError(t, ds.Close(ctx))

	ds2 := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	ds2.SetVectorSnapshotStore(store)
	require.NoError(t, ds2.Connect(ctx))
	require.NoError(t, ds2.CreateTable(ctx, vectorTable("docs", 8)))
	query := []float32{0, 0, 1, 0, 0, 0, 0, 0}
	_, err = ds2.Insert(ctx, "docs", []domain.Row{
		{"id": int64(1), "title": "a", "embedding": []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{"id": int64(2), "title": "b", "embedding": []float32{0, 1, 0, 0, 0, 0, 0, 0}},
		{"id": int64(3), "title": "c", "embedding": query},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, ds2.CreateVectorIndex("docs", "embedding", "l2", "flat", 8, nil))

	idx, err := ds2.GetIndexManager().GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	assert.Equal(t, int64(3), idx.Stats().Count)
	result, err := idx.Search(ctx, query, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, int64(3), result.IDs[0])
}

func TestCollectIndexMeta_IncludesVector(t *testing.T) {
	ctx := context.Background()
	ds := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	require.NoError(t, ds.Connect(ctx))
	require.NoError(t, ds.CreateTable(ctx, vectorTable("docs", 8)))
	require.NoError(t, ds.CreateVectorIndex("docs", "embedding", "cosine", "hnsw", 8, map[string]interface{}{"M": 8}))

	meta, err := ds.CollectIndexMeta("docs")
	require.NoError(t, err)
	require.NotEmpty(t, meta)
	var found *domain.IndexMetaInfo
	for i := range meta {
		if meta[i].IsVector {
			found = &meta[i]
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, []string{"embedding"}, found.Columns)
	assert.Equal(t, string(IndexTypeVectorHNSW), found.Type)
	assert.Equal(t, "cosine", found.Metric)
	assert.Equal(t, 8, found.Dimension)
	assert.Contains(t, found.ParamsJSON, "M")
}

func TestDropVectorIndex_RemovesSnapshotFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ds := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	ds.SetVectorSnapshotStore(NewDirVectorSnapshotStore(dir))
	require.NoError(t, ds.Connect(ctx))
	require.NoError(t, ds.CreateTable(ctx, vectorTable("docs", 8)))
	require.NoError(t, ds.CreateVectorIndex("docs", "embedding", "l2", "flat", 8, nil))
	_, err := os.Stat(filepath.Join(dir, "docs.embedding.vecidx"))
	require.NoError(t, err)

	require.NoError(t, ds.DropVectorIndex("docs", "embedding"))
	_, err = os.Stat(filepath.Join(dir, "docs.embedding.vecidx"))
	assert.True(t, os.IsNotExist(err))
}
