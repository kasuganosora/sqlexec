package memory

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vectorTable(name string, dim int) *domain.TableInfo {
	return &domain.TableInfo{
		Name: name,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "title", Type: "VARCHAR"},
			{Name: "embedding", Type: "VECTOR", VectorDim: dim, VectorType: "float32"},
		},
	}
}

func TestMVCC_CreateVectorIndexBuildsFromExistingRows(t *testing.T) {
	ctx := context.Background()
	ds := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, vectorTable("docs", 8)))
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	_, err := ds.Insert(ctx, "docs", []domain.Row{
		{"id": int64(1), "title": "a", "embedding": query},
		{"id": int64(2), "title": "b", "embedding": []float32{0, 1, 0, 0, 0, 0, 0, 0}},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, ds.CreateVectorIndex("docs", "embedding", "l2", "flat", 0, nil))
	idx, err := ds.GetIndexManager().GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	assert.Equal(t, int64(2), idx.Stats().Count)

	result, err := idx.Search(ctx, query, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, int64(1), result.IDs[0])
}

func TestMVCC_InsertUpdateDeleteMaintainVectorIndex(t *testing.T) {
	ctx := context.Background()
	ds := NewMVCCDataSource(&domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Writable: true})
	require.NoError(t, ds.Connect(ctx))
	require.NoError(t, ds.CreateTable(ctx, vectorTable("docs", 8)))
	require.NoError(t, ds.CreateVectorIndex("docs", "embedding", "l2", "hnsw", 8, nil))

	base := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	_, err := ds.Insert(ctx, "docs", []domain.Row{
		{"id": int64(10), "title": "keep", "embedding": []float32{0, 1, 0, 0, 0, 0, 0, 0}},
		{"id": int64(11), "title": "new", "embedding": base},
	}, nil)
	require.NoError(t, err)

	idx, err := ds.GetIndexManager().GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	result, err := idx.Search(ctx, base, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, int64(11), result.IDs[0])

	_, err = ds.Update(ctx, "docs", []domain.Filter{{Field: "id", Operator: "=", Value: int64(11)}}, domain.Row{
		"embedding": []float32{0, 0, 1, 0, 0, 0, 0, 0},
	}, nil)
	require.NoError(t, err)

	idx, err = ds.GetIndexManager().GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	result, err = idx.Search(ctx, []float32{0, 0, 1, 0, 0, 0, 0, 0}, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	assert.Equal(t, int64(11), result.IDs[0], "updated vector should be searchable at its new location")

	_, err = ds.Delete(ctx, "docs", []domain.Filter{{Field: "id", Operator: "=", Value: int64(10)}}, nil)
	require.NoError(t, err)
	idx, err = ds.GetIndexManager().GetVectorIndex("docs", "embedding")
	require.NoError(t, err)
	result, err = idx.Search(ctx, []float32{0, 1, 0, 0, 0, 0, 0, 0}, 5, nil)
	require.NoError(t, err)
	assert.False(t, containsID(result.IDs, 10), "deleted id should not be searchable, got %v", result.IDs)
}

func TestParseVectorValueAndRowID(t *testing.T) {
	vec, err := ParseVectorValue("[1, 2, 3]")
	require.NoError(t, err)
	assert.Equal(t, []float32{1, 2, 3}, vec)

	schema := vectorTable("docs", 3)
	id := VectorIDFromRow(domain.Row{"id": int64(9), "embedding": vec}, schema, 0)
	assert.Equal(t, int64(9), id)
	id = VectorIDFromRow(domain.Row{"embedding": vec}, schema, 3)
	assert.Equal(t, int64(4), id)
}

func TestHasVectorIndex(t *testing.T) {
	mgr := NewIndexManager()
	assert.False(t, mgr.HasVectorIndex("t", "embedding"))
	_, err := mgr.CreateVectorIndex("t", "embedding", VectorMetricL2, IndexTypeVectorFlat, 4, nil)
	require.NoError(t, err)
	assert.True(t, mgr.HasVectorIndex("t", "embedding"))
}
