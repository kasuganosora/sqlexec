package memory

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/require"
)

// TestDelete_NonTx_DeepCopiesRetainedRows verifies that non-transaction delete
// deep-copies retained rows into the new version so the old version's data
// remains isolated (MVCC guarantee).
func TestDelete_NonTx_DeepCopiesRetainedRows(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "items",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "tags", Type: "JSON"},
		},
	}))

	// Insert rows with nested data
	_, err := ds.Insert(ctx, "items", []domain.Row{
		{"id": int64(1), "tags": []interface{}{"keep_me"}},
		{"id": int64(2), "tags": []interface{}{"delete_me"}},
	}, nil)
	require.NoError(t, err)

	// Record the version before delete
	ds.mu.RLock()
	tableVer := ds.tables["items"]
	ds.mu.RUnlock()
	tableVer.mu.RLock()
	preDeleteVer := tableVer.latest
	preDeleteData := tableVer.versions[preDeleteVer]
	preDeleteRows := preDeleteData.Rows()
	tableVer.mu.RUnlock()

	// Delete row with id=2
	_, err = ds.Delete(ctx, "items", []domain.Filter{
		{Field: "id", Operator: "=", Value: int64(2)},
	}, nil)
	require.NoError(t, err)

	// Query new version
	result, err := ds.Query(ctx, "items", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(result.Rows))
	}

	// Mutate the new version's retained row's nested data
	newTags := result.Rows[0]["tags"].([]interface{})
	newTags[0] = "MUTATED"

	// The old version's data should be unaffected
	oldTags := preDeleteRows[0]["tags"].([]interface{})
	if oldTags[0] != "keep_me" {
		t.Errorf("MVCC isolation broken: old version row tags[0] = %v, want 'keep_me'", oldTags[0])
	}
}
