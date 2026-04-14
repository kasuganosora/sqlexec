package memory

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestIndexMaintenance_InsertUpdatesIndex verifies that after creating an index
// and inserting rows, the index can find the newly inserted rows.
func TestIndexMaintenance_InsertUpdatesIndex(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}))

	// Create a BTree index on "name"
	err := ds.CreateIndex("users", "name", "btree", false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert rows
	_, err = ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}, nil)
	require.NoError(t, err)

	// Verify index can find "alice"
	idx, err := ds.indexManager.GetIndex("users", "name")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}

	rowIDs, found := idx.Find("alice")
	if !found || len(rowIDs) == 0 {
		t.Errorf("index Find('alice') failed: found=%v, rowIDs=%v", found, rowIDs)
	}

	rowIDs, found = idx.Find("bob")
	if !found || len(rowIDs) == 0 {
		t.Errorf("index Find('bob') failed: found=%v, rowIDs=%v", found, rowIDs)
	}
}

// TestIndexMaintenance_DeleteUpdatesIndex verifies that deleting a row
// removes it from the index.
func TestIndexMaintenance_DeleteUpdatesIndex(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}))

	// Insert rows first
	_, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}, nil)
	require.NoError(t, err)

	// Create a BTree index on "name"
	err = ds.CreateIndex("users", "name", "btree", false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Rebuild index to include existing rows
	tableVer := ds.tables["users"]
	tableVer.mu.RLock()
	latestData := tableVer.versions[tableVer.latest]
	tableVer.mu.RUnlock()
	require.NoError(t, ds.indexManager.RebuildIndex("users", latestData.schema, latestData.Rows()))

	// Delete "alice"
	_, err = ds.Delete(ctx, "users", []domain.Filter{
		{Field: "name", Operator: "=", Value: "alice"},
	}, nil)
	require.NoError(t, err)

	// Verify "alice" is no longer in the index
	idx, err := ds.indexManager.GetIndex("users", "name")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}

	rowIDs, found := idx.Find("alice")
	if found && len(rowIDs) > 0 {
		t.Errorf("index still contains 'alice' after delete: rowIDs=%v", rowIDs)
	}

	// Verify "bob" is still in the index
	rowIDs, found = idx.Find("bob")
	if !found || len(rowIDs) == 0 {
		t.Errorf("index lost 'bob' after deleting 'alice': found=%v, rowIDs=%v", found, rowIDs)
	}
}

// TestIndexMaintenance_UpdateUpdatesIndex verifies that updating a row's
// indexed column updates the index accordingly.
func TestIndexMaintenance_UpdateUpdatesIndex(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}))

	// Insert and build index
	_, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice"},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, ds.CreateIndex("users", "name", "btree", false))

	tableVer := ds.tables["users"]
	tableVer.mu.RLock()
	latestData := tableVer.versions[tableVer.latest]
	tableVer.mu.RUnlock()
	require.NoError(t, ds.indexManager.RebuildIndex("users", latestData.schema, latestData.Rows()))

	// Update alice -> carol
	_, err = ds.Update(ctx, "users", []domain.Filter{
		{Field: "name", Operator: "=", Value: "alice"},
	}, domain.Row{"name": "carol"}, nil)
	require.NoError(t, err)

	idx, _ := ds.indexManager.GetIndex("users", "name")

	// Old value should not be in index
	rowIDs, found := idx.Find("alice")
	if found && len(rowIDs) > 0 {
		t.Errorf("index still contains 'alice' after update to 'carol': rowIDs=%v", rowIDs)
	}

	// New value should be in index
	rowIDs, found = idx.Find("carol")
	if !found || len(rowIDs) == 0 {
		t.Errorf("index does not contain 'carol' after update: found=%v, rowIDs=%v", found, rowIDs)
	}
}

// TestIndexMaintenance_IndexScanAfterInsert verifies that the query planner
// uses the index correctly for newly inserted data.
func TestIndexMaintenance_IndexScanAfterInsert(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "sku", Type: "VARCHAR"},
			{Name: "price", Type: "FLOAT"},
		},
	}))

	// Create index before inserting
	require.NoError(t, ds.CreateIndex("products", "sku", "btree", true))

	// Insert data
	_, err := ds.Insert(ctx, "products", []domain.Row{
		{"id": int64(1), "sku": "ABC-001", "price": float64(9.99)},
		{"id": int64(2), "sku": "DEF-002", "price": float64(19.99)},
		{"id": int64(3), "sku": "GHI-003", "price": float64(29.99)},
	}, nil)
	require.NoError(t, err)

	// Query using the indexed column — should use index scan
	result, err := ds.Query(ctx, "products", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "sku", Operator: "=", Value: "DEF-002"},
		},
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 result, got %d", len(result.Rows))
	} else if result.Rows[0]["price"] != float64(19.99) {
		t.Errorf("expected price 19.99, got %v", result.Rows[0]["price"])
	}
}
