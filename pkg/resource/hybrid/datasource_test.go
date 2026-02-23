package hybrid

import (
	"context"
	"testing"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHybridDataSource_BasicCRUD_Memory(t *testing.T) {
	// Create hybrid data source with Badger disabled
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	assert.True(t, ds.IsConnected())
	assert.True(t, ds.IsWritable())

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)", Nullable: false},
			{Name: "email", Type: "VARCHAR(255)", Nullable: true},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Get tables
	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "users")

	// Insert rows
	rows := []domain.Row{
		{"id": "user-1", "name": "Alice", "email": "alice@example.com"},
		{"id": "user-2", "name": "Bob", "email": "bob@example.com"},
	}
	inserted, err := ds.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	// Query rows
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)

	// Query with filter
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: "user-1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Alice", result.Rows[0]["name"])

	// Update row
	updates := domain.Row{"name": "Alice Updated"}
	updated, err := ds.Update(ctx, "users", []domain.Filter{
		{Field: "id", Operator: "=", Value: "user-1"},
	}, updates, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), updated)

	// Verify update
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: "user-1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Alice Updated", result.Rows[0]["name"])

	// Delete row
	deleted, err := ds.Delete(ctx, "users", []domain.Filter{
		{Field: "id", Operator: "=", Value: "user-1"},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify delete
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "Bob", result.Rows[0]["name"])

	// Drop table
	err = ds.DropTable(ctx, "users")
	require.NoError(t, err)

	tables, err = ds.GetTables(ctx)
	require.NoError(t, err)
	assert.NotContains(t, tables, "users")
}

func TestHybridDataSource_PersistenceControl(t *testing.T) {
	// Create hybrid data source with Badger in memory mode
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      true,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Check default persistence config (should be non-persistent)
	cfg, ok := ds.GetPersistenceConfig("products")
	require.True(t, ok)
	assert.False(t, cfg.Persistent)

	// Enable persistence
	err = ds.EnablePersistence(ctx, "products", WithSyncOnWrite(true), WithCacheInMemory(true))
	require.NoError(t, err)

	// Check updated config
	cfg, ok = ds.GetPersistenceConfig("products")
	require.True(t, ok)
	assert.True(t, cfg.Persistent)
	assert.True(t, cfg.SyncOnWrite)
	assert.True(t, cfg.CacheInMemory)

	// Disable persistence
	err = ds.DisablePersistence(ctx, "products")
	require.NoError(t, err)

	cfg, ok = ds.GetPersistenceConfig("products")
	require.True(t, ok)
	assert.False(t, cfg.Persistent)
}

func TestHybridDataSource_WithBadger(t *testing.T) {
	// Create hybrid data source with Badger in memory mode
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      true,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create memory table (default, non-persistent)
	memTableInfo := &domain.TableInfo{
		Name: "memory_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "data", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, memTableInfo)
	require.NoError(t, err)

	// Insert to memory table
	rows := []domain.Row{
		{"id": "m-1", "data": "memory data"},
	}
	inserted, err := ds.Insert(ctx, "memory_table", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted)

	// Verify memory table works
	result, err := ds.Query(ctx, "memory_table", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Enable persistence BEFORE creating the persistent table
	err = ds.EnablePersistence(ctx, "persistent_table")
	require.NoError(t, err)

	// Now create the persistent table - this should go to Badger
	persistentTableInfo := &domain.TableInfo{
		Name: "persistent_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "data", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, persistentTableInfo)
	require.NoError(t, err)

	// Insert to persistent table (should go to Badger)
	pRows := []domain.Row{
		{"id": "p-1", "data": "persistent data"},
	}
	inserted, err = ds.Insert(ctx, "persistent_table", pRows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted)

	// Query persistent table
	result, err = ds.Query(ctx, "persistent_table", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "persistent data", result.Rows[0]["data"])
}

func TestTableConfigManager(t *testing.T) {
	mgr := NewTableConfigManager(false)

	// Test default config
	cfg := mgr.GetConfig("new_table")
	assert.False(t, cfg.Persistent)

	// Set custom config
	err := mgr.SetConfig(context.Background(), &TableConfig{
		TableName:     "test_table",
		Persistent:    true,
		SyncOnWrite:   true,
		CacheInMemory: false,
	})
	require.NoError(t, err)

	// Get config
	cfg = mgr.GetConfig("test_table")
	assert.True(t, cfg.Persistent)
	assert.True(t, cfg.SyncOnWrite)
	assert.False(t, cfg.CacheInMemory)

	// List persistent tables
	persistent := mgr.ListPersistentTables()
	assert.Contains(t, persistent, "test_table")

	// Remove config
	err = mgr.RemoveConfig(context.Background(), "test_table")
	require.NoError(t, err)

	cfg = mgr.GetConfig("test_table")
	assert.False(t, cfg.Persistent) // Should return default

	// Test default persistent setting
	mgr.SetDefaultPersistent(true)
	assert.True(t, mgr.GetDefaultPersistent())
}

func TestDataSourceRouter(t *testing.T) {
	tableConfig := NewTableConfigManager(false)
	router := NewDataSourceRouter(tableConfig, nil, nil, nil)

	// Test default routing (no persistence)
	tableConfig.SetConfig(context.Background(), &TableConfig{
		TableName:  "memory_table",
		Persistent: false,
	})

	decision := router.Decide("memory_table", OpRead)
	assert.Equal(t, RouteMemoryOnly, decision)

	decision = router.Decide("memory_table", OpWrite)
	assert.Equal(t, RouteMemoryOnly, decision)

	// Test persistent routing (without Badger - should fall back to memory)
	tableConfig.SetConfig(context.Background(), &TableConfig{
		TableName:  "persistent_table",
		Persistent: true,
	})

	// Without Badger, should fall back to memory
	decision = router.Decide("persistent_table", OpRead)
	assert.Equal(t, RouteMemoryOnly, decision)

	decision = router.Decide("persistent_table", OpWrite)
	assert.Equal(t, RouteMemoryOnly, decision)

	// Test with DecideWithBadger for explicit Badger routing
	decision = router.DecideWithBadger("persistent_table", OpRead, true)
	assert.Equal(t, RouteBadgerOnly, decision)

	decision = router.DecideWithBadger("persistent_table", OpWrite, true)
	assert.Equal(t, RouteBadgerOnly, decision)
}

func TestHybridDataSource_Transaction(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "test_txn",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "value", Type: "INT"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Begin transaction
	txn, err := ds.BeginTransaction(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, txn)

	// Insert in transaction
	rows := []domain.Row{
		{"id": "txn-1", "value": 100},
	}
	inserted, err := txn.Insert(ctx, "test_txn", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted)

	// Commit
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Verify data
	result, err := ds.Query(ctx, "test_txn", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestHybridDataSource_TruncateTable(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "truncate_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Insert rows
	rows := []domain.Row{
		{"id": "1"},
		{"id": "2"},
		{"id": "3"},
	}
	_, err = ds.Insert(ctx, "truncate_test", rows, nil)
	require.NoError(t, err)

	// Verify rows
	result, err := ds.Query(ctx, "truncate_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)

	// Truncate
	err = ds.TruncateTable(ctx, "truncate_test")
	require.NoError(t, err)

	// Verify empty
	result, err = ds.Query(ctx, "truncate_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// Table should still exist
	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "truncate_test")
}

func TestHybridDataSource_DBSharing(t *testing.T) {
	// Test that when Badger is enabled, the badgerDB is shared with tableConfig
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      true,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// After Connect, badgerDB should be set (retrieved from BadgerDataSource.GetDB())
	assert.NotNil(t, ds.badgerDB, "badgerDB should be set after Connect when Badger is enabled")

	// The router should also have the badgerDB
	assert.NotNil(t, ds.router.GetBadgerDB(), "router should have badgerDB")
}

func TestHybridTransaction_MemoryOnly_Commit(t *testing.T) {
	// Test HybridTransaction with memory-only mode (no Badger)
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "txn_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Begin transaction
	txn, err := ds.BeginTransaction(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, txn)

	// Insert within transaction
	rows := []domain.Row{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"},
	}
	inserted, err := txn.Insert(ctx, "txn_test", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	// Commit
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Verify data persisted
	result, err := ds.Query(ctx, "txn_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestHybridTransaction_MemoryOnly_Rollback(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "txn_rollback_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Begin transaction
	txn, err := ds.BeginTransaction(ctx, nil)
	require.NoError(t, err)

	// Rollback should succeed without error
	err = txn.Rollback(ctx)
	require.NoError(t, err)
}

func TestHybridTransaction_WithBadger_Commit(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      true,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table (memory-only by default)
	tableInfo := &domain.TableInfo{
		Name: "txn_badger_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "value", Type: "INT"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Begin hybrid transaction
	txn, err := ds.BeginTransaction(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, txn)

	// Insert within transaction
	rows := []domain.Row{
		{"id": "t1", "value": 42},
	}
	inserted, err := txn.Insert(ctx, "txn_badger_test", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted)

	// Commit
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Verify data
	result, err := ds.Query(ctx, "txn_badger_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestHybridTransaction_Query(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "txn_query_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "label", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Begin transaction
	txn, err := ds.BeginTransaction(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, txn)

	// Insert data within the transaction
	rows := []domain.Row{
		{"id": "q1", "label": "alpha"},
		{"id": "q2", "label": "beta"},
		{"id": "q3", "label": "gamma"},
	}
	inserted, err := txn.Insert(ctx, "txn_query_test", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), inserted)

	// Query within the same transaction -- should see the uncommitted rows
	result, err := txn.Query(ctx, "txn_query_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)

	// Query with a filter within the transaction
	result, err = txn.Query(ctx, "txn_query_test", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: "q2"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "beta", result.Rows[0]["label"])

	// Commit to persist
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Verify via data source query after commit
	result, err = ds.Query(ctx, "txn_query_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestHybridTransaction_InsertUpdateDelete(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "txn_crud_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "value", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Begin transaction
	txn, err := ds.BeginTransaction(ctx, nil)
	require.NoError(t, err)

	// Insert rows
	rows := []domain.Row{
		{"id": "c1", "value": "one"},
		{"id": "c2", "value": "two"},
		{"id": "c3", "value": "three"},
	}
	inserted, err := txn.Insert(ctx, "txn_crud_test", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), inserted)

	// Update a row within the transaction
	updated, err := txn.Update(ctx, "txn_crud_test",
		[]domain.Filter{{Field: "id", Operator: "=", Value: "c2"}},
		domain.Row{"value": "TWO_UPDATED"},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), updated)

	// Delete a row within the transaction
	deleted, err := txn.Delete(ctx, "txn_crud_test",
		[]domain.Filter{{Field: "id", Operator: "=", Value: "c3"}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Commit
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Verify final state via data source
	result, err := ds.Query(ctx, "txn_crud_test", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2, "Should have 2 rows after insert(3), delete(1)")

	// Verify the update took effect
	result, err = ds.Query(ctx, "txn_crud_test", &domain.QueryOptions{
		Filters: []domain.Filter{{Field: "id", Operator: "=", Value: "c2"}},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "TWO_UPDATED", result.Rows[0]["value"])

	// Verify the deleted row is gone
	result, err = ds.Query(ctx, "txn_crud_test", &domain.QueryOptions{
		Filters: []domain.Filter{{Field: "id", Operator: "=", Value: "c3"}},
	})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestHybridDataSource_NotConnected_Errors(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	// Create data source but do NOT call Connect
	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()

	assert.False(t, ds.IsConnected())

	// Query should fail
	_, err := ds.Query(ctx, "any_table", &domain.QueryOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// Insert should fail
	_, err = ds.Insert(ctx, "any_table", []domain.Row{{"id": "1"}}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// Update should fail
	_, err = ds.Update(ctx, "any_table", []domain.Filter{}, domain.Row{"x": "y"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// Delete should fail
	_, err = ds.Delete(ctx, "any_table", []domain.Filter{}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// GetTables should fail
	_, err = ds.GetTables(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// GetTableInfo should fail
	_, err = ds.GetTableInfo(ctx, "any_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// CreateTable should fail
	err = ds.CreateTable(ctx, &domain.TableInfo{Name: "t"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// DropTable should fail
	err = ds.DropTable(ctx, "any_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// TruncateTable should fail
	err = ds.TruncateTable(ctx, "any_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	// BeginTransaction should fail
	_, err = ds.BeginTransaction(ctx, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestHybridDataSource_EnablePersistence(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      true,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Before enabling persistence, table should use default (non-persistent)
	cfg, ok := ds.GetPersistenceConfig("persist_test")
	require.True(t, ok)
	assert.False(t, cfg.Persistent)

	// Enable persistence with options
	err = ds.EnablePersistence(ctx, "persist_test",
		WithSyncOnWrite(true),
		WithCacheInMemory(false),
	)
	require.NoError(t, err)

	// Verify persistence config is now set
	cfg, ok = ds.GetPersistenceConfig("persist_test")
	require.True(t, ok)
	assert.True(t, cfg.Persistent)
	assert.True(t, cfg.SyncOnWrite)
	assert.False(t, cfg.CacheInMemory)

	// Table should appear in persistent tables list
	persistentTables := ds.ListPersistentTables()
	assert.Contains(t, persistentTables, "persist_test")

	// Disable persistence and verify
	err = ds.DisablePersistence(ctx, "persist_test")
	require.NoError(t, err)

	cfg, ok = ds.GetPersistenceConfig("persist_test")
	require.True(t, ok)
	assert.False(t, cfg.Persistent)
}

func TestHybridDataSource_Stats(t *testing.T) {
	config := &HybridDataSourceConfig{
		DataDir:           "",
		DefaultPersistent: false,
		EnableBadger:      false,
	}

	domainCfg := &domain.DataSourceConfig{
		Name:     "test",
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds := NewHybridDataSource(domainCfg, config)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Initial stats should be zero
	stats := ds.Stats()
	assert.Equal(t, int64(0), stats.TotalReads)
	assert.Equal(t, int64(0), stats.TotalWrites)
	assert.Equal(t, int64(0), stats.MemoryReads)
	assert.Equal(t, int64(0), stats.MemoryWrites)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "stats_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "data", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Perform writes (Insert)
	rows := []domain.Row{
		{"id": "s1", "data": "hello"},
		{"id": "s2", "data": "world"},
	}
	_, err = ds.Insert(ctx, "stats_test", rows, nil)
	require.NoError(t, err)

	stats = ds.Stats()
	assert.Equal(t, int64(1), stats.TotalWrites, "Insert should increment TotalWrites")
	assert.Equal(t, int64(1), stats.MemoryWrites, "Insert to memory should increment MemoryWrites")

	// Perform a read (Query)
	_, err = ds.Query(ctx, "stats_test", &domain.QueryOptions{})
	require.NoError(t, err)

	stats = ds.Stats()
	assert.Equal(t, int64(1), stats.TotalReads, "Query should increment TotalReads")
	assert.Equal(t, int64(1), stats.MemoryReads, "Query from memory should increment MemoryReads")

	// Perform another write (Update)
	_, err = ds.Update(ctx, "stats_test",
		[]domain.Filter{{Field: "id", Operator: "=", Value: "s1"}},
		domain.Row{"data": "updated"},
		nil,
	)
	require.NoError(t, err)

	stats = ds.Stats()
	assert.Equal(t, int64(2), stats.TotalWrites, "Update should increment TotalWrites")
	assert.Equal(t, int64(2), stats.MemoryWrites, "Update to memory should increment MemoryWrites")

	// Perform another write (Delete)
	_, err = ds.Delete(ctx, "stats_test",
		[]domain.Filter{{Field: "id", Operator: "=", Value: "s2"}},
		nil,
	)
	require.NoError(t, err)

	stats = ds.Stats()
	assert.Equal(t, int64(3), stats.TotalWrites, "Delete should increment TotalWrites")
	assert.Equal(t, int64(3), stats.MemoryWrites, "Delete to memory should increment MemoryWrites")

	// Perform another read
	_, err = ds.Query(ctx, "stats_test", &domain.QueryOptions{})
	require.NoError(t, err)

	stats = ds.Stats()
	assert.Equal(t, int64(2), stats.TotalReads, "Second query should increment TotalReads")
	assert.Equal(t, int64(2), stats.MemoryReads, "Second query should increment MemoryReads")
}
