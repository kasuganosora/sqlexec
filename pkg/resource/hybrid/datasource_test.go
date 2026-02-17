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
