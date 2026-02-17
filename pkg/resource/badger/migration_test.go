package badger

import (
	"bytes"
	"context"
	"os"
	"testing"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationManager_ExportImport(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create source data source
	config := &domain.DataSourceConfig{
		Name: "source",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)
	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table and insert data
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	rows := []domain.Row{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"},
	}
	_, err = ds.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)

	// Create migration manager
	mgr := NewMigrationManager(ds)

	// Export to buffer
	var buf bytes.Buffer
	err = mgr.ExportData(ctx, &buf, &ExportConfig{
		IncludeSchema: true,
		IncludeData:   true,
	})
	require.NoError(t, err)
	assert.True(t, buf.Len() > 0)

	// Create destination data source
	destConfig := &domain.DataSourceConfig{
		Name: "dest",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	destDS := NewBadgerDataSource(destConfig)
	err = destDS.Connect(ctx)
	require.NoError(t, err)
	defer destDS.Close(ctx)

	// Import from buffer
	destMgr := NewMigrationManager(destDS)
	err = destMgr.ImportData(ctx, &buf, &ImportConfig{Mode: "create"})
	require.NoError(t, err)

	// Verify import
	result, err := destDS.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestMigrationManager_ExportToFile(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)
	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Create migration manager
	mgr := NewMigrationManager(ds)

	// Export to file
	filePath := tmpDir + "/export.json"
	err = mgr.ExportToFile(ctx, filePath, DefaultExportConfig())
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(filePath)
	require.NoError(t, err)
}

func TestMaintenanceManager_GetStats(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)
	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "test_table",
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
	_, err = ds.Insert(ctx, "test_table", rows, nil)
	require.NoError(t, err)

	// Create maintenance manager
	mgr := NewMaintenanceManager(ds)

	// Get stats
	stats, err := mgr.GetStats()
	require.NoError(t, err)
	assert.Equal(t, 1, stats.TableCount)
	// KeyCount includes table metadata + row data, so >= 3
	assert.GreaterOrEqual(t, stats.KeyCount, int64(3))
}

func TestMaintenanceManager_RunGC(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source with persistent mode (GC not supported in memory mode)
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"data_dir":  tmpDir,
			"in_memory": false,
		},
	}

	ds := NewBadgerDataSource(config)
	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create maintenance manager
	mgr := NewMaintenanceManager(ds)

	// Run GC (should succeed even if there's nothing to collect)
	err = mgr.RunGC(0.5)
	// GC might return nil or ErrNoRewrite, both are acceptable
	if err != nil {
		// ErrNoRewrite is expected when there's nothing to GC
		assert.Contains(t, err.Error(), "no rewrite")
	}
}

func TestMaintenanceManager_VerifyIntegrity(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)
	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "integrity_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Create maintenance manager
	mgr := NewMaintenanceManager(ds)

	// Verify integrity
	err = mgr.VerifyIntegrity(ctx)
	assert.NoError(t, err)
}
