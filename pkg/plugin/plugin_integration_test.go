//go:build windows

package plugin_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/plugin"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sharedDLLPath holds the path to the pre-built demo plugin DLL.
// Built once in TestMain and reused across all tests.
var sharedDLLPath string

func TestMain(m *testing.M) {
	projectRoot := findProjectRoot()
	if projectRoot == "" {
		fmt.Fprintln(os.Stderr, "could not find project root (go.mod)")
		os.Exit(1)
	}

	// Build demo plugin DLL once
	tmpDir, err := os.MkdirTemp("", "plugin_test_dll_*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	sharedDLLPath = filepath.Join(tmpDir, "demo_plugin.dll")
	pluginSrc := filepath.Join(projectRoot, "examples", "demo_plugin")

	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", sharedDLLPath, pluginSrc)
	cmd.Dir = projectRoot
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build demo plugin DLL:\n%s\n%v\n", string(output), err)
		os.Exit(1)
	}

	code := m.Run()

	// Best-effort cleanup (DLL may be locked if still loaded)
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// findProjectRoot walks up from the current file to find the project root (containing go.mod).
func findProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// TestDemoPlugin_LoadAndCRUD is the main integration test that verifies the full
// plugin lifecycle: load → register → create datasource → CRUD → close.
func TestDemoPlugin_LoadAndCRUD(t *testing.T) {
	require.NotEmpty(t, sharedDLLPath, "DLL not built")

	// Set up fresh registry and manager
	registry := application.NewRegistry()
	dsManager := application.NewDataSourceManagerWithRegistry(registry)
	pluginMgr := plugin.NewPluginManager(registry, dsManager, "")

	// ── Load plugin ──
	err := pluginMgr.LoadPlugin(sharedDLLPath)
	require.NoError(t, err, "LoadPlugin should succeed")

	// Verify plugin info
	plugins := pluginMgr.GetLoadedPlugins()
	require.Len(t, plugins, 1)
	assert.Equal(t, domain.DataSourceType("demo"), plugins[0].Type)
	assert.Equal(t, "1.0.0", plugins[0].Version)
	assert.Contains(t, plugins[0].Description, "Demo")

	// ── Create datasource via registry ──
	ds, err := registry.Create(&domain.DataSourceConfig{
		Type:     "demo",
		Name:     "test_ds",
		Writable: true,
	})
	require.NoError(t, err, "registry.Create should succeed for demo type")

	ctx := context.Background()

	// ── Connect ──
	err = ds.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, ds.IsConnected())
	assert.True(t, ds.IsWritable())

	// ── CreateTable ──
	err = ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Nullable: false, Primary: true},
			{Name: "name", Type: "varchar(100)", Nullable: false},
			{Name: "email", Type: "varchar(255)", Nullable: true},
		},
	})
	require.NoError(t, err)

	// ── GetTables ──
	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "users")

	// ── GetTableInfo ──
	info, err := ds.GetTableInfo(ctx, "users")
	require.NoError(t, err)
	assert.Equal(t, "users", info.Name)
	assert.Len(t, info.Columns, 3)

	// ── Insert ──
	affected, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": 1, "name": "Alice", "email": "alice@example.com"},
		{"id": 2, "name": "Bob", "email": "bob@example.com"},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)

	// ── Query all ──
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
	assert.Len(t, result.Rows, 2)

	// ── Query with filter ──
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "name", Operator: "=", Value: "Alice"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Total)
	assert.Len(t, result.Rows, 1)

	// ── Update ──
	affected, err = ds.Update(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "Alice"}},
		domain.Row{"email": "alice_updated@example.com"},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Verify update took effect
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "name", Operator: "=", Value: "Alice"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "alice_updated@example.com", result.Rows[0]["email"])

	// ── Delete ──
	affected, err = ds.Delete(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "Bob"}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Verify delete took effect
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Total)

	// ── TruncateTable ──
	err = ds.TruncateTable(ctx, "users")
	require.NoError(t, err)

	result, err = ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Total)

	// ── DropTable ──
	err = ds.DropTable(ctx, "users")
	require.NoError(t, err)

	tables, err = ds.GetTables(ctx)
	require.NoError(t, err)
	assert.NotContains(t, tables, "users")

	// ── Execute ──
	result, err = ds.Execute(ctx, "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Total)

	// ── Close ──
	err = ds.Close(ctx)
	require.NoError(t, err)
	assert.False(t, ds.IsConnected())
}

// TestDemoPlugin_ScanAndLoad tests the ScanAndLoad directory scanning.
func TestDemoPlugin_ScanAndLoad(t *testing.T) {
	require.NotEmpty(t, sharedDLLPath, "DLL not built")

	// Copy DLL to a scan directory (use os.MkdirTemp to avoid t.TempDir cleanup issues)
	scanDir, err := os.MkdirTemp("", "plugin_scan_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(scanDir) // best-effort; may fail if DLL is loaded

	dllDest := filepath.Join(scanDir, "demo_plugin.dll")
	data, err := os.ReadFile(sharedDLLPath)
	require.NoError(t, err)
	err = os.WriteFile(dllDest, data, 0644)
	require.NoError(t, err)

	// Set up and scan
	registry := application.NewRegistry()
	dsManager := application.NewDataSourceManagerWithRegistry(registry)
	pluginMgr := plugin.NewPluginManager(registry, dsManager, "")

	err = pluginMgr.ScanAndLoad(scanDir)
	require.NoError(t, err)

	plugins := pluginMgr.GetLoadedPlugins()
	require.Len(t, plugins, 1)
	assert.Equal(t, domain.DataSourceType("demo"), plugins[0].Type)

	// Verify factory was registered: we can create a datasource
	ds, err := registry.Create(&domain.DataSourceConfig{
		Type: "demo",
		Name: "scan_test",
	})
	require.NoError(t, err)
	require.NotNil(t, ds)
}

// TestDemoPlugin_ScanAndLoad_EmptyDir tests scanning a directory with no DLLs.
func TestDemoPlugin_ScanAndLoad_EmptyDir(t *testing.T) {
	emptyDir := t.TempDir()

	registry := application.NewRegistry()
	dsManager := application.NewDataSourceManagerWithRegistry(registry)
	pluginMgr := plugin.NewPluginManager(registry, dsManager, "")

	err := pluginMgr.ScanAndLoad(emptyDir)
	assert.NoError(t, err)
	assert.Empty(t, pluginMgr.GetLoadedPlugins())
}

// TestDemoPlugin_ScanAndLoad_NonExistentDir tests scanning a non-existent directory.
func TestDemoPlugin_ScanAndLoad_NonExistentDir(t *testing.T) {
	registry := application.NewRegistry()
	dsManager := application.NewDataSourceManagerWithRegistry(registry)
	pluginMgr := plugin.NewPluginManager(registry, dsManager, "")

	err := pluginMgr.ScanAndLoad(filepath.Join(t.TempDir(), "does_not_exist"))
	assert.NoError(t, err) // should not error, just skip
	assert.Empty(t, pluginMgr.GetLoadedPlugins())
}

// TestDemoPlugin_MultipleInstances tests creating multiple datasource instances from the same plugin.
func TestDemoPlugin_MultipleInstances(t *testing.T) {
	require.NotEmpty(t, sharedDLLPath, "DLL not built")

	registry := application.NewRegistry()
	dsManager := application.NewDataSourceManagerWithRegistry(registry)
	pluginMgr := plugin.NewPluginManager(registry, dsManager, "")

	err := pluginMgr.LoadPlugin(sharedDLLPath)
	require.NoError(t, err)

	ctx := context.Background()

	// Create two independent instances
	ds1, err := registry.Create(&domain.DataSourceConfig{Type: "demo", Name: "inst1", Writable: true})
	require.NoError(t, err)
	ds2, err := registry.Create(&domain.DataSourceConfig{Type: "demo", Name: "inst2", Writable: true})
	require.NoError(t, err)

	require.NoError(t, ds1.Connect(ctx))
	require.NoError(t, ds2.Connect(ctx))

	// Create table in ds1 only
	require.NoError(t, ds1.CreateTable(ctx, &domain.TableInfo{
		Name:    "t1",
		Columns: []domain.ColumnInfo{{Name: "val", Type: "text"}},
	}))

	// ds1 should have the table
	tables1, err := ds1.GetTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables1, "t1")

	// ds2 should NOT have the table (independent instances)
	tables2, err := ds2.GetTables(ctx)
	require.NoError(t, err)
	assert.NotContains(t, tables2, "t1")

	require.NoError(t, ds1.Close(ctx))
	require.NoError(t, ds2.Close(ctx))
}
