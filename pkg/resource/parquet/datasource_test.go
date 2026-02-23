package parquet

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewParquetAdapter tests creating a Parquet datasource.
func TestNewParquetAdapter(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: filepath.Join(t.TempDir(), "testdir"),
	}

	ps := NewParquetAdapter(config)
	if ps == nil {
		t.Fatal("NewParquetAdapter() returned nil")
	}
	if ps.dataDir != config.Name {
		t.Errorf("dataDir = %v, want %v", ps.dataDir, config.Name)
	}
}

// TestParquetSource_GetConfig tests configuration retrieval.
func TestParquetSource_GetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: filepath.Join(t.TempDir(), "testdir"),
	}

	ps := NewParquetAdapter(config)
	got := ps.GetConfig()
	if got == nil {
		t.Fatal("GetConfig() returned nil")
	}
	if got.Type != config.Type {
		t.Errorf("GetConfig().Type = %v, want %v", got.Type, config.Type)
	}
}

// TestParquetSource_IsWritable tests writable flag.
func TestParquetSource_IsWritable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: filepath.Join(t.TempDir(), "testdir"),
	}
	ps := NewParquetAdapter(config)
	if ps.IsWritable() {
		t.Error("Expected IsWritable() to return false by default")
	}

	config2 := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     filepath.Join(t.TempDir(), "testdir2"),
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	ps2 := NewParquetAdapter(config2)
	if !ps2.IsWritable() {
		t.Error("Expected IsWritable() to return true when writable=true")
	}
}

// TestParquetSource_ReadOnly_Operations tests that write operations fail on read-only adapter.
func TestParquetSource_ReadOnly_Operations(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "readonly")

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: dir,
	}

	ps := NewParquetAdapter(config)
	ctx := context.Background()

	if err := ps.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer ps.Close(ctx)

	// Insert should fail
	_, err := ps.Insert(ctx, "test", []domain.Row{{"id": int64(1)}}, nil)
	if err == nil {
		t.Error("Expected error for Insert on read-only adapter")
	}

	// Update should fail
	_, err = ps.Update(ctx, "test", nil, domain.Row{"id": int64(1)}, nil)
	if err == nil {
		t.Error("Expected error for Update on read-only adapter")
	}

	// Delete should fail
	_, err = ps.Delete(ctx, "test", nil, nil)
	if err == nil {
		t.Error("Expected error for Delete on read-only adapter")
	}

	// CreateTable should fail
	err = ps.CreateTable(ctx, &domain.TableInfo{Name: "new"})
	if err == nil {
		t.Error("Expected error for CreateTable on read-only adapter")
	}

	// DropTable should fail
	err = ps.DropTable(ctx, "test")
	if err == nil {
		t.Error("Expected error for DropTable on read-only adapter")
	}

	// TruncateTable should fail
	err = ps.TruncateTable(ctx, "test")
	if err == nil {
		t.Error("Expected error for TruncateTable on read-only adapter")
	}
}

// TestParquetSource_Execute tests that Execute is unsupported.
func TestParquetSource_Execute(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: filepath.Join(t.TempDir(), "testdir"),
	}
	ps := NewParquetAdapter(config)

	_, err := ps.Execute(context.Background(), "SELECT 1")
	if err == nil {
		t.Error("Expected error for Execute")
	}
}

// TestParquetSource_Connect_Disconnected tests operations before Connect.
func TestParquetSource_Connect_Disconnected(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: filepath.Join(t.TempDir(), "testdir"),
	}
	ps := NewParquetAdapter(config)
	ctx := context.Background()

	_, err := ps.Query(ctx, "test", &domain.QueryOptions{})
	if err == nil {
		t.Error("Expected error when querying while disconnected")
	}

	_, err = ps.GetTables(ctx)
	if err == nil {
		t.Error("Expected error when getting tables while disconnected")
	}
}

// TestParquetSource_ConnectEmpty tests connecting to an empty directory.
func TestParquetSource_ConnectEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "emptydb")

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     dir,
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}

	ps := NewParquetAdapter(config)
	ctx := context.Background()

	if err := ps.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	if !ps.IsConnected() {
		t.Error("Expected connected")
	}

	tables, err := ps.GetTables(ctx)
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables in empty directory, got %d", len(tables))
	}

	if err := ps.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	if ps.IsConnected() {
		t.Error("Expected disconnected after Close()")
	}
}
