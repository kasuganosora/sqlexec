package parquet

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestParquetAdapter_SupportsWrite tests write support flag.
func TestParquetAdapter_SupportsWrite(t *testing.T) {
	dir := t.TempDir()

	writable := NewParquetAdapter(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     filepath.Join(dir, "w"),
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	})
	if !writable.SupportsWrite() {
		t.Error("Expected SupportsWrite() = true")
	}

	readonly := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: filepath.Join(dir, "r"),
	})
	if readonly.SupportsWrite() {
		t.Error("Expected SupportsWrite() = false")
	}
}

// TestParquetFactory tests the factory.
func TestParquetFactory(t *testing.T) {
	factory := NewParquetFactory()
	if factory.GetType() != domain.DataSourceTypeParquet {
		t.Errorf("GetType() = %v, want Parquet", factory.GetType())
	}

	config := &domain.DataSourceConfig{
		Name:     filepath.Join(t.TempDir(), "factory_test"),
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}

	ds, err := factory.Create(config)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if ds == nil {
		t.Fatal("Create() returned nil")
	}
}

// TestParquetAdapter_DirectoryMode_DDL tests CreateTable/DropTable/TruncateTable.
func TestParquetAdapter_DirectoryMode_DDL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ddltest")

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     dir,
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}

	adapter := NewParquetAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer adapter.Close(ctx)

	// CreateTable
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "age", Type: "int64", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Verify table exists
	tables, _ := adapter.GetTables(ctx)
	found := false
	for _, table := range tables {
		if table == "users" {
			found = true
		}
	}
	if !found {
		t.Error("Table 'users' not found after CreateTable")
	}

	// Insert data
	rows := []domain.Row{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
	}
	n, err := adapter.Insert(ctx, "users", rows, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if n != 2 {
		t.Errorf("Expected 2 inserted, got %d", n)
	}

	// Query
	result, err := adapter.Query(ctx, "users", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// TruncateTable
	if err := adapter.TruncateTable(ctx, "users"); err != nil {
		t.Fatalf("TruncateTable: %v", err)
	}

	result, _ = adapter.Query(ctx, "users", &domain.QueryOptions{})
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after truncate, got %d", len(result.Rows))
	}

	// DropTable
	if err := adapter.DropTable(ctx, "users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	tables, _ = adapter.GetTables(ctx)
	for _, table := range tables {
		if table == "users" {
			t.Error("Table 'users' should not exist after DropTable")
		}
	}
}

// TestParquetAdapter_DirectoryMode_Persistence tests data persistence across reconnections.
func TestParquetAdapter_DirectoryMode_Persistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "persist")

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     dir,
		Writable: true,
		Options:  map[string]interface{}{"writable": true, "compression": "none"},
	}
	ctx := context.Background()

	// Phase 1: Create table and insert data
	adapter := NewParquetAdapter(config)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	tableInfo := &domain.TableInfo{
		Name: "items",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "price", Type: "float64", Nullable: true},
		},
	}
	adapter.CreateTable(ctx, tableInfo)

	rows := []domain.Row{
		{"id": int64(1), "name": "Widget", "price": float64(9.99)},
		{"id": int64(2), "name": "Gadget", "price": float64(19.99)},
		{"id": int64(3), "name": "Doohickey", "price": float64(4.99)},
	}
	adapter.Insert(ctx, "items", rows, nil)

	// Close triggers flush
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: Reconnect and verify data persisted
	adapter2 := NewParquetAdapter(config)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("Re-Connect: %v", err)
	}
	defer adapter2.Close(ctx)

	result, err := adapter2.Query(ctx, "items", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query after reconnect: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows after reconnect, got %d", len(result.Rows))
	}

	// Verify data integrity
	found := false
	for _, row := range result.Rows {
		if row["name"] == "Widget" {
			found = true
			if price, ok := row["price"].(float64); !ok || price != 9.99 {
				t.Errorf("Expected price=9.99, got %v (%T)", row["price"], row["price"])
			}
		}
	}
	if !found {
		t.Error("Widget not found after reconnect")
	}
}

// TestParquetAdapter_DirectoryMode_MultiTable tests multiple tables.
func TestParquetAdapter_DirectoryMode_MultiTable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "multitable")

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     dir,
		Writable: true,
		Options:  map[string]interface{}{"writable": true, "compression": "none"},
	}
	ctx := context.Background()

	adapter := NewParquetAdapter(config)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Create two tables
	adapter.CreateTable(ctx, &domain.TableInfo{
		Name: "table_a",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "data", Type: "string", Nullable: true},
		},
	})
	adapter.CreateTable(ctx, &domain.TableInfo{
		Name: "table_b",
		Columns: []domain.ColumnInfo{
			{Name: "key", Type: "string"},
			{Name: "val", Type: "int64", Nullable: true},
		},
	})

	// Insert into both
	adapter.Insert(ctx, "table_a", []domain.Row{{"id": int64(1), "data": "hello"}}, nil)
	adapter.Insert(ctx, "table_b", []domain.Row{{"key": "x", "val": int64(42)}}, nil)

	// Verify both exist
	tables, _ := adapter.GetTables(ctx)
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}

	// Close and reconnect
	adapter.Close(ctx)

	adapter2 := NewParquetAdapter(config)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("Re-Connect: %v", err)
	}
	defer adapter2.Close(ctx)

	tables2, _ := adapter2.GetTables(ctx)
	if len(tables2) != 2 {
		t.Errorf("Expected 2 tables after reconnect, got %d", len(tables2))
	}

	resultA, _ := adapter2.Query(ctx, "table_a", &domain.QueryOptions{})
	if len(resultA.Rows) != 1 {
		t.Errorf("Expected 1 row in table_a, got %d", len(resultA.Rows))
	}

	resultB, _ := adapter2.Query(ctx, "table_b", &domain.QueryOptions{})
	if len(resultB.Rows) != 1 {
		t.Errorf("Expected 1 row in table_b, got %d", len(resultB.Rows))
	}
}
