package parquet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestConnect_PAR1MagicBytes verifies that a file with real Parquet magic bytes
// is rejected with a descriptive error.
func TestConnect_PAR1MagicBytes(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "real.parquet")

	// Write a file starting with PAR1 magic bytes.
	if err := os.WriteFile(testFile, []byte("PAR1some-binary-data"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_par1",
	}, testFile)

	err := adapter.Connect(context.Background())
	if err == nil {
		t.Fatal("Expected error for PAR1 file, got nil")
	}
	if got := err.Error(); !contains(got, "PAR1 magic bytes") {
		t.Errorf("Error message should mention PAR1 magic bytes, got: %s", got)
	}
}

// TestConnect_StructuredJSON verifies that the structured JSON interchange
// format (with "columns" and "rows") is correctly read.
func TestConnect_StructuredJSON(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "structured.parquet")

	data := parquetSerializedData{
		TableName: "my_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "score", Type: "float64", Nullable: true},
		},
		Rows: []domain.Row{
			{"id": float64(1), "name": "Alice", "score": 95.5},
			{"id": float64(2), "name": "Bob", "score": 87.0},
		},
	}
	raw, _ := json.Marshal(data)
	if err := os.WriteFile(testFile, raw, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type:    domain.DataSourceTypeParquet,
		Name:    "test_structured",
		Options: map[string]interface{}{"table_name": "my_table"},
	}, testFile)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Verify table info
	info, err := adapter.GetTableInfo(ctx, "my_table")
	if err != nil {
		t.Fatalf("GetTableInfo: %v", err)
	}
	if len(info.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(info.Columns))
	}

	// Verify row data
	result, err := adapter.Query(ctx, "my_table", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Verify int64 normalisation (JSON float64 -> int64 for int64 columns)
	if id, ok := result.Rows[0]["id"].(int64); !ok || id != 1 {
		t.Errorf("Expected id=1 (int64), got %v (%T)", result.Rows[0]["id"], result.Rows[0]["id"])
	}

	adapter.Close(ctx)
}

// TestConnect_PlainJSONArray verifies that a plain JSON array of objects is
// correctly loaded with columns inferred from the first row.
func TestConnect_PlainJSONArray(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "array.parquet")

	raw := `[
		{"id": 10, "city": "Tokyo"},
		{"id": 20, "city": "Berlin"}
	]`
	if err := os.WriteFile(testFile, []byte(raw), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_array",
	}, testFile)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	result, err := adapter.Query(ctx, "parquet_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	adapter.Close(ctx)
}

// TestConnect_EmptyFile verifies that an empty file yields a default schema
// and zero rows.
func TestConnect_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty.parquet")

	if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_empty",
	}, testFile)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	result, err := adapter.Query(ctx, "parquet_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows from empty file, got %d", len(result.Rows))
	}

	adapter.Close(ctx)
}

// TestConnect_InvalidJSON verifies that an unrecognised file format falls back
// to a default schema with zero rows.
func TestConnect_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "garbage.parquet")

	if err := os.WriteFile(testFile, []byte("this is not json"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_invalid",
	}, testFile)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v (should fall back gracefully)", err)
	}

	result, err := adapter.Query(ctx, "parquet_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows from invalid file, got %d", len(result.Rows))
	}

	adapter.Close(ctx)
}

// TestWriteBack_RoundTrip verifies the full write-read cycle: connect, insert,
// writeBack, reconnect, and verify data.
func TestWriteBack_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "roundtrip.parquet")

	// Start with an empty file.
	if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     "test_roundtrip",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}

	ctx := context.Background()

	// Phase 1: Insert data and write back.
	adapter := NewParquetAdapter(config, testFile)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	rows := []domain.Row{
		{"id": int64(1), "value": "first"},
		{"id": int64(2), "value": "second"},
	}
	if _, err := adapter.Insert(ctx, "parquet_data", rows, nil); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Close triggers writeBack for writable adapters.
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: Re-open and verify.
	adapter2 := NewParquetAdapter(config, testFile)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("Re-Connect: %v", err)
	}

	result, err := adapter2.Query(ctx, "parquet_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows after round-trip, got %d", len(result.Rows))
	}

	// Verify values.
	found := false
	for _, row := range result.Rows {
		if row["value"] == "first" {
			found = true
		}
	}
	if !found {
		t.Error("Expected to find row with value='first' after round-trip")
	}

	adapter2.Close(ctx)
}

// TestWriteBack_FileContent verifies the JSON structure written by writeBack.
func TestWriteBack_FileContent(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "content.parquet")

	initialData := `{"table_name":"t","columns":[{"name":"x","type":"int64"}],"rows":[{"x":42}]}`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:    domain.DataSourceTypeParquet,
		Name:    "test_content",
		Options: map[string]interface{}{"writable": true, "table_name": "t"},
	}
	adapter := NewParquetAdapter(config, testFile)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack: %v", err)
	}

	// Read and parse the written file.
	written, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var out parquetSerializedData
	if err := json.Unmarshal(written, &out); err != nil {
		t.Fatalf("Unmarshal written data: %v", err)
	}

	if out.TableName != "t" {
		t.Errorf("Expected table_name='t', got %q", out.TableName)
	}
	if len(out.Columns) != 1 || out.Columns[0].Name != "x" {
		t.Errorf("Unexpected columns: %+v", out.Columns)
	}
	if len(out.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(out.Rows))
	}

	adapter.Close(ctx)
}

// TestNormaliseRows verifies that float64 values in int64 columns are properly
// converted.
func TestNormaliseRows(t *testing.T) {
	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int64"},
		{Name: "name", Type: "string"},
	}
	rows := []domain.Row{
		{"id": float64(42), "name": "Alice"},
		{"id": float64(99), "name": "Bob"},
	}

	result := normaliseRows(rows, columns)

	for _, row := range result {
		if _, ok := row["id"].(int64); !ok {
			t.Errorf("Expected id to be int64, got %T", row["id"])
		}
	}
}

// TestConnect_StructuredJSON_TypeNormalization verifies that JSON float64
// values in int64 columns are correctly normalized to int64 after Connect.
func TestConnect_StructuredJSON_TypeNormalization(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "typed.parquet")

	data := parquetSerializedData{
		TableName: "typed_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "count", Type: "int64", Nullable: true},
			{Name: "ratio", Type: "float64", Nullable: true},
			{Name: "label", Type: "string", Nullable: true},
		},
		Rows: []domain.Row{
			{"id": float64(10), "count": float64(100), "ratio": 3.14, "label": "first"},
			{"id": float64(20), "count": float64(200), "ratio": 2.71, "label": "second"},
		},
	}
	raw, _ := json.Marshal(data)
	if err := os.WriteFile(testFile, raw, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type:    domain.DataSourceTypeParquet,
		Name:    "test_type_norm",
		Options: map[string]interface{}{"table_name": "typed_table"},
	}, testFile)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer adapter.Close(ctx)

	result, err := adapter.Query(ctx, "typed_table", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result.Rows))
	}

	// int64 columns should be normalized from float64 to int64
	for i, row := range result.Rows {
		if _, ok := row["id"].(int64); !ok {
			t.Errorf("Row %d: expected id to be int64, got %T (%v)", i, row["id"], row["id"])
		}
		if _, ok := row["count"].(int64); !ok {
			t.Errorf("Row %d: expected count to be int64, got %T (%v)", i, row["count"], row["count"])
		}
		// float64 column should remain float64
		if _, ok := row["ratio"].(float64); !ok {
			t.Errorf("Row %d: expected ratio to be float64, got %T (%v)", i, row["ratio"], row["ratio"])
		}
		// string column should remain string
		if _, ok := row["label"].(string); !ok {
			t.Errorf("Row %d: expected label to be string, got %T (%v)", i, row["label"], row["label"])
		}
	}
}

// TestConnect_PlainJSONArray_TypeInference verifies that a plain JSON array
// correctly infers column types from the values in the first row.
func TestConnect_PlainJSONArray_TypeInference(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "infer.parquet")

	// JSON: bool, integer float64, float64 with decimals, string, null
	raw := `[
		{"active": true, "count": 42, "score": 3.14, "name": "test", "extra": null},
		{"active": false, "count": 7, "score": 2.0, "name": "other", "extra": null}
	]`
	if err := os.WriteFile(testFile, []byte(raw), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_infer",
	}, testFile)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer adapter.Close(ctx)

	info, err := adapter.GetTableInfo(ctx, "parquet_data")
	if err != nil {
		t.Fatalf("GetTableInfo: %v", err)
	}

	// Build a type map for easy lookup
	typeMap := make(map[string]string)
	for _, col := range info.Columns {
		typeMap[col.Name] = col.Type
	}

	// Verify inferred types
	if typeMap["active"] != "bool" {
		t.Errorf("Expected 'active' type to be 'bool', got %q", typeMap["active"])
	}
	if typeMap["count"] != "int64" {
		t.Errorf("Expected 'count' type to be 'int64', got %q", typeMap["count"])
	}
	if typeMap["score"] != "float64" {
		t.Errorf("Expected 'score' type to be 'float64', got %q", typeMap["score"])
	}
	if typeMap["name"] != "string" {
		t.Errorf("Expected 'name' type to be 'string', got %q", typeMap["name"])
	}
	// null values should be inferred as "string" (default)
	if typeMap["extra"] != "string" {
		t.Errorf("Expected 'extra' (null) type to be 'string', got %q", typeMap["extra"])
	}
}

// TestParquetAdapter_RoundTrip_MultipleTypes verifies a full round-trip
// (insert -> writeBack -> reconnect -> query) with various data types.
func TestParquetAdapter_RoundTrip_MultipleTypes(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "multi_type.parquet")

	// Start with structured JSON defining columns of multiple types
	data := parquetSerializedData{
		TableName: "multi",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "score", Type: "float64", Nullable: true},
			{Name: "label", Type: "string", Nullable: true},
			{Name: "active", Type: "bool", Nullable: true},
		},
		Rows: []domain.Row{},
	}
	raw, _ := json.Marshal(data)
	if err := os.WriteFile(testFile, raw, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     "test_multi_rt",
		Writable: true,
		Options:  map[string]interface{}{"writable": true, "table_name": "multi"},
	}

	ctx := context.Background()

	// Phase 1: Insert data and close (triggers writeBack)
	adapter := NewParquetAdapter(config, testFile)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	rows := []domain.Row{
		{"id": int64(1), "score": 95.5, "label": "alpha", "active": true},
		{"id": int64(2), "score": 87.3, "label": "beta", "active": false},
		{"id": int64(3), "score": 0.0, "label": "gamma", "active": true},
	}
	n, err := adapter.Insert(ctx, "multi", rows, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if n != 3 {
		t.Fatalf("Expected 3 inserted, got %d", n)
	}
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: Reconnect and verify all types survived the round-trip
	adapter2 := NewParquetAdapter(config, testFile)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("Re-Connect: %v", err)
	}
	defer adapter2.Close(ctx)

	result, err := adapter2.Query(ctx, "multi", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 rows after round-trip, got %d", len(result.Rows))
	}

	// Verify that int64 column survived
	foundAlpha := false
	for _, row := range result.Rows {
		if row["label"] == "alpha" {
			foundAlpha = true
			if _, ok := row["id"].(int64); !ok {
				t.Errorf("Expected id to be int64 after round-trip, got %T", row["id"])
			}
			if _, ok := row["score"].(float64); !ok {
				t.Errorf("Expected score to be float64 after round-trip, got %T", row["score"])
			}
		}
	}
	if !foundAlpha {
		t.Error("Expected to find row with label='alpha' after round-trip")
	}
}

// TestParquetAdapter_WriteBack_ReadOnly verifies that Close() on a read-only
// adapter does NOT write back to file.
func TestParquetAdapter_WriteBack_ReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "readonly.parquet")

	originalData := parquetSerializedData{
		TableName: "ro_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "value", Type: "string", Nullable: true},
		},
		Rows: []domain.Row{
			{"id": float64(1), "value": "original"},
		},
	}
	raw, _ := json.Marshal(originalData)
	if err := os.WriteFile(testFile, raw, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Record original file content
	origContent, _ := os.ReadFile(testFile)

	// Open as read-only (writable=false, which is the default when not set)
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     "test_readonly",
		Writable: false,
		Options:  map[string]interface{}{"table_name": "ro_table"},
	}

	adapter := NewParquetAdapter(config, testFile)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Close the read-only adapter
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify the file has not been modified
	afterContent, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile after close: %v", err)
	}

	if string(origContent) != string(afterContent) {
		t.Error("File content should not have changed for a read-only adapter on Close")
	}
}

// contains is a test helper that checks if s contains substr.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
