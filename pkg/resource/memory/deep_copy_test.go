package memory

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestDeepCopyRow_SliceIsolation verifies that deepCopyRow truly deep-copies
// slice values so that mutating the copy does not affect the original.
func TestDeepCopyRow_SliceIsolation(t *testing.T) {
	original := domain.Row{
		"name":   "alice",
		"tags":   []string{"a", "b", "c"},
		"data":   []byte{1, 2, 3},
		"vector": []float32{0.1, 0.2, 0.3},
	}

	copied := deepCopyRow(original)

	// Mutate the copy's slice values
	copied["tags"].([]string)[0] = "MUTATED"
	copied["data"].([]byte)[0] = 99
	copied["vector"].([]float32)[0] = 999.0

	// Original must remain unchanged
	if original["tags"].([]string)[0] != "a" {
		t.Errorf("deepCopyRow failed: original tags[0] was mutated to %v", original["tags"].([]string)[0])
	}
	if original["data"].([]byte)[0] != 1 {
		t.Errorf("deepCopyRow failed: original data[0] was mutated to %v", original["data"].([]byte)[0])
	}
	if original["vector"].([]float32)[0] != 0.1 {
		t.Errorf("deepCopyRow failed: original vector[0] was mutated to %v", original["vector"].([]float32)[0])
	}
}

// TestDeepCopyRow_MapIsolation verifies that nested maps are deep-copied.
func TestDeepCopyRow_MapIsolation(t *testing.T) {
	nested := map[string]interface{}{
		"inner_key": "inner_val",
	}
	original := domain.Row{
		"nested": nested,
	}

	copied := deepCopyRow(original)

	// Mutate the copy's nested map
	copied["nested"].(map[string]interface{})["inner_key"] = "MUTATED"

	// Original must remain unchanged
	if original["nested"].(map[string]interface{})["inner_key"] != "inner_val" {
		t.Errorf("deepCopyRow failed: original nested map was mutated")
	}
}

// TestDeepCopyRow_InterfaceSliceIsolation verifies that []interface{} slices
// are deep-copied recursively.
func TestDeepCopyRow_InterfaceSliceIsolation(t *testing.T) {
	original := domain.Row{
		"items": []interface{}{"a", int64(1), map[string]interface{}{"k": "v"}},
	}

	copied := deepCopyRow(original)

	// Mutate a nested map inside the []interface{}
	items := copied["items"].([]interface{})
	items[2].(map[string]interface{})["k"] = "MUTATED"

	// Original must remain unchanged
	origItems := original["items"].([]interface{})
	if origItems[2].(map[string]interface{})["k"] != "v" {
		t.Errorf("deepCopyRow failed: nested map inside []interface{} was mutated")
	}
}

// TestDeepCopySchema_PointerAndSliceIsolation verifies that deepCopySchema
// deep-copies pointer fields (ForeignKeyInfo) and slices (GeneratedDepends).
func TestDeepCopySchema_PointerAndSliceIsolation(t *testing.T) {
	original := &domain.TableInfo{
		Name: "test",
		Columns: []domain.ColumnInfo{
			{
				Name: "id",
				ForeignKey: &domain.ForeignKeyInfo{
					Table:  "other",
					Column: "oid",
				},
				GeneratedDepends: []string{"col_a", "col_b"},
			},
		},
	}

	copied := deepCopySchema(original)

	// Mutate the copy
	copied.Columns[0].ForeignKey.Table = "MUTATED"
	copied.Columns[0].GeneratedDepends[0] = "MUTATED"

	// Original must remain unchanged
	if original.Columns[0].ForeignKey.Table != "other" {
		t.Errorf("deepCopySchema failed: original ForeignKey.Table was mutated to %v", original.Columns[0].ForeignKey.Table)
	}
	if original.Columns[0].GeneratedDepends[0] != "col_a" {
		t.Errorf("deepCopySchema failed: original GeneratedDepends was mutated to %v", original.Columns[0].GeneratedDepends[0])
	}
}

// TestMVCC_InsertRowIsolation verifies that after inserting a row, mutating
// the original row data does not affect the stored version.
func TestMVCC_InsertRowIsolation(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	ds.Connect(ctx)

	ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "name", Type: "VARCHAR"},
			{Name: "tags", Type: "JSON"},
		},
	})

	// Insert a row with a nested slice
	tags := []interface{}{"admin", "user"}
	row := domain.Row{"name": "alice", "tags": tags}
	ds.Insert(ctx, "users", []domain.Row{row}, nil)

	// Mutate the original slice
	tags[0] = "HACKED"

	// Query and verify the stored row is unaffected
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	storedTags, ok := result.Rows[0]["tags"].([]interface{})
	if !ok {
		t.Fatalf("stored tags is not []interface{}: %T", result.Rows[0]["tags"])
	}
	if storedTags[0] != "admin" {
		t.Errorf("MVCC isolation broken: stored tags[0] = %v, want 'admin'", storedTags[0])
	}
}
