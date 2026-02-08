package optimizer

import (
	"testing"
)

func TestNewIndexManager(t *testing.T) {
	im := NewIndexManager()

	if im == nil {
		t.Fatalf("Expected non-nil manager")
	}

	if im.indices == nil {
		t.Errorf("Expected indices map to be initialized")
	}

	if im.stats == nil {
		t.Errorf("Expected stats map to be initialized")
	}
}

func TestAddIndex(t *testing.T) {
	im := NewIndexManager()

	// Add first index
	index1 := &Index{
		Name:       "idx_id",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Unique:     true,
		Primary:     true,
		Cardinality: 1000,
	}

	im.AddIndex(index1)

	indices := im.GetIndices("test_table")
	if len(indices) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indices))
	}

	if indices[0].Name != "idx_id" {
		t.Errorf("Expected index name 'idx_id', got '%s'", indices[0].Name)
	}

	// Check stats
	stats := im.stats["idx_id"]
	if stats == nil {
		t.Errorf("Expected stats to be created")
	}

	if stats.Name != "idx_id" {
		t.Errorf("Expected stats name 'idx_id', got '%s'", stats.Name)
	}
}

func TestGetIndices(t *testing.T) {
	im := NewIndexManager()

	// Get indices from non-existent table
	indices := im.GetIndices("non_existent_table")
	if indices != nil {
		t.Errorf("Expected nil for non-existent table, got %v", indices)
	}

	// Add indices
	im.AddIndex(&Index{
		Name:       "idx_id",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Unique:     true,
		Primary:     true,
		Cardinality: 1000,
	})

	// Get indices from existing table
	indices = im.GetIndices("test_table")
	if indices == nil {
		t.Errorf("Expected indices to be returned")
	}

	if len(indices) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indices))
	}
}

func TestFindBestIndex(t *testing.T) {
	im := NewIndexManager()

	// Add multiple indices
	im.AddIndex(&Index{
		Name:       "idx_id_primary",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Unique:     true,
		Primary:     true,
		Cardinality: 1000,
	})

	im.AddIndex(&Index{
		Name:       "idx_name",
		TableName:  "test_table",
		Columns:    []string{"name"},
		Unique:     false,
		Primary:     false,
		Cardinality: 100,
	})

	im.AddIndex(&Index{
		Name:       "idx_id_name",
		TableName:  "test_table",
		Columns:    []string{"id", "name"},
		Unique:     true,
		Primary:     false,
		Cardinality: 500,
	})

	// Test 1: Find best index for id column
	bestIndex := im.FindBestIndex("test_table", []string{"id"})
	if bestIndex == nil {
		t.Errorf("Expected best index for id column")
	}

	if bestIndex.Name != "idx_id_primary" {
		t.Errorf("Expected 'idx_id_primary', got '%s'", bestIndex.Name)
	}

	// Test 2: Find best index for name column
	bestIndex = im.FindBestIndex("test_table", []string{"name"})
	if bestIndex == nil {
		t.Errorf("Expected best index for name column")
	}

	if bestIndex.Name != "idx_name" {
		t.Errorf("Expected 'idx_name', got '%s'", bestIndex.Name)
	}

	// Test 3: Find best index for both id and name
	bestIndex = im.FindBestIndex("test_table", []string{"id", "name"})
	if bestIndex == nil {
		t.Errorf("Expected best index for id and name columns")
	}

	if bestIndex.Name != "idx_id_name" {
		t.Errorf("Expected 'idx_id_name', got '%s'", bestIndex.Name)
	}

	// Test 4: Find best index for non-existent column
	bestIndex = im.FindBestIndex("test_table", []string{"age"})
	if bestIndex != nil {
		t.Errorf("Expected nil for non-existent column")
	}

	// Test 5: Find best index from non-existent table
	bestIndex = im.FindBestIndex("non_existent_table", []string{"id"})
	if bestIndex != nil {
		t.Errorf("Expected nil for non-existent table")
	}
}
