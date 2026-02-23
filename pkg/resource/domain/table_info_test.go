package domain

import (
	"testing"
)

func TestTableInfo_AddColumn(t *testing.T) {
	table := &TableInfo{
		Name:    "test_table",
		Columns: []ColumnInfo{},
	}

	// Test adding a column
	col := ColumnInfo{Name: "id", Type: "int", Primary: true}
	err := table.AddColumn(col)
	if err != nil {
		t.Fatalf("failed to add column: %v", err)
	}

	if !table.HasColumn("id") {
		t.Error("column should exist after adding")
	}

	// Test adding duplicate column
	err = table.AddColumn(col)
	if err == nil {
		t.Error("should fail when adding duplicate column")
	}
}

func TestTableInfo_RemoveColumn(t *testing.T) {
	table := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "varchar(255)", Primary: false},
		},
	}

	// Test removing a non-primary column
	err := table.RemoveColumn("name")
	if err != nil {
		t.Fatalf("failed to remove column: %v", err)
	}

	if table.HasColumn("name") {
		t.Error("column should not exist after removal")
	}

	// Test removing a primary key column
	err = table.RemoveColumn("id")
	if err == nil {
		t.Error("should fail when removing primary key column")
	}
}

func TestTableInfo_HasColumn(t *testing.T) {
	table := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}

	if !table.HasColumn("id") {
		t.Error("should find existing column")
	}

	if table.HasColumn("nonexistent") {
		t.Error("should not find non-existent column")
	}
}

func TestTableInfo_GetColumn(t *testing.T) {
	table := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}

	col, found := table.GetColumn("id")
	if !found {
		t.Fatal("should find existing column")
	}

	if col.Name != "id" {
		t.Errorf("expected column name 'id', got '%s'", col.Name)
	}

	_, found = table.GetColumn("nonexistent")
	if found {
		t.Error("should not find non-existent column")
	}
}

func TestTableInfo_GetPrimaryKey(t *testing.T) {
	table := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "varchar(255)", Primary: false},
		},
	}

	pk := table.GetPrimaryKey()
	if len(pk) != 1 {
		t.Fatalf("expected 1 primary key column, got %d", len(pk))
	}

	if pk[0].Name != "id" {
		t.Errorf("expected primary key 'id', got '%s'", pk[0].Name)
	}
}

func TestTableInfo_HasPrimaryKey(t *testing.T) {
	tableWithPK := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	}

	if !tableWithPK.HasPrimaryKey() {
		t.Error("table should have primary key")
	}

	tableWithoutPK := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Primary: false},
		},
	}

	if tableWithoutPK.HasPrimaryKey() {
		t.Error("table should not have primary key")
	}
}

func TestTableInfo_SetPrimaryKey(t *testing.T) {
	table := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Primary: false},
			{Name: "name", Type: "varchar(255)", Primary: false},
		},
	}

	err := table.SetPrimaryKey("id")
	if err != nil {
		t.Fatalf("failed to set primary key: %v", err)
	}

	if !table.Columns[0].Primary {
		t.Error("column 'id' should be primary key")
	}

	// Test setting non-existent column
	err = table.SetPrimaryKey("nonexistent")
	if err == nil {
		t.Error("should fail when setting non-existent column as primary key")
	}
}

func TestTableInfo_GetColumnNames(t *testing.T) {
	table := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar(255)"},
		},
	}

	names := table.GetColumnNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 column names, got %d", len(names))
	}

	if names[0] != "id" || names[1] != "name" {
		t.Errorf("unexpected column names: %v", names)
	}
}

func TestTableInfo_Validate(t *testing.T) {
	// Test valid table
	validTable := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}

	if err := validTable.Validate(); err != nil {
		t.Errorf("valid table should pass validation: %v", err)
	}

	// Test table without name
	noNameTable := &TableInfo{
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}

	if err := noNameTable.Validate(); err == nil {
		t.Error("table without name should fail validation")
	}

	// Test table without columns
	noColumnsTable := &TableInfo{
		Name: "test_table",
	}

	if err := noColumnsTable.Validate(); err == nil {
		t.Error("table without columns should fail validation")
	}

	// Test table with duplicate columns
	duplicateTable := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "id", Type: "varchar(255)"},
		},
	}

	if err := duplicateTable.Validate(); err == nil {
		t.Error("table with duplicate columns should fail validation")
	}
}

func TestTableInfo_Clone(t *testing.T) {
	original := &TableInfo{
		Name:    "test_table",
		Schema:  "test_schema",
		Columns: []ColumnInfo{{Name: "id", Type: "int"}},
		Atts:    map[string]interface{}{"key": "value"},
	}

	clone := original.Clone()

	if clone.Name != original.Name {
		t.Error("clone should have same name")
	}

	// Modify original and verify clone is unaffected
	original.Name = "modified"
	if clone.Name == "modified" {
		t.Error("clone should be independent from original")
	}
}

func TestTableInfo_FullName(t *testing.T) {
	tableWithSchema := &TableInfo{
		Name:   "table",
		Schema: "schema",
	}

	if tableWithSchema.FullName() != "schema.table" {
		t.Errorf("expected 'schema.table', got '%s'", tableWithSchema.FullName())
	}

	tableWithoutSchema := &TableInfo{
		Name: "table",
	}

	if tableWithoutSchema.FullName() != "table" {
		t.Errorf("expected 'table', got '%s'", tableWithoutSchema.FullName())
	}
}

func TestColumnInfo_Validate(t *testing.T) {
	// Test valid column
	validCol := ColumnInfo{Name: "id", Type: "int"}
	if err := validCol.Validate(); err != nil {
		t.Errorf("valid column should pass validation: %v", err)
	}

	// Test column without name
	noNameCol := ColumnInfo{Type: "int"}
	if err := noNameCol.Validate(); err == nil {
		t.Error("column without name should fail validation")
	}

	// Test column without type
	noTypeCol := ColumnInfo{Name: "id"}
	if err := noTypeCol.Validate(); err == nil {
		t.Error("column without type should fail validation")
	}

	// Test generated column without dependencies
	genCol := ColumnInfo{Name: "computed", Type: "int", IsGenerated: true}
	if err := genCol.Validate(); err == nil {
		t.Error("generated column without dependencies should fail validation")
	}

	// Test vector column without type
	vectorCol := ColumnInfo{Name: "embedding", Type: "vector", VectorDim: 128}
	if err := vectorCol.Validate(); err == nil {
		t.Error("vector column without vector_type should fail validation")
	}
}

func TestColumnInfo_IsVectorType(t *testing.T) {
	vectorCol := ColumnInfo{Name: "embedding", VectorDim: 128}
	if !vectorCol.IsVectorType() {
		t.Error("column with VectorDim > 0 should be vector type")
	}

	nonVectorCol := ColumnInfo{Name: "id"}
	if nonVectorCol.IsVectorType() {
		t.Error("column without VectorDim should not be vector type")
	}
}

func TestColumnInfo_Clone(t *testing.T) {
	original := ColumnInfo{
		Name:             "id",
		Type:             "int",
		ForeignKey:       &ForeignKeyInfo{Table: "users", Column: "id"},
		GeneratedDepends: []string{"col1", "col2"},
	}

	clone := original.Clone()

	// Modify original
	original.Name = "modified"
	original.ForeignKey.Table = "modified_table"

	if clone.Name == "modified" {
		t.Error("clone should be independent from original")
	}

	if clone.ForeignKey.Table == "modified_table" {
		t.Error("clone's foreign key should be independent from original")
	}
}
