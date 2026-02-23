package parquet

import (
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestWAL_AppendAndReadAll(t *testing.T) {
	dir := t.TempDir()

	wal, err := newWAL(dir)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}

	// Append entries
	entries := []WALEntry{
		{Type: WALInsert, TableName: "t1", Rows: []domain.Row{{"id": int64(1)}}},
		{Type: WALUpdate, TableName: "t1", Filters: []domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}}, Updates: domain.Row{"name": "updated"}},
		{Type: WALDelete, TableName: "t1", Filters: []domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}}},
	}

	for _, e := range entries {
		if err := wal.Append(&e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	wal.Close()

	// Read all
	read, err := ReadAll(dir)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(read) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(read))
	}

	if read[0].Type != WALInsert {
		t.Errorf("Entry 0: expected INSERT, got %d", read[0].Type)
	}
	if read[1].Type != WALUpdate {
		t.Errorf("Entry 1: expected UPDATE, got %d", read[1].Type)
	}
	if read[2].Type != WALDelete {
		t.Errorf("Entry 2: expected DELETE, got %d", read[2].Type)
	}
}

func TestWAL_Checkpoint(t *testing.T) {
	dir := t.TempDir()

	wal, err := newWAL(dir)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}

	// Write entries, then checkpoint, then more entries
	wal.Append(&WALEntry{Type: WALInsert, TableName: "t1", Rows: []domain.Row{{"id": int64(1)}}})
	wal.Append(&WALEntry{Type: WALInsert, TableName: "t1", Rows: []domain.Row{{"id": int64(2)}}})
	wal.Append(&WALEntry{Type: WALCheckpoint})
	wal.Append(&WALEntry{Type: WALInsert, TableName: "t1", Rows: []domain.Row{{"id": int64(3)}}})
	wal.Close()

	// ReadAll should only return entries after checkpoint
	read, err := ReadAll(dir)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(read) != 1 {
		t.Fatalf("Expected 1 entry after checkpoint, got %d", len(read))
	}
	if len(read[0].Rows) != 1 || read[0].Rows[0]["id"] != int64(3) {
		t.Error("Expected the entry after checkpoint to have id=3")
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()

	wal, err := newWAL(dir)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}

	wal.Append(&WALEntry{Type: WALInsert, TableName: "t1", Rows: []domain.Row{{"id": int64(1)}}})

	if err := wal.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// New entries after truncate
	wal.Append(&WALEntry{Type: WALInsert, TableName: "t1", Rows: []domain.Row{{"id": int64(2)}}})
	wal.Close()

	read, err := ReadAll(dir)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(read) != 1 {
		t.Fatalf("Expected 1 entry after truncate, got %d", len(read))
	}
}

func TestWAL_ReadAll_NoFile(t *testing.T) {
	dir := t.TempDir()

	read, err := ReadAll(dir)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(read) != 0 {
		t.Errorf("Expected 0 entries for nonexistent WAL, got %d", len(read))
	}
}

func TestWAL_DDLEntries(t *testing.T) {
	dir := t.TempDir()

	wal, err := newWAL(dir)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}

	wal.Append(&WALEntry{
		Type:      WALCreateTable,
		TableName: "new_table",
		Schema: &domain.TableInfo{
			Name: "new_table",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64"},
			},
		},
	})
	wal.Append(&WALEntry{Type: WALDropTable, TableName: "new_table"})
	wal.Append(&WALEntry{Type: WALTruncateTable, TableName: "other"})
	wal.Close()

	read, err := ReadAll(dir)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(read) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(read))
	}
	if read[0].Type != WALCreateTable || read[0].Schema.Name != "new_table" {
		t.Error("Entry 0: expected CreateTable with schema")
	}
	if read[1].Type != WALDropTable {
		t.Error("Entry 1: expected DropTable")
	}
	if read[2].Type != WALTruncateTable {
		t.Error("Entry 2: expected TruncateTable")
	}
}

func TestWAL_Path(t *testing.T) {
	dir := "/tmp/testdir"
	expected := filepath.Join(dir, ".wal")
	if got := walPath(dir); got != expected {
		t.Errorf("walPath(%q) = %q, want %q", dir, got, expected)
	}
}
