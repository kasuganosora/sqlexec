package xml

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStorageMode(t *testing.T) {
	tests := []struct {
		comment  string
		expected StorageMode
	}{
		{"", StorageModeFilePerRow},
		{"xml_mode=file_per_row", StorageModeFilePerRow},
		{"xml_mode=single_file", StorageModeSingleFile},
		{"  xml_mode=single_file  ", StorageModeSingleFile},
		{"other_key=val; xml_mode=single_file", StorageModeSingleFile},
		{"xml_mode=unknown", StorageModeFilePerRow},
		{"no_mode_here", StorageModeFilePerRow},
	}

	for _, tt := range tests {
		t.Run(tt.comment, func(t *testing.T) {
			got := ParseStorageMode(tt.comment)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPersistTableSchema(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "User",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false, Primary: true, AutoIncrement: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
			{Name: "email", Type: "VARCHAR", Nullable: true, Unique: true},
		},
	}

	err := PersistTableSchema(cfg, tableInfo)
	require.NoError(t, err)

	// Verify file exists
	schemaPath := filepath.Join(tmpDir, "users", "__schema__.xml")
	assert.FileExists(t, schemaPath)

	// Verify it can be loaded back
	loadedInfo, err := loadSchema(schemaPath)
	require.NoError(t, err)
	assert.Equal(t, "users", loadedInfo.Name)
	assert.Len(t, loadedInfo.Columns, 3)
	assert.Equal(t, "id", loadedInfo.Columns[0].Name)
	assert.True(t, loadedInfo.Columns[0].Primary)
	assert.True(t, loadedInfo.Columns[0].AutoIncrement)
	assert.Equal(t, "email", loadedInfo.Columns[2].Name)
	assert.True(t, loadedInfo.Columns[2].Unique)
}

func TestPersistTableData_FilePerRow(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "User",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
		},
	}

	rows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}

	// First persist schema
	require.NoError(t, PersistTableSchema(cfg, tableInfo))

	// Then persist data
	err := PersistTableData(cfg, tableInfo, rows)
	require.NoError(t, err)

	// Verify individual files
	assert.FileExists(t, filepath.Join(tmpDir, "users", "1.xml"))
	assert.FileExists(t, filepath.Join(tmpDir, "users", "2.xml"))

	// Verify data can be loaded back
	loadedRows, err := loadFilePerRowData(filepath.Join(tmpDir, "users"), tableInfo)
	require.NoError(t, err)
	assert.Len(t, loadedRows, 2)

	// Verify values (order may vary, check both exist)
	found := map[int64]string{}
	for _, row := range loadedRows {
		id := row["id"].(int64)
		name := row["name"].(string)
		found[id] = name
	}
	assert.Equal(t, "Alice", found[1])
	assert.Equal(t, "Bob", found[2])
}

func TestPersistTableData_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "logs",
		RootTag:     "Log",
		StorageMode: StorageModeSingleFile,
	}

	tableInfo := &domain.TableInfo{
		Name: "logs",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "msg", Type: "TEXT", Nullable: true},
		},
	}

	rows := []domain.Row{
		{"id": int64(1), "msg": "hello"},
		{"id": int64(2), "msg": "world"},
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	err := PersistTableData(cfg, tableInfo, rows)
	require.NoError(t, err)

	// Verify single data file
	assert.FileExists(t, filepath.Join(tmpDir, "logs", "data.xml"))
	// No individual row files
	assert.NoFileExists(t, filepath.Join(tmpDir, "logs", "1.xml"))

	// Verify data can be loaded back
	loadedRows, err := loadSingleFileData(filepath.Join(tmpDir, "logs"), tableInfo)
	require.NoError(t, err)
	assert.Len(t, loadedRows, 2)

	found := map[int64]string{}
	for _, row := range loadedRows {
		id := row["id"].(int64)
		msg := row["msg"].(string)
		found[id] = msg
	}
	assert.Equal(t, "hello", found[1])
	assert.Equal(t, "world", found[2])
}

func TestPersistIndexMeta(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "User",
		StorageMode: StorageModeFilePerRow,
	}

	indexes := []*IndexMeta{
		{Name: "idx_name", Table: "users", Type: "btree", Unique: false, Columns: []string{"name"}},
		{Name: "idx_email", Table: "users", Type: "hash", Unique: true, Columns: []string{"email"}},
	}

	require.NoError(t, os.MkdirAll(cfg.TableDir(), 0755))
	err := PersistIndexMeta(cfg, indexes)
	require.NoError(t, err)

	// Verify file exists
	metaPath := filepath.Join(tmpDir, "users", "__meta__.xml")
	assert.FileExists(t, metaPath)

	// Verify it can be loaded back
	loaded, err := loadIndexMeta(metaPath)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, "idx_name", loaded[0].Name)
	assert.Equal(t, "btree", loaded[0].Type)
	assert.Equal(t, []string{"name"}, loaded[0].Columns)
	assert.Equal(t, "idx_email", loaded[1].Name)
	assert.True(t, loaded[1].Unique)
}

func TestLoadPersistedTables(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two persisted table directories
	cfg1 := &TablePersistConfig{BasePath: tmpDir, TableName: "users", RootTag: "User", StorageMode: StorageModeFilePerRow}
	cfg2 := &TablePersistConfig{BasePath: tmpDir, TableName: "logs", RootTag: "Log", StorageMode: StorageModeSingleFile}

	PersistTableSchema(cfg1, &domain.TableInfo{Name: "users", Columns: []domain.ColumnInfo{{Name: "id", Type: "INT", Primary: true}}})
	PersistTableSchema(cfg2, &domain.TableInfo{Name: "logs", Columns: []domain.ColumnInfo{{Name: "id", Type: "INT", Primary: true}}})

	// Create a directory without schema (should be skipped)
	os.MkdirAll(filepath.Join(tmpDir, "no_schema"), 0755)

	configs, err := LoadPersistedTables(tmpDir)
	require.NoError(t, err)
	assert.Len(t, configs, 2)

	// Check configs
	tableNames := map[string]StorageMode{}
	for _, c := range configs {
		tableNames[c.TableName] = c.StorageMode
	}
	assert.Equal(t, StorageModeFilePerRow, tableNames["users"])
	assert.Equal(t, StorageModeSingleFile, tableNames["logs"])
}

func TestLoadPersistedTables_NonexistentDir(t *testing.T) {
	configs, err := LoadPersistedTables("/nonexistent/path")
	assert.NoError(t, err)
	assert.Nil(t, configs)
}

func TestLoadTableFromDisk(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "User",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
		},
	}

	rows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}

	indexes := []*IndexMeta{
		{Name: "idx_name", Table: "users", Type: "btree", Unique: false, Columns: []string{"name"}},
	}

	PersistTableSchema(cfg, tableInfo)
	PersistTableData(cfg, tableInfo, rows)
	PersistIndexMeta(cfg, indexes)

	// Load everything back
	loadedInfo, loadedRows, loadedIndexes, err := LoadTableFromDisk(cfg)
	require.NoError(t, err)
	assert.Equal(t, "users", loadedInfo.Name)
	assert.Len(t, loadedInfo.Columns, 2)
	assert.Len(t, loadedRows, 2)
	require.Len(t, loadedIndexes, 1)
	assert.Equal(t, "idx_name", loadedIndexes[0].Name)
}

func TestPersistTableData_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "User",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	// Write initial data (3 rows)
	PersistTableSchema(cfg, tableInfo)
	PersistTableData(cfg, tableInfo, []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Charlie"},
	})
	assert.FileExists(t, filepath.Join(tmpDir, "users", "3.xml"))

	// Overwrite with fewer rows (2 rows) - old file 3.xml should be cleaned
	PersistTableData(cfg, tableInfo, []domain.Row{
		{"id": int64(1), "name": "Alice Updated"},
		{"id": int64(2), "name": "Bob Updated"},
	})

	// 3.xml should be gone
	assert.NoFileExists(t, filepath.Join(tmpDir, "users", "3.xml"))

	// Verify correct data
	loadedRows, err := loadFilePerRowData(filepath.Join(tmpDir, "users"), tableInfo)
	require.NoError(t, err)
	assert.Len(t, loadedRows, 2)
}

func TestPersistTableData_EmptyRows(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "empty",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name:    "empty",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INT", Primary: true}},
	}

	PersistTableSchema(cfg, tableInfo)
	err := PersistTableData(cfg, tableInfo, nil)
	require.NoError(t, err)
}

func TestConvertXMLValue(t *testing.T) {
	tests := []struct {
		value    string
		colType  string
		expected interface{}
	}{
		{"42", "INT", int64(42)},
		{"42", "BIGINT", int64(42)},
		{"3.14", "FLOAT", float64(3.14)},
		{"3.14", "DOUBLE", float64(3.14)},
		{"true", "BOOL", true},
		{"false", "BOOLEAN", false},
		{"hello", "VARCHAR", "hello"},
		{"hello", "TEXT", "hello"},
		{"notanumber", "INT", "notanumber"},
	}

	for _, tt := range tests {
		t.Run(tt.value+"_"+tt.colType, func(t *testing.T) {
			got := convertXMLValue(tt.value, tt.colType)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDeleteTableDir(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:  tmpDir,
		TableName: "to_delete",
	}

	// Create the directory
	os.MkdirAll(cfg.TableDir(), 0755)
	os.WriteFile(filepath.Join(cfg.TableDir(), "test.xml"), []byte("<test/>"), 0644)
	assert.DirExists(t, cfg.TableDir())

	err := DeleteTableDir(cfg)
	require.NoError(t, err)
	assert.NoDirExists(t, cfg.TableDir())
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, ""},
		{"hello", "hello"},
		{int64(42), "42"},
		{42, "42"},
		{3.14, "3.14"},
		{true, "true"},
		{false, "false"},
	}

	for _, tt := range tests {
		got := formatValue(tt.input)
		assert.Equal(t, tt.expected, got)
	}
}

func TestXMLSpecialCharsInData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "special",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "special",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "data", Type: "VARCHAR"},
		},
	}

	rows := []domain.Row{
		{"id": int64(1), "data": `hello "world" & <test>`},
	}

	PersistTableSchema(cfg, tableInfo)
	err := PersistTableData(cfg, tableInfo, rows)
	require.NoError(t, err)

	// Load back and verify
	loadedRows, err := loadFilePerRowData(filepath.Join(tmpDir, "special"), tableInfo)
	require.NoError(t, err)
	require.Len(t, loadedRows, 1)
	assert.Equal(t, `hello "world" & <test>`, loadedRows[0]["data"])
}

// --- Index Persistence Integration Tests ---

func TestPersistIndexMeta_MultipleColumns(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:  tmpDir,
		TableName: "orders",
		RootTag:   "Order",
	}

	indexes := []*IndexMeta{
		{Name: "idx_composite", Table: "orders", Type: "btree", Unique: false, Columns: []string{"user_id", "created_at"}},
	}

	require.NoError(t, os.MkdirAll(cfg.TableDir(), 0755))
	require.NoError(t, PersistIndexMeta(cfg, indexes))

	loaded, err := loadIndexMeta(filepath.Join(cfg.TableDir(), "__meta__.xml"))
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "idx_composite", loaded[0].Name)
	assert.Equal(t, []string{"user_id", "created_at"}, loaded[0].Columns)
}

func TestPersistIndexMeta_EmptyIndexes(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:  tmpDir,
		TableName: "empty_idx",
		RootTag:   "Row",
	}

	require.NoError(t, os.MkdirAll(cfg.TableDir(), 0755))
	require.NoError(t, PersistIndexMeta(cfg, []*IndexMeta{}))

	loaded, err := loadIndexMeta(filepath.Join(cfg.TableDir(), "__meta__.xml"))
	require.NoError(t, err)
	assert.Len(t, loaded, 0)
}

func TestPersistIndexMeta_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:  tmpDir,
		TableName: "users",
		RootTag:   "User",
	}

	require.NoError(t, os.MkdirAll(cfg.TableDir(), 0755))

	// Write initial indexes
	indexes1 := []*IndexMeta{
		{Name: "idx_name", Table: "users", Type: "btree", Unique: false, Columns: []string{"name"}},
		{Name: "idx_email", Table: "users", Type: "hash", Unique: true, Columns: []string{"email"}},
	}
	require.NoError(t, PersistIndexMeta(cfg, indexes1))

	// Overwrite with different indexes (simulating DROP + CREATE)
	indexes2 := []*IndexMeta{
		{Name: "idx_name", Table: "users", Type: "btree", Unique: false, Columns: []string{"name"}},
	}
	require.NoError(t, PersistIndexMeta(cfg, indexes2))

	loaded, err := loadIndexMeta(filepath.Join(cfg.TableDir(), "__meta__.xml"))
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "idx_name", loaded[0].Name)
}

func TestPersistIndexMeta_AllTypes(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:  tmpDir,
		TableName: "multi_idx",
		RootTag:   "Row",
	}

	indexes := []*IndexMeta{
		{Name: "idx_btree", Table: "multi_idx", Type: "btree", Unique: false, Columns: []string{"col_a"}},
		{Name: "idx_hash", Table: "multi_idx", Type: "hash", Unique: false, Columns: []string{"col_b"}},
		{Name: "idx_unique", Table: "multi_idx", Type: "btree", Unique: true, Columns: []string{"col_c"}},
	}

	require.NoError(t, os.MkdirAll(cfg.TableDir(), 0755))
	require.NoError(t, PersistIndexMeta(cfg, indexes))

	loaded, err := loadIndexMeta(filepath.Join(cfg.TableDir(), "__meta__.xml"))
	require.NoError(t, err)
	require.Len(t, loaded, 3)

	for i, idx := range loaded {
		assert.Equal(t, indexes[i].Name, idx.Name)
		assert.Equal(t, indexes[i].Type, idx.Type)
		assert.Equal(t, indexes[i].Unique, idx.Unique)
		assert.Equal(t, indexes[i].Columns, idx.Columns)
	}
}

func TestLoadIndexMeta_NoFile(t *testing.T) {
	loaded, err := loadIndexMeta("/nonexistent/__meta__.xml")
	assert.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestLoadIndexMeta_InvalidXML(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "__meta__.xml")
	os.WriteFile(metaPath, []byte("not valid xml <><><>"), 0644)

	loaded, err := loadIndexMeta(metaPath)
	assert.Error(t, err)
	assert.Nil(t, loaded)
}

// --- End-to-End Persistence Tests ---

func TestE2E_CreatePersistReload_FilePerRow(t *testing.T) {
	tmpDir := t.TempDir()

	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
			{Name: "price", Type: "FLOAT", Nullable: true},
			{Name: "active", Type: "BOOLEAN", Nullable: true},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "products",
		RootTag:     "Product",
		StorageMode: StorageModeFilePerRow,
	}

	// Step 1: CREATE TABLE - persist schema
	require.NoError(t, PersistTableSchema(cfg, tableInfo))

	// Step 2: INSERT - persist data
	rows := []domain.Row{
		{"id": int64(1), "name": "Widget", "price": float64(9.99), "active": true},
		{"id": int64(2), "name": "Gadget", "price": float64(19.5), "active": false},
		{"id": int64(3), "name": "Doohickey", "price": float64(4.25), "active": true},
	}
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	// Step 3: CREATE INDEX - persist index metadata
	indexes := []*IndexMeta{
		{Name: "idx_name", Table: "products", Type: "btree", Unique: false, Columns: []string{"name"}},
		{Name: "idx_active", Table: "products", Type: "hash", Unique: false, Columns: []string{"active"}},
	}
	require.NoError(t, PersistIndexMeta(cfg, indexes))

	// Step 4: Simulate restart - discover tables
	configs, err := LoadPersistedTables(tmpDir)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "products", configs[0].TableName)
	assert.Equal(t, StorageModeFilePerRow, configs[0].StorageMode)
	assert.Equal(t, "Product", configs[0].RootTag)

	// Step 5: Load everything from disk
	loadedInfo, loadedRows, loadedIndexes, err := LoadTableFromDisk(configs[0])
	require.NoError(t, err)

	// Verify schema
	assert.Equal(t, "products", loadedInfo.Name)
	require.Len(t, loadedInfo.Columns, 4)
	assert.True(t, loadedInfo.Columns[0].Primary)
	assert.True(t, loadedInfo.Columns[0].AutoIncrement)

	// Verify data with type conversion
	assert.Len(t, loadedRows, 3)
	byID := map[int64]domain.Row{}
	for _, r := range loadedRows {
		byID[r["id"].(int64)] = r
	}
	assert.Equal(t, "Widget", byID[1]["name"])
	assert.InDelta(t, 9.99, byID[1]["price"].(float64), 0.001)
	assert.Equal(t, true, byID[1]["active"])
	assert.Equal(t, false, byID[2]["active"])

	// Verify indexes
	require.Len(t, loadedIndexes, 2)
	idxByName := map[string]*IndexMeta{}
	for _, idx := range loadedIndexes {
		idxByName[idx.Name] = idx
	}
	assert.Equal(t, "btree", idxByName["idx_name"].Type)
	assert.Equal(t, "hash", idxByName["idx_active"].Type)
}

func TestE2E_CreatePersistReload_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()

	tableInfo := &domain.TableInfo{
		Name: "events",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "event", Type: "VARCHAR"},
			{Name: "count", Type: "BIGINT"},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "events",
		RootTag:     "Event",
		StorageMode: StorageModeSingleFile,
	}

	// Schema + Data + Index
	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	rows := []domain.Row{
		{"id": int64(1), "event": "login", "count": int64(100)},
		{"id": int64(2), "event": "logout", "count": int64(50)},
	}
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	indexes := []*IndexMeta{
		{Name: "idx_event", Table: "events", Type: "btree", Unique: false, Columns: []string{"event"}},
	}
	require.NoError(t, PersistIndexMeta(cfg, indexes))

	// Reload from disk
	configs, err := LoadPersistedTables(tmpDir)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, StorageModeSingleFile, configs[0].StorageMode)

	loadedInfo, loadedRows, loadedIndexes, err := LoadTableFromDisk(configs[0])
	require.NoError(t, err)

	assert.Equal(t, "events", loadedInfo.Name)
	assert.Len(t, loadedRows, 2)
	require.Len(t, loadedIndexes, 1)
	assert.Equal(t, "idx_event", loadedIndexes[0].Name)
}

func TestE2E_UpdateAndReload(t *testing.T) {
	tmpDir := t.TempDir()

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "User",
		StorageMode: StorageModeFilePerRow,
	}

	// Initial data
	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	require.NoError(t, PersistTableData(cfg, tableInfo, []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Charlie"},
	}))

	// Simulate UPDATE (Alice -> Alicia) + DELETE (Charlie removed)
	require.NoError(t, PersistTableData(cfg, tableInfo, []domain.Row{
		{"id": int64(1), "name": "Alicia"},
		{"id": int64(2), "name": "Bob"},
	}))

	// Reload
	_, loadedRows, _, err := LoadTableFromDisk(cfg)
	require.NoError(t, err)
	assert.Len(t, loadedRows, 2)

	byID := map[int64]string{}
	for _, r := range loadedRows {
		byID[r["id"].(int64)] = r["name"].(string)
	}
	assert.Equal(t, "Alicia", byID[1])
	assert.Equal(t, "Bob", byID[2])
}

func TestE2E_DropAndRecreate(t *testing.T) {
	tmpDir := t.TempDir()

	tableInfo := &domain.TableInfo{
		Name: "temp",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "temp",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	// Create and persist
	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	require.NoError(t, PersistTableData(cfg, tableInfo, []domain.Row{{"id": int64(1)}}))

	// Drop
	require.NoError(t, DeleteTableDir(cfg))
	assert.NoDirExists(t, cfg.TableDir())

	// Verify LoadPersistedTables returns nothing
	configs, err := LoadPersistedTables(tmpDir)
	require.NoError(t, err)
	assert.Len(t, configs, 0)

	// Recreate
	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	require.NoError(t, PersistTableData(cfg, tableInfo, []domain.Row{{"id": int64(99)}}))

	// Reload
	_, loadedRows, _, err := LoadTableFromDisk(cfg)
	require.NoError(t, err)
	require.Len(t, loadedRows, 1)
	assert.Equal(t, int64(99), loadedRows[0]["id"])
}

func TestE2E_MultipleTablesWithIndexes(t *testing.T) {
	tmpDir := t.TempDir()

	// Table 1: users (file_per_row)
	usersInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "email", Type: "VARCHAR"},
		},
	}
	usersCfg := &TablePersistConfig{BasePath: tmpDir, TableName: "users", RootTag: "User", StorageMode: StorageModeFilePerRow}

	// Table 2: logs (single_file)
	logsInfo := &domain.TableInfo{
		Name: "logs",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "level", Type: "VARCHAR"},
			{Name: "msg", Type: "TEXT"},
		},
	}
	logsCfg := &TablePersistConfig{BasePath: tmpDir, TableName: "logs", RootTag: "Log", StorageMode: StorageModeSingleFile}

	// Persist both tables
	require.NoError(t, PersistTableSchema(usersCfg, usersInfo))
	require.NoError(t, PersistTableData(usersCfg, usersInfo, []domain.Row{
		{"id": int64(1), "name": "Alice", "email": "a@test.com"},
	}))
	require.NoError(t, PersistIndexMeta(usersCfg, []*IndexMeta{
		{Name: "idx_email", Table: "users", Type: "btree", Unique: true, Columns: []string{"email"}},
	}))

	require.NoError(t, PersistTableSchema(logsCfg, logsInfo))
	require.NoError(t, PersistTableData(logsCfg, logsInfo, []domain.Row{
		{"id": int64(1), "level": "INFO", "msg": "started"},
		{"id": int64(2), "level": "ERROR", "msg": "failed"},
	}))
	require.NoError(t, PersistIndexMeta(logsCfg, []*IndexMeta{
		{Name: "idx_level", Table: "logs", Type: "hash", Unique: false, Columns: []string{"level"}},
	}))

	// Discover all tables
	configs, err := LoadPersistedTables(tmpDir)
	require.NoError(t, err)
	assert.Len(t, configs, 2)

	// Load and verify each table
	for _, cfg := range configs {
		info, rows, indexes, err := LoadTableFromDisk(cfg)
		require.NoError(t, err)

		switch cfg.TableName {
		case "users":
			assert.Equal(t, StorageModeFilePerRow, cfg.StorageMode)
			assert.Len(t, info.Columns, 3)
			assert.Len(t, rows, 1)
			require.Len(t, indexes, 1)
			assert.True(t, indexes[0].Unique)
		case "logs":
			assert.Equal(t, StorageModeSingleFile, cfg.StorageMode)
			assert.Len(t, info.Columns, 3)
			assert.Len(t, rows, 2)
			require.Len(t, indexes, 1)
			assert.False(t, indexes[0].Unique)
		default:
			t.Errorf("unexpected table: %s", cfg.TableName)
		}
	}
}

func TestLoadTableFromDisk_NoData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "empty",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name:    "empty",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INT", Primary: true}},
	}

	// Only persist schema, no data, no indexes
	require.NoError(t, PersistTableSchema(cfg, tableInfo))

	loadedInfo, loadedRows, loadedIndexes, err := LoadTableFromDisk(cfg)
	require.NoError(t, err)
	assert.Equal(t, "empty", loadedInfo.Name)
	assert.Len(t, loadedRows, 0)
	assert.Nil(t, loadedIndexes)
}

func TestLoadTableFromDisk_SingleFile_NoData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "empty_single",
		RootTag:     "Row",
		StorageMode: StorageModeSingleFile,
	}

	tableInfo := &domain.TableInfo{
		Name:    "empty_single",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INT", Primary: true}},
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))

	loadedInfo, loadedRows, _, err := LoadTableFromDisk(cfg)
	require.NoError(t, err)
	assert.Equal(t, "empty_single", loadedInfo.Name)
	assert.Len(t, loadedRows, 0)
}

func TestTablePersistConfig_TableDir(t *testing.T) {
	cfg := &TablePersistConfig{
		BasePath:  "/data/mydb",
		TableName: "users",
	}
	assert.Equal(t, filepath.Join("/data/mydb", "users"), cfg.TableDir())
}

func TestPersistTableData_NoPrimaryKey(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "no_pk",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "no_pk",
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "VARCHAR"},
			{Name: "b", Type: "INT"},
		},
	}

	rows := []domain.Row{
		{"a": "x", "b": int64(1)},
		{"a": "y", "b": int64(2)},
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	// Without PK, files are named 1.xml, 2.xml (by index+1)
	assert.FileExists(t, filepath.Join(tmpDir, "no_pk", "1.xml"))
	assert.FileExists(t, filepath.Join(tmpDir, "no_pk", "2.xml"))

	loadedRows, err := loadFilePerRowData(filepath.Join(tmpDir, "no_pk"), tableInfo)
	require.NoError(t, err)
	assert.Len(t, loadedRows, 2)
}

func TestPersistTableData_NullValues(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "nulls",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name: "nulls",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
			{Name: "age", Type: "INT", Nullable: true},
		},
	}

	rows := []domain.Row{
		{"id": int64(1), "name": nil, "age": int64(25)},
		{"id": int64(2), "name": "Bob", "age": nil},
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	loadedRows, err := loadFilePerRowData(filepath.Join(tmpDir, "nulls"), tableInfo)
	require.NoError(t, err)
	assert.Len(t, loadedRows, 2)

	// NULL values are omitted from XML, so loaded rows won't have the nil keys
	byID := map[int64]domain.Row{}
	for _, r := range loadedRows {
		byID[r["id"].(int64)] = r
	}
	// Row 1: name should be absent (nil was skipped), age should be 25
	_, hasName1 := byID[1]["name"]
	assert.False(t, hasName1)
	assert.Equal(t, int64(25), byID[1]["age"])
	// Row 2: name should be Bob, age should be absent
	assert.Equal(t, "Bob", byID[2]["name"])
	_, hasAge2 := byID[2]["age"]
	assert.False(t, hasAge2)
}

func TestConvertXMLValue_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		colType  string
		expected interface{}
	}{
		{"int_negative", "-42", "INT", int64(-42)},
		{"int_zero", "0", "INT", int64(0)},
		{"bigint_large", "9223372036854775807", "BIGINT", int64(9223372036854775807)},
		{"float_negative", "-3.14", "FLOAT", float64(-3.14)},
		{"float_zero", "0.0", "DOUBLE", float64(0)},
		{"decimal", "123.456", "DECIMAL", float64(123.456)},
		{"numeric", "99.9", "NUMERIC", float64(99.9)},
		{"bool_1", "1", "BOOL", true},
		{"bool_0", "0", "BOOL", false},
		{"bool_TRUE", "true", "BOOLEAN", true},
		{"smallint", "100", "SMALLINT", int64(100)},
		{"tinyint", "5", "TINYINT", int64(5)},
		{"empty_string", "", "VARCHAR", ""},
		{"empty_int", "", "INT", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertXMLValue(tt.value, tt.colType)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPersistTableSchema_DefaultRootTag(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "test",
		RootTag:     "",
		StorageMode: StorageModeFilePerRow,
	}

	tableInfo := &domain.TableInfo{
		Name:    "test",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INT", Primary: true}},
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))

	// When loading back, empty rootTag should default to "Row"
	loadedCfg, err := loadSchemaConfig(tmpDir, "test", filepath.Join(tmpDir, "test", "__schema__.xml"))
	require.NoError(t, err)
	assert.Equal(t, "Row", loadedCfg.RootTag)
}

func TestPersistSingleFile_SuffixS(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "events",
		RootTag:     "Events", // ends with "s" — container tag should be "EventsList"
		StorageMode: StorageModeSingleFile,
	}

	tableInfo := &domain.TableInfo{
		Name: "events",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	rows := []domain.Row{
		{"id": int64(1), "name": "click"},
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	// Verify data can still be loaded
	loadedRows, err := loadSingleFileData(filepath.Join(tmpDir, "events"), tableInfo)
	require.NoError(t, err)
	require.Len(t, loadedRows, 1)
	assert.Equal(t, "click", loadedRows[0]["name"])
}

// --- Benchmark Tests ---

func BenchmarkPersistTableSchema(b *testing.B) {
	tmpDir := b.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "bench",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
			{Name: "email", Type: "VARCHAR", Nullable: true, Unique: true},
			{Name: "age", Type: "INT", Nullable: true},
			{Name: "score", Type: "FLOAT", Nullable: true},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg := &TablePersistConfig{
			BasePath:    tmpDir,
			TableName:   fmt.Sprintf("bench_%d", i),
			RootTag:     "Row",
			StorageMode: StorageModeFilePerRow,
		}
		PersistTableSchema(cfg, tableInfo)
	}
}

func BenchmarkPersistTableData_FilePerRow_10Rows(b *testing.B) {
	benchPersistData(b, StorageModeFilePerRow, 10)
}

func BenchmarkPersistTableData_FilePerRow_100Rows(b *testing.B) {
	benchPersistData(b, StorageModeFilePerRow, 100)
}

func BenchmarkPersistTableData_FilePerRow_1000Rows(b *testing.B) {
	benchPersistData(b, StorageModeFilePerRow, 1000)
}

func BenchmarkPersistTableData_SingleFile_10Rows(b *testing.B) {
	benchPersistData(b, StorageModeSingleFile, 10)
}

func BenchmarkPersistTableData_SingleFile_100Rows(b *testing.B) {
	benchPersistData(b, StorageModeSingleFile, 100)
}

func BenchmarkPersistTableData_SingleFile_1000Rows(b *testing.B) {
	benchPersistData(b, StorageModeSingleFile, 1000)
}

func benchPersistData(b *testing.B, mode StorageMode, numRows int) {
	b.Helper()
	tmpDir := b.TempDir()

	tableInfo := &domain.TableInfo{
		Name: "bench",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "email", Type: "VARCHAR"},
			{Name: "age", Type: "INT"},
		},
	}

	rows := make([]domain.Row, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = domain.Row{
			"id":    int64(i + 1),
			"name":  fmt.Sprintf("User_%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"age":   int64(20 + i%50),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg := &TablePersistConfig{
			BasePath:    tmpDir,
			TableName:   fmt.Sprintf("bench_%d", i),
			RootTag:     "Row",
			StorageMode: mode,
		}
		os.MkdirAll(cfg.TableDir(), 0755)
		PersistTableData(cfg, tableInfo, rows)
	}
}

func BenchmarkLoadTableFromDisk_FilePerRow_100Rows(b *testing.B) {
	benchLoadTable(b, StorageModeFilePerRow, 100)
}

func BenchmarkLoadTableFromDisk_SingleFile_100Rows(b *testing.B) {
	benchLoadTable(b, StorageModeSingleFile, 100)
}

func BenchmarkLoadTableFromDisk_FilePerRow_1000Rows(b *testing.B) {
	benchLoadTable(b, StorageModeFilePerRow, 1000)
}

func BenchmarkLoadTableFromDisk_SingleFile_1000Rows(b *testing.B) {
	benchLoadTable(b, StorageModeSingleFile, 1000)
}

func benchLoadTable(b *testing.B, mode StorageMode, numRows int) {
	b.Helper()
	tmpDir := b.TempDir()

	tableInfo := &domain.TableInfo{
		Name: "bench",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "email", Type: "VARCHAR"},
			{Name: "age", Type: "INT"},
		},
	}

	rows := make([]domain.Row, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = domain.Row{
			"id":    int64(i + 1),
			"name":  fmt.Sprintf("User_%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"age":   int64(20 + i%50),
		}
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "bench",
		RootTag:     "Row",
		StorageMode: mode,
	}
	PersistTableSchema(cfg, tableInfo)
	PersistTableData(cfg, tableInfo, rows)

	indexes := []*IndexMeta{
		{Name: "idx_name", Table: "bench", Type: "btree", Unique: false, Columns: []string{"name"}},
		{Name: "idx_email", Table: "bench", Type: "btree", Unique: true, Columns: []string{"email"}},
	}
	PersistIndexMeta(cfg, indexes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LoadTableFromDisk(cfg)
	}
}

func BenchmarkPersistIndexMeta(b *testing.B) {
	tmpDir := b.TempDir()

	indexes := make([]*IndexMeta, 10)
	for i := 0; i < 10; i++ {
		indexes[i] = &IndexMeta{
			Name:    fmt.Sprintf("idx_%d", i),
			Table:   "bench",
			Type:    "btree",
			Unique:  i%2 == 0,
			Columns: []string{fmt.Sprintf("col_%d", i)},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg := &TablePersistConfig{
			BasePath:  tmpDir,
			TableName: fmt.Sprintf("bench_%d", i),
		}
		os.MkdirAll(cfg.TableDir(), 0755)
		PersistIndexMeta(cfg, indexes)
	}
}

func BenchmarkConvertXMLValue(b *testing.B) {
	b.Run("INT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertXMLValue("12345", "INT")
		}
	})
	b.Run("FLOAT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertXMLValue("3.14159", "FLOAT")
		}
	})
	b.Run("BOOL", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertXMLValue("true", "BOOL")
		}
	})
	b.Run("VARCHAR", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertXMLValue("hello world", "VARCHAR")
		}
	})
}

func BenchmarkFormatValue(b *testing.B) {
	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue("hello world")
		}
	})
	b.Run("int64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue(int64(12345))
		}
	})
	b.Run("float64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue(float64(3.14))
		}
	})
	b.Run("bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue(true)
		}
	})
}

func BenchmarkParseRowXMLFast(b *testing.B) {
	colTypes := map[string]string{
		"id":    "INT",
		"name":  "VARCHAR",
		"email": "VARCHAR",
		"age":   "INT",
	}
	data := []byte(`<Row id="42" name="Alice" email="alice@example.com" age="30" />`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseRowXMLFast(data, colTypes)
	}
}

func BenchmarkParseRowXMLFast_SpecialChars(b *testing.B) {
	colTypes := map[string]string{
		"id":   "INT",
		"data": "VARCHAR",
	}
	data := []byte(`<Row id="1" data="hello &amp; &lt;world&gt; &quot;test&quot;" />`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseRowXMLFast(data, colTypes)
	}
}

func TestParseRowXMLFast(t *testing.T) {
	colTypes := map[string]string{
		"id":    "INT",
		"name":  "VARCHAR",
		"email": "VARCHAR",
	}

	data := []byte(`<Row id="1" name="Alice" email="alice@example.com" />`)
	row, err := parseRowXMLFast(data, colTypes)
	require.NoError(t, err)
	assert.Equal(t, int64(1), row["id"])
	assert.Equal(t, "Alice", row["name"])
	assert.Equal(t, "alice@example.com", row["email"])
}

func TestParseRowXMLFast_SpecialChars(t *testing.T) {
	colTypes := map[string]string{
		"id":   "INT",
		"data": "VARCHAR",
	}

	data := []byte(`<Row id="1" data="hello &amp; &lt;world&gt; &quot;test&quot;" />`)
	row, err := parseRowXMLFast(data, colTypes)
	require.NoError(t, err)
	assert.Equal(t, int64(1), row["id"])
	assert.Equal(t, `hello & <world> "test"`, row["data"])
}

func TestParseRowXMLFast_EmptyAttrs(t *testing.T) {
	colTypes := map[string]string{
		"id": "INT",
	}

	data := []byte(`<Row />`)
	row, err := parseRowXMLFast(data, colTypes)
	require.NoError(t, err)
	assert.Len(t, row, 0)
}

func TestEscapeXMLAttrInto(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"a & b", "a &amp; b"},
		{`say "hi"`, "say &quot;hi&quot;"},
		{"<tag>", "&lt;tag&gt;"},
		{`& < > "`, "&amp; &lt; &gt; &quot;"},
		{"", ""},
		{"no special chars at all", "no special chars at all"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var buf bytes.Buffer
			escapeXMLAttrInto(&buf, tt.input)
			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

// --- LoadTableFromDiskBatched Tests ---

func TestLoadTableFromDiskBatched_FilePerRow(t *testing.T) {
	tmpDir := t.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "users",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	// Persist schema + 10 rows
	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	rows := make([]domain.Row, 10)
	for i := 0; i < 10; i++ {
		rows[i] = domain.Row{"id": int64(i + 1), "name": fmt.Sprintf("user_%d", i+1)}
	}
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	// Load in batches of 3 (expect 4 batches: 3+3+3+1)
	var batches [][]domain.Row
	loadedInfo, indexes, err := LoadTableFromDiskBatched(cfg, 3, func(batch []domain.Row) {
		batches = append(batches, batch)
	})
	require.NoError(t, err)
	assert.Equal(t, "users", loadedInfo.Name)
	assert.Len(t, indexes, 0)

	// Verify batches
	assert.Len(t, batches, 4) // ceil(10/3) = 4
	totalRows := 0
	for _, b := range batches {
		assert.LessOrEqual(t, len(b), 3)
		totalRows += len(b)
	}
	assert.Equal(t, 10, totalRows)
}

func TestLoadTableFromDiskBatched_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "logs",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "msg", Type: "TEXT"},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "logs",
		RootTag:     "Row",
		StorageMode: StorageModeSingleFile,
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	rows := make([]domain.Row, 8)
	for i := 0; i < 8; i++ {
		rows[i] = domain.Row{"id": int64(i + 1), "msg": fmt.Sprintf("message_%d", i+1)}
	}
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	// Load in batches of 5 (expect 2 batches: 5+3)
	var batches [][]domain.Row
	loadedInfo, _, err := LoadTableFromDiskBatched(cfg, 5, func(batch []domain.Row) {
		batches = append(batches, batch)
	})
	require.NoError(t, err)
	assert.Equal(t, "logs", loadedInfo.Name)
	assert.Len(t, batches, 2)
	assert.Len(t, batches[0], 5)
	assert.Len(t, batches[1], 3)

	// Verify data integrity
	allRows := append(batches[0], batches[1]...)
	for i, row := range allRows {
		assert.Equal(t, int64(i+1), row["id"])
		assert.Equal(t, fmt.Sprintf("message_%d", i+1), row["msg"])
	}
}

func TestLoadTableFromDiskBatched_NoData(t *testing.T) {
	tmpDir := t.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "empty",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "empty",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	// Don't persist any data

	var batchCount int
	_, _, err := LoadTableFromDiskBatched(cfg, 10, func(batch []domain.Row) {
		batchCount++
	})
	require.NoError(t, err)
	assert.Equal(t, 0, batchCount)
}

func TestLoadTableFromDiskBatched_SpecialChars(t *testing.T) {
	tmpDir := t.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "special",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "data", Type: "VARCHAR"},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "special",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	rows := []domain.Row{
		{"id": int64(1), "data": `a & b < c > d "e"`},
		{"id": int64(2), "data": "normal"},
	}
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	var loaded []domain.Row
	_, _, err := LoadTableFromDiskBatched(cfg, 100, func(batch []domain.Row) {
		loaded = append(loaded, batch...)
	})
	require.NoError(t, err)
	assert.Len(t, loaded, 2)

	// Find the special chars row
	var found bool
	for _, row := range loaded {
		if row["id"] == int64(1) {
			assert.Equal(t, `a & b < c > d "e"`, row["data"])
			found = true
		}
	}
	assert.True(t, found, "special chars row not found")
}

func TestLoadTableFromDiskBatched_SingleFile_SpecialChars(t *testing.T) {
	tmpDir := t.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "special_sf",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "val", Type: "VARCHAR"},
		},
	}

	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "special_sf",
		RootTag:     "Row",
		StorageMode: StorageModeSingleFile,
	}

	require.NoError(t, PersistTableSchema(cfg, tableInfo))
	rows := []domain.Row{
		{"id": int64(1), "val": `<script>alert("xss")</script>`},
		{"id": int64(2), "val": "a & b"},
	}
	require.NoError(t, PersistTableData(cfg, tableInfo, rows))

	var loaded []domain.Row
	_, _, err := LoadTableFromDiskBatched(cfg, 100, func(batch []domain.Row) {
		loaded = append(loaded, batch...)
	})
	require.NoError(t, err)
	assert.Len(t, loaded, 2)
}

func BenchmarkLoadBatched_FilePerRow_1000Rows(b *testing.B) {
	tmpDir := b.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "bench",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "value", Type: "FLOAT"},
			{Name: "active", Type: "BOOL"},
		},
	}
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "bench",
		RootTag:     "Row",
		StorageMode: StorageModeFilePerRow,
	}
	PersistTableSchema(cfg, tableInfo)

	rows := make([]domain.Row, 1000)
	for i := range rows {
		rows[i] = domain.Row{
			"id": int64(i), "name": fmt.Sprintf("name_%d", i),
			"value": 3.14, "active": true,
		}
	}
	PersistTableData(cfg, tableInfo, rows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LoadTableFromDiskBatched(cfg, 4096, func(batch []domain.Row) {})
	}
}

func BenchmarkLoadBatched_SingleFile_1000Rows(b *testing.B) {
	tmpDir := b.TempDir()
	tableInfo := &domain.TableInfo{
		Name: "bench",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "value", Type: "FLOAT"},
			{Name: "active", Type: "BOOL"},
		},
	}
	cfg := &TablePersistConfig{
		BasePath:    tmpDir,
		TableName:   "bench",
		RootTag:     "Row",
		StorageMode: StorageModeSingleFile,
	}
	PersistTableSchema(cfg, tableInfo)

	rows := make([]domain.Row, 1000)
	for i := range rows {
		rows[i] = domain.Row{
			"id": int64(i), "name": fmt.Sprintf("name_%d", i),
			"value": 3.14, "active": true,
		}
	}
	PersistTableData(cfg, tableInfo, rows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LoadTableFromDiskBatched(cfg, 4096, func(batch []domain.Row) {})
	}
}
