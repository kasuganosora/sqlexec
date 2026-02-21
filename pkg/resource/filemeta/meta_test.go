package filemeta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaPath(t *testing.T) {
	assert.Equal(t, "/data/test.csv.sqlexec_meta", MetaPath("/data/test.csv"))
	assert.Equal(t, "data.jsonl.sqlexec_meta", MetaPath("data.jsonl"))
}

func TestSaveLoad_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "test.csv.sqlexec_meta")

	original := &FileMeta{
		Schema: SchemaMeta{
			TableName: "csv_data",
			Columns: []ColumnMeta{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
				{Name: "score", Type: "float64", Nullable: true},
			},
		},
		Indexes: []IndexMeta{
			{Name: "idx_id", Table: "csv_data", Type: "btree", Unique: true, Columns: []string{"id"}},
			{Name: "idx_name", Table: "csv_data", Type: "hash", Unique: false, Columns: []string{"name"}},
			{Name: "idx_composite", Table: "csv_data", Type: "btree", Unique: false, Columns: []string{"name", "score"}},
		},
	}

	require.NoError(t, Save(metaPath, original))

	loaded, err := Load(metaPath)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	assert.Equal(t, original.Schema.TableName, loaded.Schema.TableName)
	assert.Equal(t, original.Schema.Columns, loaded.Schema.Columns)
	assert.Equal(t, original.Indexes, loaded.Indexes)
}

func TestLoad_NonExistent(t *testing.T) {
	meta, err := Load(filepath.Join(t.TempDir(), "nonexistent.sqlexec_meta"))
	assert.NoError(t, err)
	assert.Nil(t, meta)
}

func TestSaveLoad_EmptyIndexes(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "test.json.sqlexec_meta")

	original := &FileMeta{
		Schema: SchemaMeta{
			TableName: "json_data",
			Columns: []ColumnMeta{
				{Name: "id", Type: "int64", Nullable: false},
			},
		},
		Indexes: nil,
	}

	require.NoError(t, Save(metaPath, original))

	loaded, err := Load(metaPath)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	assert.Equal(t, "json_data", loaded.Schema.TableName)
	assert.Len(t, loaded.Schema.Columns, 1)
	assert.Nil(t, loaded.Indexes)
}

func TestLoad_CorruptFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "corrupt.sqlexec_meta")

	require.NoError(t, os.WriteFile(metaPath, []byte("not a gob"), 0644))

	meta, err := Load(metaPath)
	assert.Error(t, err)
	assert.Nil(t, meta)
}
