package badger

import (
	"context"
	"os"
	"testing"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerDataSource_BasicCRUD(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"data_dir":  tmpDir,
			"in_memory": true, // Use in-memory mode for tests
		},
	}

	ds := NewBadgerDataSource(config)

	// Connect
	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Verify connection
	assert.True(t, ds.IsConnected())
	assert.True(t, ds.IsWritable())

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)", Nullable: false},
			{Name: "email", Type: "VARCHAR(255)", Nullable: true},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Get tables
	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "users")

	// Get table info
	info, err := ds.GetTableInfo(ctx, "users")
	require.NoError(t, err)
	assert.Equal(t, "users", info.Name)
	assert.Len(t, info.Columns, 3)

	// Insert rows
	rows := []domain.Row{
		{"id": "user-1", "name": "Alice", "email": "alice@example.com"},
		{"id": "user-2", "name": "Bob", "email": "bob@example.com"},
	}
	inserted, err := ds.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	// Query rows
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)

	// Query with filter
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: "user-1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Alice", result.Rows[0]["name"])

	// Update row
	updates := domain.Row{"name": "Alice Updated"}
	updated, err := ds.Update(ctx, "users", []domain.Filter{
		{Field: "id", Operator: "=", Value: "user-1"},
	}, updates, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), updated)

	// Verify update
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: "user-1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Alice Updated", result.Rows[0]["name"])

	// Delete row
	deleted, err := ds.Delete(ctx, "users", []domain.Filter{
		{Field: "id", Operator: "=", Value: "user-1"},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify delete
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "Bob", result.Rows[0]["name"])

	// Drop table
	err = ds.DropTable(ctx, "users")
	require.NoError(t, err)

	tables, err = ds.GetTables(ctx)
	require.NoError(t, err)
	assert.NotContains(t, tables, "users")
}

func TestBadgerDataSource_TruncateTable(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source with in-memory mode
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"data_dir":  tmpDir,
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)

	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Insert rows
	rows := []domain.Row{
		{"id": "p-1", "name": "Product 1"},
		{"id": "p-2", "name": "Product 2"},
		{"id": "p-3", "name": "Product 3"},
	}
	_, err = ds.Insert(ctx, "products", rows, nil)
	require.NoError(t, err)

	// Verify rows
	result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)

	// Truncate
	err = ds.TruncateTable(ctx, "products")
	require.NoError(t, err)

	// Verify empty
	result, err = ds.Query(ctx, "products", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// Table should still exist
	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "products")
}

func TestKeyEncoding(t *testing.T) {
	encoder := NewKeyEncoder()

	// Test table key
	tableKey := encoder.EncodeTableKey("users")
	assert.Equal(t, []byte("table:users"), tableKey)

	tableName, ok := encoder.DecodeTableKey(tableKey)
	assert.True(t, ok)
	assert.Equal(t, "users", tableName)

	// Test row key
	rowKey := encoder.EncodeRowKey("users", "user-1")
	assert.Equal(t, []byte("row:users:user-1"), rowKey)

	tableName, pk, ok := encoder.DecodeRowKey(rowKey)
	assert.True(t, ok)
	assert.Equal(t, "users", tableName)
	assert.Equal(t, "user-1", pk)

	// Test row prefix
	prefix := encoder.EncodeRowPrefix("users")
	assert.Equal(t, []byte("row:users:"), prefix)

	// Test index key
	idxKey := encoder.EncodeIndexKey("users", "email", "test@example.com")
	assert.Equal(t, []byte("idx:users:email:test@example.com"), idxKey)

	tableName, colName, value, ok := encoder.DecodeIndexKey(idxKey)
	assert.True(t, ok)
	assert.Equal(t, "users", tableName)
	assert.Equal(t, "email", colName)
	assert.Equal(t, "test@example.com", value)

	// Test sequence key
	seqKey := encoder.EncodeSeqKey("users", "id")
	assert.Equal(t, []byte("seq:users:id"), seqKey)

	tableName, colName, ok = encoder.DecodeSeqKey(seqKey)
	assert.True(t, ok)
	assert.Equal(t, "users", tableName)
	assert.Equal(t, "id", colName)

	// Test config key
	cfgKey := encoder.EncodeConfigKey("users")
	assert.Equal(t, []byte("config:users"), cfgKey)

	tableName, ok = encoder.DecodeConfigKey(cfgKey)
	assert.True(t, ok)
	assert.Equal(t, "users", tableName)
}

func TestRowCodec(t *testing.T) {
	codec := NewRowCodec()

	// Test encode/decode
	row := domain.Row{
		"id":    "user-1",
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	}

	data, err := codec.Encode(row)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := codec.Decode(data)
	require.NoError(t, err)
	assert.Equal(t, row["id"], decoded["id"])
	assert.Equal(t, row["name"], decoded["name"])
	assert.Equal(t, row["email"], decoded["email"])
}

func TestPrimaryKeyGenerator(t *testing.T) {
	generator := NewPrimaryKeyGenerator()

	// Test format int key
	key := generator.FormatIntKey(12345)
	assert.Equal(t, "00000000000000012345", key)

	// Test parse int key
	id, err := generator.ParseIntKey(key)
	require.NoError(t, err)
	assert.Equal(t, int64(12345), id)

	// Test generate from row
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	row := domain.Row{"id": "user-123", "name": "Test"}
	pk, err := generator.GenerateFromRow(tableInfo, row)
	require.NoError(t, err)
	assert.Equal(t, "user-123", pk)
}

func TestBadgerDataSource_OrderBy(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)

	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
			{Name: "price", Type: "INT"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Insert rows
	rows := []domain.Row{
		{"id": "p-3", "name": "Product C", "price": 300},
		{"id": "p-1", "name": "Product A", "price": 100},
		{"id": "p-2", "name": "Product B", "price": 200},
	}
	_, err = ds.Insert(ctx, "products", rows, nil)
	require.NoError(t, err)

	// Test ORDER BY ASC
	result, err := ds.Query(ctx, "products", &domain.QueryOptions{
		OrderBy: "price",
		Order:   "ASC",
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	assert.Equal(t, "p-1", result.Rows[0]["id"])
	assert.Equal(t, "p-2", result.Rows[1]["id"])
	assert.Equal(t, "p-3", result.Rows[2]["id"])

	// Test ORDER BY DESC
	result, err = ds.Query(ctx, "products", &domain.QueryOptions{
		OrderBy: "price",
		Order:   "DESC",
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	assert.Equal(t, "p-3", result.Rows[0]["id"])
	assert.Equal(t, "p-2", result.Rows[1]["id"])
	assert.Equal(t, "p-1", result.Rows[2]["id"])
}

func TestBadgerDataSource_LikePattern(t *testing.T) {
	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create data source
	config := &domain.DataSourceConfig{
		Name: "test",
		Type: domain.DataSourceTypeMemory,
		Options: map[string]interface{}{
			"in_memory": true,
		},
	}

	ds := NewBadgerDataSource(config)

	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	defer ds.Close(ctx)

	// Create table
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "VARCHAR(64)", Primary: true},
			{Name: "email", Type: "VARCHAR(255)"},
		},
	}
	err = ds.CreateTable(ctx, tableInfo)
	require.NoError(t, err)

	// Insert rows
	rows := []domain.Row{
		{"id": "1", "email": "alice@example.com"},
		{"id": "2", "email": "bob@test.org"},
		{"id": "3", "email": "charlie@example.org"},
		{"id": "4", "email": "david@example.com"},
	}
	_, err = ds.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)

	// Test LIKE with %
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "email", Operator: "LIKE", Value: "%example.com"},
		},
	})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2) // alice, david (charlie has .org not .com)

	// Test LIKE with _ (single character)
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "LIKE", Value: "_"},
		},
	})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 4) // all single character IDs
}
