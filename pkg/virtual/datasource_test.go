package virtual

import (
	"context"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simple mock for testing
type mockTable struct{}

func (m *mockTable) GetName() string {
	return "mock_table"
}

func (m *mockTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR(255)"},
	}
}

func (m *mockTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	rows := []domain.Row{{"id": 1, "name": "test"}}
	return &domain.QueryResult{
		Columns: m.GetSchema(),
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

func (m *mockTable) ListVirtualTables() []string {
	return []string{"mock_table"}
}

func (m *mockTable) GetVirtualTable(name string) (VirtualTable, error) {
	if name == "mock_table" {
		return m, nil
	}
	return nil, fmt.Errorf("table %s not found", name)
}

func (m *mockTable) HasTable(name string) bool {
	return name == "mock_table"
}

func TestNewVirtualDataSource(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	assert.NotNil(t, ds)
	assert.Equal(t, table, ds.provider)
}

func TestVirtualDataSourceConnect(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	err := ds.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, ds.IsConnected())
}

func TestVirtualDataSourceGetConfig(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	config := ds.GetConfig()
	assert.NotNil(t, config)
	assert.Equal(t, "virtual", string(config.Type))
}

func TestVirtualDataSourceGetTables(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)
	ctx := context.Background()

	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
}

func TestVirtualDataSourceReadOnly(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	// All write operations should return errors
	ctx := context.Background()
	
	_, err := ds.Insert(ctx, "test", []domain.Row{{"data": "test"}}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
	
	_, err = ds.Update(ctx, "test", []domain.Filter{}, domain.Row{"data": "test"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
	
	_, err = ds.Delete(ctx, "test", []domain.Filter{}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestVirtualDataSourceClose(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	err := ds.Close(ctx)
	require.NoError(t, err)
}

func TestVirtualDataSourceIsWritable(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	assert.False(t, ds.IsWritable())
}

func TestVirtualDataSourceGetTableInfo(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	tableInfo, err := ds.GetTableInfo(ctx, "mock_table")
	require.NoError(t, err)
	assert.NotNil(t, tableInfo)
	assert.Equal(t, "mock_table", tableInfo.Name)
	assert.Equal(t, "information_schema", tableInfo.Schema)
	assert.Len(t, tableInfo.Columns, 2)
}

func TestVirtualDataSourceGetTableInfoNotFound(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	_, err := ds.GetTableInfo(ctx, "nonexistent_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestVirtualDataSourceQuery(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	result, err := ds.Query(ctx, "mock_table", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Total)
}

func TestVirtualDataSourceQueryNotFound(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	_, err := ds.Query(ctx, "nonexistent_table", &domain.QueryOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestVirtualDataSourceCreateTable(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	err := ds.CreateTable(ctx, &domain.TableInfo{Name: "new_table"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestVirtualDataSourceDropTable(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	err := ds.DropTable(ctx, "mock_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestVirtualDataSourceTruncateTable(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	err := ds.TruncateTable(ctx, "mock_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestVirtualDataSourceExecute(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	_, err := ds.Execute(ctx, "SELECT * FROM mock_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestVirtualDataSourceIsConnected(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	assert.True(t, ds.IsConnected())
}

func TestVirtualDataSourceMultipleTables(t *testing.T) {
	// 创建一个支持多表的mock
	multiTableMock := &multiMockTable{}
	ds := NewVirtualDataSource(multiTableMock)

	ctx := context.Background()
	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, "table1")
	assert.Contains(t, tables, "table2")
}

func TestVirtualDataSourceQueryWithFilters(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	result, err := ds.Query(ctx, "mock_table", &domain.QueryOptions{Filters: filters})
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// multiMockTable 支持多表的mock实现
type multiMockTable struct{}

func (m *multiMockTable) GetName() string {
	return "multi_table"
}

func (m *multiMockTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR(255)"},
	}
}

func (m *multiMockTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	rows := []domain.Row{{"id": 1, "name": "test"}}
	return &domain.QueryResult{
		Columns: m.GetSchema(),
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

func (m *multiMockTable) ListVirtualTables() []string {
	return []string{"table1", "table2"}
}

func (m *multiMockTable) GetVirtualTable(name string) (VirtualTable, error) {
	return &mockTable{}, nil
}

func (m *multiMockTable) HasTable(name string) bool {
	return name == "table1" || name == "table2"
}

