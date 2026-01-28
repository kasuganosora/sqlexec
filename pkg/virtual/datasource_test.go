package virtual

import (
	"context"
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
	assert.Equal(t, "virtual", config.Type)
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
