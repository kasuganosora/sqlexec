package virtual

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestVirtualTableInterface(t *testing.T) {
	table := &mockTable{}

	assert.Implements(t, (*virtual.VirtualTable)(nil), table)
}

func TestVirtualTableGetName(t *testing.T) {
	table := &mockTable{}

	assert.Equal(t, "mock_table", table.GetName())
}

func TestVirtualTableGetSchema(t *testing.T) {
	columns := []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR(255)"},
	}

	assert.Equal(t, columns, table.GetSchema())
}

func TestVirtualTableQuery(t *testing.T) {
	table := &mockTable{}
	ctx := context.Background()

	// Query with no filters
	result1, err1 := table.Query(ctx, nil, nil)
	require.NoError(t, err1)
	assert.Equal(t, int64(1), result1.Total)
	assert.Len(t, result1.Rows, 1)

	// Query with limit
	options := &domain.QueryOptions{Limit: 10}
	result2, err2 := table.Query(ctx, nil, options)
	require.NoError(t, err2)
	assert.Equal(t, int64(1), result2.Total)
}

func TestVirtualTableQueryWithFilters(t *testing.T) {
	table := &mockTable{}
	ctx := context.Background()

	// Query should not support filters in this simple mock
	_, err := table.Query(ctx, []domain.Filter{{Field: "id", Operator: ">", Value: 0}}, nil)
	assert.Error(t, err)
}

// Simple mock implementation
type mockTable struct {
	columns []domain.ColumnInfo
}

func (m *mockTable) GetName() string {
	return "mock_table"
}

func (m *mockTable) GetSchema() []domain.ColumnInfo {
	return m.columns
}

func (m *mockTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	rows := []domain.Row{{"id": 1}}
	if len(filters) > 0 {
		return nil, errors.New("filters not supported")
	}
	return &domain.QueryResult{
		Columns: m.columns,
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}
