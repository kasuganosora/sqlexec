package dataaccess

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockDataSource struct {
	insertCalled bool
	queryCalled  bool
	tableExists  map[string]bool
}

func (m *MockDataSource) Connect(ctx context.Context) error {
	return nil
}

func (m *MockDataSource) Close(ctx context.Context) error {
	return nil
}

func (m *MockDataSource) IsConnected() bool {
	return true
}

func (m *MockDataSource) IsWritable() bool {
	return true
}

func (m *MockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
	}
}

func (m *MockDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{"table1", "table2"}, nil
}

func (m *MockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	m.queryCalled = true
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice"},
		},
	}, nil
}

func (m *MockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	m.insertCalled = true
	return int64(len(rows)), nil
}

func (m *MockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

func (m *MockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}

func (m *MockDataSource) CreateTable(ctx context.Context, info *domain.TableInfo) error {
	if m.tableExists == nil {
		m.tableExists = make(map[string]bool)
	}
	m.tableExists[info.Name] = true
	return nil
}

func (m *MockDataSource) DropTable(ctx context.Context, tableName string) error {
	delete(m.tableExists, tableName)
	return nil
}

func (m *MockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}

func (m *MockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}, nil
}

func (m *MockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

func TestNewDataService(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	assert.NotNil(t, service)
	assert.NotNil(t, service.(*DataService).dataSource)
}

func TestDataService_Query(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id", "name"},
	}
	
	result, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, mockDataSource.queryCalled)
}

func TestDataService_Insert(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"id":   1,
		"name": "Alice",
	}
	
	err := service.Insert(ctx, "test_table", data)
	require.NoError(t, err)
	assert.True(t, mockDataSource.insertCalled)
}

func TestDataService_Update(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"name": "Bob",
	}
	where := []domain.Filter{
		{
			Field:    "id",
			Operator: "=",
			Value:    1,
		},
	}
	
	err := service.Update(ctx, "test_table", data, &where[0])
	require.NoError(t, err)
}

func TestDataService_Delete(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	ctx := context.Background()
	where := []domain.Filter{
		{
			Field:    "id",
			Operator: "=",
			Value:    1,
		},
	}
	
	err := service.Delete(ctx, "test_table", &where[0])
	require.NoError(t, err)
}

func TestDataService_GetTableInfo(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	ctx := context.Background()
	
	info, err := service.GetTableInfo(ctx, "test_table")
	require.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, "test_table", info.Name)
}

func TestDataService_Filter(t *testing.T) {
	mockDataSource := &MockDataSource{}
	service := NewDataService(mockDataSource)
	
	ctx := context.Background()
	filter := domain.Filter{
		Field:    "id",
		Operator: ">",
		Value:    0,
	}
	
	rows, total, err := service.Filter(ctx, "test_table", filter, 0, 10)
	require.NoError(t, err)
	assert.NotNil(t, rows)
	assert.Equal(t, int64(0), total)
}
