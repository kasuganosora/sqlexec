package executor

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockDataService struct {
	queryCalled bool
	queryResult *domain.QueryResult
	queryError  error
}

func (m *MockDataService) Query(ctx context.Context, tableName string, options *dataaccess.QueryOptions) (*domain.QueryResult, error) {
	m.queryCalled = true
	if m.queryError != nil {
		return nil, m.queryError
	}
	return m.queryResult, nil
}

func (m *MockDataService) Filter(ctx context.Context, tableName string, filter domain.Filter, offset, limit int) ([]domain.Row, int64, error) {
	return nil, 0, nil
}

func (m *MockDataService) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return nil, nil
}

func (m *MockDataService) Insert(ctx context.Context, tableName string, data map[string]interface{}) error {
	return nil
}

func (m *MockDataService) Update(ctx context.Context, tableName string, data map[string]interface{}, where *domain.Filter) error {
	return nil
}

func (m *MockDataService) Delete(ctx context.Context, tableName string, where *domain.Filter) error {
	return nil
}

func TestNewExecutor(t *testing.T) {
	mockService := &MockDataService{}
	executor := NewExecutor(mockService)
	
	assert.NotNil(t, executor)
	assert.NotNil(t, executor.(*BaseExecutor).runtime)
}

func TestBaseExecutor_Execute_UnsupportedPlanType(t *testing.T) {
	mockService := &MockDataService{}
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test",
		Type: "UnknownType",
	}
	
	_, err := executor.Execute(context.Background(), testPlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported plan type")
}

func TestBaseExecutor_Execute_TableScan(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice"},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test_scan",
		Type:  plan.TypeTableScan,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "string"},
			},
		},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Selection(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{
			{"id": 1},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	childPlan := &plan.Plan{
		ID:   "child_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	testPlan := &plan.Plan{
		ID:   "test_selection",
		Type:  plan.TypeSelection,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Config:   &plan.SelectionConfig{},
		Children: []*plan.Plan{childPlan},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Projection(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"name": "Alice"},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	childPlan := &plan.Plan{
		ID:   "child_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "name", Type: "string"},
			},
		},
	}
	
	testPlan := &plan.Plan{
		ID:   "test_projection",
		Type:  plan.TypeProjection,
		OutputSchema: []types.ColumnInfo{
			{Name: "name", Type: "string"},
		},
		Config:   &plan.ProjectionConfig{},
		Children: []*plan.Plan{childPlan},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Limit(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{
			{"id": 1},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	childPlan := &plan.Plan{
		ID:   "child_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	testPlan := &plan.Plan{
		ID:   "test_limit",
		Type:  plan.TypeLimit,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Config: &plan.LimitConfig{},
		Children: []*plan.Plan{childPlan},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Aggregate(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "count", Type: "int"},
		},
		Rows: []domain.Row{
			{"count": 10},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	childPlan := &plan.Plan{
		ID:   "child_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	testPlan := &plan.Plan{
		ID:   "test_aggregate",
		Type:  plan.TypeAggregate,
		OutputSchema: []types.ColumnInfo{
			{Name: "count", Type: "int"},
		},
		Config:   &plan.AggregateConfig{},
		Children: []*plan.Plan{childPlan},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_HashJoin(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice"},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	leftPlan := &plan.Plan{
		ID:   "left_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	rightPlan := &plan.Plan{
		ID:   "right_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	testPlan := &plan.Plan{
		ID:   "test_hashjoin",
		Type:  plan.TypeHashJoin,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Config:   &plan.HashJoinConfig{},
		Children: []*plan.Plan{leftPlan, rightPlan},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Insert(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test_insert",
		Type:  plan.TypeInsert,
		OutputSchema: []types.ColumnInfo{},
		Config: &plan.InsertConfig{},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Update(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test_update",
		Type:  plan.TypeUpdate,
		OutputSchema: []types.ColumnInfo{},
		Config: &plan.UpdateConfig{},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Delete(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test_delete",
		Type:  plan.TypeDelete,
		OutputSchema: []types.ColumnInfo{},
		Config: &plan.DeleteConfig{},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Sort(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{
			{"id": 1},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test_sort",
		Type:  plan.TypeSort,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Config: &plan.SortConfig{},
		Children: []*plan.Plan{
			{
				ID:   "child_scan",
				Type:  plan.TypeTableScan,
				Config: &plan.TableScanConfig{
					TableName: "test_table",
					Columns: []types.ColumnInfo{
						{Name: "id", Type: "int"},
					},
				},
			},
		},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBaseExecutor_Execute_Union(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryResult = &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{
			{"id": 1},
		},
	}
	
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	childPlan := &plan.Plan{
		ID:   "child_scan",
		Type:  plan.TypeTableScan,
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	testPlan := &plan.Plan{
		ID:   "test_union",
		Type:  plan.TypeUnion,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Config: &plan.UnionConfig{},
		Children: []*plan.Plan{childPlan, childPlan},
	}
	
	result, err := executor.Execute(context.Background(), testPlan)
	require.NoError(t, err)
	assert.NotNil(t, result)
}
