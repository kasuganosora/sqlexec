package dataaccess

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataService_Insert_WithReplace(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"id":1,
		"name": "Alice",
	}
	
	_, err := service.Insert(ctx, "test_table", data)
	require.NoError(t, err)
}

func TestDataService_Update_WithNilFilter(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"name": "Updated",
	}
	
	err := service.Update(ctx, "test_table", data, nil)
	require.NoError(t, err)
}

func TestDataService_Query_WithNoColumns(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{},
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_WithNoFilters(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id", "name"},
		Filters:       []domain.Filter{},
		Offset:        0,
		Limit:         10,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Filter_WithZeroOffset(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
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

func TestDataService_GetTableInfo_Success(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	
	info, err := service.GetTableInfo(ctx, "test_table")
	require.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, "test_table", info.Name)
}

func TestRouter_GetRoutes_Empty(t *testing.T) {
	router := NewRouter()
	
	routes := router.GetRoutes()
	assert.Equal(t, 0, len(routes))
}

func TestManager_GetStats_Empty(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)
	
	stats := manager.GetStats()
	assert.Equal(t, 1, stats["data_sources"])
	assert.Equal(t, 0, stats["connections"])
}

func TestDataService_Insert_Multiple(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"id": 1,
		"name": "Alice",
	}
	
	_, err := service.Insert(ctx, "test_table", data)
	require.NoError(t, err)
}

func TestDataService_Query_ManyColumns(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	// 查询5个列
	options := &QueryOptions{
		SelectColumns: []string{"id", "name", "age", "active", "score"},
		Offset:        0,
		Limit:         100,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_SingleColumn(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	// 只查询一列
	options := &QueryOptions{
		SelectColumns: []string{"id"},
		Offset:        0,
		Limit:         10,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_ThreeColumns(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	// 查询3列
	options := &QueryOptions{
		SelectColumns: []string{"id", "name", "age"},
		Offset:        0,
		Limit:         10,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_OffsetAndLimit(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	// 查询带偏移和限制
	options := &QueryOptions{
		SelectColumns: []string{"id", "name"},
		Offset:        50,
		Limit:         20,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Filter_WithFilter(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	filter := domain.Filter{
		Field:    "id",
		Operator: ">",
		Value:    10,
	}
	
	rows, total, err := service.Filter(ctx, "test_table", filter, 0, 10)
	require.NoError(t, err)
	assert.NotNil(t, rows)
	assert.Equal(t, int64(0), total)
}

func TestDataService_Filter_MultipleConditions(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	filter := domain.Filter{
		Field:    "active",
		Operator: "=",
		Value:    true,
	}
	
	rows, _, err := service.Filter(ctx, "test_table", filter, 10, 50)
	require.NoError(t, err)
	assert.NotNil(t, rows)
}

func TestDataService_Update_MultipleFields(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"name":   "Bob",
		"age":    25,
		"active": false,
	}
	where := &domain.Filter{
		Field:    "id",
		Operator: "=",
		Value:    2,
	}
	
	err := service.Update(ctx, "test_table", data, where)
	require.NoError(t, err)
}

func TestDataService_Delete_WithCondition(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	where := &domain.Filter{
		Field:    "age",
		Operator: "<",
		Value:    18,
	}
	
	err := service.Delete(ctx, "test_table", where)
	require.NoError(t, err)
}

func TestManager_RegisterDataSource_AndGet(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)
	
	newDs := &TestDataSource{}
	err := manager.RegisterDataSource("new_source", newDs)
	require.NoError(t, err)
	
	retrievedDs, err := manager.GetDataSource("new_source")
	require.NoError(t, err)
	assert.Equal(t, newDs, retrievedDs)
}

func TestDataService_Query_WithLimitZero(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	options := &QueryOptions{
		SelectColumns: []string{"id"},
		Limit:         0,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_WithOffsetZero(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	options := &QueryOptions{
		SelectColumns: []string{"id"},
		Offset:        0,
		Limit:         10,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_OffsetLimit(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	options := &QueryOptions{
		SelectColumns: []string{"id", "name"},
		Offset:        0,
		Limit:         10,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_OffsetLimitColumns(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	
	options := &QueryOptions{
		SelectColumns: []string{"id", "name"},
		Offset:        5,
		Limit:         20,
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Filter_OffsetLimit(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	filter := domain.Filter{
		Field:    "id",
		Operator: ">",
		Value:    0,
	}
	
	rows, total, err := service.Filter(ctx, "test_table", filter, 10, 100)
	require.NoError(t, err)
	assert.NotNil(t, rows)
	assert.Equal(t, int64(0), total)
}
