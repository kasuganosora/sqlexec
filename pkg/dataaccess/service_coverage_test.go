package dataaccess

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataService_selectColumns_AllColumns(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	// selectColumns 是私有方法，这里无法直接测试
	// 但可以通过 Query 方法间接测试
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{},
	}
	
	_, err := service.Query(ctx, "test_table", options)
	assert.NoError(t, err)
}

func TestDataService_selectColumns_SpecificColumns(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id"},
	}
	
	_, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
}

func TestDataService_Query_WithOptions(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id", "name"},
		Offset:        0,
		Limit:         10,
	}
	
	result, err := service.Query(ctx, "test_table", options)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestDataService_Update_WithNilWhere(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"name": "Updated",
	}
	
	err := service.Update(ctx, "test_table", data, nil)
	require.NoError(t, err)
}

func TestDataService_Delete_WithNilWhere(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	
	err := service.Delete(ctx, "test_table", nil)
	require.NoError(t, err)
}

func TestDataService_Filter_WithOffsetLimit(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	filter := domain.Filter{
		Field:    "id",
		Operator: ">",
		Value:    0,
	}
	
	rows, _, err := service.Filter(ctx, "test_table", filter, 5, 10)
	require.NoError(t, err)
	assert.NotNil(t, rows)
}

func TestDataService_Insert_EmptyData(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	data := map[string]interface{}{}
	
	err := service.Insert(ctx, "test_table", data)
	require.NoError(t, err)
}

func TestDataService_Update_EmptyData(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	data := map[string]interface{}{}
	where := &domain.Filter{
		Field:    "id",
		Operator: "=",
		Value:    1,
	}
	
	err := service.Update(ctx, "test_table", data, where)
	require.NoError(t, err)
}

func TestDataService_Filter_WithEmptyFilter(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	filter := domain.Filter{}
	
	rows, _, err := service.Filter(ctx, "test_table", filter, 0, 10)
	require.NoError(t, err)
	assert.NotNil(t, rows)
}
