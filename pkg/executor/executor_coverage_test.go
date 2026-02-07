package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestBaseExecutor_Execute_BuildOperatorError(t *testing.T) {
	mockService := &MockDataService{}
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	// 测试一个会导致 buildOperator 失败的计划
	testPlan := &plan.Plan{
		ID:   "test_error",
		Type:  plan.TypeTableScan,
		OutputSchema: []types.ColumnInfo{},
		Config: nil, // Config 为 nil 应该返回错误
	}
	
	_, err := executor.Execute(context.Background(), testPlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "build operator failed")
}

func TestBaseExecutor_Execute_OperatorExecutionError(t *testing.T) {
	mockService := &MockDataService{}
	mockService.queryError = errors.New("mock execution error")
	executor := NewExecutor(mockService).(*BaseExecutor)
	
	testPlan := &plan.Plan{
		ID:   "test_scan",
		Type:  plan.TypeTableScan,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Config: &plan.TableScanConfig{
			TableName: "test_table",
			Columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		},
	}
	
	_, err := executor.Execute(context.Background(), testPlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute operator failed")
}
