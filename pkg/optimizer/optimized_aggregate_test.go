package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestOptimizedAggregateCount 测试 COUNT 聚合
func TestOptimizedAggregateCount(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "value": int64(10)},
			{"id": int64(2), "value": int64(20)},
			{"id": int64(3), "value": int64(30)},
		},
		Total: 3,
	}

	// 创建聚合函数：COUNT(*)
	aggFuncs := []*AggregationItem{
		{
			Type:  Count,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "*"},
			Alias: "count_star",
		},
	}

	// 创建 OptimizedAggregate（无 GROUP BY）
	agg := NewOptimizedAggregate(aggFuncs, []string{}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	countVal, ok := result.Rows[0]["count_star"].(int64)
	if !ok {
		t.Fatalf("Expected count_star to be int64")
	}

	if countVal != 3 {
		t.Errorf("Expected count=3, got %d", countVal)
	}
}

// TestOptimizedAggregateSum 测试 SUM 聚合
func TestOptimizedAggregateSum(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "value": int64(10)},
			{"id": int64(2), "value": int64(20)},
			{"id": int64(3), "value": int64(30)},
		},
		Total: 3,
	}

	// 创建聚合函数：SUM(value)
	aggFuncs := []*AggregationItem{
		{
			Type:  Sum,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "sum_value",
		},
	}

	// 创建 OptimizedAggregate（无 GROUP BY）
	agg := NewOptimizedAggregate(aggFuncs, []string{}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	sumVal, ok := result.Rows[0]["sum_value"].(float64)
	if !ok {
		t.Fatalf("Expected sum_value to be float64")
	}

	expectedSum := 10.0 + 20.0 + 30.0
	if sumVal != expectedSum {
		t.Errorf("Expected sum=%f, got %f", expectedSum, sumVal)
	}
}

// TestOptimizedAggregateAvg 测试 AVG 聚合
func TestOptimizedAggregateAvg(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "value": int64(10)},
			{"id": int64(2), "value": int64(20)},
			{"id": int64(3), "value": int64(30)},
		},
		Total: 3,
	}

	// 创建聚合函数：AVG(value)
	aggFuncs := []*AggregationItem{
		{
			Type:  Avg,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "avg_value",
		},
	}

	// 创建 OptimizedAggregate（无 GROUP BY）
	agg := NewOptimizedAggregate(aggFuncs, []string{}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	avgVal, ok := result.Rows[0]["avg_value"].(float64)
	if !ok {
		t.Fatalf("Expected avg_value to be float64")
	}

	expectedAvg := (10.0 + 20.0 + 30.0) / 3.0
	if avgVal != expectedAvg {
		t.Errorf("Expected avg=%f, got %f", expectedAvg, avgVal)
	}
}

// TestOptimizedAggregateMin 测试 MIN 聚合
func TestOptimizedAggregateMin(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "value": int64(30)},
			{"id": int64(2), "value": int64(10)},
			{"id": int64(3), "value": int64(20)},
		},
		Total: 3,
	}

	// 创建聚合函数：MIN(value)
	aggFuncs := []*AggregationItem{
		{
			Type:  Min,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "min_value",
		},
	}

	// 创建 OptimizedAggregate（无 GROUP BY）
	agg := NewOptimizedAggregate(aggFuncs, []string{}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	minVal := result.Rows[0]["min_value"]
	if minVal != int64(10) {
		t.Errorf("Expected min=10, got %v", minVal)
	}
}

// TestOptimizedAggregateMax 测试 MAX 聚合
func TestOptimizedAggregateMax(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "value": int64(30)},
			{"id": int64(2), "value": int64(10)},
			{"id": int64(3), "value": int64(20)},
		},
		Total: 3,
	}

	// 创建聚合函数：MAX(value)
	aggFuncs := []*AggregationItem{
		{
			Type:  Max,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "max_value",
		},
	}

	// 创建 OptimizedAggregate（无 GROUP BY）
	agg := NewOptimizedAggregate(aggFuncs, []string{}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	maxVal := result.Rows[0]["max_value"]
	if maxVal != int64(30) {
		t.Errorf("Expected max=30, got %v", maxVal)
	}
}

// TestOptimizedAggregateGroupBy 测试 GROUP BY 聚合
func TestOptimizedAggregateGroupBy(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "category", Type: "string"},
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(20)},
			{"category": "B", "value": int64(30)},
			{"category": "B", "value": int64(40)},
		},
		Total: 4,
	}

	// 创建聚合函数：SUM(value) GROUP BY category
	aggFuncs := []*AggregationItem{
		{
			Type:  Sum,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "sum_value",
		},
	}

	// 创建 OptimizedAggregate
	agg := NewOptimizedAggregate(aggFuncs, []string{"category"}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果：应该有 2 个分组
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(result.Rows))
	}

	// 验证分组和聚合值
	for _, row := range result.Rows {
		category, ok := row["category"].(string)
		if !ok {
			continue
		}

		sumVal, ok := row["sum_value"].(float64)
		if !ok {
			t.Fatalf("Expected sum_value to be float64")
		}

		switch category {
		case "A":
			if sumVal != 30.0 {
				t.Errorf("Group A: Expected sum=30.0, got %f", sumVal)
			}
		case "B":
			if sumVal != 70.0 {
				t.Errorf("Group B: Expected sum=70.0, got %f", sumVal)
			}
		default:
			t.Errorf("Unexpected category: %s", category)
		}
	}
}

// TestOptimizedAggregateMultipleAggFuncs 测试多个聚合函数
func TestOptimizedAggregateMultipleAggFuncs(t *testing.T) {
	// 准备测试数据
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "value", Type: "int64"},
		},
		Rows: []domain.Row{
			{"value": int64(10)},
			{"value": int64(20)},
			{"value": int64(30)},
		},
		Total: 3,
	}

	// 创建多个聚合函数
	aggFuncs := []*AggregationItem{
		{
			Type:  Count,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "count_val",
		},
		{
			Type:  Sum,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "sum_val",
		},
		{
			Type:  Avg,
			Expr:  &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"},
			Alias: "avg_val",
		},
	}

	// 创建 OptimizedAggregate
	agg := NewOptimizedAggregate(aggFuncs, []string{}, &MockPhysicalPlan{result: input})

	// 执行聚合
	result, err := agg.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// 验证 COUNT
	countVal := result.Rows[0]["count_val"].(int64)
	if countVal != 3 {
		t.Errorf("Expected count=3, got %d", countVal)
	}

	// 验证 SUM
	sumVal := result.Rows[0]["sum_val"].(float64)
	if sumVal != 60.0 {
		t.Errorf("Expected sum=60.0, got %f", sumVal)
	}

	// 验证 AVG
	avgVal := result.Rows[0]["avg_val"].(float64)
	if avgVal != 20.0 {
		t.Errorf("Expected avg=20.0, got %f", avgVal)
	}
}

// MockPhysicalPlan 用于测试的 mock 物理计划
type MockPhysicalPlan struct {
	result *domain.QueryResult
}

func (m *MockPhysicalPlan) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return m.result, nil
}

func (m *MockPhysicalPlan) Children() []PhysicalPlan {
	return nil
}

func (m *MockPhysicalPlan) SetChildren(children ...PhysicalPlan) {
}

func (m *MockPhysicalPlan) Schema() []ColumnInfo {
	return []ColumnInfo{}
}

func (m *MockPhysicalPlan) Cost() float64 {
	return 0
}

func (m *MockPhysicalPlan) Explain() string {
	return "MockPhysicalPlan"
}
