package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TestHintAwareAggRule_Name 测试规则名称
func TestHintAwareAggRule_Name(t *testing.T) {
	rule := NewHintAwareAggRule()
	expected := "HintAwareAgg"
	if rule.Name() != expected {
		t.Errorf("Expected name %s, got %s", expected, rule.Name())
	}
}

// TestHintAwareAggRule_Match 测试规则匹配
func TestHintAwareAggRule_Match(t *testing.T) {
	rule := NewHintAwareAggRule()

	// 匹配 LogicalAggregate
	agg := NewLogicalAggregate(nil, nil, nil)
	if !rule.Match(agg) {
		t.Error("Should match LogicalAggregate")
	}

	// 不匹配其他类型
	dataSource := NewLogicalDataSource("test_table", nil)
	if rule.Match(dataSource) {
		t.Error("Should not match LogicalDataSource")
	}
}

// TestHintAwareAggRule_HashAgg 测试 HASH_AGG
func TestHintAwareAggRule_HashAgg(t *testing.T) {
	rule := NewHintAwareAggRule()
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			HashAgg: true,
		},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedAgg := plan.(*LogicalAggregate)
	if modifiedAgg.Algorithm() != HashAggAlgorithm {
		t.Errorf("Expected algorithm = HashAggAlgorithm, got %v", modifiedAgg.Algorithm())
	}
}

// TestHintAwareAggRule_StreamAgg 测试 STREAM_AGG
func TestHintAwareAggRule_StreamAgg(t *testing.T) {
	rule := NewHintAwareAggRule()
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			StreamAgg: true,
		},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedAgg := plan.(*LogicalAggregate)
	if modifiedAgg.Algorithm() != StreamAggAlgorithm {
		t.Errorf("Expected algorithm = StreamAggAlgorithm, got %v", modifiedAgg.Algorithm())
	}
}

// TestHintAwareAggRule_MPP1PhaseAgg 测试 MPP_1PHASE_AGG
func TestHintAwareAggRule_MPP1PhaseAgg(t *testing.T) {
	rule := NewHintAwareAggRule()
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			MPP1PhaseAgg: true,
		},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedAgg := plan.(*LogicalAggregate)
	if modifiedAgg.Algorithm() != MPP1PhaseAggAlgorithm {
		t.Errorf("Expected algorithm = MPP1PhaseAggAlgorithm, got %v", modifiedAgg.Algorithm())
	}
}

// TestHintAwareAggRule_MPP2PhaseAgg 测试 MPP_2PHASE_AGG
func TestHintAwareAggRule_MPP2PhaseAgg(t *testing.T) {
	rule := NewHintAwareAggRule()
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			MPP2PhaseAgg: true,
		},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedAgg := plan.(*LogicalAggregate)
	if modifiedAgg.Algorithm() != MPP2PhaseAggAlgorithm {
		t.Errorf("Expected algorithm = MPP2PhaseAggAlgorithm, got %v", modifiedAgg.Algorithm())
	}
}

// TestHintAwareAggRule_NoHints 测试无 hints 的情况
func TestHintAwareAggRule_NoHints(t *testing.T) {
	rule := NewHintAwareAggRule()
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该返回原始 plan，不做修改
	if plan != agg {
		t.Error("Plan should not be modified when no hints are present")
	}

	// 算法应该是默认的 HashAggAlgorithm
	if agg.Algorithm() != HashAggAlgorithm {
		t.Errorf("Expected default algorithm = HashAggAlgorithm, got %v", agg.Algorithm())
	}
}

// TestHintAwareAggRule_NoContext 测试无优化上下文的情况
func TestHintAwareAggRule_NoContext(t *testing.T) {
	rule := NewHintAwareAggRule()
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)

	plan, err := rule.Apply(context.Background(), agg, nil)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该返回原始 plan
	if plan != agg {
		t.Error("Plan should not be modified when optCtx is nil")
	}
}

// TestLogicalAggregate_GetAppliedHints 测试获取已应用的 hints
func TestLogicalAggregate_GetAppliedHints(t *testing.T) {
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	agg.SetAlgorithm(HashAggAlgorithm)
	agg.SetHintApplied("HASH_AGG")

	hints := agg.GetAppliedHints()
	if len(hints) != 1 || hints[0] != "HASH_AGG" {
		t.Errorf("Expected [HASH_AGG], got %v", hints)
	}
}

// TestAggAlgorithmPriority 测试 hint 优先级
func TestAggAlgorithmPriority(t *testing.T) {
	rule := NewHintAwareAggRule()

	// HASH_AGG 优先级最高
	agg := NewLogicalAggregate(nil, []string{"col1"}, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			HashAgg:   true,
			StreamAgg: true,
		},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedAgg := plan.(*LogicalAggregate)
	// HASH_AGG 应该覆盖 STREAM_AGG
	if modifiedAgg.Algorithm() != HashAggAlgorithm {
		t.Errorf("Expected HASH_AGG to override STREAM_AGG")
	}
}

// TestAggAlgorithmString 测试算法字符串表示
func TestAggAlgorithmString(t *testing.T) {
	tests := []struct {
		algo     AggregationAlgorithm
		expected string
	}{
		{HashAggAlgorithm, "HASH_AGG"},
		{StreamAggAlgorithm, "STREAM_AGG"},
		{MPP1PhaseAggAlgorithm, "MPP_1PHASE_AGG"},
		{MPP2PhaseAggAlgorithm, "MPP_2PHASE_AGG"},
	}

	for _, tt := range tests {
		if tt.algo.String() != tt.expected {
			t.Errorf("Expected %s, got %s", tt.expected, tt.algo.String())
		}
	}
}

// TestHintAwareAggRule_ComplexAggregation 测试复杂聚合场景
func TestHintAwareAggRule_ComplexAggregation(t *testing.T) {
	rule := NewHintAwareAggRule()

	// 创建包含多个聚合函数的 LogicalAggregate
	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}},
		{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "amount"}},
		{Type: Avg, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "price"}},
	}
	agg := NewLogicalAggregate(aggFuncs, []string{"category"}, nil)

	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			StreamAgg: true,
		},
	}

	plan, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedAgg := plan.(*LogicalAggregate)
	if modifiedAgg.Algorithm() != StreamAggAlgorithm {
		t.Errorf("Expected algorithm = StreamAggAlgorithm, got %v", modifiedAgg.Algorithm())
	}
}
