package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TestHintAwareSubqueryRule_Name 测试规则名称
func TestHintAwareSubqueryRule_Name(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)
	expected := "HintAwareSubquery"
	if rule.Name() != expected {
		t.Errorf("Expected name %s, got %s", expected, rule.Name())
	}
}

// TestHintAwareSubqueryRule_Match 测试规则匹配
func TestHintAwareSubqueryRule_Match(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 匹配 LogicalApply
	apply := NewLogicalApply(SemiJoin, nil, nil, nil)
	if !rule.Match(apply) {
		t.Error("Should match LogicalApply")
	}

	// 不匹配其他类型
	dataSource := NewLogicalDataSource("test_table", nil)
	if rule.Match(dataSource) {
		t.Error("Should not match LogicalDataSource")
	}

	agg := NewLogicalAggregate(nil, nil, nil)
	if rule.Match(agg) {
		t.Error("Should not match LogicalAggregate")
	}
}

// TestHintAwareSubqueryRule_SemiJoinRewrite 测试 SEMI_JOIN_REWRITE
func TestHintAwareSubqueryRule_SemiJoinRewrite(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建 LogicalApply，包含实际的子节点
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			SemiJoinRewrite: true,
		},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该尝试 Semi Join 改写
	// 这里我们只验证没有错误发生
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}

// TestHintAwareSubqueryRule_NoDecorrelate 测试 NO_DECORRELATE
func TestHintAwareSubqueryRule_NoDecorrelate(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建 LogicalApply
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			NoDecorrelate: true,
		},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该返回原始 plan（不进行去关联）
	if plan != apply {
		t.Error("Plan should not be modified when NO_DECORRELATE hint is present")
	}
}

// TestHintAwareSubqueryRule_UseTOJA 测试 USE_TOJA
func TestHintAwareSubqueryRule_UseTOJA(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建 LogicalApply
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			UseTOJA: true,
		},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该尝试 TOJA 转换
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}

// TestHintAwareSubqueryRule_NoHints 测试无 hints 的情况
func TestHintAwareSubqueryRule_NoHints(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建 LogicalApply，包含实际的子节点
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该尝试应用默认规则
	// 这里我们只验证没有错误发生
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}

// TestHintAwareSubqueryRule_NoContext 测试无优化上下文的情况
func TestHintAwareSubqueryRule_NoContext(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建 LogicalApply
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)

	plan, err := rule.Apply(nil, apply, nil)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该尝试应用默认规则
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}

// TestHintPriority_Subquery 测试 subquery hints 的优先级
func TestHintPriority_Subquery(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// SEMI_JOIN_REWRITE 优先级最高
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			SemiJoinRewrite: true,
			NoDecorrelate:   true,
		},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// SEMI_JOIN_REWRITE 应该被应用（即使 NO_DECORRELATE 也设置了）
	// 注意：优先级由实现决定，这里只是测试逻辑
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}

// TestLogicalApply_Creation 测试 LogicalApply 的创建
func TestLogicalApply_Creation(t *testing.T) {
	outer := NewLogicalDataSource("test_table", nil)
	inner := NewLogicalDataSource("subquery_table", nil)

	// 创建 LogicalApply
	conditions := []*parser.Expression{
		{Type: parser.ExprTypeOperator, Operator: "="},
	}
	apply := NewLogicalApply(SemiJoin, outer, inner, conditions)

	if apply == nil {
		t.Error("LogicalApply should not be nil")
	}

	if len(apply.Children()) != 2 {
		t.Errorf("Expected 2 children, got %d", len(apply.Children()))
	}
}

// TestHintAwareSubqueryRule_ComplexSubquery 测试复杂子查询场景
func TestHintAwareSubqueryRule_ComplexSubquery(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建包含复杂条件的 LogicalApply
	conditions := []*parser.Expression{
		{Type: parser.ExprTypeOperator, Operator: "AND"},
		{Type: parser.ExprTypeOperator, Operator: "="},
	}

	outer := NewLogicalDataSource("test_table", nil)
	inner := NewLogicalDataSource("subquery_table", nil)

	apply := NewLogicalApply(SemiJoin, outer, inner, conditions)

	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			SemiJoinRewrite: true,
		},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该尝试 Semi Join 改写
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}

// TestHintAwareSubqueryRule_MultipleHints 测试多个 subquery hints
func TestHintAwareSubqueryRule_MultipleHints(t *testing.T) {
	rule := NewHintAwareSubqueryRule(nil)

	// 创建 LogicalApply
	outer := NewLogicalDataSource("outer_table", nil)
	inner := NewLogicalDataSource("inner_table", nil)
	apply := NewLogicalApply(SemiJoin, outer, inner, nil)

	// 设置多个 hints（但只有优先级最高的会被应用）
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			SemiJoinRewrite: true,
			NoDecorrelate:   true,
			UseTOJA:         true,
		},
	}

	plan, err := rule.Apply(nil, apply, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该应用优先级最高的 hint (SEMI_JOIN_REWRITE)
	if plan == nil {
		t.Error("Plan should not be nil")
	}
}
