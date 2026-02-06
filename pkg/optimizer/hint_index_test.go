package optimizer

import (
	"testing"
)

// TestHintAwareIndexRule_Name 测试规则名称
func TestHintAwareIndexRule_Name(t *testing.T) {
	rule := NewHintAwareIndexRule()
	expected := "HintAwareIndex"
	if rule.Name() != expected {
		t.Errorf("Expected name %s, got %s", expected, rule.Name())
	}
}

// TestHintAwareIndexRule_Match 测试规则匹配
func TestHintAwareIndexRule_Match(t *testing.T) {
	rule := NewHintAwareIndexRule()

	// 匹配 LogicalDataSource
	dataSource := NewLogicalDataSource("test_table", nil)
	if !rule.Match(dataSource) {
		t.Error("Should match LogicalDataSource")
	}

	// 不匹配其他类型
	agg := NewLogicalAggregate(nil, nil, nil)
	if rule.Match(agg) {
		t.Error("Should not match LogicalAggregate")
	}
}

// TestHintAwareIndexRule_ForceIndex 测试 FORCE_INDEX
func TestHintAwareIndexRule_ForceIndex(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			ForceIndex: map[string][]string{
				"test_table": {"idx_col1", "idx_col2"},
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	if modifiedDataSource.forceUseIndex != "idx_col1" {
		t.Errorf("Expected forceUseIndex = idx_col1, got %s", modifiedDataSource.forceUseIndex)
	}
}

// TestHintAwareIndexRule_UseIndex 测试 USE_INDEX
func TestHintAwareIndexRule_UseIndex(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			UseIndex: map[string][]string{
				"test_table": {"idx_col1"},
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	if modifiedDataSource.preferIndex != "idx_col1" {
		t.Errorf("Expected preferIndex = idx_col1, got %s", modifiedDataSource.preferIndex)
	}
}

// TestHintAwareIndexRule_IgnoreIndex 测试 IGNORE_INDEX
func TestHintAwareIndexRule_IgnoreIndex(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			IgnoreIndex: map[string][]string{
				"test_table": {"idx_old"},
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	if len(modifiedDataSource.ignoreIndexes) != 1 || modifiedDataSource.ignoreIndexes[0] != "idx_old" {
		t.Errorf("Expected ignoreIndexes = [idx_old], got %v", modifiedDataSource.ignoreIndexes)
	}
}

// TestHintAwareIndexRule_OrderIndex 测试 ORDER_INDEX
func TestHintAwareIndexRule_OrderIndex(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			OrderIndex: map[string]string{
				"test_table": "idx_order_col",
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	if modifiedDataSource.orderIndex != "idx_order_col" {
		t.Errorf("Expected orderIndex = idx_order_col, got %s", modifiedDataSource.orderIndex)
	}
}

// TestHintAwareIndexRule_NoOrderIndex 测试 NO_ORDER_INDEX
func TestHintAwareIndexRule_NoOrderIndex(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			NoOrderIndex: map[string]string{
				"test_table": "idx_order_col",
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	if modifiedDataSource.ignoreOrderIndex != "idx_order_col" {
		t.Errorf("Expected ignoreOrderIndex = idx_order_col, got %s", modifiedDataSource.ignoreOrderIndex)
	}
}

// TestHintAwareIndexRule_NoHints 测试无 hints 的情况
func TestHintAwareIndexRule_NoHints(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该返回原始 plan，不做修改
	if plan != dataSource {
		t.Error("Plan should not be modified when no hints are present")
	}
}

// TestHintAwareIndexRule_NoContext 测试无优化上下文的情况
func TestHintAwareIndexRule_NoContext(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)

	plan, err := rule.Apply(nil, dataSource, nil)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// 应该返回原始 plan
	if plan != dataSource {
		t.Error("Plan should not be modified when optCtx is nil")
	}
}

// TestHintAwareIndexRule_WrongTableName 测试表名不匹配
func TestHintAwareIndexRule_WrongTableName(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			UseIndex: map[string][]string{
				"other_table": {"idx_col1"},
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	// 表名不匹配，不应该应用 hint
	if modifiedDataSource.preferIndex != "" {
		t.Errorf("Expected preferIndex to be empty for wrong table name")
	}
}

// TestLogicalDataSource_GetAppliedHints 测试获取已应用的 hints
func TestLogicalDataSource_GetAppliedHints(t *testing.T) {
	dataSource := NewLogicalDataSource("test_table", nil)

	dataSource.ForceUseIndex("idx1")
	dataSource.IgnoreIndex("idx2")

	hints := dataSource.GetAppliedHints()
	if len(hints) != 2 {
		t.Errorf("Expected 2 applied hints, got %d", len(hints))
	}
}

// TestHintPriority 测试 hint 优先级
func TestHintPriority(t *testing.T) {
	rule := NewHintAwareIndexRule()
	dataSource := NewLogicalDataSource("test_table", nil)

	// FORCE_INDEX 优先级最高
	optCtx := &OptimizationContext{
		Hints: &OptimizerHints{
			ForceIndex: map[string][]string{
				"test_table": {"idx_force"},
			},
			UseIndex: map[string][]string{
				"test_table": {"idx_use"},
			},
		},
	}

	plan, err := rule.Apply(nil, dataSource, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	modifiedDataSource := plan.(*LogicalDataSource)
	// FORCE_INDEX 应该覆盖 USE_INDEX
	if modifiedDataSource.forceUseIndex != "idx_force" {
		t.Errorf("Expected FORCE_INDEX to override USE_INDEX")
	}
	if modifiedDataSource.preferIndex != "" {
		t.Error("preferIndex should not be set when FORCE_INDEX is present")
	}
}
